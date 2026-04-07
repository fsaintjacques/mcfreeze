package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	"github.com/fsaintjacques/frostmap/go/internal/controller"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/builder"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/volume"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var controlPlaneScheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(controlPlaneScheme))
	utilruntime.Must(v1alpha1.AddToScheme(controlPlaneScheme))
}

// runControlPlane boots the Phase 5 controller-manager: it hosts the
// DatasetVersion / NodeAssignment / Dataset reconcilers, the leader-elected
// CronRunnable, and the HTTP long-poll server backed by the shared
// AssignmentBroker. Single binary; the existing fmtctl control-plane
// subcommand is the only entry point.
func runControlPlane(args []string) {
	fs := flag.NewFlagSet("control-plane", flag.ExitOnError)

	listen := fs.String("listen", ":8080", "HTTP server bind address (long-poll node-agent API)")
	namespace := fs.String("namespace", envOrDefault("NAMESPACE", "default"), "Kubernetes namespace to watch")
	image := fs.String("image", "", "container image for build Jobs (required)")
	pullPolicy := fs.String("image-pull-policy", "IfNotPresent", "image pull policy (Always, IfNotPresent, Never)")
	storageClass := fs.String("storage-class", "", "StorageClass for build PVCs (required)")
	diskSizeGB := fs.Int64("disk-size-gb", 10, "PVC size in GiB")
	buildTimeout := fs.Duration("build-timeout", 30*time.Minute, "max build duration before cancellation")
	leaderElect := fs.Bool("leader-elect", true, "enable leader election (Lease-based)")
	metricsAddr := fs.String("metrics-bind-address", ":8081", "metrics server bind address")
	probeAddr := fs.String("health-probe-bind-address", ":8082", "health probe bind address")
	volumeBase := fs.String("volume-base", "", "FSVolumeManager base directory (legacy fork-builder; leave empty for K8s Job builder)")
	fs.Parse(args)

	ctrllog.SetLogger(zap.New(zap.UseDevMode(false)))
	log := ctrl.Log.WithName("control-plane")

	if *image == "" || *storageClass == "" {
		log.Error(nil, "--image and --storage-class are required")
		os.Exit(1)
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		log.Error(err, "get kubeconfig")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                  controlPlaneScheme,
		LeaderElection:          *leaderElect,
		LeaderElectionID:        "frostmap-control-plane-leader",
		LeaderElectionNamespace: *namespace,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{*namespace: {}},
		},
		HealthProbeBindAddress: *probeAddr,
		Metrics: metricsserver.Options{
			BindAddress: *metricsAddr,
		},
	})
	if err != nil {
		log.Error(err, "create manager")
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Error(err, "create kubernetes client")
		os.Exit(1)
	}

	// Shared in-memory broker: the only state crossing the boundary between
	// the HTTP long-poll handlers and the reconcilers.
	broker := controlplane.NewAssignmentBroker()

	volumes := &volume.LocalPathManager{Client: kubeClient, Namespace: *namespace}
	jb := &builder.Job{
		Client:          kubeClient,
		Volumes:         volumes,
		Namespace:       *namespace,
		Image:           *image,
		ImagePullPolicy: corev1.PullPolicy(*pullPolicy),
		StorageClass:    *storageClass,
		DiskSizeGB:      *diskSizeGB,
	}

	if err := (&controller.DatasetVersionReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Builder:      jb,
		Volume:       volumes,
		Broker:       broker,
		VolumeBase:   *volumeBase,
		BuildTimeout: *buildTimeout,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "setup DatasetVersionReconciler")
		os.Exit(1)
	}

	if err := (&controller.NodeAssignmentReconciler{
		Client: mgr.GetClient(),
		Broker: broker,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "setup NodeAssignmentReconciler")
		os.Exit(1)
	}

	cronRun := &controller.CronRunnable{
		Client:    mgr.GetClient(),
		Namespace: *namespace,
	}
	if err := (&controller.DatasetReconciler{
		Client: mgr.GetClient(),
		Cron:   cronRun,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "setup DatasetReconciler")
		os.Exit(1)
	}
	if err := mgr.Add(cronRun); err != nil {
		log.Error(err, "add CronRunnable")
		os.Exit(1)
	}

	// HTTP long-poll server: leader-only for Phase 5 single-replica scope.
	if err := mgr.Add(&httpServerRunnable{
		listen:    *listen,
		broker:    broker,
		client:    mgr.GetClient(),
		namespace: *namespace,
		log:       log.WithName("http"),
	}); err != nil {
		log.Error(err, "add HTTP server runnable")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", func(*http.Request) error { return nil }); err != nil {
		log.Error(err, "add healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", func(*http.Request) error { return nil }); err != nil {
		log.Error(err, "add readyz")
		os.Exit(1)
	}

	log.Info("starting manager",
		"namespace", *namespace,
		"image", *image,
		"storage-class", *storageClass,
		"leader-elect", *leaderElect,
	)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited")
		os.Exit(1)
	}
}

// httpServerRunnable adapts controlplane.Server to manager.Runnable. It is
// leader-elected because the in-memory AssignmentBroker only exists in the
// leader pod.
type httpServerRunnable struct {
	listen    string
	broker    *controlplane.AssignmentBroker
	client    client.Client
	namespace string
	log       logr.Logger
}

func (h *httpServerRunnable) NeedLeaderElection() bool { return true }

func (h *httpServerRunnable) Start(ctx context.Context) error {
	store := controlplane.NewMemStoreWithBroker(h.broker)
	srv, err := controlplane.NewServer(store, h.listen)
	if err != nil {
		return err
	}
	srv.SetBuildStarter(&crdBuildStarter{client: h.client, namespace: h.namespace})

	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	h.log.Info("HTTP server listening", "addr", h.listen)
	if err := srv.Serve(); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

// crdBuildStarter implements controlplane.BuildStarter by creating a
// DatasetVersion CR. The reconciler picks it up and runs the build.
type crdBuildStarter struct {
	client    client.Client
	namespace string
}

func (b *crdBuildStarter) StartBuild(ctx context.Context, spec api.DatasetSpec, versionID string) error {
	v := &v1alpha1.DatasetVersion{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: b.namespace,
			Name:      v1alpha1.VersionCRName(spec.Name, versionID),
			Labels:    map[string]string{v1alpha1.DatasetLabel: spec.Name},
		},
		Spec: v1alpha1.DatasetVersionSpec{
			Dataset:    spec.Name,
			VersionID:  versionID,
			ShardCount: spec.ShardCount,
		},
	}
	return b.client.Create(ctx, v)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
