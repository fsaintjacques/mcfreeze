package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"frostmap.io/fmtctl/internal/controlplane"
	"frostmap.io/fmtctl/internal/controlplane/builder"
	"frostmap.io/fmtctl/internal/controlplane/volume"
)

func runControlPlane(args []string) {
	fs := flag.NewFlagSet("control-plane", flag.ExitOnError)

	listen := fs.String("listen", ":8080", "HTTP server bind address")
	namespace := fs.String("namespace", envOrDefault("NAMESPACE", "default"), "K8s namespace for builds")
	image := fs.String("image", "", "container image for build Jobs (required)")
	pullPolicy := fs.String("image-pull-policy", "IfNotPresent", "image pull policy (Always, IfNotPresent, Never)")
	storageClass := fs.String("storage-class", "", "StorageClass for build PVCs (required)")
	diskSizeGB := fs.Int64("disk-size-gb", 10, "PVC size in GiB")
	reconcileInterval := fs.Duration("reconcile-interval", 5*time.Second, "reconcile loop interval")
	buildTimeout := fs.Duration("build-timeout", 30*time.Minute, "max build duration before cancellation")
	fs.Parse(args)

	if *image == "" || *storageClass == "" {
		slog.Error("--image and --storage-class are required")
		fs.Usage()
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		slog.Error("build in-cluster config", "err", err)
		os.Exit(1)
	}
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		slog.Error("create kubernetes client", "err", err)
		os.Exit(1)
	}

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

	store := controlplane.NewStore()
	srv, err := controlplane.NewServer(store, *listen)
	if err != nil {
		slog.Error("create server", "err", err)
		os.Exit(1)
	}

	orch := &controlplane.Orchestrator{
		Store:             store,
		Builder:           jb,
		Server:            srv,
		ReconcileInterval: *reconcileInterval,
		BuildTimeout:      *buildTimeout,
	}
	srv.SetBuildStarter(orch)

	go func() {
		if err := srv.Serve(); err != nil {
			slog.Error("server stopped", "err", err)
		}
	}()
	slog.Info("control-plane started",
		"addr", *listen,
		"namespace", *namespace,
		"image", *image,
		"storage-class", *storageClass,
	)

	if err := orch.Run(ctx); err != nil {
		slog.Info("orchestrator stopped", "reason", err)
	}

	srv.Close()
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
