// SPDX-License-Identifier: Apache-2.0

//go:build integration

package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"log/slog"

	"github.com/go-logr/logr"

	v1alpha1 "github.com/fsaintjacques/mcfreeze/go/api/v1alpha1"
	"github.com/fsaintjacques/mcfreeze/go/internal/controller"
	"github.com/fsaintjacques/mcfreeze/go/internal/controlplane"
	"github.com/fsaintjacques/mcfreeze/go/internal/controlplane/builder"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ControlPlane is a fully wired in-process control-plane backed by a real
// kube-apiserver+etcd from envtest. It hosts the Phase 5 reconcilers and the
// HTTP long-poll server, sharing one AssignmentBroker.
//
// Use NewControlPlane in tests that need real API semantics (status
// subresource, watches, CEL validation, ownerRef cascade) but don't need a
// full KIND cluster.
type ControlPlane struct {
	Env       *envtest.Environment
	Cfg       *rest.Config
	Client    client.Client
	Mgr       ctrl.Manager
	Broker    *controlplane.AssignmentBroker
	Server    *controlplane.Server
	Namespace string

	cancel context.CancelFunc
}

// NewControlPlane boots envtest, installs the mcfreeze CRDs, starts a
// controller-manager with the three Phase 5 reconcilers, registers the given
// builder.Async, starts the HTTP long-poll server on a random port, and
// returns a handle. Cleanup is registered with t.Cleanup.
func NewControlPlane(t *testing.T, b builder.Async, volumeBase string) *ControlPlane {
	t.Helper()

	logf.SetLogger(logr.FromSlogHandler(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug})))

	scheme := apiruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDirFromCaller()},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest start: %v", err)
	}

	cli, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	const namespace = "default"
	broker := controlplane.NewAssignmentBroker()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,
		// envtest is single-process; default cache covers everything.
	})
	if err != nil {
		_ = env.Stop()
		t.Fatalf("ctrl.NewManager: %v", err)
	}

	if err := (&controller.DatasetVersionReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		Builder:    b,
		VolumeBase: volumeBase,
	}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setup DatasetVersionReconciler: %v", err)
	}
	if err := (&controller.NodeAssignmentReconciler{
		Client:    mgr.GetClient(),
		Broker:    broker,
		Namespace: namespace,
	}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setup NodeAssignmentReconciler: %v", err)
	}
	cronRun := &controller.CronRunnable{
		Client:    mgr.GetClient(),
		Namespace: namespace,
	}
	if err := (&controller.DatasetReconciler{
		Client: mgr.GetClient(),
		Cron:   cronRun,
	}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setup DatasetReconciler: %v", err)
	}

	srv, err := controlplane.NewServer(broker, "127.0.0.1:0")
	if err != nil {
		_ = env.Stop()
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager exited: %v", err)
		}
	}()
	go func() {
		if err := srv.Serve(); err != nil {
			t.Logf("HTTP server exited: %v", err)
		}
	}()

	cp := &ControlPlane{
		Env:       env,
		Cfg:       cfg,
		Client:    cli,
		Mgr:       mgr,
		Broker:    broker,
		Server:    srv,
		Namespace: namespace,
		cancel:    cancel,
	}
	t.Cleanup(cp.Stop)

	// Wait briefly for the manager cache to sync so the first List/Get
	// against the manager client returns reliably.
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer syncCancel()
	if !mgr.GetCache().WaitForCacheSync(syncCtx) {
		t.Fatal("manager cache failed to sync")
	}

	return cp
}

// Addr returns the HTTP server address (host:port).
func (cp *ControlPlane) Addr() string { return cp.Server.Addr() }

// Stop tears down the manager and the apiserver. Safe to call multiple times.
func (cp *ControlPlane) Stop() {
	if cp.cancel != nil {
		cp.cancel()
		cp.cancel = nil
	}
	_ = cp.Server.Close()
	_ = cp.Env.Stop()
}

// CreateDataset creates a Dataset CR in the test namespace.
func (cp *ControlPlane) CreateDataset(t *testing.T, ds *v1alpha1.Dataset) {
	t.Helper()
	ds.Namespace = cp.Namespace
	if err := cp.Client.Create(context.Background(), ds); err != nil {
		t.Fatalf("create Dataset %q: %v", ds.Name, err)
	}
}

// CreateVersion creates a DatasetVersion CR for an existing dataset.
func (cp *ControlPlane) CreateVersion(t *testing.T, dataset, versionID string, shardCount int) {
	t.Helper()
	v := &v1alpha1.DatasetVersion{}
	v.Namespace = cp.Namespace
	v.Name = v1alpha1.VersionCRName(dataset, versionID)
	v.Labels = map[string]string{v1alpha1.DatasetLabel: dataset}
	v.Spec.Dataset = dataset
	v.Spec.VersionID = versionID
	v.Spec.ShardCount = shardCount
	if err := cp.Client.Create(context.Background(), v); err != nil {
		t.Fatalf("create DatasetVersion %s/%s: %v", dataset, versionID, err)
	}
}

// WaitForVersionState polls the API server until the named version reaches
// the desired state, or t.Fatal on timeout.
func (cp *ControlPlane) WaitForVersionState(t *testing.T, dataset, versionID, state string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	key := client.ObjectKey{Namespace: cp.Namespace, Name: v1alpha1.VersionCRName(dataset, versionID)}
	for time.Now().Before(deadline) {
		v := &v1alpha1.DatasetVersion{}
		if err := cp.Client.Get(context.Background(), key, v); err == nil && v.Status.State == state {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("DatasetVersion %s/%s did not reach state %q within %v", dataset, versionID, state, timeout)
}

// crdDirFromCaller returns the absolute path to k8s/crds, computed from this
// source file's location so the lookup is independent of the test's CWD.
func crdDirFromCaller() string {
	_, file, _, _ := runtime.Caller(0)
	// file = .../go/internal/testutil/envtest.go
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "k8s", "charts", "mcfreeze", "crds")
}

// testWriter adapts *testing.T to io.Writer so envtest/zap log output is
// attributed to the test that produced it.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// EnvtestSkipIfBinariesMissing skips the test when the envtest apiserver
// binaries are not installed and KUBEBUILDER_ASSETS is unset. Use this at
// the top of any test that calls NewControlPlane to keep CI green for
// developers who haven't installed setup-envtest.
func EnvtestSkipIfBinariesMissing(t *testing.T) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		return
	}
	if _, err := os.Stat(filepath.Join(envtestDefaultDir(), "kube-apiserver")); err == nil {
		return
	}
	t.Skip("envtest binaries not found; install with: go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest && setup-envtest use 1.31.0 -p path | xargs -I{} export KUBEBUILDER_ASSETS={}")
}

func envtestDefaultDir() string {
	// setup-envtest's default install location on macOS.
	home, _ := os.UserHomeDir()
	return fmt.Sprintf("%s/Library/Application Support/io.kubebuilder.envtest/k8s/1.31.0-%s-%s",
		home, runtime.GOOS, runtime.GOARCH)
}
