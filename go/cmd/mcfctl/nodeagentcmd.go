package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/fsaintjacques/frostmap/go/internal/nodeagent"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/assignment"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/mount"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/ui"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/version"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/volume"
)

func runNodeAgent(args []string) {
	fs := flag.NewFlagSet("node-agent", flag.ExitOnError)

	cfg := nodeagent.Config{}
	fs.StringVar(&cfg.ControlPlaneURL, "control-plane-url", "", "base URL of the control-plane API (required)")
	fs.StringVar(&cfg.NodeName, "node-name", os.Getenv("NODE_NAME"), "Kubernetes node name (defaults to $NODE_NAME)")
	fs.StringVar(&cfg.MountBase, "mount-base", "/mnt/kv", "root directory for version mounts")
	fs.StringVar(&cfg.CatalogDir, "catalog-dir", "/run/kv", "shared EmptyDir for catalog.json")

	csiDriver := fs.String("csi-driver", "pd.csi.storage.gke.io", "CSI driver name for VolumeAttachment")
	mounterType := fs.String("mounter", "linux", "mount implementation: linux (real mount syscall) or fs (symlinks, for KIND)")

	uiAddr := fs.String("ui-addr", ":8090", "address for the web UI (empty to disable)")
	kvMemcacheAddr := fs.String("kv-memcache-addr", "localhost:11211", "kv-server memcache protocol address")
	kvMetricsAddr := fs.String("kv-metrics-addr", "localhost:9090", "kv-server HTTP metrics address")
	fs.Parse(args)

	if cfg.ControlPlaneURL == "" || cfg.NodeName == "" {
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

	disks := &volume.K8sManager{Client: kubeClient, CSIDriver: *csiDriver}
	var mounter mount.Mounter
	switch *mounterType {
	case "fs":
		mounter = mount.NewFSMounter()
	default:
		mounter = mount.NewLinuxMounter()
	}
	assignments := assignment.NewHTTPSource(cfg.ControlPlaneURL, cfg.NodeName)
	reporter := assignment.NewHTTPStateReporter(cfg.ControlPlaneURL, cfg.NodeName)
	versions := version.NewHTTPChecker("http://localhost:7777")

	startTime := time.Now()
	agent := nodeagent.New(cfg, disks, mounter, assignments, reporter, versions)

	// Start the web UI server if enabled.
	if *uiAddr != "" {
		uiCfg := ui.Config{
			KVMemcacheAddr:       *kvMemcacheAddr,
			KVMetricsAddr:        *kvMetricsAddr,
			CatalogDir:           cfg.CatalogDir,
			AgentNodeName:        cfg.NodeName,
			AgentControlPlaneURL: cfg.ControlPlaneURL,
			AgentStartTime:       startTime,
		}
		handler := ui.NewHandler(agent, uiCfg)
		mux := http.NewServeMux()
		handler.RegisterRoutes(mux)

		l, err := net.Listen("tcp", *uiAddr)
		if err != nil {
			slog.Error("start UI server", "addr", *uiAddr, "err", err)
			os.Exit(1)
		}
		slog.Info("UI server listening", "addr", l.Addr().String())
		srv := &http.Server{Handler: mux}
		go srv.Serve(l)
		go func() {
			<-ctx.Done()
			srv.Close()
		}()
	}

	if err := agent.Run(ctx); err != nil {
		slog.Info("agent stopped", "reason", err)
	}

	// Graceful shutdown: unmount all datasets and detach disks.
	// Use a fresh context with the remaining grace period (Kubernetes default 30s).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()
	agent.Shutdown(shutdownCtx)
}
