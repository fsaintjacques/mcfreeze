package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"frostmap.io/fmtctl/internal/nodeagent"
	"frostmap.io/fmtctl/internal/nodeagent/assignment"
	"frostmap.io/fmtctl/internal/nodeagent/mount"
	"frostmap.io/fmtctl/internal/nodeagent/version"
	"frostmap.io/fmtctl/internal/nodeagent/volume"
)

func runNodeAgent(args []string) {
	fs := flag.NewFlagSet("node-agent", flag.ExitOnError)

	cfg := nodeagent.Config{}
	fs.StringVar(&cfg.ControlPlaneURL, "control-plane-url", "", "base URL of the control-plane API (required)")
	fs.StringVar(&cfg.NodeName, "node-name", os.Getenv("NODE_NAME"), "Kubernetes node name (defaults to $NODE_NAME)")
	fs.StringVar(&cfg.MountBase, "mount-base", "/mnt/kv", "root directory for version mounts")
	fs.StringVar(&cfg.CatalogDir, "catalog-dir", "/run/kv", "shared EmptyDir for catalog.json")

	project := fs.String("gcp-project", "", "GCP project ID (required)")
	zone := fs.String("gcp-zone", "", "Compute Engine zone of this node (required)")
	fs.Parse(args)

	if cfg.ControlPlaneURL == "" || cfg.NodeName == "" || *project == "" || *zone == "" {
		fs.Usage()
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	disks := volume.NewComputeDiskManager(*project, *zone)
	mounter := mount.NewLinuxMounter()
	assignments := assignment.NewHTTPSource(cfg.ControlPlaneURL, cfg.NodeName)
	reporter := assignment.NewHTTPStateReporter(cfg.ControlPlaneURL, cfg.NodeName)
	versions := version.NewHTTPChecker("http://localhost:7777")

	agent := nodeagent.New(cfg, disks, mounter, assignments, reporter, versions)
	if err := agent.Run(ctx); err != nil {
		slog.Info("agent stopped", "reason", err)
	}

	// Graceful shutdown: unmount all datasets and detach disks.
	// Use a fresh context with the remaining grace period (Kubernetes default 30s).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()
	agent.Shutdown(shutdownCtx)
}
