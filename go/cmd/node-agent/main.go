package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"frostmap.io/fmtctl/internal/mount"
	"frostmap.io/fmtctl/internal/nodeagent"
	"frostmap.io/fmtctl/internal/volume"
)

func main() {
	cfg := nodeagent.Config{}
	flag.StringVar(&cfg.ControlPlaneURL, "control-plane-url", "", "base URL of the control-plane API (required)")
	flag.StringVar(&cfg.NodeName, "node-name", os.Getenv("NODE_NAME"), "Kubernetes node name (defaults to $NODE_NAME)")
	flag.StringVar(&cfg.MountBase, "mount-base", "/mnt/kv", "root directory for version mounts")
	flag.StringVar(&cfg.CatalogDir, "catalog-dir", "/run/kv", "shared EmptyDir for catalog.json")

	project := flag.String("gcp-project", "", "GCP project ID (required)")
	zone := flag.String("gcp-zone", "", "Compute Engine zone of this node (required)")
	flag.Parse()

	if cfg.ControlPlaneURL == "" || cfg.NodeName == "" || *project == "" || *zone == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	disks := volume.NewComputeDiskManager(*project, *zone)
	mounter := mount.NewLinuxMounter()

	agent := nodeagent.New(cfg, disks, mounter)
	if err := agent.Run(ctx); err != nil {
		slog.Error("agent exited", "err", err)
		os.Exit(1)
	}
}
