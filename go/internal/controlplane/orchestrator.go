package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"frostmap.io/fmtctl/api"
)

// Orchestrator ties the store, builder, and server together. It provides a
// high-level API for tests to trigger builds and promotions.
type Orchestrator struct {
	Store   *Store
	Builder VersionBuilder
	Server  *Server

	// VolumeBase is the FSVolumeManager base directory. BuildAndPromote
	// symlinks the snapshot into this directory so the node-agent's
	// FSVolumeManager can find it.
	VolumeBase string
}

// RegisterNode registers a node so Promote pushes assignments to it.
func (o *Orchestrator) RegisterNode(nodeName string) {
	o.Store.RegisterNode(nodeName)
}

// NewOrchestrator creates an Orchestrator with an HTTP server bound to a free port.
func NewOrchestrator(builder VersionBuilder, volumeBase string) (*Orchestrator, error) {
	store := NewStore()
	srv, err := NewServer(store, "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	go srv.Serve()

	return &Orchestrator{
		Store:      store,
		Builder:    builder,
		Server:     srv,
		VolumeBase: volumeBase,
	}, nil
}

// Addr returns the control-plane HTTP address.
func (o *Orchestrator) Addr() string {
	return "http://" + o.Server.Addr()
}

// BuildAndPromote runs the full version lifecycle: register dataset, create
// version (building), build snapshot, mark ready, promote (active). This
// triggers the node-agent's long-poll to return for all registered nodes.
func (o *Orchestrator) BuildAndPromote(ctx context.Context, spec api.DatasetSpec, versionID string) error {
	o.Store.RegisterDataset(spec)

	if err := o.Store.CreateVersion(spec.Name, versionID); err != nil {
		return fmt.Errorf("create version: %w", err)
	}

	snapPath, err := o.Builder.Build(ctx, spec, versionID)
	if err != nil {
		if mfErr := o.Store.MarkFailed(spec.Name, versionID, err.Error()); mfErr != nil {
			slog.Error("failed to mark version as failed", "dataset", spec.Name, "version", versionID, "err", mfErr)
		}
		return fmt.Errorf("build: %w", err)
	}

	// Create a PV name and symlink the snapshot into the volume base so
	// FSVolumeManager.AttachDisk finds it.
	pvName := fmt.Sprintf("pv-%s-%s", spec.Name, versionID)
	pvLink := filepath.Join(o.VolumeBase, pvName)
	if err := os.Symlink(snapPath, pvLink); err != nil && !os.IsExist(err) {
		if mfErr := o.Store.MarkFailed(spec.Name, versionID, err.Error()); mfErr != nil {
			slog.Error("failed to mark version as failed", "dataset", spec.Name, "version", versionID, "err", mfErr)
		}
		return fmt.Errorf("symlink pv: %w", err)
	}

	if err := o.Store.MarkReady(spec.Name, versionID, snapPath, pvName); err != nil {
		return fmt.Errorf("mark ready: %w", err)
	}

	if err := o.Store.Promote(spec.Name, versionID); err != nil {
		return fmt.Errorf("promote: %w", err)
	}

	return nil
}

// WaitForConvergence polls until all registered nodes report the given
// version as active for the dataset, or ctx is cancelled.
func (o *Orchestrator) WaitForConvergence(ctx context.Context, dataset, versionID string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		status := o.Store.RolloutStatus(dataset)
		if status.ActiveVersion == "" {
			return fmt.Errorf("no active version for dataset %q", dataset)
		}
		if status.ActiveVersion == versionID && len(status.PendingNodes) == 0 && len(status.ErrorNodes) == 0 {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			status = o.Store.RolloutStatus(dataset)
			return fmt.Errorf("convergence timeout: active=%s converged=%d pending=%v error=%v: %w",
				status.ActiveVersion, len(status.ConvergedNodes), status.PendingNodes, status.ErrorNodes, ctx.Err())
		}
	}
}

// Close shuts down the HTTP server.
func (o *Orchestrator) Close() error {
	return o.Server.Close()
}
