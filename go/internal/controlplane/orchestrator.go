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
	Builder AsyncBuilder
	Server  *Server

	// VolumeBase is the FSVolumeManager base directory. Snapshots are
	// symlinked into this directory so the node-agent can find them.
	VolumeBase string

	// ReconcileInterval is the ticker period for the Run loop.
	// Defaults to 5s if zero.
	ReconcileInterval time.Duration

	// BuildTimeout is the maximum duration a build may remain in building
	// state before being cancelled. Defaults to 30m if zero.
	BuildTimeout time.Duration
}

// RegisterNode registers a node so Promote pushes assignments to it.
func (o *Orchestrator) RegisterNode(nodeName string) {
	o.Store.RegisterNode(nodeName)
}

// NewOrchestrator creates an Orchestrator with an HTTP server bound to a free port.
func NewOrchestrator(builder AsyncBuilder, volumeBase string) (*Orchestrator, error) {
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

// StartBuild registers the dataset, creates a version in building state,
// kicks off an async build, and persists the handle.
func (o *Orchestrator) StartBuild(ctx context.Context, spec api.DatasetSpec, versionID string) error {
	o.Store.RegisterDataset(spec)

	if err := o.Store.CreateVersion(spec.Name, versionID); err != nil {
		return fmt.Errorf("create version: %w", err)
	}

	handle, err := o.Builder.Start(ctx, spec, versionID)
	if err != nil {
		if mfErr := o.Store.MarkFailed(spec.Name, versionID, err.Error()); mfErr != nil {
			slog.Error("failed to mark version as failed", "dataset", spec.Name, "version", versionID, "err", mfErr)
		}
		return fmt.Errorf("start build: %w", err)
	}

	if err := o.Store.SetBuildHandle(spec.Name, versionID, handle); err != nil {
		return fmt.Errorf("set build handle: %w", err)
	}

	return nil
}

// ReconcileBuilds polls all in-flight builds and transitions them as needed:
// complete → ready (+ symlink + promote), failed → failed, timeout → cancel,
// not_found → failed (orphan).
func (o *Orchestrator) ReconcileBuilds(ctx context.Context) error {
	building := o.Store.GetBuildingVersions()

	for _, v := range building {
		if v.BuildHandle == "" {
			continue
		}

		// Check for build timeout.
		timeout := o.BuildTimeout
		if timeout == 0 {
			timeout = 30 * time.Minute
		}
		if time.Since(v.CreatedAt) > timeout {
			slog.Info("build timeout exceeded", "dataset", v.Dataset, "version", v.ID)
			if err := o.Builder.Cancel(ctx, v.BuildHandle); err != nil {
				slog.Error("cancel timed-out build", "dataset", v.Dataset, "version", v.ID, "err", err)
			}
			if err := o.Store.MarkFailed(v.Dataset, v.ID, "build timeout exceeded"); err != nil {
				slog.Error("mark timed-out build failed", "dataset", v.Dataset, "version", v.ID, "err", err)
			}
			continue
		}

		status, err := o.Builder.Poll(ctx, v.BuildHandle)
		if err != nil {
			slog.Error("poll build", "dataset", v.Dataset, "version", v.ID, "err", err)
			continue
		}

		switch status.Phase {
		case BuildRunning:
			// Still in progress, nothing to do.

		case BuildComplete:
			snapPath := status.Result.SnapshotPath
			pvName := fmt.Sprintf("pv-%s-%s", v.Dataset, v.ID)
			pvLink := filepath.Join(o.VolumeBase, pvName)
			if err := os.Symlink(snapPath, pvLink); err != nil && !os.IsExist(err) {
				slog.Error("symlink pv", "dataset", v.Dataset, "version", v.ID, "err", err)
				if mfErr := o.Store.MarkFailed(v.Dataset, v.ID, err.Error()); mfErr != nil {
					slog.Error("mark failed after symlink error", "dataset", v.Dataset, "version", v.ID, "err", mfErr)
				}
				continue
			}

			if err := o.Store.MarkReady(v.Dataset, v.ID, snapPath, pvName); err != nil {
				slog.Error("mark ready", "dataset", v.Dataset, "version", v.ID, "err", err)
				continue
			}

			if err := o.Store.Promote(v.Dataset, v.ID); err != nil {
				slog.Error("promote", "dataset", v.Dataset, "version", v.ID, "err", err)
			} else {
				slog.Info("build complete, promoted", "dataset", v.Dataset, "version", v.ID)
			}

		case BuildFailed:
			slog.Info("build failed", "dataset", v.Dataset, "version", v.ID, "error", status.Error)
			if err := o.Store.MarkFailed(v.Dataset, v.ID, status.Error); err != nil {
				slog.Error("mark failed", "dataset", v.Dataset, "version", v.ID, "err", err)
			}

		case BuildNotFound:
			slog.Info("build handle not found; orphaned", "dataset", v.Dataset, "version", v.ID)
			if err := o.Store.MarkFailed(v.Dataset, v.ID, "build handle not found; orphaned"); err != nil {
				slog.Error("mark orphan failed", "dataset", v.Dataset, "version", v.ID, "err", err)
			}
		}
	}

	return nil
}

// Run drives build reconciliation on a ticker. It blocks until ctx is
// cancelled. The next tick is scheduled only after the current
// ReconcileBuilds call returns (tick-with-skip).
func (o *Orchestrator) Run(ctx context.Context) error {
	interval := o.ReconcileInterval
	if interval == 0 {
		interval = 5 * time.Second
	}

	// Run one reconciliation immediately on startup.
	if err := o.ReconcileBuilds(ctx); err != nil {
		slog.Error("initial reconcile", "err", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := o.ReconcileBuilds(ctx); err != nil {
				slog.Error("reconcile builds", "err", err)
			}
		}
	}
}

// BuildAndPromote is a synchronous convenience for tests: it calls
// StartBuild, then loops ReconcileBuilds until the version leaves
// building state.
func (o *Orchestrator) BuildAndPromote(ctx context.Context, spec api.DatasetSpec, versionID string) error {
	if err := o.StartBuild(ctx, spec, versionID); err != nil {
		return err
	}

	// The FakeBuilder completes synchronously in Start, so a single
	// ReconcileBuilds call should be enough. Loop for robustness.
	for {
		if err := o.ReconcileBuilds(ctx); err != nil {
			return fmt.Errorf("reconcile: %w", err)
		}

		versions := o.Store.GetVersions(spec.Name)
		for _, v := range versions {
			if v.ID != versionID {
				continue
			}
			switch v.State {
			case api.StateActive:
				return nil
			case api.StateFailed:
				return fmt.Errorf("build failed")
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
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
