package controlplane

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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

// BuildAndPromote builds a snapshot for the dataset, creates a PV symlink in
// VolumeBase, and sets the assignment for the node. This triggers the
// node-agent's long-poll to return.
func (o *Orchestrator) BuildAndPromote(ctx context.Context, spec api.DatasetSpec, versionID, nodeName string) error {
	snapPath, err := o.Builder.Build(ctx, spec, versionID)
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}

	// Create a PV name and symlink the snapshot into the volume base so
	// FSVolumeManager.AttachDisk finds it.
	pvName := fmt.Sprintf("pv-%s-%s", spec.Name, versionID)
	pvLink := filepath.Join(o.VolumeBase, pvName)
	if err := os.Symlink(snapPath, pvLink); err != nil && !os.IsExist(err) {
		return fmt.Errorf("symlink pv: %w", err)
	}

	assignment := api.NodeAssignment{
		Dataset:   spec.Name,
		KeyPrefix: spec.KeyPrefix,
		Version: api.VersionRecord{
			ID:     versionID,
			PVName: pvName,
		},
	}

	// Merge with existing assignments for other datasets on this node.
	o.Store.mu.RLock()
	existing := o.Store.assignments[nodeName]
	o.Store.mu.RUnlock()

	merged := make([]api.NodeAssignment, 0, len(existing)+1)
	for _, a := range existing {
		if a.Dataset != spec.Name {
			merged = append(merged, a)
		}
	}
	merged = append(merged, assignment)

	o.Store.SetAssignments(nodeName, merged)
	return nil
}

// Close shuts down the HTTP server.
func (o *Orchestrator) Close() error {
	return o.Server.Close()
}
