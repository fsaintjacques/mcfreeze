// Package nodeagent implements the fmtctl node-agent: it watches the
// control-plane for dataset version assignments, attaches Hyperdisk ML volumes,
// mounts them read-only, and signals the KV server via catalog.json.
//
// The agent follows a converging reconciliation model: it maintains its full
// actual state in memory, reconciles it against the desired assignments on
// every control-plane poll, and periodically reports the complete NodeState
// back.  A missed report never causes permanent divergence — the next report
// self-heals.
package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/assignment"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/mount"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/version"
	"github.com/fsaintjacques/frostmap/go/internal/nodeagent/volume"
)

// Config holds all parameters needed to run the node-agent.
type Config struct {
	// ControlPlaneURL is the base URL of the control-plane HTTP API,
	// e.g. "http://fmtctl-control-plane:8080".
	ControlPlaneURL string
	// NodeName is the Kubernetes node name this agent runs on.
	NodeName string
	// MountBase is the root directory under which versions are mounted,
	// e.g. "/mnt/kv".  Versions are mounted at <MountBase>/<dataset>/v<N>/.
	MountBase string
	// CatalogDir is the path to the shared EmptyDir where catalog.json is
	// written.  The KV server watches this directory.
	CatalogDir string
	// ReportInterval is how often the agent pushes its full NodeState to the
	// control-plane regardless of assignment changes.
	ReportInterval time.Duration
}

// Agent is the node-agent main loop.
type Agent struct {
	cfg         Config
	disks       volume.Manager
	mounter     mount.Mounter
	assignments assignment.Source
	reporter    assignment.StateReporter
	versions    version.Checker
	log         *slog.Logger

	mu       sync.Mutex
	datasets map[string]api.DatasetState // keyed by dataset name
}

// New creates an Agent.  All dependencies are injected to allow testing with
// fakes.
func New(
	cfg Config,
	disks volume.Manager,
	mounter mount.Mounter,
	assignments assignment.Source,
	reporter assignment.StateReporter,
	versions version.Checker,
) *Agent {
	if cfg.ReportInterval == 0 {
		cfg.ReportInterval = 30 * time.Second
	}
	return &Agent{
		cfg:         cfg,
		disks:       disks,
		mounter:     mounter,
		assignments: assignments,
		reporter:    reporter,
		versions:    versions,
		log:         slog.Default().With("component", "node-agent", "node", cfg.NodeName),
		datasets:    make(map[string]api.DatasetState),
	}
}

// Run starts the reconciliation and reporting loops.  It blocks until ctx is
// cancelled.
func (a *Agent) Run(ctx context.Context) error {
	a.log.Info("starting")

	// Periodic reporter runs in a separate goroutine so it fires even
	// while FetchAssignments blocks on the long-poll.
	go a.reportLoop(ctx)

	var generation int64
	var backoff time.Duration
	const (
		backoffMin = 1 * time.Second
		backoffMax = 30 * time.Second
	)

	for {
		resp, err := a.assignments.FetchAssignments(ctx, generation)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.log.Error("fetch assignments failed", "err", err)

			// Exponential backoff on failure.
			if backoff == 0 {
				backoff = backoffMin
			} else {
				backoff = min(backoff*2, backoffMax)
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		backoff = 0 // reset on success

		for _, assign := range resp.Assignments {
			a.reconcile(ctx, assign)
		}
		generation = resp.Generation

		// Report immediately after processing assignments.
		a.doReport(ctx)
	}
}

// reportLoop periodically reports the full NodeState to the control-plane.
// Runs until ctx is cancelled.
func (a *Agent) reportLoop(ctx context.Context) {
	// Report initial state immediately.
	a.doReport(ctx)

	ticker := time.NewTicker(a.cfg.ReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.doReport(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (a *Agent) doReport(ctx context.Context) {
	if err := a.reporter.ReportState(ctx, a.nodeState()); err != nil {
		a.log.Error("report state failed", "err", err)
	}
}

// reconcile brings the local node into the desired state for one assignment.
func (a *Agent) reconcile(ctx context.Context, assign api.NodeAssignment) {
	log := a.log.With("dataset", assign.Dataset, "version", assign.Version.ID)

	prev := a.datasetState(assign.Dataset)
	if prev.VersionID == assign.Version.ID && prev.Phase == api.PhaseActive {
		return
	}

	kp := assign.KeyPrefix
	pv := assign.Version.PVName

	if pv == "" {
		a.setPhase(assign.Dataset, kp, assign.Version.ID, "", api.PhaseError,
			fmt.Sprintf("version %s has no PersistentVolume name", assign.Version.ID))
		return
	}

	mountPath := a.mountPath(assign.Dataset, assign.Version.ID)

	log.Info("attaching disk", "pv", pv)
	a.setPhase(assign.Dataset, kp, assign.Version.ID, pv, api.PhaseAttaching, "")
	if err := a.disks.AttachDisk(ctx, a.cfg.NodeName, pv); err != nil {
		a.setPhase(assign.Dataset, kp, assign.Version.ID, pv, api.PhaseError, err.Error())
		log.Error("attach disk failed", "err", err)
		return
	}

	log.Info("waiting for block device")
	device, err := a.disks.WaitForDevice(ctx, pv)
	if err != nil {
		a.setPhase(assign.Dataset, kp, assign.Version.ID, pv, api.PhaseError, err.Error())
		log.Error("wait for device failed", "err", err)
		return
	}

	log.Info("mounting", "device", device, "target", mountPath)
	a.setPhase(assign.Dataset, kp, assign.Version.ID, pv, api.PhaseMounting, "")
	if err := a.mounter.Mount(ctx, device, mountPath); err != nil {
		a.setPhase(assign.Dataset, kp, assign.Version.ID, pv, api.PhaseError, err.Error())
		log.Error("mount failed", "err", err)
		return
	}

	log.Info("writing catalog.json")
	if err := a.writeCatalog(assign.Dataset, assign.Version.ID, kp, mountPath); err != nil {
		a.setPhase(assign.Dataset, kp, assign.Version.ID, pv, api.PhaseError, err.Error())
		log.Error("write catalog failed", "err", err)
		return
	}

	log.Info("waiting for KV server version confirmation")
	versionTimeout := 2 * time.Minute
	vctx, vcancel := context.WithTimeout(ctx, versionTimeout)
	defer vcancel()
	if err := a.versions.WaitForVersion(vctx, assign.Dataset, assign.Version.ID); err != nil {
		a.setPhase(assign.Dataset, kp, assign.Version.ID, pv, api.PhaseError, err.Error())
		log.Error("version confirmation failed", "err", err)
		return
	}

	a.setPhaseWithMount(assign.Dataset, kp, assign.Version.ID, pv, api.PhaseActive, mountPath)
	log.Info("dataset active")

	// Clean up the previous version's mount and disk attachment.
	// Also clean up partially-failed reconciliations that left a disk attached.
	if prev.VersionID != assign.Version.ID && prev.VersionID != "" {
		a.cleanupOldVersion(ctx, assign.Dataset, prev)
	}
}

// cleanupOldVersion unmounts and detaches the previous version's disk.
// Errors are logged but not fatal — the old version is already superseded.
func (a *Agent) cleanupOldVersion(ctx context.Context, dataset string, prev api.DatasetState) {
	log := a.log.With("dataset", dataset, "old_version", prev.VersionID)

	if prev.MountPath != "" {
		log.Info("unmounting old version", "path", prev.MountPath)
		if err := a.mounter.Unmount(ctx, prev.MountPath); err != nil {
			log.Error("unmount old version failed", "err", err)
		}
	}

	if prev.PVName != "" {
		log.Info("detaching old disk", "pv", prev.PVName)
		if err := a.disks.DetachDisk(ctx, a.cfg.NodeName, prev.PVName); err != nil {
			log.Error("detach old disk failed", "err", err)
		}
	}
}

// Shutdown gracefully unmounts all datasets and detaches their disks.
// Call this after Run() returns. The provided context controls the overall
// deadline — typically the remaining Kubernetes termination grace period.
//
// Unmount is retried with backoff because the KV server may still hold mmapped
// fds when SIGTERM arrives (Kubernetes sends SIGTERM to all containers in
// parallel). The retry loop waits for the KV server to exit and release its
// mmaps.
func (a *Agent) Shutdown(ctx context.Context) {
	a.log.Info("shutting down: cleaning up mounts and disks")

	a.mu.Lock()
	snapshot := make([]api.DatasetState, 0, len(a.datasets))
	for _, ds := range a.datasets {
		snapshot = append(snapshot, ds)
	}
	a.mu.Unlock()

	for _, ds := range snapshot {
		log := a.log.With("dataset", ds.Dataset, "version", ds.VersionID)

		if ds.MountPath != "" {
			if err := a.unmountWithRetry(ctx, ds.MountPath); err != nil {
				log.Error("shutdown unmount failed", "path", ds.MountPath, "err", err)
			} else {
				log.Info("unmounted", "path", ds.MountPath)
			}
		}

		if ds.PVName != "" {
			if err := a.disks.DetachDisk(ctx, a.cfg.NodeName, ds.PVName); err != nil {
				log.Error("shutdown detach failed", "pv", ds.PVName, "err", err)
			} else {
				log.Info("detached", "pv", ds.PVName)
			}
		}
	}

	a.log.Info("shutdown complete")
}

// unmountWithRetry retries Unmount with exponential backoff until it succeeds
// or ctx is cancelled.  This handles the case where the KV server still holds
// mmapped fds (EBUSY) and hasn't exited yet.
func (a *Agent) unmountWithRetry(ctx context.Context, path string) error {
	backoff := 500 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		err := a.mounter.Unmount(ctx, path)
		if err == nil {
			return nil
		}

		// Only retry on EBUSY (KV server still holds mmapped fds).
		// Non-transient errors (e.g. path not found) return immediately.
		if !errors.Is(err, syscall.EBUSY) {
			return err
		}

		a.log.Warn("unmount busy, retrying", "path", path, "backoff", backoff, "err", err)

		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		case <-ctx.Done():
			return fmt.Errorf("unmount %s: gave up: %w (last error: %v)", path, ctx.Err(), err)
		}
	}
}

// nodeState returns a snapshot of the current NodeState.
func (a *Agent) nodeState() api.NodeState {
	a.mu.Lock()
	defer a.mu.Unlock()
	datasets := make([]api.DatasetState, 0, len(a.datasets))
	for _, ds := range a.datasets {
		datasets = append(datasets, ds)
	}
	return api.NodeState{
		Node:       a.cfg.NodeName,
		Datasets:   datasets,
		ReportedAt: time.Now(),
	}
}

func (a *Agent) datasetState(dataset string) api.DatasetState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.datasets[dataset]
}

func (a *Agent) setPhase(dataset, keyPrefix, versionID, pvName string, phase api.DatasetPhase, errMsg string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	prev := a.datasets[dataset]
	if keyPrefix == "" {
		keyPrefix = prev.KeyPrefix
	}
	a.datasets[dataset] = api.DatasetState{
		Dataset:   dataset,
		KeyPrefix: keyPrefix,
		VersionID: versionID,
		Phase:     phase,
		PVName:    pvName,
		Error:     errMsg,
		UpdatedAt: time.Now(),
	}
}

func (a *Agent) setPhaseWithMount(dataset, keyPrefix, versionID, pvName string, phase api.DatasetPhase, mountPath string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.datasets[dataset] = api.DatasetState{
		Dataset:   dataset,
		KeyPrefix: keyPrefix,
		VersionID: versionID,
		Phase:     phase,
		PVName:    pvName,
		MountPath: mountPath,
		UpdatedAt: time.Now(),
	}
}

func (a *Agent) mountPath(dataset, versionID string) string {
	return filepath.Join(a.cfg.MountBase, dataset, versionID)
}

// writeCatalog atomically writes catalog.json to the shared EmptyDir using a
// temp file + rename(2) so the KV server never sees a partial file.
//
// The catalog includes entries for ALL datasets that are active or being
// promoted (the current call's dataset plus any already-active datasets).
func (a *Agent) writeCatalog(dataset, versionID, keyPrefix, mountPath string) error {
	a.mu.Lock()
	entries := make([]api.CatalogEntry, 0, len(a.datasets)+1)
	// Include all currently-active datasets.
	for _, ds := range a.datasets {
		if ds.Phase == api.PhaseActive && ds.Dataset != dataset {
			entries = append(entries, api.CatalogEntry{
				Dataset:   ds.Dataset,
				KeyPrefix: ds.KeyPrefix,
				VersionID: ds.VersionID,
				MountPath: ds.MountPath,
			})
		}
	}
	a.mu.Unlock()

	// Add the dataset being promoted.
	entries = append(entries, api.CatalogEntry{
		Dataset:   dataset,
		KeyPrefix: keyPrefix,
		VersionID: versionID,
		MountPath: mountPath,
	})

	catalog := api.CatalogFile{Entries: entries}
	data, err := json.Marshal(catalog)
	if err != nil {
		return err
	}
	dst := filepath.Join(a.cfg.CatalogDir, "catalog.json")
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write tmp catalog: %w", err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename catalog: %w", err)
	}
	return nil
}
