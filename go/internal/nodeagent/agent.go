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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"frostmap.io/fmtctl/api"
	"frostmap.io/fmtctl/internal/mount"
	"frostmap.io/fmtctl/internal/volume"
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
	cfg     Config
	disks   volume.VolumeManager
	mounter mount.Mounter
	log     *slog.Logger

	mu       sync.Mutex
	datasets map[string]api.DatasetState // keyed by dataset name
}

// New creates an Agent.  disks and mounter are injected to allow testing with
// fakes.
func New(cfg Config, disks volume.VolumeManager, mounter mount.Mounter) *Agent {
	if cfg.ReportInterval == 0 {
		cfg.ReportInterval = 30 * time.Second
	}
	return &Agent{
		cfg:      cfg,
		disks:    disks,
		mounter:  mounter,
		log:      slog.Default().With("component", "node-agent", "node", cfg.NodeName),
		datasets: make(map[string]api.DatasetState),
	}
}

// Run starts the reconciliation and reporting loops.  It blocks until ctx is
// cancelled.
func (a *Agent) Run(ctx context.Context) error {
	a.log.Info("starting")

	reportTicker := time.NewTicker(a.cfg.ReportInterval)
	defer reportTicker.Stop()

	var generation int64
	for {
		resp, err := a.fetchAssignments(ctx, generation)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.log.Error("fetch assignments failed", "err", err)
		} else {
			for _, assign := range resp.Assignments {
				a.reconcile(ctx, assign)
			}
			generation = resp.Generation
		}

		select {
		case <-reportTicker.C:
		default:
		}
		if err := a.reportState(ctx); err != nil {
			a.log.Error("report state failed", "err", err)
		}
	}
}

// reconcile brings the local node into the desired state for one assignment.
func (a *Agent) reconcile(ctx context.Context, assign api.NodeAssignment) {
	log := a.log.With("dataset", assign.Dataset, "version", assign.Version.ID)

	if cur := a.datasetState(assign.Dataset); cur.VersionID == assign.Version.ID && cur.Phase == api.PhaseActive {
		return
	}

	if assign.Version.PVName == "" {
		a.setPhase(assign.Dataset, assign.Version.ID, api.PhaseError,
			fmt.Sprintf("version %s has no PersistentVolume name", assign.Version.ID))
		return
	}

	mountPath := a.mountPath(assign.Dataset, assign.Version.ID)

	log.Info("attaching disk", "pv", assign.Version.PVName)
	a.setPhase(assign.Dataset, assign.Version.ID, api.PhaseAttaching, "")
	if err := a.disks.AttachDisk(ctx, a.cfg.NodeName, assign.Version.PVName); err != nil {
		a.setPhase(assign.Dataset, assign.Version.ID, api.PhaseError, err.Error())
		log.Error("attach disk failed", "err", err)
		return
	}

	log.Info("waiting for block device")
	device, err := a.disks.WaitForDevice(ctx, assign.Version.PVName)
	if err != nil {
		a.setPhase(assign.Dataset, assign.Version.ID, api.PhaseError, err.Error())
		log.Error("wait for device failed", "err", err)
		return
	}

	log.Info("mounting", "device", device, "target", mountPath)
	a.setPhase(assign.Dataset, assign.Version.ID, api.PhaseMounting, "")
	if err := a.mounter.Mount(ctx, device, mountPath); err != nil {
		a.setPhase(assign.Dataset, assign.Version.ID, api.PhaseError, err.Error())
		log.Error("mount failed", "err", err)
		return
	}

	log.Info("writing catalog.json")
	if err := a.writeCatalog(assign.Dataset, assign.Version.ID, assign.KeyPrefix, mountPath); err != nil {
		a.setPhase(assign.Dataset, assign.Version.ID, api.PhaseError, err.Error())
		log.Error("write catalog failed", "err", err)
		return
	}

	a.setPhaseWithMount(assign.Dataset, assign.Version.ID, api.PhaseActive, mountPath)

	// TODO: poll GET http://localhost:7777/version until KV server confirms
	// the new version, then unmount the previous version's disk and delete its
	// VolumeAttachment.
}

// reportState pushes the full NodeState to the control-plane.
func (a *Agent) reportState(ctx context.Context) error {
	// TODO: implement HTTP POST against the control-plane.
	_ = a.nodeState()
	return fmt.Errorf("reportState: not implemented")
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

func (a *Agent) setPhase(dataset, versionID string, phase api.DatasetPhase, errMsg string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.datasets[dataset] = api.DatasetState{
		Dataset:   dataset,
		VersionID: versionID,
		Phase:     phase,
		Error:     errMsg,
		UpdatedAt: time.Now(),
	}
}

func (a *Agent) setPhaseWithMount(dataset, versionID string, phase api.DatasetPhase, mountPath string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.datasets[dataset] = api.DatasetState{
		Dataset:   dataset,
		VersionID: versionID,
		Phase:     phase,
		MountPath: mountPath,
		UpdatedAt: time.Now(),
	}
}

func (a *Agent) mountPath(dataset, versionID string) string {
	return filepath.Join(a.cfg.MountBase, dataset, "v"+versionID)
}

// writeCatalog atomically writes catalog.json to the shared EmptyDir using a
// temp file + rename(2) so the KV server never sees a partial file.
func (a *Agent) writeCatalog(dataset, versionID, keyPrefix, mountPath string) error {
	entry := api.CatalogEntry{
		Dataset:   dataset,
		KeyPrefix: keyPrefix,
		VersionID: versionID,
		MountPath: mountPath,
	}
	data, err := json.Marshal(entry)
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

// fetchAssignments calls GET /api/v1/node/{node}/assignments?generation=N.
// It blocks at the server until the generation changes (long-poll).
func (a *Agent) fetchAssignments(ctx context.Context, generation int64) (*api.AssignmentsResponse, error) {
	// TODO: implement HTTP long-poll against the control-plane.
	_ = generation
	return nil, fmt.Errorf("fetchAssignments: not implemented")
}
