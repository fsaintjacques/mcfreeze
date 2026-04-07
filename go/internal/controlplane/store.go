// Package controlplane implements the fmtctl control-plane: an HTTP server
// that manages dataset version assignments and collects node state reports.
package controlplane

import (
	"fmt"
	"sync"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/builder"
)

// VersionEntry extends api.VersionRecord with the local snapshot path
// (used by the orchestrator to symlink into the volume base) and the
// build handle (non-empty while State == building).
type VersionEntry struct {
	api.VersionRecord
	SnapshotPath string
	BuildHandle  builder.Handle
}

// Store is the control-plane state interface. Implementations must be safe
// for concurrent use.
type Store interface {
	// Broker returns the AssignmentBroker that owns ephemeral per-node state
	// (assignments, generation, notify channels, nodeStates). Reconcilers
	// and the HTTP server share the same broker instance.
	Broker() *AssignmentBroker

	// Assignment management (delegated to Broker; kept on Store for backwards
	// compatibility with the existing Server and Orchestrator).
	SetAssignments(nodeName string, assignments []api.NodeAssignment)
	GetAssignments(nodeName string, afterGeneration int64) (api.AssignmentsResponse, <-chan struct{})
	MergeAssignment(nodeName string, assignment api.NodeAssignment)
	Generation(nodeName string) int64
	ReportState(nodeName string, state api.NodeState)
	GetNodeState(nodeName string) (api.NodeState, bool)

	// Dataset and version lifecycle
	RegisterDataset(spec api.DatasetSpec)
	RegisterNode(nodeName string)
	GetDatasetSpec(name string) (api.DatasetSpec, bool)
	CreateVersion(dataset, versionID string) error
	MarkReady(dataset, versionID, snapshotPath, pvName string) error
	SetDescriptor(dataset, versionID, descriptor, messageName string) error
	MarkFailed(dataset, versionID, reason string) error
	Promote(dataset, versionID string) error
	GetVersions(dataset string) []VersionEntry
	GetActiveVersion(dataset string) (VersionEntry, bool)

	// Rollout and retirement
	RolloutStatus(dataset string) RolloutStatus
	CheckRetirement(dataset string) []VersionEntry
	DeleteVersion(dataset, versionID string) error

	// Build tracking
	SetBuildHandle(dataset, versionID string, handle builder.Handle) error
	GetBuildingVersions() []VersionEntry
}

// MemStore holds the control-plane state in memory. The ephemeral per-node
// state (assignments, generation, notify channels, nodeStates, registered
// nodes) lives in an embedded *AssignmentBroker so the HTTP server and the
// reconcilers introduced in Phase 5 can share it. All methods are safe for
// concurrent use.
type MemStore struct {
	*AssignmentBroker

	mu sync.Mutex

	// Dataset specs keyed by dataset name.
	specs map[string]api.DatasetSpec
	// Versions per dataset, ordered by creation time.
	versions map[string][]VersionEntry
}

// NewMemStore creates an empty MemStore backed by a fresh AssignmentBroker.
func NewMemStore() *MemStore {
	return NewMemStoreWithBroker(NewAssignmentBroker())
}

// NewMemStoreWithBroker creates a MemStore that shares the given broker.
// Useful when the broker must be wired into multiple components (HTTP server,
// reconcilers) that all need the same in-memory state.
func NewMemStoreWithBroker(broker *AssignmentBroker) *MemStore {
	return &MemStore{
		AssignmentBroker: broker,
		specs:            make(map[string]api.DatasetSpec),
		versions:         make(map[string][]VersionEntry),
	}
}

// Broker returns the underlying AssignmentBroker.
func (s *MemStore) Broker() *AssignmentBroker { return s.AssignmentBroker }

// ---------------------------------------------------------------------------
// Dataset and version lifecycle
// ---------------------------------------------------------------------------

// RegisterDataset registers a dataset spec. Idempotent.
func (s *MemStore) RegisterDataset(spec api.DatasetSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specs[spec.Name] = spec
}

// GetDatasetSpec returns the spec for a dataset.
func (s *MemStore) GetDatasetSpec(name string) (api.DatasetSpec, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := s.specs[name]
	return spec, ok
}

// CreateVersion creates a new VersionRecord in building state.
// Returns an error if a building version already exists for this dataset.
func (s *MemStore) CreateVersion(dataset, versionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range s.versions[dataset] {
		if v.State == api.StateBuilding {
			return fmt.Errorf("dataset %q already has a building version %q", dataset, v.ID)
		}
	}

	// Populate ShardCount from the spec if registered.
	var shardCount int
	if spec, ok := s.specs[dataset]; ok {
		shardCount = spec.ShardCount
	}

	s.versions[dataset] = append(s.versions[dataset], VersionEntry{
		VersionRecord: api.VersionRecord{
			ID:         versionID,
			Dataset:    dataset,
			State:      api.StateBuilding,
			ShardCount: shardCount,
			CreatedAt:  time.Now(),
		},
	})
	return nil
}

// MarkReady transitions a version from building to ready.
func (s *MemStore) MarkReady(dataset, versionID, snapshotPath, pvName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, err := s.findVersion(dataset, versionID)
	if err != nil {
		return err
	}
	if v.State != api.StateBuilding {
		return fmt.Errorf("version %q is %q, expected building", versionID, v.State)
	}
	v.State = api.StateReady
	v.PVName = pvName
	v.SnapshotPath = snapshotPath
	return nil
}

// SetDescriptor sets the protobuf descriptor and message name on a version.
// No-op if both values are empty. Safe to call at any lifecycle state.
func (s *MemStore) SetDescriptor(dataset, versionID, descriptor, messageName string) error {
	if descriptor == "" && messageName == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	v, err := s.findVersion(dataset, versionID)
	if err != nil {
		return err
	}
	v.Descriptor = descriptor
	v.MessageName = messageName
	return nil
}

// MarkFailed transitions a version from building to failed.
func (s *MemStore) MarkFailed(dataset, versionID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, err := s.findVersion(dataset, versionID)
	if err != nil {
		return err
	}
	if v.State != api.StateBuilding {
		return fmt.Errorf("version %q is %q, expected building", versionID, v.State)
	}
	v.State = api.StateFailed
	return nil
}

// Promote transitions a version from ready to active. The previously active
// version (if any) moves to retired. Assignments are updated for all
// registered nodes.
func (s *MemStore) Promote(dataset, versionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, err := s.findVersion(dataset, versionID)
	if err != nil {
		return err
	}
	if v.State != api.StateReady {
		return fmt.Errorf("version %q is %q, expected ready", versionID, v.State)
	}

	spec, ok := s.specs[dataset]
	if !ok {
		return fmt.Errorf("dataset %q not registered", dataset)
	}

	// Retire the current active version.
	for i := range s.versions[dataset] {
		if s.versions[dataset][i].State == api.StateActive {
			s.versions[dataset][i].State = api.StateRetired
		}
	}

	v.State = api.StateActive

	// Push assignment to all registered nodes via the broker.
	assignment := api.NodeAssignment{
		Dataset:   dataset,
		KeyPrefix: spec.KeyPrefix,
		Version:   v.VersionRecord,
	}
	for _, nodeName := range s.AssignmentBroker.Nodes() {
		s.AssignmentBroker.MergeAssignment(nodeName, assignment)
	}

	return nil
}

// GetVersions returns all versions for a dataset.
func (s *MemStore) GetVersions(dataset string) []VersionEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]VersionEntry, len(s.versions[dataset]))
	copy(out, s.versions[dataset])
	return out
}

// GetActiveVersion returns the active version for a dataset, if any.
func (s *MemStore) GetActiveVersion(dataset string) (VersionEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.versions[dataset] {
		if v.State == api.StateActive {
			return v, true
		}
	}
	return VersionEntry{}, false
}

// ---------------------------------------------------------------------------
// Rollout and retirement
// ---------------------------------------------------------------------------

// RolloutStatus summarises per-node convergence for a dataset.
type RolloutStatus struct {
	Dataset        string
	ActiveVersion  string
	NodeCounts     map[string]int // version_id → count of nodes reporting it active
	ConvergedNodes []string       // nodes reporting the active version
	PendingNodes   []string       // nodes not yet on the active version
	ErrorNodes     []string       // nodes in error state for this dataset
}

// RolloutStatus returns the convergence status for a dataset by diffing the
// active assignment against reported NodeStates.
func (s *MemStore) RolloutStatus(dataset string) RolloutStatus {
	// Snapshot broker state without holding s.mu (lock order: s.mu → broker.mu).
	nodes := s.AssignmentBroker.Nodes()
	nodeStates := s.AssignmentBroker.SnapshotNodeStates()

	s.mu.Lock()
	defer s.mu.Unlock()

	status := RolloutStatus{
		Dataset:    dataset,
		NodeCounts: make(map[string]int),
	}

	// Find the active version.
	for _, v := range s.versions[dataset] {
		if v.State == api.StateActive {
			status.ActiveVersion = v.ID
			break
		}
	}

	for _, nodeName := range nodes {
		ns, ok := nodeStates[nodeName]
		if !ok {
			status.PendingNodes = append(status.PendingNodes, nodeName)
			continue
		}

		found := false
		for _, ds := range ns.Datasets {
			if ds.Dataset != dataset {
				continue
			}
			found = true
			if ds.Phase == api.PhaseError {
				status.ErrorNodes = append(status.ErrorNodes, nodeName)
			} else if ds.Phase == api.PhaseActive && ds.VersionID == status.ActiveVersion {
				status.ConvergedNodes = append(status.ConvergedNodes, nodeName)
			} else {
				status.PendingNodes = append(status.PendingNodes, nodeName)
			}
			status.NodeCounts[ds.VersionID]++
			break
		}
		if !found {
			status.PendingNodes = append(status.PendingNodes, nodeName)
		}
	}

	return status
}

// CheckRetirement returns retired versions eligible for cleanup: all
// registered nodes have reported state AND none of them report the
// retired version.
func (s *MemStore) CheckRetirement(dataset string) []VersionEntry {
	nodes := s.AssignmentBroker.Nodes()
	nodeStates := s.AssignmentBroker.SnapshotNodeStates()

	s.mu.Lock()
	defer s.mu.Unlock()

	// If any registered node has never reported, no version is eligible —
	// we can't know what that node is still serving.
	for _, nodeName := range nodes {
		if _, ok := nodeStates[nodeName]; !ok {
			return nil
		}
	}

	// Build set of versions still reported by any node.
	reportedVersions := make(map[string]bool)
	for _, ns := range nodeStates {
		for _, ds := range ns.Datasets {
			if ds.Dataset == dataset {
				reportedVersions[ds.VersionID] = true
			}
		}
	}

	var eligible []VersionEntry
	for _, v := range s.versions[dataset] {
		if v.State == api.StateRetired && !reportedVersions[v.ID] {
			eligible = append(eligible, v)
		}
	}
	return eligible
}

// DeleteVersion removes a retired version from the store. Returns an error
// if the version is not in retired state.
func (s *MemStore) DeleteVersion(dataset, versionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions := s.versions[dataset]
	for i, v := range versions {
		if v.ID == versionID {
			if v.State != api.StateRetired {
				return fmt.Errorf("version %q is %q, expected retired", versionID, v.State)
			}
			s.versions[dataset] = append(versions[:i], versions[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("version %q not found for dataset %q", versionID, dataset)
}

// SetBuildHandle sets the build handle for a version in building state.
func (s *MemStore) SetBuildHandle(dataset, versionID string, handle builder.Handle) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, err := s.findVersion(dataset, versionID)
	if err != nil {
		return err
	}
	if v.State != api.StateBuilding {
		return fmt.Errorf("version %q is %q, expected building", versionID, v.State)
	}
	v.BuildHandle = handle
	return nil
}

// GetBuildingVersions returns all versions in building state across all datasets.
func (s *MemStore) GetBuildingVersions() []VersionEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []VersionEntry
	for _, versions := range s.versions {
		for _, v := range versions {
			if v.State == api.StateBuilding {
				out = append(out, v)
			}
		}
	}
	return out
}

// findVersion returns a pointer to the version entry (caller must hold mu).
func (s *MemStore) findVersion(dataset, versionID string) (*VersionEntry, error) {
	for i := range s.versions[dataset] {
		if s.versions[dataset][i].ID == versionID {
			return &s.versions[dataset][i], nil
		}
	}
	return nil, fmt.Errorf("version %q not found for dataset %q", versionID, dataset)
}
