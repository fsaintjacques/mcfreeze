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
	// Assignment management
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

// MemStore holds the control-plane state in memory. All methods are safe for
// concurrent use.
type MemStore struct {
	mu sync.Mutex

	// Dataset specs keyed by dataset name.
	specs map[string]api.DatasetSpec
	// Versions per dataset, ordered by creation time.
	versions map[string][]VersionEntry
	// Registered node names (assignments are pushed to all registered nodes).
	nodes map[string]struct{}

	// assignments keyed by node name.
	assignments map[string][]api.NodeAssignment
	// generation per node; incremented on every assignment change.
	generation map[string]int64
	// notify channels per node; closed and replaced when assignments change
	// to wake blocked long-poll requests.
	notify map[string]chan struct{}

	// nodeStates keyed by node name; last reported state.
	nodeStates map[string]api.NodeState
}

// NewMemStore creates an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{
		specs:       make(map[string]api.DatasetSpec),
		versions:    make(map[string][]VersionEntry),
		nodes:       make(map[string]struct{}),
		assignments: make(map[string][]api.NodeAssignment),
		generation:  make(map[string]int64),
		notify:      make(map[string]chan struct{}),
		nodeStates:  make(map[string]api.NodeState),
	}
}

// SetAssignments replaces the assignments for a node, bumps the generation,
// and wakes any blocked long-poll.
func (s *MemStore) SetAssignments(nodeName string, assignments []api.NodeAssignment) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.assignments[nodeName] = assignments
	s.generation[nodeName]++

	// Wake blocked long-poll by closing the old channel and creating a new one.
	if ch, ok := s.notify[nodeName]; ok {
		close(ch)
	}
	s.notify[nodeName] = make(chan struct{})
}

// GetAssignments returns the current assignments and generation for a node.
// If the store's generation is greater than afterGeneration, it returns
// immediately. Otherwise it returns a channel that will be closed when the
// assignments change — the caller should select on it.
func (s *MemStore) GetAssignments(nodeName string, afterGeneration int64) (api.AssignmentsResponse, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	gen := s.generation[nodeName]
	resp := api.AssignmentsResponse{
		Generation:  gen,
		Assignments: s.assignments[nodeName],
	}

	if gen > afterGeneration {
		return resp, nil // caller should return immediately
	}

	// Ensure a notify channel exists for long-poll blocking.
	ch, ok := s.notify[nodeName]
	if !ok {
		ch = make(chan struct{})
		s.notify[nodeName] = ch
	}

	return resp, ch
}

// ReportState stores the latest NodeState for a node.
func (s *MemStore) ReportState(nodeName string, state api.NodeState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeStates[nodeName] = state
}

// GetNodeState returns the last reported state for a node.
func (s *MemStore) GetNodeState(nodeName string) (api.NodeState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.nodeStates[nodeName]
	return state, ok
}

// MergeAssignment atomically updates or adds an assignment for a single
// dataset on a node. Existing assignments for other datasets are preserved.
func (s *MemStore) MergeAssignment(nodeName string, assignment api.NodeAssignment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mergeAssignmentLocked(nodeName, assignment)
}

// Generation returns the current generation for a node.
func (s *MemStore) Generation(nodeName string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation[nodeName]
}

// ---------------------------------------------------------------------------
// Dataset and version lifecycle
// ---------------------------------------------------------------------------

// RegisterDataset registers a dataset spec. Idempotent.
func (s *MemStore) RegisterDataset(spec api.DatasetSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specs[spec.Name] = spec
}

// RegisterNode registers a node name so Promote can push assignments to it.
func (s *MemStore) RegisterNode(nodeName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[nodeName] = struct{}{}
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

	// Push assignment to all registered nodes.
	assignment := api.NodeAssignment{
		Dataset:   dataset,
		KeyPrefix: spec.KeyPrefix,
		Version:   v.VersionRecord,
	}
	for nodeName := range s.nodes {
		s.mergeAssignmentLocked(nodeName, assignment)
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

	for nodeName := range s.nodes {
		ns, ok := s.nodeStates[nodeName]
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
	s.mu.Lock()
	defer s.mu.Unlock()

	// If any registered node has never reported, no version is eligible —
	// we can't know what that node is still serving.
	for nodeName := range s.nodes {
		if _, ok := s.nodeStates[nodeName]; !ok {
			return nil
		}
	}

	// Build set of versions still reported by any node.
	reportedVersions := make(map[string]bool)
	for _, ns := range s.nodeStates {
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

// mergeAssignmentLocked updates or adds an assignment for a node (caller must hold mu).
func (s *MemStore) mergeAssignmentLocked(nodeName string, assignment api.NodeAssignment) {
	existing := s.assignments[nodeName]
	merged := make([]api.NodeAssignment, 0, len(existing)+1)
	for _, a := range existing {
		if a.Dataset != assignment.Dataset {
			merged = append(merged, a)
		}
	}
	merged = append(merged, assignment)

	s.assignments[nodeName] = merged
	s.generation[nodeName]++

	if ch, ok := s.notify[nodeName]; ok {
		close(ch)
	}
	s.notify[nodeName] = make(chan struct{})
}
