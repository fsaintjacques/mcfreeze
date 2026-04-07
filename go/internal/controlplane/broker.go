package controlplane

import (
	"reflect"
	"sync"

	"github.com/fsaintjacques/frostmap/go/api"
)

// AssignmentBroker owns the ephemeral per-node state shared between the HTTP
// long-poll handlers and the NodeAssignmentReconciler that computes desired
// assignments from active DatasetVersion CRs.
//
// It is the only piece of state that crosses the boundary between the HTTP
// server and the reconcilers introduced in Phase 5. All methods are safe for
// concurrent use.
type AssignmentBroker struct {
	mu sync.Mutex

	// nodes is the set of registered node names. Nodes auto-register on first
	// long-poll.
	nodes map[string]struct{}

	// assignments keyed by node name.
	assignments map[string][]api.NodeAssignment
	// generation per node; incremented when assignments change.
	generation map[string]int64
	// notify channels per node; closed and replaced on assignment change to
	// wake blocked long-poll requests.
	notify map[string]chan struct{}

	// nodeStates keyed by node name; last reported state from the node-agent.
	nodeStates map[string]api.NodeState
}

// NewAssignmentBroker creates an empty broker.
func NewAssignmentBroker() *AssignmentBroker {
	return &AssignmentBroker{
		nodes:       make(map[string]struct{}),
		assignments: make(map[string][]api.NodeAssignment),
		generation:  make(map[string]int64),
		notify:      make(map[string]chan struct{}),
		nodeStates:  make(map[string]api.NodeState),
	}
}

// RegisterNode adds a node to the registered set. Idempotent.
func (b *AssignmentBroker) RegisterNode(nodeName string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes[nodeName] = struct{}{}
}

// Nodes returns a snapshot of all registered node names.
func (b *AssignmentBroker) Nodes() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.nodes))
	for n := range b.nodes {
		out = append(out, n)
	}
	return out
}

// SetAssignments replaces the assignments for a node. Generation is bumped
// and waiting long-polls woken on the first set for a node and on every
// subsequent set whose value differs from the previous one — the diff check
// avoids inflating the generation counter under periodic reconciler resyncs
// that re-push identical content.
func (b *AssignmentBroker) SetAssignments(nodeName string, assignments []api.NodeAssignment) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, seen := b.assignments[nodeName]; seen && reflect.DeepEqual(b.assignments[nodeName], assignments) {
		return
	}
	// Normalize nil → empty slice so the "seen" check is unambiguous on
	// the next call: subsequent identical sets must hit the fast path.
	if assignments == nil {
		assignments = []api.NodeAssignment{}
	}

	b.assignments[nodeName] = assignments
	b.generation[nodeName]++

	if ch, ok := b.notify[nodeName]; ok {
		close(ch)
	}
	b.notify[nodeName] = make(chan struct{})
}

// MergeAssignment atomically updates or adds the assignment for a single
// dataset on a node, preserving assignments for other datasets.
func (b *AssignmentBroker) MergeAssignment(nodeName string, assignment api.NodeAssignment) {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing := b.assignments[nodeName]
	merged := make([]api.NodeAssignment, 0, len(existing)+1)
	for _, a := range existing {
		if a.Dataset != assignment.Dataset {
			merged = append(merged, a)
		}
	}
	merged = append(merged, assignment)

	if reflect.DeepEqual(existing, merged) {
		return
	}

	b.assignments[nodeName] = merged
	b.generation[nodeName]++

	if ch, ok := b.notify[nodeName]; ok {
		close(ch)
	}
	b.notify[nodeName] = make(chan struct{})
}

// GetAssignments returns the current assignments and generation for a node.
// If the broker's generation is greater than afterGeneration, the returned
// channel is nil and the caller should respond immediately. Otherwise the
// caller should select on the returned channel; it is closed when the
// assignments change.
func (b *AssignmentBroker) GetAssignments(nodeName string, afterGeneration int64) (api.AssignmentsResponse, <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	gen := b.generation[nodeName]
	resp := api.AssignmentsResponse{
		Generation:  gen,
		Assignments: b.assignments[nodeName],
	}

	if gen > afterGeneration {
		return resp, nil
	}

	ch, ok := b.notify[nodeName]
	if !ok {
		ch = make(chan struct{})
		b.notify[nodeName] = ch
	}
	return resp, ch
}

// Generation returns the current generation for a node.
func (b *AssignmentBroker) Generation(nodeName string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.generation[nodeName]
}

// ReportState stores the latest NodeState for a node.
func (b *AssignmentBroker) ReportState(nodeName string, state api.NodeState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodeStates[nodeName] = state
}

// GetNodeState returns the last reported state for a node.
func (b *AssignmentBroker) GetNodeState(nodeName string) (api.NodeState, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.nodeStates[nodeName]
	return state, ok
}

// SnapshotNodeStates returns a copy of the per-node state map.
func (b *AssignmentBroker) SnapshotNodeStates() map[string]api.NodeState {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]api.NodeState, len(b.nodeStates))
	for k, v := range b.nodeStates {
		out[k] = v
	}
	return out
}

// IsDrained returns true if no registered node still reports the given
// (dataset, versionID) pair AND every registered node has reported state at
// least once. Used by the retirement reconciler to gate PV deletion.
func (b *AssignmentBroker) IsDrained(dataset, versionID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	for node := range b.nodes {
		ns, ok := b.nodeStates[node]
		if !ok {
			return false
		}
		for _, ds := range ns.Datasets {
			if ds.Dataset == dataset && ds.VersionID == versionID {
				return false
			}
		}
	}
	return true
}
