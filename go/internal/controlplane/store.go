// Package controlplane implements the fmtctl control-plane: an HTTP server
// that manages dataset version assignments and collects node state reports.
package controlplane

import (
	"sync"

	"frostmap.io/fmtctl/api"
)

// Store holds the control-plane state in memory. All methods are safe for
// concurrent use. Tests manipulate it directly; production (Phase 4) will
// back it with Kubernetes CRDs.
type Store struct {
	mu sync.RWMutex

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

// NewStore creates an empty Store.
func NewStore() *Store {
	return &Store{
		assignments: make(map[string][]api.NodeAssignment),
		generation:  make(map[string]int64),
		notify:      make(map[string]chan struct{}),
		nodeStates:  make(map[string]api.NodeState),
	}
}

// SetAssignments replaces the assignments for a node, bumps the generation,
// and wakes any blocked long-poll.
func (s *Store) SetAssignments(nodeName string, assignments []api.NodeAssignment) {
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
func (s *Store) GetAssignments(nodeName string, afterGeneration int64) (api.AssignmentsResponse, <-chan struct{}) {
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
func (s *Store) ReportState(nodeName string, state api.NodeState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeStates[nodeName] = state
}

// GetNodeState returns the last reported state for a node.
func (s *Store) GetNodeState(nodeName string) (api.NodeState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.nodeStates[nodeName]
	return state, ok
}

// Generation returns the current generation for a node.
func (s *Store) Generation(nodeName string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.generation[nodeName]
}
