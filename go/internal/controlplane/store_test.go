package controlplane

import (
	"testing"
	"time"

	"frostmap.io/fmtctl/api"
)

func TestStore_SetAndGetAssignments(t *testing.T) {
	s := NewStore()

	assignments := []api.NodeAssignment{{
		Dataset:   "ds",
		KeyPrefix: "ds",
		Version:   api.VersionRecord{ID: "v1", PVName: "pv-1"},
	}}

	s.SetAssignments("node-1", assignments)

	resp, ch := s.GetAssignments("node-1", 0)
	if ch != nil {
		t.Fatal("expected nil channel (generation advanced)")
	}
	if resp.Generation != 1 {
		t.Fatalf("generation = %d, want 1", resp.Generation)
	}
	if len(resp.Assignments) != 1 || resp.Assignments[0].Dataset != "ds" {
		t.Fatalf("assignments = %+v", resp.Assignments)
	}
}

func TestStore_GetAssignments_BlocksOnSameGeneration(t *testing.T) {
	s := NewStore()

	s.SetAssignments("node-1", nil)

	// Generation is now 1. Asking for generation=1 should block.
	_, ch := s.GetAssignments("node-1", 1)
	if ch == nil {
		t.Fatal("expected non-nil channel (should block)")
	}

	// Channel should not be closed yet.
	select {
	case <-ch:
		t.Fatal("channel closed before assignment change")
	default:
	}

	// Update assignments — channel should close.
	s.SetAssignments("node-1", nil)

	select {
	case <-ch:
		// OK
	case <-time.After(time.Second):
		t.Fatal("channel not closed after assignment change")
	}

	// Now generation is 2.
	resp, ch := s.GetAssignments("node-1", 1)
	if ch != nil {
		t.Fatal("expected nil channel (generation 2 > 1)")
	}
	if resp.Generation != 2 {
		t.Fatalf("generation = %d, want 2", resp.Generation)
	}
}

func TestStore_GetAssignments_UnknownNode(t *testing.T) {
	s := NewStore()

	resp, ch := s.GetAssignments("unknown", 0)
	if ch == nil {
		t.Fatal("expected non-nil channel for unknown node")
	}
	if resp.Generation != 0 {
		t.Fatalf("generation = %d, want 0", resp.Generation)
	}
	if len(resp.Assignments) != 0 {
		t.Fatalf("expected empty assignments, got %d", len(resp.Assignments))
	}
}

func TestStore_ReportAndGetState(t *testing.T) {
	s := NewStore()

	state := api.NodeState{
		Node: "node-1",
		Datasets: []api.DatasetState{{
			Dataset:   "ds",
			VersionID: "v1",
			Phase:     api.PhaseActive,
		}},
		ReportedAt: time.Now(),
	}

	s.ReportState("node-1", state)

	got, ok := s.GetNodeState("node-1")
	if !ok {
		t.Fatal("expected state to exist")
	}
	if got.Node != "node-1" || len(got.Datasets) != 1 {
		t.Fatalf("got = %+v", got)
	}

	_, ok = s.GetNodeState("unknown")
	if ok {
		t.Fatal("expected no state for unknown node")
	}
}
