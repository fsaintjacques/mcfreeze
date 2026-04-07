package controlplane

import (
	"testing"

	"github.com/fsaintjacques/frostmap/go/api"
)

func TestBroker_DiffOnSet_IdenticalContentDoesNotBumpGeneration(t *testing.T) {
	b := NewAssignmentBroker()
	a := []api.NodeAssignment{{Dataset: "ds", KeyPrefix: "ds", Version: api.VersionRecord{ID: "v1"}}}

	b.SetAssignments("node-1", a)
	if g := b.Generation("node-1"); g != 1 {
		t.Fatalf("first set: gen = %d, want 1", g)
	}

	// Identical re-set should not bump.
	b.SetAssignments("node-1", a)
	if g := b.Generation("node-1"); g != 1 {
		t.Fatalf("identical re-set: gen = %d, want 1 (diff-on-set)", g)
	}

	// Different content bumps.
	b.SetAssignments("node-1", append(a, api.NodeAssignment{Dataset: "ds2", KeyPrefix: "ds2"}))
	if g := b.Generation("node-1"); g != 2 {
		t.Fatalf("changed: gen = %d, want 2", g)
	}
}

func TestBroker_IsDrained(t *testing.T) {
	b := NewAssignmentBroker()
	b.RegisterNode("node-1")
	b.RegisterNode("node-2")

	// No nodes have reported yet → not drained.
	if b.IsDrained("ds", "v1") {
		t.Fatal("expected not drained when no states reported")
	}

	// node-1 reports v1 still active.
	b.ReportState("node-1", api.NodeState{
		Datasets: []api.DatasetState{{Dataset: "ds", VersionID: "v1", Phase: api.PhaseActive}},
	})
	b.ReportState("node-2", api.NodeState{
		Datasets: []api.DatasetState{{Dataset: "ds", VersionID: "v2", Phase: api.PhaseActive}},
	})
	if b.IsDrained("ds", "v1") {
		t.Fatal("v1 still reported by node-1 — should not be drained")
	}

	// node-1 moves to v2 → v1 fully drained.
	b.ReportState("node-1", api.NodeState{
		Datasets: []api.DatasetState{{Dataset: "ds", VersionID: "v2", Phase: api.PhaseActive}},
	})
	if !b.IsDrained("ds", "v1") {
		t.Fatal("v1 not reported by any node — should be drained")
	}
}
