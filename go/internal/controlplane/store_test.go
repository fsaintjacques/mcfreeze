package controlplane

import (
	"testing"
	"time"

	"frostmap.io/fmtctl/api"
)

// --- version state machine tests ---

func TestVersion_FullLifecycle(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})
	s.RegisterNode("node-1")

	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady("ds", "v1", "/snap/v1", "pv-v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Promote("ds", "v1"); err != nil {
		t.Fatal(err)
	}

	v, ok := s.GetActiveVersion("ds")
	if !ok || v.ID != "v1" || v.State != api.StateActive {
		t.Fatalf("active version: %+v, ok=%v", v, ok)
	}

	// Assignments should have been pushed to node-1.
	resp, ch := s.GetAssignments("node-1", 0)
	if ch != nil {
		t.Fatal("expected immediate response")
	}
	if len(resp.Assignments) != 1 || resp.Assignments[0].Version.ID != "v1" {
		t.Fatalf("assignments: %+v", resp.Assignments)
	}
}

func TestVersion_PromoteRetiresOldActive(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})
	s.RegisterNode("node-1")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/snap/v1", "pv-v1")
	s.Promote("ds", "v1")

	s.CreateVersion("ds", "v2")
	s.MarkReady("ds", "v2", "/snap/v2", "pv-v2")
	s.Promote("ds", "v2")

	versions := s.GetVersions("ds")
	for _, v := range versions {
		switch v.ID {
		case "v1":
			if v.State != api.StateRetired {
				t.Errorf("v1 state = %q, want retired", v.State)
			}
		case "v2":
			if v.State != api.StateActive {
				t.Errorf("v2 state = %q, want active", v.State)
			}
		}
	}
}

func TestVersion_DuplicateBuildingRejected(t *testing.T) {
	s := NewStore()
	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateVersion("ds", "v2"); err == nil {
		t.Fatal("expected error for duplicate building version")
	}
}

func TestVersion_CreateAfterFailedSucceeds(t *testing.T) {
	s := NewStore()
	s.CreateVersion("ds", "v1")
	s.MarkFailed("ds", "v1", "build error")

	if err := s.CreateVersion("ds", "v2"); err != nil {
		t.Fatalf("expected success after failed version: %v", err)
	}
}

func TestVersion_InvalidTransitions(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})

	// Can't promote a building version.
	s.CreateVersion("ds", "v1")
	if err := s.Promote("ds", "v1"); err == nil {
		t.Error("expected error promoting building version")
	}

	// Can't mark ready a failed version.
	s.MarkFailed("ds", "v1", "oops")
	if err := s.MarkReady("ds", "v1", "/snap", "pv"); err == nil {
		t.Error("expected error marking failed version ready")
	}

	// Can't mark failed a ready version.
	s.CreateVersion("ds", "v2")
	s.MarkReady("ds", "v2", "/snap", "pv")
	if err := s.MarkFailed("ds", "v2", "oops"); err == nil {
		t.Error("expected error marking ready version failed")
	}
}

// --- rollout and retirement tests ---

func TestRolloutStatus_AllConverged(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})
	s.RegisterNode("node-1")
	s.RegisterNode("node-2")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/snap", "pv")
	s.Promote("ds", "v1")

	// Both nodes report PhaseActive for v1.
	s.ReportState("node-1", api.NodeState{Node: "node-1", Datasets: []api.DatasetState{
		{Dataset: "ds", VersionID: "v1", Phase: api.PhaseActive},
	}})
	s.ReportState("node-2", api.NodeState{Node: "node-2", Datasets: []api.DatasetState{
		{Dataset: "ds", VersionID: "v1", Phase: api.PhaseActive},
	}})

	status := s.RolloutStatus("ds")
	if len(status.ConvergedNodes) != 2 {
		t.Errorf("converged = %d, want 2", len(status.ConvergedNodes))
	}
	if len(status.PendingNodes) != 0 {
		t.Errorf("pending = %v, want empty", status.PendingNodes)
	}
}

func TestRolloutStatus_PartialConvergence(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})
	s.RegisterNode("node-1")
	s.RegisterNode("node-2")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/snap", "pv")
	s.Promote("ds", "v1")

	// Only node-1 has reported.
	s.ReportState("node-1", api.NodeState{Node: "node-1", Datasets: []api.DatasetState{
		{Dataset: "ds", VersionID: "v1", Phase: api.PhaseActive},
	}})

	status := s.RolloutStatus("ds")
	if len(status.ConvergedNodes) != 1 {
		t.Errorf("converged = %d, want 1", len(status.ConvergedNodes))
	}
	if len(status.PendingNodes) != 1 {
		t.Errorf("pending = %d, want 1", len(status.PendingNodes))
	}
}

func TestRolloutStatus_ErrorNode(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})
	s.RegisterNode("node-1")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/snap", "pv")
	s.Promote("ds", "v1")

	s.ReportState("node-1", api.NodeState{Node: "node-1", Datasets: []api.DatasetState{
		{Dataset: "ds", VersionID: "v1", Phase: api.PhaseError, Error: "mount failed"},
	}})

	status := s.RolloutStatus("ds")
	if len(status.ErrorNodes) != 1 {
		t.Errorf("error nodes = %d, want 1", len(status.ErrorNodes))
	}
}

func TestCheckRetirement_EligibleAfterConvergence(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})
	s.RegisterNode("node-1")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/snap/v1", "pv-v1")
	s.Promote("ds", "v1")

	// Node converges on v1.
	s.ReportState("node-1", api.NodeState{Node: "node-1", Datasets: []api.DatasetState{
		{Dataset: "ds", VersionID: "v1", Phase: api.PhaseActive},
	}})

	// Promote v2 — v1 moves to retired.
	s.CreateVersion("ds", "v2")
	s.MarkReady("ds", "v2", "/snap/v2", "pv-v2")
	s.Promote("ds", "v2")

	// v1 is retired but node still reports v1 — not yet eligible.
	eligible := s.CheckRetirement("ds")
	if len(eligible) != 0 {
		t.Fatalf("expected 0 eligible (node still on v1), got %d", len(eligible))
	}

	// Node converges on v2 — v1 is now eligible.
	s.ReportState("node-1", api.NodeState{Node: "node-1", Datasets: []api.DatasetState{
		{Dataset: "ds", VersionID: "v2", Phase: api.PhaseActive},
	}})

	eligible = s.CheckRetirement("ds")
	if len(eligible) != 1 || eligible[0].ID != "v1" {
		t.Fatalf("expected v1 eligible, got %+v", eligible)
	}
}

func TestDeleteVersion_Retired(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})
	s.RegisterNode("node-1")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/snap", "pv")
	s.Promote("ds", "v1")

	s.CreateVersion("ds", "v2")
	s.MarkReady("ds", "v2", "/snap", "pv")
	s.Promote("ds", "v2") // v1 → retired

	if err := s.DeleteVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	versions := s.GetVersions("ds")
	if len(versions) != 1 || versions[0].ID != "v2" {
		t.Fatalf("expected only v2, got %+v", versions)
	}
}

func TestDeleteVersion_RejectsNonRetired(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/snap", "pv")
	s.Promote("ds", "v1")

	if err := s.DeleteVersion("ds", "v1"); err == nil {
		t.Fatal("expected error deleting active version")
	}
}

func TestVersion_PromoteMultiNode(t *testing.T) {
	s := NewStore()
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds"})
	s.RegisterNode("node-1")
	s.RegisterNode("node-2")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/snap/v1", "pv-v1")
	s.Promote("ds", "v1")

	for _, node := range []string{"node-1", "node-2"} {
		resp, _ := s.GetAssignments(node, 0)
		if len(resp.Assignments) != 1 || resp.Assignments[0].Version.ID != "v1" {
			t.Errorf("%s: assignments = %+v", node, resp.Assignments)
		}
	}
}

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
