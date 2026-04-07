package controller

import (
	"context"
	"testing"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newNAR(c client.Client, br *controlplane.AssignmentBroker) *NodeAssignmentReconciler {
	return &NodeAssignmentReconciler{Client: c, Broker: br}
}

func TestNodeAssignment_ActivePushesAssignmentsToAllNodes(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}
	c := newFakeClient(t, ds, v)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("node-a")
	br.RegisterNode("node-b")

	r := newNAR(c, br)
	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}

	for _, n := range []string{"node-a", "node-b"} {
		resp, _ := br.GetAssignments(n, 0)
		if len(resp.Assignments) != 1 {
			t.Fatalf("%s: assignments=%d, want 1", n, len(resp.Assignments))
		}
		a := resp.Assignments[0]
		if a.Dataset != "users" || a.KeyPrefix != "users" || a.Version.ID != "v1" || a.Version.PVName != "pv-users-v1" {
			t.Fatalf("%s: %+v", n, a)
		}
	}
}

func TestNodeAssignment_RolloutAggregation(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}
	c := newFakeClient(t, ds, v)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("node-a")
	br.RegisterNode("node-b")
	br.RegisterNode("node-c")
	br.RegisterNode("node-d")

	// node-a: converged on v1
	br.ReportState("node-a", api.NodeState{
		Datasets: []api.DatasetState{{Dataset: "users", VersionID: "v1", Phase: api.PhaseActive}},
	})
	// node-b: still on v0 → pending
	br.ReportState("node-b", api.NodeState{
		Datasets: []api.DatasetState{{Dataset: "users", VersionID: "v0", Phase: api.PhaseActive}},
	})
	// node-c: error on this dataset
	br.ReportState("node-c", api.NodeState{
		Datasets: []api.DatasetState{{Dataset: "users", VersionID: "v1", Phase: api.PhaseError}},
	})
	// node-d: never reported → pending

	r := newNAR(c, br)
	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}

	got := getVersion(t, c, v)
	if got.Status.Rollout == nil {
		t.Fatal("Rollout not patched")
	}
	r0 := got.Status.Rollout
	if r0.TotalNodes != 4 || r0.ConvergedNodes != 1 || r0.PendingNodes != 2 || r0.ErrorNodes != 1 {
		t.Fatalf("rollout: %+v", r0)
	}
}

func TestNodeAssignment_NonActiveSkipsRolloutPatch(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateBuilding)}
	c := newFakeClient(t, ds, v)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("node-a")
	r := newNAR(c, br)
	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}

	got := getVersion(t, c, v)
	if got.Status.Rollout != nil {
		t.Fatalf("Rollout unexpectedly set: %+v", got.Status.Rollout)
	}
}

func TestNodeAssignment_DiffOnSetDoesNotInflateGeneration(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}
	c := newFakeClient(t, ds, v)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("node-a")

	r := newNAR(c, br)
	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}
	g1 := br.Generation("node-a")
	if g1 == 0 {
		t.Fatal("expected first reconcile to bump generation")
	}

	// Re-reconcile with no change.
	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}
	if g2 := br.Generation("node-a"); g2 != g1 {
		t.Fatalf("identical resync inflated generation: %d → %d", g1, g2)
	}
}

func TestNodeAssignment_DeletedVersionRecomputes(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}
	c := newFakeClient(t, ds, v)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("node-a")
	r := newNAR(c, br)

	// First reconcile pushes the assignment.
	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}
	resp, _ := br.GetAssignments("node-a", 0)
	if len(resp.Assignments) != 1 {
		t.Fatalf("first push: %d", len(resp.Assignments))
	}

	// Delete the CR; reconcile again on the now-missing key.
	if err := c.Delete(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(v)}); err != nil {
		t.Fatal(err)
	}

	resp, _ = br.GetAssignments("node-a", 0)
	if len(resp.Assignments) != 0 {
		t.Fatalf("after delete: %d, want 0", len(resp.Assignments))
	}
}
