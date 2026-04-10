package controller

import (
	"context"
	"testing"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newNAR(c client.Client, br *controlplane.AssignmentBroker) *NodeAssignmentReconciler {
	return &NodeAssignmentReconciler{Client: c, Broker: br}
}

func newNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
	}
}

func newBinding(name string, nodeSelector, datasetSelector *metav1.LabelSelector) *v1alpha1.DatasetBinding {
	return &v1alpha1.DatasetBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1alpha1.DatasetBindingSpec{
			NodeSelector:    nodeSelector,
			DatasetSelector: datasetSelector,
		},
	}
}

func TestNodeAssignment_ActivePushesAssignmentsToAllNodes(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}
	c := newFakeClient(t, ds, v, newNode("node-a", nil), newNode("node-b", nil))

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
	c := newFakeClient(t, ds, v,
		newNode("node-a", nil), newNode("node-b", nil),
		newNode("node-c", nil), newNode("node-d", nil),
	)

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
	c := newFakeClient(t, ds, v, newNode("node-a", nil))

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
	c := newFakeClient(t, ds, v, newNode("node-a", nil))

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
	c := newFakeClient(t, ds, v, newNode("node-a", nil))

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

// ---------------------------------------------------------------------------
// DatasetBinding tests
// ---------------------------------------------------------------------------

func TestNodeAssignment_NoBindingsDefaultAll(t *testing.T) {
	ds := newDataset("users")
	ds.Labels = map[string]string{"domain": "ml"}
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}
	c := newFakeClient(t, ds, v,
		newNode("node-a", map[string]string{"role": "gpu"}),
	)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("node-a")
	r := newNAR(c, br)

	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}

	resp, _ := br.GetAssignments("node-a", 0)
	if len(resp.Assignments) != 1 {
		t.Fatalf("no bindings: got %d assignments, want 1", len(resp.Assignments))
	}
}

func TestNodeAssignment_BindingFiltersDatasets(t *testing.T) {
	// Two datasets: "users" (domain=ml) and "pricing" (domain=finance).
	dsUsers := newDataset("users")
	dsUsers.Labels = map[string]string{"domain": "ml"}
	vUsers := newDatasetVersion("users", "v1")
	vUsers.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}

	dsPricing := newDataset("pricing")
	dsPricing.Labels = map[string]string{"domain": "finance"}
	vPricing := newDatasetVersion("pricing", "v1")
	vPricing.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-pricing-v1"}

	// Binding: GPU nodes get only ML datasets.
	binding := newBinding("gpu-ml", &metav1.LabelSelector{
		MatchLabels: map[string]string{"role": "gpu"},
	}, &metav1.LabelSelector{
		MatchLabels: map[string]string{"domain": "ml"},
	})

	gpuNode := newNode("gpu-node", map[string]string{"role": "gpu"})
	cpuNode := newNode("cpu-node", map[string]string{"role": "cpu"})

	c := newFakeClient(t, dsUsers, vUsers, dsPricing, vPricing, binding, gpuNode, cpuNode)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("gpu-node")
	br.RegisterNode("cpu-node")
	r := newNAR(c, br)

	if _, err := r.Reconcile(context.Background(), reqFor(vUsers)); err != nil {
		t.Fatal(err)
	}

	// GPU node: binding matches → only ML datasets.
	resp, _ := br.GetAssignments("gpu-node", 0)
	if len(resp.Assignments) != 1 || resp.Assignments[0].Dataset != "users" {
		t.Fatalf("gpu-node: got %+v, want [users]", resp.Assignments)
	}

	// CPU node: no binding matches → open-world default, gets all datasets.
	resp, _ = br.GetAssignments("cpu-node", 0)
	if len(resp.Assignments) != 2 {
		t.Fatalf("cpu-node: got %d assignments, want 2 (open-world default)", len(resp.Assignments))
	}
}

func TestNodeAssignment_MultipleBindingsUnion(t *testing.T) {
	dsUsers := newDataset("users")
	dsUsers.Labels = map[string]string{"domain": "ml"}
	vUsers := newDatasetVersion("users", "v1")
	vUsers.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}

	dsPricing := newDataset("pricing")
	dsPricing.Labels = map[string]string{"domain": "finance"}
	vPricing := newDatasetVersion("pricing", "v1")
	vPricing.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-pricing-v1"}

	dsLogs := newDataset("logs")
	dsLogs.Labels = map[string]string{"domain": "ops"}
	vLogs := newDatasetVersion("logs", "v1")
	vLogs.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-logs-v1"}

	// Two bindings both match the same node; their datasets should union.
	b1 := newBinding("b1", &metav1.LabelSelector{
		MatchLabels: map[string]string{"role": "serving"},
	}, &metav1.LabelSelector{
		MatchLabels: map[string]string{"domain": "ml"},
	})
	b2 := newBinding("b2", &metav1.LabelSelector{
		MatchLabels: map[string]string{"role": "serving"},
	}, &metav1.LabelSelector{
		MatchLabels: map[string]string{"domain": "finance"},
	})

	node := newNode("serving-node", map[string]string{"role": "serving"})
	c := newFakeClient(t, dsUsers, vUsers, dsPricing, vPricing, dsLogs, vLogs, b1, b2, node)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("serving-node")
	r := newNAR(c, br)

	if _, err := r.Reconcile(context.Background(), reqFor(vUsers)); err != nil {
		t.Fatal(err)
	}

	// Union of b1 (ml) + b2 (finance) = users + pricing. Logs excluded.
	resp, _ := br.GetAssignments("serving-node", 0)
	if len(resp.Assignments) != 2 {
		t.Fatalf("serving-node: got %d assignments, want 2", len(resp.Assignments))
	}
	datasets := map[string]bool{}
	for _, a := range resp.Assignments {
		datasets[a.Dataset] = true
	}
	if !datasets["users"] || !datasets["pricing"] || datasets["logs"] {
		t.Fatalf("serving-node: got datasets %v, want {users, pricing}", datasets)
	}
}

func TestNodeAssignment_BindingNilSelectorsMatchAll(t *testing.T) {
	ds := newDataset("users")
	ds.Labels = map[string]string{"domain": "ml"}
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}

	// Binding with nil selectors → matches all nodes AND all datasets.
	binding := newBinding("catch-all", nil, nil)
	node := newNode("node-a", map[string]string{"role": "gpu"})
	c := newFakeClient(t, ds, v, binding, node)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("node-a")
	r := newNAR(c, br)

	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}

	resp, _ := br.GetAssignments("node-a", 0)
	if len(resp.Assignments) != 1 {
		t.Fatalf("catch-all binding: got %d, want 1", len(resp.Assignments))
	}
}

func TestFilterAssignmentsForNode_Unit(t *testing.T) {
	ml := datasetAssignment{
		NodeAssignment: api.NodeAssignment{Dataset: "ml-data", KeyPrefix: "ml"},
		DatasetLabels:  map[string]string{"domain": "ml"},
	}
	fin := datasetAssignment{
		NodeAssignment: api.NodeAssignment{Dataset: "fin-data", KeyPrefix: "fin"},
		DatasetLabels:  map[string]string{"domain": "finance"},
	}
	all := []datasetAssignment{ml, fin}

	t.Run("no bindings returns all", func(t *testing.T) {
		got := filterAssignmentsForNode(map[string]string{"role": "gpu"}, all, nil)
		if len(got) != 2 {
			t.Fatalf("got %d, want 2", len(got))
		}
	})

	t.Run("non-matching binding returns all", func(t *testing.T) {
		b := v1alpha1.DatasetBinding{Spec: v1alpha1.DatasetBindingSpec{
			NodeSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "other"}},
		}}
		got := filterAssignmentsForNode(map[string]string{"role": "gpu"}, all, []v1alpha1.DatasetBinding{b})
		if len(got) != 2 {
			t.Fatalf("got %d, want 2 (open-world)", len(got))
		}
	})

	t.Run("matching binding filters datasets", func(t *testing.T) {
		b := v1alpha1.DatasetBinding{Spec: v1alpha1.DatasetBindingSpec{
			NodeSelector:    &metav1.LabelSelector{MatchLabels: map[string]string{"role": "gpu"}},
			DatasetSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"domain": "ml"}},
		}}
		got := filterAssignmentsForNode(map[string]string{"role": "gpu"}, all, []v1alpha1.DatasetBinding{b})
		if len(got) != 1 || got[0].Dataset != "ml-data" {
			t.Fatalf("got %+v, want [ml-data]", got)
		}
	})

	t.Run("matching binding with no dataset match returns empty", func(t *testing.T) {
		b := v1alpha1.DatasetBinding{Spec: v1alpha1.DatasetBindingSpec{
			NodeSelector:    &metav1.LabelSelector{MatchLabels: map[string]string{"role": "gpu"}},
			DatasetSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"domain": "ops"}},
		}}
		got := filterAssignmentsForNode(map[string]string{"role": "gpu"}, all, []v1alpha1.DatasetBinding{b})
		if len(got) != 0 {
			t.Fatalf("got %d, want 0", len(got))
		}
	})
}
