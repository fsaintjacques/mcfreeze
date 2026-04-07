package controlplane

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
)

const testNamespace = "default"

// newTestCRDStore returns a CRDStore wired to fake dynamic and kubernetes
// clients. The dynamic client uses a bare scheme (no v1alpha1 types
// registered) so it stores/returns *unstructured.Unstructured directly.
// Registering typed kinds would make the fake client try to round-trip
// items through the typed objects, which fails on a strict-but-unrelated
// converter check inside the fake tracker.
func newTestCRDStore(t *testing.T) *CRDStore {
	t.Helper()
	scheme := runtime.NewScheme()
	gvrToListKind := map[schema.GroupVersionResource]string{
		datasetGVR:        "DatasetList",
		datasetVersionGVR: "DatasetVersionList",
	}
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind)
	kube := kubefake.NewSimpleClientset()
	return NewCRDStore(dyn, kube, testNamespace)
}

func TestCRDStore_RegisterAndGetDataset(t *testing.T) {
	s := newTestCRDStore(t)
	spec := api.DatasetSpec{
		Name:       "users",
		KeyPrefix:  "users",
		ShardCount: 4,
		Retention:  2,
		Source: api.SourceSpec{
			KeyColumn:   "k",
			ValueColumn: "v",
			CSV:         &api.CsvSource{Data: "k,v\na,1"},
		},
	}
	s.RegisterDataset(spec)

	got, ok := s.GetDatasetSpec("users")
	if !ok {
		t.Fatal("dataset not found after RegisterDataset")
	}
	if got.Name != "users" || got.KeyPrefix != "users" || got.ShardCount != 4 || got.Retention != 2 {
		t.Errorf("got = %+v, want spec round-trip", got)
	}
	if got.Source.CSV == nil || got.Source.CSV.Data != "k,v\na,1" {
		t.Errorf("CSV source not preserved: %+v", got.Source.CSV)
	}
}

func TestCRDStore_RegisterDataset_Idempotent(t *testing.T) {
	s := newTestCRDStore(t)
	spec := api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 1, Retention: 1}
	s.RegisterDataset(spec)
	spec.ShardCount = 8 // update
	s.RegisterDataset(spec)
	got, ok := s.GetDatasetSpec("ds")
	if !ok {
		t.Fatal("dataset not found")
	}
	if got.ShardCount != 8 {
		t.Errorf("ShardCount = %d, want 8 (update should overwrite)", got.ShardCount)
	}
}

func TestCRDStore_VersionLifecycle(t *testing.T) {
	s := newTestCRDStore(t)
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 2, Retention: 2})

	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}

	building := s.GetBuildingVersions()
	if len(building) != 1 || building[0].ID != "v1" || building[0].State != api.StateBuilding {
		t.Fatalf("building = %+v", building)
	}

	if err := s.SetBuildHandle("ds", "v1", "fm-build-ds-v1"); err != nil {
		t.Fatal(err)
	}
	versions := s.GetVersions("ds")
	if len(versions) != 1 || string(versions[0].BuildHandle) != "fm-build-ds-v1" {
		t.Fatalf("BuildHandle not persisted: %+v", versions)
	}

	if err := s.MarkReady("ds", "v1", "/snap", "pv-1"); err != nil {
		t.Fatal(err)
	}
	versions = s.GetVersions("ds")
	if versions[0].State != api.StateReady || versions[0].PVName != "pv-1" {
		t.Fatalf("after MarkReady: %+v", versions[0])
	}

	if err := s.Promote("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	active, ok := s.GetActiveVersion("ds")
	if !ok || active.ID != "v1" || active.State != api.StateActive {
		t.Fatalf("active = %+v, ok=%v", active, ok)
	}
}

func TestCRDStore_DuplicateBuildingRejected(t *testing.T) {
	s := newTestCRDStore(t)
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 1})
	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateVersion("ds", "v2"); err == nil {
		t.Fatal("expected error for duplicate building version")
	}
}

func TestCRDStore_PromoteRetiresOldActive(t *testing.T) {
	s := newTestCRDStore(t)
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 1})
	s.RegisterNode("node-1")

	// v1 → active
	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady("ds", "v1", "/s", "pv-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Promote("ds", "v1"); err != nil {
		t.Fatal(err)
	}

	// v2 → active, v1 → retired
	if err := s.CreateVersion("ds", "v2"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady("ds", "v2", "/s", "pv-2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Promote("ds", "v2"); err != nil {
		t.Fatal(err)
	}

	versions := s.GetVersions("ds")
	if len(versions) != 2 {
		t.Fatalf("versions = %d, want 2", len(versions))
	}
	states := map[string]api.VersionState{}
	for _, v := range versions {
		states[v.ID] = v.State
	}
	if states["v1"] != api.StateRetired {
		t.Errorf("v1 = %q, want retired", states["v1"])
	}
	if states["v2"] != api.StateActive {
		t.Errorf("v2 = %q, want active", states["v2"])
	}

	// Promote should have pushed an assignment to node-1.
	resp, _ := s.GetAssignments("node-1", 0)
	if len(resp.Assignments) != 1 || resp.Assignments[0].Version.ID != "v2" {
		t.Errorf("assignments = %+v, want one assignment for v2", resp.Assignments)
	}
}

func TestCRDStore_MarkFailed(t *testing.T) {
	s := newTestCRDStore(t)
	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkFailed("ds", "v1", "boom"); err != nil {
		t.Fatal(err)
	}
	versions := s.GetVersions("ds")
	if len(versions) != 1 || versions[0].State != api.StateFailed {
		t.Fatalf("expected failed, got %+v", versions)
	}
	// CreateVersion should succeed after a previous failure.
	if err := s.CreateVersion("ds", "v2"); err != nil {
		t.Fatalf("create after failure: %v", err)
	}
}

func TestCRDStore_DeleteVersion(t *testing.T) {
	s := newTestCRDStore(t)
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 1})

	// v1 active → retired by promoting v2
	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	s.MarkReady("ds", "v1", "/s", "pv-1")
	s.Promote("ds", "v1")
	s.CreateVersion("ds", "v2")
	s.MarkReady("ds", "v2", "/s", "pv-2")
	s.Promote("ds", "v2")

	// Cannot delete active.
	if err := s.DeleteVersion("ds", "v2"); err == nil {
		t.Error("expected error deleting active version")
	}
	// Can delete retired.
	if err := s.DeleteVersion("ds", "v1"); err != nil {
		t.Fatalf("delete retired: %v", err)
	}
	versions := s.GetVersions("ds")
	if len(versions) != 1 || versions[0].ID != "v2" {
		t.Fatalf("expected only v2, got %+v", versions)
	}
}

func TestCRDStore_SetDescriptor(t *testing.T) {
	s := newTestCRDStore(t)
	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDescriptor("ds", "v1", "AQID", "pkg.Msg"); err != nil {
		t.Fatal(err)
	}
	versions := s.GetVersions("ds")
	if versions[0].Descriptor != "AQID" || versions[0].MessageName != "pkg.Msg" {
		t.Errorf("descriptor not persisted: %+v", versions[0])
	}
}

func TestCRDStore_CreateVersion_SetsOwnerRef(t *testing.T) {
	s := newTestCRDStore(t)
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 1})
	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	// Read the raw CR to verify ownerReferences point at the Dataset.
	u, err := s.dyn.Resource(datasetVersionGVR).Namespace(testNamespace).Get(
		context.Background(), v1alpha1.VersionCRName("ds", "v1"), metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	owners := u.GetOwnerReferences()
	if len(owners) != 1 {
		t.Fatalf("ownerReferences = %d, want 1", len(owners))
	}
	if owners[0].Kind != "Dataset" || owners[0].Name != "ds" {
		t.Errorf("owner = %+v, want Dataset/ds", owners[0])
	}
}

func TestCRDStore_RolloutStatus(t *testing.T) {
	s := newTestCRDStore(t)
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 1})
	s.RegisterNode("node-1")
	s.RegisterNode("node-2")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/s", "pv-1")
	s.Promote("ds", "v1")

	// node-1 reports v1 active, node-2 has not reported.
	s.ReportState("node-1", api.NodeState{
		Node: "node-1",
		Datasets: []api.DatasetState{
			{Dataset: "ds", VersionID: "v1", Phase: api.PhaseActive},
		},
	})

	status := s.RolloutStatus("ds")
	if status.ActiveVersion != "v1" {
		t.Errorf("active = %q, want v1", status.ActiveVersion)
	}
	if len(status.ConvergedNodes) != 1 || status.ConvergedNodes[0] != "node-1" {
		t.Errorf("converged = %+v", status.ConvergedNodes)
	}
	if len(status.PendingNodes) != 1 || status.PendingNodes[0] != "node-2" {
		t.Errorf("pending = %+v", status.PendingNodes)
	}
}

func TestCRDStore_CheckRetirement(t *testing.T) {
	s := newTestCRDStore(t)
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 1})
	s.RegisterNode("node-1")

	s.CreateVersion("ds", "v1")
	s.MarkReady("ds", "v1", "/s", "pv-1")
	s.Promote("ds", "v1")

	// node-1 converged on v1.
	s.ReportState("node-1", api.NodeState{
		Node:     "node-1",
		Datasets: []api.DatasetState{{Dataset: "ds", VersionID: "v1", Phase: api.PhaseActive}},
	})

	// Promote v2 → v1 retired but node still on v1, so not eligible.
	s.CreateVersion("ds", "v2")
	s.MarkReady("ds", "v2", "/s", "pv-2")
	s.Promote("ds", "v2")

	if eligible := s.CheckRetirement("ds"); len(eligible) != 0 {
		t.Fatalf("expected 0 eligible (node still on v1), got %d", len(eligible))
	}

	// node-1 converges on v2 → v1 now eligible.
	s.ReportState("node-1", api.NodeState{
		Node:     "node-1",
		Datasets: []api.DatasetState{{Dataset: "ds", VersionID: "v2", Phase: api.PhaseActive}},
	})

	eligible := s.CheckRetirement("ds")
	if len(eligible) != 1 || eligible[0].ID != "v1" {
		t.Fatalf("expected v1 eligible, got %+v", eligible)
	}
}

func TestCRDStore_AssignmentLongPoll(t *testing.T) {
	s := newTestCRDStore(t)

	// First call with generation=0 should return a notify channel (no
	// assignments yet).
	resp, ch := s.GetAssignments("node-1", 0)
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
	if resp.Generation != 0 {
		t.Fatalf("generation = %d, want 0", resp.Generation)
	}

	// Set assignments → channel should close.
	s.SetAssignments("node-1", []api.NodeAssignment{{Dataset: "ds", KeyPrefix: "ds"}})
	select {
	case <-ch:
	default:
		t.Fatal("notify channel not closed after SetAssignments")
	}

	resp, ch = s.GetAssignments("node-1", 0)
	if ch != nil {
		t.Fatal("expected nil channel after generation advanced")
	}
	if resp.Generation != 1 || len(resp.Assignments) != 1 {
		t.Errorf("got = %+v", resp)
	}
}

// Sanity check: the fake dynamic client supports unstructured Patch.
func TestCRDStore_FakeDynamicClient_StatusPatch(t *testing.T) {
	s := newTestCRDStore(t)
	s.RegisterDataset(api.DatasetSpec{Name: "ds", KeyPrefix: "ds", ShardCount: 1})
	if err := s.CreateVersion("ds", "v1"); err != nil {
		t.Fatal(err)
	}
	// Verify the status subresource is reachable on the fake client and
	// that our patches are observable via Get → unstructured.
	u, err := s.dyn.Resource(datasetVersionGVR).Namespace(testNamespace).Get(
		context.Background(), v1alpha1.VersionCRName("ds", "v1"), metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if state, found, _ := unstructured.NestedString(u.Object, "status", "state"); !found || state != "building" {
		t.Errorf("status.state = %q (found=%v), want building", state, found)
	}
}
