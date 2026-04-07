package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/builder"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// stubBuilder is a fully synchronous in-memory builder.Async for tests.
// Each (handle) is mapped to a Status the test sets directly.
type stubBuilder struct {
	mu       sync.Mutex
	statuses map[builder.Handle]builder.Status
	started  map[string]builder.Handle
}

func newStubBuilder() *stubBuilder {
	return &stubBuilder{
		statuses: map[builder.Handle]builder.Status{},
		started:  map[string]builder.Handle{},
	}
}

func (b *stubBuilder) Start(_ context.Context, spec api.DatasetSpec, versionID string) (builder.Handle, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := spec.Name + "/" + versionID
	if h, ok := b.started[key]; ok {
		return h, nil
	}
	h := builder.Handle("job-" + key)
	b.started[key] = h
	b.statuses[h] = builder.Status{Phase: builder.Running}
	return h, nil
}

func (b *stubBuilder) Poll(_ context.Context, h builder.Handle) (builder.Status, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.statuses[h]
	if !ok {
		return builder.Status{Phase: builder.NotFound}, nil
	}
	return s, nil
}

func (b *stubBuilder) Cancel(_ context.Context, h builder.Handle) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.statuses, h)
	return nil
}

func (b *stubBuilder) setStatus(h builder.Handle, s builder.Status) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.statuses[h] = s
}

// stubVolume is a no-op volume.Manager for tests.
type stubVolume struct {
	deleted []string
}

func (v *stubVolume) CreateBuildPVC(_ context.Context, _, _ string, _ int64) error {
	return nil
}
func (v *stubVolume) FinalizeBuild(_ context.Context, _ string) (string, error) { return "", nil }
func (v *stubVolume) DeletePV(_ context.Context, pvName string) error {
	v.deleted = append(v.deleted, pvName)
	return nil
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.DatasetVersion{}, &v1alpha1.Dataset{}).
		Build()
}

func newDataset(name string) *v1alpha1.Dataset {
	return &v1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1alpha1.DatasetSpec{
			KeyPrefix:  name,
			ShardCount: 4,
			Retention:  2,
			Source:     v1alpha1.SourceSpec{KeyColumn: "key", ValueColumn: "value"},
		},
	}
}

func newDatasetVersion(dataset, vid string) *v1alpha1.DatasetVersion {
	return &v1alpha1.DatasetVersion{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      v1alpha1.VersionCRName(dataset, vid),
			Labels:    map[string]string{v1alpha1.DatasetLabel: dataset},
		},
		Spec: v1alpha1.DatasetVersionSpec{
			Dataset:    dataset,
			VersionID:  vid,
			ShardCount: 4,
		},
	}
}

func reqFor(v *v1alpha1.DatasetVersion) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(v)}
}

func reconcileN(t *testing.T, r *DatasetVersionReconciler, v *v1alpha1.DatasetVersion, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
}

func getVersion(t *testing.T, c client.Client, v *v1alpha1.DatasetVersion) *v1alpha1.DatasetVersion {
	t.Helper()
	out := &v1alpha1.DatasetVersion{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(v), out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestReconcile_BuildingStartsBuilder(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	c := newFakeClient(t, ds, v)
	b := newStubBuilder()
	r := &DatasetVersionReconciler{Client: c, Builder: b, Broker: controlplane.NewAssignmentBroker()}

	reconcileN(t, r, v, 1)

	got := getVersion(t, c, v)
	if got.Status.State != string(api.StateBuilding) {
		t.Fatalf("state = %q, want building", got.Status.State)
	}
	if got.Status.BuildJob == "" {
		t.Fatalf("BuildJob not set after Reconcile")
	}
}

func TestReconcile_BuildingPollsAndPromotes(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	c := newFakeClient(t, ds, v)
	b := newStubBuilder()
	r := &DatasetVersionReconciler{Client: c, Builder: b, Broker: controlplane.NewAssignmentBroker()}

	// 1: kick off build (status=building, BuildJob set, builder phase=Running).
	reconcileN(t, r, v, 1)

	// Make the builder report Complete with a synthetic PV name.
	b.setStatus(builder.Handle("job-users/v1"), builder.Status{
		Phase:  builder.Complete,
		Result: builder.Result{PVName: "pv-users-v1"},
	})

	// 2: poll → ready, requeue immediately.
	// 3: ready → active.
	reconcileN(t, r, v, 2)

	got := getVersion(t, c, v)
	if got.Status.State != string(api.StateActive) {
		t.Fatalf("state = %q, want active", got.Status.State)
	}
	if got.Status.PVName != "pv-users-v1" {
		t.Fatalf("PVName = %q", got.Status.PVName)
	}
}

func TestReconcile_BuildingFailedTransitionsToFailed(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	c := newFakeClient(t, ds, v)
	b := newStubBuilder()
	r := &DatasetVersionReconciler{Client: c, Builder: b, Broker: controlplane.NewAssignmentBroker()}

	reconcileN(t, r, v, 1)
	b.setStatus(builder.Handle("job-users/v1"), builder.Status{Phase: builder.Failed, Error: "boom"})
	reconcileN(t, r, v, 1)

	got := getVersion(t, c, v)
	if got.Status.State != string(api.StateFailed) {
		t.Fatalf("state = %q, want failed", got.Status.State)
	}
	if got.Status.Error != "boom" {
		t.Fatalf("error = %q", got.Status.Error)
	}
}

func TestReconcile_PromoteRetiresPreviousActive(t *testing.T) {
	ds := newDataset("users")
	v1 := newDatasetVersion("users", "v1")
	v1.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive), PVName: "pv-users-v1"}
	v2 := newDatasetVersion("users", "v2")
	v2.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateReady), PVName: "pv-users-v2"}

	c := newFakeClient(t, ds, v1, v2)
	r := &DatasetVersionReconciler{Client: c, Builder: newStubBuilder(), Broker: controlplane.NewAssignmentBroker()}

	if _, err := r.Reconcile(context.Background(), reqFor(v2)); err != nil {
		t.Fatal(err)
	}

	gotV1 := getVersion(t, c, v1)
	if gotV1.Status.State != string(api.StateRetired) {
		t.Fatalf("v1 state = %q, want retired", gotV1.Status.State)
	}
	gotV2 := getVersion(t, c, v2)
	if gotV2.Status.State != string(api.StateActive) {
		t.Fatalf("v2 state = %q, want active", gotV2.Status.State)
	}
}

func TestReconcile_RetiredDeletesPVAndCRWhenDrained(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateRetired), PVName: "pv-users-v1"}
	c := newFakeClient(t, ds, v)
	vol := &stubVolume{}
	br := controlplane.NewAssignmentBroker()
	// No nodes registered → IsDrained returns true (vacuously).
	r := &DatasetVersionReconciler{Client: c, Builder: newStubBuilder(), Volume: vol, Broker: br}

	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}

	if len(vol.deleted) != 1 || vol.deleted[0] != "pv-users-v1" {
		t.Fatalf("DeletePV not called: %v", vol.deleted)
	}
	out := &v1alpha1.DatasetVersion{}
	err := c.Get(context.Background(), client.ObjectKeyFromObject(v), out)
	if err == nil {
		t.Fatalf("CR still exists after retirement")
	}
}

func TestReconcile_RetiredRequeueWhenNotDrained(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateRetired), PVName: "pv-users-v1"}
	c := newFakeClient(t, ds, v)

	br := controlplane.NewAssignmentBroker()
	br.RegisterNode("node-a") // a registered node that has not yet reported state
	r := &DatasetVersionReconciler{Client: c, Builder: newStubBuilder(), Volume: &stubVolume{}, Broker: br}

	res, err := r.Reconcile(context.Background(), reqFor(v))
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("expected RequeueAfter > 0 when not drained")
	}
	// CR must still exist.
	out := getVersion(t, c, v)
	if out.Status.State != string(api.StateRetired) {
		t.Fatalf("state changed unexpectedly: %q", out.Status.State)
	}
}

func TestReconcile_BuildingTimeoutMarksFailed(t *testing.T) {
	ds := newDataset("users")
	v := newDatasetVersion("users", "v1")
	v.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	v.Status = v1alpha1.DatasetVersionStatus{
		State:    string(api.StateBuilding),
		BuildJob: "job-users/v1",
	}
	c := newFakeClient(t, ds, v)
	b := newStubBuilder()
	r := &DatasetVersionReconciler{
		Client:       c,
		Builder:      b,
		Broker:       controlplane.NewAssignmentBroker(),
		BuildTimeout: time.Hour,
	}

	if _, err := r.Reconcile(context.Background(), reqFor(v)); err != nil {
		t.Fatal(err)
	}

	got := getVersion(t, c, v)
	if got.Status.State != string(api.StateFailed) {
		t.Fatalf("state = %q, want failed", got.Status.State)
	}
	if got.Status.Error != "build timeout exceeded" {
		t.Fatalf("error = %q", got.Status.Error)
	}
}
