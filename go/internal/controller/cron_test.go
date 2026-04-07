package controller

import (
	"context"
	"testing"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newCron(c client.Client) *CronRunnable {
	return &CronRunnable{
		Client:      c,
		Namespace:   "default",
		VersionIDFn: func(t time.Time) string { return "v-" + t.UTC().Format("150405") },
	}
}

// trigger calls the cron entry's func directly, simulating a tick without
// waiting for wall-clock time.
func (c *CronRunnable) triggerForTest(ds *v1alpha1.Dataset) {
	c.fire(context.Background(), ds)
}

func TestCron_SyncAddRemove(t *testing.T) {
	ds := newDataset("users")
	c := newFakeClient(t, ds)
	cr := newCron(c)

	// Initially no trigger.
	if err := cr.Sync(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
	if len(cr.entries) != 0 {
		t.Fatalf("entries=%d, want 0", len(cr.entries))
	}

	// Add a cron trigger.
	ds.Spec.Trigger = &v1alpha1.TriggerSpec{Cron: &v1alpha1.CronTrigger{Schedule: "*/5 * * * *"}}
	if err := cr.Sync(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
	if len(cr.entries) != 1 {
		t.Fatalf("entries=%d, want 1", len(cr.entries))
	}

	// Same schedule: no churn (stable EntryID).
	id1 := cr.entries[clientKey(ds)].id
	if err := cr.Sync(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
	if cr.entries[clientKey(ds)].id != id1 {
		t.Fatal("identical Sync replaced the cron entry")
	}

	// Change schedule: entry replaced.
	ds.Spec.Trigger.Cron.Schedule = "0 * * * *"
	if err := cr.Sync(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
	if cr.entries[clientKey(ds)].id == id1 {
		t.Fatal("changed schedule did not replace entry")
	}

	// Remove trigger: entry dropped.
	ds.Spec.Trigger = nil
	if err := cr.Sync(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
	if len(cr.entries) != 0 {
		t.Fatalf("entries=%d, want 0", len(cr.entries))
	}
}

func TestCron_FireCreatesVersion(t *testing.T) {
	ds := newDataset("users")
	c := newFakeClient(t, ds)
	cr := newCron(c)

	cr.triggerForTest(ds)

	var list v1alpha1.DatasetVersionList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 version created, got %d", len(list.Items))
	}
	if list.Items[0].Spec.Dataset != "users" || list.Items[0].Spec.ShardCount != 4 {
		t.Fatalf("created CR: %+v", list.Items[0].Spec)
	}
}

func TestCron_FireSkipsWhenBuilding(t *testing.T) {
	ds := newDataset("users")
	existing := newDatasetVersion("users", "old")
	existing.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateBuilding)}
	c := newFakeClient(t, ds, existing)
	cr := newCron(c)

	cr.triggerForTest(ds)

	var list v1alpha1.DatasetVersionList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected no new version (build in flight); got %d", len(list.Items))
	}
}

func TestCron_InvalidScheduleReturnsError(t *testing.T) {
	ds := newDataset("users")
	ds.Spec.Trigger = &v1alpha1.TriggerSpec{Cron: &v1alpha1.CronTrigger{Schedule: "not a real schedule"}}
	c := newFakeClient(t, ds)
	cr := newCron(c)

	if err := cr.Sync(context.Background(), ds); err == nil {
		t.Fatal("expected error for invalid schedule")
	}
}

func clientKey(ds *v1alpha1.Dataset) types.NamespacedName {
	return types.NamespacedName{Namespace: ds.Namespace, Name: ds.Name}
}
