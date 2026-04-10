package controller

import (
	"context"
	"testing"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func dsReq(ds *v1alpha1.Dataset) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ds)}
}

func TestDatasetReconciler_AggregatesActiveVersion(t *testing.T) {
	ds := newDataset("users")
	v1 := newDatasetVersion("users", "v1")
	v1.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateRetired)}
	v2 := newDatasetVersion("users", "v2")
	v2.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive)}
	c := newFakeClient(t, ds, v1, v2)

	r := &DatasetReconciler{Client: c}
	if _, err := r.Reconcile(context.Background(), dsReq(ds)); err != nil {
		t.Fatal(err)
	}

	got := &v1alpha1.Dataset{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ds), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ActiveVersion != "v2" {
		t.Fatalf("activeVersion = %q, want v2", got.Status.ActiveVersion)
	}
}

func TestDatasetReconciler_RetentionDeletesOldestRetired(t *testing.T) {
	ds := newDataset("users")
	ds.Spec.Retention = 2

	now := time.Now()
	mk := func(id string, ageMin int) *v1alpha1.DatasetVersion {
		v := newDatasetVersion("users", id)
		v.CreationTimestamp = metav1.NewTime(now.Add(-time.Duration(ageMin) * time.Minute))
		v.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateRetired)}
		return v
	}
	// 5 retired versions; oldest 3 should be deleted, keeping 2 newest.
	v1 := mk("v1", 50)
	v2 := mk("v2", 40)
	v3 := mk("v3", 30)
	v4 := mk("v4", 20)
	v5 := mk("v5", 10)

	c := newFakeClient(t, ds, v1, v2, v3, v4, v5)
	r := &DatasetReconciler{Client: c}
	if _, err := r.Reconcile(context.Background(), dsReq(ds)); err != nil {
		t.Fatal(err)
	}

	var list v1alpha1.DatasetVersionList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, v := range list.Items {
		got[v.Spec.VersionID] = true
	}
	if got["v1"] || got["v2"] || got["v3"] {
		t.Fatalf("expected v1,v2,v3 deleted; got %v", got)
	}
	if !got["v4"] || !got["v5"] {
		t.Fatalf("expected v4,v5 retained; got %v", got)
	}
}

func TestDatasetReconciler_EnsureVersionCreatesWhenNoneExist(t *testing.T) {
	ds := newDataset("users")
	c := newFakeClient(t, ds)
	r := &DatasetReconciler{Client: c}

	if _, err := r.Reconcile(context.Background(), dsReq(ds)); err != nil {
		t.Fatal(err)
	}

	var list v1alpha1.DatasetVersionList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 auto-created version, got %d", len(list.Items))
	}
	if list.Items[0].Spec.VersionID != "v1" {
		t.Fatalf("versionID = %q, want v1", list.Items[0].Spec.VersionID)
	}
}

func TestDatasetReconciler_EnsureVersionSkipsWhenFailedExists(t *testing.T) {
	ds := newDataset("users")
	v1 := newDatasetVersion("users", "v1")
	v1.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateFailed)}
	c := newFakeClient(t, ds, v1)
	r := &DatasetReconciler{Client: c}

	if _, err := r.Reconcile(context.Background(), dsReq(ds)); err != nil {
		t.Fatal(err)
	}

	var list v1alpha1.DatasetVersionList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected no new version created (failed blocks auto-create), got %d versions", len(list.Items))
	}
}

func TestDatasetReconciler_RetentionLeavesNonRetiredAlone(t *testing.T) {
	ds := newDataset("users")
	ds.Spec.Retention = 1
	v1 := newDatasetVersion("users", "v1")
	v1.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateActive)}
	v2 := newDatasetVersion("users", "v2")
	v2.Status = v1alpha1.DatasetVersionStatus{State: string(api.StateBuilding)}
	c := newFakeClient(t, ds, v1, v2)

	r := &DatasetReconciler{Client: c}
	if _, err := r.Reconcile(context.Background(), dsReq(ds)); err != nil {
		t.Fatal(err)
	}

	var list v1alpha1.DatasetVersionList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 versions retained, got %d", len(list.Items))
	}
}
