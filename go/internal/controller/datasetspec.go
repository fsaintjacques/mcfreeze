package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"k8s.io/utils/ptr"
)

// DatasetReconciler aggregates child DatasetVersion status into the parent
// Dataset's status, enforces spec.retention by deleting the oldest retired
// versions in excess, and keeps the CronRunnable's in-memory schedule in
// lockstep with the CR's spec.trigger.cron.
type DatasetReconciler struct {
	client.Client

	// Cron is the leader-elected runnable that owns the cron entry table.
	// May be nil in tests that exercise retention/aggregation in isolation.
	Cron *CronRunnable
}

// Reconcile implements the loop described above.
func (r *DatasetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ds v1alpha1.Dataset
	if err := r.Get(ctx, req.NamespacedName, &ds); err != nil {
		if apierrors.IsNotFound(err) {
			if r.Cron != nil {
				r.Cron.Forget(req.Namespace, req.Name)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Sync cron entry first so a freshly-applied trigger fires on schedule
	// even if the rest of the reconcile errors.
	if r.Cron != nil {
		if err := r.Cron.Sync(ctx, &ds); err != nil {
			return ctrl.Result{}, err
		}
	}

	var versions v1alpha1.DatasetVersionList
	if err := r.List(ctx, &versions,
		client.InNamespace(ds.Namespace),
		client.MatchingLabels{v1alpha1.DatasetLabel: ds.Name},
	); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.ensureVersion(ctx, &ds, versions.Items); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.enforceRetention(ctx, &ds, versions.Items); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchAggregateStatus(ctx, &ds, versions.Items); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// ensureVersion creates a new DatasetVersion if no version is currently
// building or active. This guarantees that applying a Dataset CR always
// kicks off a build without requiring a separate API call.
func (r *DatasetReconciler) ensureVersion(ctx context.Context, ds *v1alpha1.Dataset, versions []v1alpha1.DatasetVersion) error {
	for i := range versions {
		switch versions[i].Status.State {
		case string(api.StateBuilding), string(api.StateActive), string(api.StateReady), string(api.StateFailed):
			return nil
		}
	}

	versionID := fmt.Sprintf("v%d", r.nextVersion(versions))
	v := &v1alpha1.DatasetVersion{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ds.Namespace,
			Name:      v1alpha1.VersionCRName(ds.Name, versionID),
			Labels:    map[string]string{v1alpha1.DatasetLabel: ds.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         v1alpha1.GroupVersion.String(),
				Kind:               "Dataset",
				Name:               ds.Name,
				UID:                ds.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec: v1alpha1.DatasetVersionSpec{
			Dataset:    ds.Name,
			VersionID:  versionID,
			ShardCount: ds.Spec.ShardCount,
		},
	}
	if err := r.Create(ctx, v); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("auto-create DatasetVersion for %q: %w", ds.Name, err)
	}
	slog.Info("auto-created DatasetVersion", "dataset", ds.Name, "version", versionID)
	return nil
}

// nextVersion returns the next version number by scanning existing versions
// for the highest "vN" suffix and incrementing. Returns 1 if no versions exist.
func (r *DatasetReconciler) nextVersion(versions []v1alpha1.DatasetVersion) int {
	max := 0
	for i := range versions {
		var n int
		if _, err := fmt.Sscanf(versions[i].Spec.VersionID, "v%d", &n); err == nil && n > max {
			max = n
		}
	}
	return max + 1
}

// enforceRetention deletes the oldest retired versions in excess of
// spec.retention. Versions in other states are untouched.
func (r *DatasetReconciler) enforceRetention(ctx context.Context, ds *v1alpha1.Dataset, versions []v1alpha1.DatasetVersion) error {
	keep := ds.Spec.Retention
	if keep <= 0 {
		return nil
	}

	var retired []v1alpha1.DatasetVersion
	for i := range versions {
		if versions[i].Status.State == string(api.StateRetired) {
			retired = append(retired, versions[i])
		}
	}
	if len(retired) <= keep {
		return nil
	}

	sort.Slice(retired, func(i, j int) bool {
		return retired[i].CreationTimestamp.Before(&retired[j].CreationTimestamp)
	})

	excess := len(retired) - keep
	for i := 0; i < excess; i++ {
		v := retired[i]
		if err := r.Delete(ctx, &v); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// patchAggregateStatus updates Dataset.status.activeVersion to point at the
// current active child, if any. No-op if unchanged.
func (r *DatasetReconciler) patchAggregateStatus(ctx context.Context, ds *v1alpha1.Dataset, versions []v1alpha1.DatasetVersion) error {
	active := ""
	for i := range versions {
		if versions[i].Status.State == string(api.StateActive) {
			active = versions[i].Spec.VersionID
			break
		}
	}
	if ds.Status.ActiveVersion == active {
		return nil
	}

	patch := client.MergeFrom(ds.DeepCopy())
	ds.Status.ActiveVersion = active
	return r.Status().Patch(ctx, ds, patch)
}

// SetupWithManager registers the reconciler with a controller-runtime manager.
func (r *DatasetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Dataset{}).
		Named("dataset").
		Complete(r)
}
