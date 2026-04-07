package controller

import (
	"context"
	"sort"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

	if err := r.enforceRetention(ctx, &ds, versions.Items); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchAggregateStatus(ctx, &ds, versions.Items); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
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
