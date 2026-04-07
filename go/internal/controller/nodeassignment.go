package controller

import (
	"context"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeAssignmentReconciler keeps the AssignmentBroker in sync with the set of
// DatasetVersions in state=active and aggregates per-node convergence into
// each active version's status.rollout.
//
// It watches DatasetVersion (any state). On every reconcile it:
//
//  1. Lists every DatasetVersion in the requested namespace whose
//     status.state == active.
//  2. For each registered node in the broker, builds the desired
//     []NodeAssignment from those active versions joined with their parent
//     Dataset (for KeyPrefix), and calls Broker.SetAssignments — diff-on-set
//     in the broker means identical resyncs are no-ops, but real changes
//     wake any blocked long-poll.
//  3. If the requested version is itself active, snapshots node states from
//     the broker and patches its status.rollout.
//
// Push semantics on top of a level-triggered reconciler: the broker is the
// only state crossing the boundary between this reconciler and the HTTP
// long-poll handlers in controlplane.Server.
type NodeAssignmentReconciler struct {
	client.Client
	Broker *controlplane.AssignmentBroker
}

// Reconcile implements the loop described above.
func (r *NodeAssignmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var self v1alpha1.DatasetVersion
	if err := r.Get(ctx, req.NamespacedName, &self); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted: recompute assignments from whatever is left.
			return ctrl.Result{}, r.syncBroker(ctx, req.Namespace)
		}
		return ctrl.Result{}, err
	}

	if err := r.syncBroker(ctx, req.Namespace); err != nil {
		return ctrl.Result{}, err
	}

	// Only the active CR carries rollout status.
	if self.Status.State != string(api.StateActive) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.patchRollout(ctx, &self)
}

// syncBroker recomputes the desired assignments for every registered node and
// pushes them through the broker. It is safe to call repeatedly; identical
// pushes are no-ops thanks to the broker's diff-on-set.
func (r *NodeAssignmentReconciler) syncBroker(ctx context.Context, namespace string) error {
	if r.Broker == nil {
		return nil
	}

	var versions v1alpha1.DatasetVersionList
	if err := r.List(ctx, &versions, client.InNamespace(namespace)); err != nil {
		return err
	}

	// Resolve KeyPrefix per dataset (cache to avoid repeated Gets).
	keyPrefixes := map[string]string{}

	// Build the desired assignment slice once and push it to every node.
	// Multi-node support today is "every node serves every active dataset";
	// per-node sharding can replace this loop later.
	var desired []api.NodeAssignment
	for i := range versions.Items {
		v := &versions.Items[i]
		if v.Status.State != string(api.StateActive) {
			continue
		}

		prefix, ok := keyPrefixes[v.Spec.Dataset]
		if !ok {
			var ds v1alpha1.Dataset
			if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: v.Spec.Dataset}, &ds); err != nil {
				if apierrors.IsNotFound(err) {
					// Parent Dataset gone — drop this assignment.
					continue
				}
				return err
			}
			prefix = ds.Spec.KeyPrefix
			keyPrefixes[v.Spec.Dataset] = prefix
		}

		desired = append(desired, api.NodeAssignment{
			Dataset:   v.Spec.Dataset,
			KeyPrefix: prefix,
			Version: api.VersionRecord{
				ID:          v.Spec.VersionID,
				Dataset:     v.Spec.Dataset,
				PVName:      v.Status.PVName,
				State:       api.StateActive,
				ShardCount:  v.Spec.ShardCount,
				CreatedAt:   v.CreationTimestamp.Time,
				Descriptor:  v.Status.Descriptor,
				MessageName: v.Status.MessageName,
				DiskURL:     v.Status.DiskURL,
			},
		})
	}

	for _, node := range r.Broker.Nodes() {
		r.Broker.SetAssignments(node, desired)
	}
	return nil
}

// patchRollout aggregates broker node states into v.Status.Rollout and
// patches the status subresource.
func (r *NodeAssignmentReconciler) patchRollout(ctx context.Context, v *v1alpha1.DatasetVersion) error {
	nodes := r.Broker.Nodes()
	states := r.Broker.SnapshotNodeStates()

	rollout := &v1alpha1.RolloutStatus{TotalNodes: len(nodes)}
	for _, node := range nodes {
		ns, ok := states[node]
		if !ok {
			rollout.PendingNodes++
			continue
		}
		matched := false
		for _, ds := range ns.Datasets {
			if ds.Dataset != v.Spec.Dataset {
				continue
			}
			matched = true
			switch {
			case ds.Phase == api.PhaseError:
				rollout.ErrorNodes++
			case ds.Phase == api.PhaseActive && ds.VersionID == v.Spec.VersionID:
				rollout.ConvergedNodes++
			default:
				rollout.PendingNodes++
			}
			break
		}
		if !matched {
			rollout.PendingNodes++
		}
	}

	// No-op if rollout is unchanged — avoids a hot reconcile loop on the
	// status subresource (each Patch generates another watch event).
	if rolloutsEqual(v.Status.Rollout, rollout) {
		return nil
	}

	patch := client.MergeFrom(v.DeepCopy())
	v.Status.Rollout = rollout
	return r.Status().Patch(ctx, v, patch)
}

func rolloutsEqual(a, b *v1alpha1.RolloutStatus) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// SetupWithManager registers the reconciler with a controller-runtime manager.
func (r *NodeAssignmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DatasetVersion{}).
		Named("nodeassignment").
		Complete(r)
}
