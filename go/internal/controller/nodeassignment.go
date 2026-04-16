package controller

import (
	"context"

	"github.com/fsaintjacques/mcfreeze/go/api"
	v1alpha1 "github.com/fsaintjacques/mcfreeze/go/api/v1alpha1"
	"github.com/fsaintjacques/mcfreeze/go/internal/controlplane"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// stateReportEventBuffer caps the in-memory queue of node-state-report
// signals between the HTTP /state handler and the reconciler. Reports drop
// when the buffer is full; the reconciler will still pick up the latest
// state on its next normal reconcile.
const stateReportEventBuffer = 64

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

	// Namespace scopes every List call made by this reconciler. It mirrors the
	// manager's namespace filter; OnStateReport uses it because its callback
	// signature does not carry a namespace from the HTTP handler.
	Namespace string

	// stateReportEvents is fed by OnStateReport (called from the HTTP /state
	// handler). source.Channel in SetupWithManager pumps events from it into
	// the controller work queue, so a node-agent state report wakes the
	// reconciler immediately rather than waiting for an unrelated watch event.
	stateReportEvents chan event.TypedGenericEvent[client.Object]
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

// OnStateReport is called by controlplane.Server.handlePostState after a
// node-agent reports its state into the broker. It enqueues a generic event
// for every active DatasetVersion in the namespace so the reconciler picks
// up the new state immediately. Drops events if the buffer is full —
// fallback consistency is provided by the next normal watch-driven reconcile.
func (r *NodeAssignmentReconciler) OnStateReport(ctx context.Context, _ string) {
	if r.stateReportEvents == nil {
		return
	}
	var list v1alpha1.DatasetVersionList
	if err := r.List(ctx, &list, client.InNamespace(r.Namespace)); err != nil {
		return
	}
	for i := range list.Items {
		v := &list.Items[i]
		if v.Status.State != string(api.StateActive) {
			continue
		}
		select {
		case r.stateReportEvents <- event.TypedGenericEvent[client.Object]{Object: v}:
		default:
			// Buffer full; the next watch event will catch up.
		}
	}
}

// datasetAssignment pairs a NodeAssignment with the parent Dataset's labels
// so that DatasetBinding selectors can be evaluated without re-fetching.
type datasetAssignment struct {
	api.NodeAssignment
	DatasetLabels map[string]string
}

// syncBroker recomputes the desired assignments for every registered node and
// pushes them through the broker. It is safe to call repeatedly; identical
// pushes are no-ops thanks to the broker's diff-on-set.
//
// When DatasetBindings exist, each node receives only the union of datasets
// selected by bindings whose nodeSelector matches the node. When no binding
// matches a node, it receives all datasets (open-world default).
func (r *NodeAssignmentReconciler) syncBroker(ctx context.Context, namespace string) error {
	if r.Broker == nil {
		return nil
	}

	var versions v1alpha1.DatasetVersionList
	if err := r.List(ctx, &versions, client.InNamespace(namespace)); err != nil {
		return err
	}

	// Cache parent Dataset lookups (KeyPrefix + labels).
	type datasetInfo struct {
		keyPrefix string
		labels    map[string]string
	}
	datasets := map[string]*datasetInfo{}

	var allAssignments []datasetAssignment
	for i := range versions.Items {
		v := &versions.Items[i]
		if v.Status.State != string(api.StateActive) {
			continue
		}

		info, ok := datasets[v.Spec.Dataset]
		if !ok {
			var ds v1alpha1.Dataset
			if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: v.Spec.Dataset}, &ds); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			info = &datasetInfo{keyPrefix: ds.Spec.KeyPrefix, labels: ds.Labels}
			datasets[v.Spec.Dataset] = info
		}

		descriptor, messageName, indexBytes := parseMetaFields(v.Status.Meta)

		allAssignments = append(allAssignments, datasetAssignment{
			NodeAssignment: api.NodeAssignment{
				Dataset:   v.Spec.Dataset,
				KeyPrefix: info.keyPrefix,
				Version: api.VersionRecord{
					ID:          v.Spec.VersionID,
					Dataset:     v.Spec.Dataset,
					PVName:      v.Status.PVName,
					State:       api.StateActive,
					ShardCount:  v.Spec.ShardCount,
					CreatedAt:   v.CreationTimestamp.Time,
					Descriptor:  descriptor,
					MessageName: messageName,
					DiskURL:     v.Status.DiskURL,
					IndexBytes:  indexBytes,
				},
			},
			DatasetLabels: info.labels,
		})
	}

	// List DatasetBindings.
	var bindings v1alpha1.DatasetBindingList
	if err := r.List(ctx, &bindings, client.InNamespace(namespace)); err != nil {
		return err
	}

	for _, nodeName := range r.Broker.Nodes() {
		// Read the K8s Node object to get its labels.
		var node corev1.Node
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
			if apierrors.IsNotFound(err) {
				// Node gone — push empty assignments.
				r.Broker.SetAssignments(nodeName, nil)
				continue
			}
			return err
		}

		desired := filterAssignmentsForNode(node.Labels, allAssignments, bindings.Items)
		r.Broker.SetAssignments(nodeName, desired)
	}
	return nil
}

// filterAssignmentsForNode returns the assignments a node should receive based
// on DatasetBindings. If no binding's nodeSelector matches the node, all
// assignments are returned (open-world default). Otherwise, the union of
// datasets selected by all matching bindings is returned.
func filterAssignmentsForNode(
	nodeLabels map[string]string,
	all []datasetAssignment,
	bindings []v1alpha1.DatasetBinding,
) []api.NodeAssignment {
	// Collect bindings whose nodeSelector matches this node.
	var matchingBindings []v1alpha1.DatasetBinding
	for _, b := range bindings {
		if selectorMatchesLabels(b.Spec.NodeSelector, nodeLabels) {
			matchingBindings = append(matchingBindings, b)
		}
	}

	// Open-world default: no matching bindings → all datasets.
	if len(matchingBindings) == 0 {
		out := make([]api.NodeAssignment, len(all))
		for i := range all {
			out[i] = all[i].NodeAssignment
		}
		return out
	}

	// Union: a dataset is included if any matching binding's datasetSelector matches it.
	var result []api.NodeAssignment
	for i := range all {
		for _, b := range matchingBindings {
			if selectorMatchesLabels(b.Spec.DatasetSelector, all[i].DatasetLabels) {
				result = append(result, all[i].NodeAssignment)
				break
			}
		}
	}
	return result
}

// selectorMatchesLabels returns true if the given LabelSelector matches the
// label set. A nil or empty selector matches everything. Malformed selectors
// are logged and treated as non-matching.
func selectorMatchesLabels(sel *metav1.LabelSelector, lbls map[string]string) bool {
	if sel == nil {
		return true
	}
	s, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		log.Log.Error(err, "malformed LabelSelector, treating as non-matching", "selector", sel)
		return false
	}
	return s.Matches(labels.Set(lbls))
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
// It also creates the state-report event channel that OnStateReport pushes
// to and binds it to the controller via source.Channel.
func (r *NodeAssignmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.stateReportEvents = make(chan event.TypedGenericEvent[client.Object], stateReportEventBuffer)
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DatasetVersion{}).
		// Re-sync assignments when DatasetBindings or Node labels change.
		Watches(&v1alpha1.DatasetBinding{}, handler.EnqueueRequestsFromMapFunc(r.enqueueActiveVersions)).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.enqueueActiveVersions),
			builder.WithPredicates(predicate.LabelChangedPredicate{})).
		Named("nodeassignment").
		WatchesRawSource(source.Channel(r.stateReportEvents, &handler.EnqueueRequestForObject{})).
		Complete(r)
}

// enqueueActiveVersions returns reconcile requests for every active
// DatasetVersion in the namespace. Used by the DatasetBinding and Node watches
// to trigger a full re-sync when binding or node labels change.
func (r *NodeAssignmentReconciler) enqueueActiveVersions(ctx context.Context, _ client.Object) []ctrl.Request {
	var list v1alpha1.DatasetVersionList
	if err := r.List(ctx, &list, client.InNamespace(r.Namespace)); err != nil {
		log.FromContext(ctx).Error(err, "listing active DatasetVersions for re-sync")
		return nil
	}
	var reqs []ctrl.Request
	for i := range list.Items {
		if list.Items[i].Status.State == string(api.StateActive) {
			reqs = append(reqs, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}
	}
	return reqs
}
