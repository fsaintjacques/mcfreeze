package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/builder"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/volume"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// DatasetVersion state machine driven by DatasetVersionReconciler.Reconcile.
//
// Each box is a value of DatasetVersion.Status.State. Edges are labeled with
// the trigger that causes the transition; "rq" notes how Reconcile asks to
// be re-invoked after handling the state.
//
//	                         (CR created, Status.State == "")
//	                                       │
//	                                       ▼
//	                          ┌──────────────────────┐
//	          Builder.Start───│      building        │◀──┐ Builder.Poll == Running
//	          (first call)    │  BuildJob persisted  │   │   rq: 5s
//	                          └──────────┬───────────┘───┘
//	                                     │
//	             ┌───────────────────────┼────────────────────────┐
//	             │ Poll == Complete      │ Poll == Failed         │ timeout
//	             │ (symlink + descriptor)│   or NotFound          │ exceeded
//	             ▼                       ▼                        ▼
//	   ┌───────────────────┐   ┌───────────────────┐    ┌───────────────────┐
//	   │       ready       │   │      failed       │    │      failed       │
//	   │ rq: immediate     │   │ (terminal no-op)  │    │ (terminal no-op)  │
//	   └────────┬──────────┘   └───────────────────┘    └───────────────────┘
//	            │
//	            │ list siblings, demote any active → retired,
//	            │ promote self
//	            ▼
//	   ┌───────────────────┐
//	   │      active       │   no-op (NodeAssignmentReconciler owns rollout;
//	   │ (no requeue)      │   transition to retired is driven by another
//	   └────────┬──────────┘   sibling promoting in reconcileReady)
//	            │
//	            │ another version becomes active → demoted here
//	            ▼
//	   ┌───────────────────┐
//	   │     retired       │──── VolumeAttachment exists ────┐
//	   │ rq: 30s safety    │     for status.pvName            │
//	   │   net; watch on   │◀─────────────────────────────────┘
//	   │   VA delete fires │
//	   └────────┬──────────┘
//	            │ no more VolumeAttachments for this PV
//	            ▼
//	   Volume.DeletePV  +  client.Delete(CR)   ── end of life ──
//
// Reconcile is called by the manager work queue on: (1) watch events from
// the API server (CR create/update/delete, including the reconciler's own
// status patches), (2) explicit RequeueAfter from a prior Reconcile, and
// (3) the informer's periodic resync. Every state transition is therefore
// level-triggered and idempotent.

// DefaultBuildTimeout is the maximum duration a DatasetVersion may stay in
// state=building before the reconciler cancels the build and marks it failed.
const DefaultBuildTimeout = 30 * time.Minute

// DatasetVersionReconciler drives a single DatasetVersion through its
// lifecycle: building → ready → active → retired → (deleted), or
// building → failed (terminal).
//
// State transitions are level-triggered: each Reconcile call observes the
// current CR state, advances it at most one step, and either patches the
// status or returns a RequeueAfter to keep polling.
type DatasetVersionReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Builder kicks off and polls underlying snapshot builds (Job, fork, fake).
	Builder builder.Async

	// Volume manages PV deletion when a retired version is fully drained.
	Volume volume.Manager

	// VolumeBase is the FSVolumeManager base directory used by the legacy
	// fork builder; snapshots are symlinked into it so the node-agent can
	// find them. Empty when running with the K8s Job builder.
	VolumeBase string

	// BuildTimeout overrides DefaultBuildTimeout when non-zero.
	BuildTimeout time.Duration
}

// Reconcile implements the state machine.
func (r *DatasetVersionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var v v1alpha1.DatasetVersion
	if err := r.Get(ctx, req.NamespacedName, &v); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Treat empty state as "just created, transition to building".
	state := v.Status.State
	if state == "" {
		state = string(api.StateBuilding)
	}

	switch state {
	case string(api.StateBuilding):
		return r.reconcileBuilding(ctx, &v)
	case string(api.StateReady):
		return r.reconcileReady(ctx, &v)
	case string(api.StateActive):
		// NodeAssignmentReconciler owns rollout state; nothing to do here.
		return ctrl.Result{}, nil
	case string(api.StateRetired):
		return r.reconcileRetired(ctx, &v)
	case string(api.StateFailed):
		return r.reconcileFailed(ctx, &v)
	default:
		return ctrl.Result{}, fmt.Errorf("unknown state %q", state)
	}
}

// reconcileBuilding starts the underlying build (if not already started),
// polls it, and transitions on completion/failure/timeout.
func (r *DatasetVersionReconciler) reconcileBuilding(ctx context.Context, v *v1alpha1.DatasetVersion) (ctrl.Result, error) {
	// First reconcile: kick off the build.
	if v.Status.BuildJob == "" {
		// Fetch parent Dataset for the spec.
		var ds v1alpha1.Dataset
		if err := r.Get(ctx, client.ObjectKey{Namespace: v.Namespace, Name: v.Spec.Dataset}, &ds); err != nil {
			if apierrors.IsNotFound(err) {
				return r.markFailed(ctx, v, fmt.Sprintf("parent Dataset %q not found", v.Spec.Dataset))
			}
			return ctrl.Result{}, err
		}
		spec := v1alpha1.ToAPIDatasetSpec(&ds)
		spec.Name = ds.Name

		handle, err := r.Builder.Start(ctx, spec, v.Spec.VersionID)
		if err != nil {
			return r.markFailed(ctx, v, fmt.Sprintf("start build: %v", err))
		}

		// Persist the handle and mark explicitly as building (covers the
		// empty-state case where we treated it as building implicitly).
		patch := client.MergeFrom(v.DeepCopy())
		v.Status.State = string(api.StateBuilding)
		v.Status.BuildJob = string(handle)
		if err := r.Status().Patch(ctx, v, patch); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Build timeout?
	timeout := r.BuildTimeout
	if timeout == 0 {
		timeout = DefaultBuildTimeout
	}
	if !v.CreationTimestamp.IsZero() && time.Since(v.CreationTimestamp.Time) > timeout {
		slog.Info("build timeout exceeded", "dataset", v.Spec.Dataset, "version", v.Spec.VersionID)
		_ = r.Builder.Cancel(ctx, builder.Handle(v.Status.BuildJob))
		return r.markFailed(ctx, v, "build timeout exceeded")
	}

	status, err := r.Builder.Poll(ctx, builder.Handle(v.Status.BuildJob))
	if err != nil {
		// Transient error: requeue.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	switch status.Phase {
	case builder.Running:
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil

	case builder.Complete:
		snapPath := status.Result.SnapshotPath
		pvName := status.Result.PVName

		// Local fork builder: symlink the snapshot into VolumeBase under a
		// synthetic PV name so the node-agent can find it.
		if pvName == "" {
			pvName = fmt.Sprintf("pv-%s-%s", v.Spec.Dataset, v.Spec.VersionID)
			if r.VolumeBase != "" {
				pvLink := filepath.Join(r.VolumeBase, pvName)
				if err := os.Symlink(snapPath, pvLink); err != nil && !os.IsExist(err) {
					return r.markFailed(ctx, v, fmt.Sprintf("symlink pv: %v", err))
				}
			}
		}

		// Optional descriptor extraction (local builds only).
		var descriptor, msgName string
		if snapPath != "" {
			descriptor, msgName = readDescriptorFromMeta(snapPath)
		}

		patch := client.MergeFrom(v.DeepCopy())
		v.Status.State = string(api.StateReady)
		v.Status.PVName = pvName
		v.Status.SnapshotPath = snapPath
		if descriptor != "" {
			v.Status.Descriptor = descriptor
		}
		if msgName != "" {
			v.Status.MessageName = msgName
		}
		if err := r.Status().Patch(ctx, v, patch); err != nil {
			return ctrl.Result{}, err
		}
		// Immediately requeue so reconcileReady runs and promotes.
		return ctrl.Result{Requeue: true}, nil

	case builder.Failed:
		return r.markFailed(ctx, v, status.Error)

	case builder.NotFound:
		return r.markFailed(ctx, v, "build handle not found; orphaned")
	}

	return ctrl.Result{}, nil
}

// reconcileReady auto-promotes a ready version to active and demotes any
// existing active sibling to retired.
func (r *DatasetVersionReconciler) reconcileReady(ctx context.Context, v *v1alpha1.DatasetVersion) (ctrl.Result, error) {
	// List sibling versions for this dataset.
	var siblings v1alpha1.DatasetVersionList
	if err := r.List(ctx, &siblings,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{v1alpha1.DatasetLabel: v.Spec.Dataset},
	); err != nil {
		return ctrl.Result{}, err
	}

	// Demote any current active sibling.
	for i := range siblings.Items {
		s := &siblings.Items[i]
		if s.Name == v.Name || s.Status.State != string(api.StateActive) {
			continue
		}
		patch := client.MergeFrom(s.DeepCopy())
		s.Status.State = string(api.StateRetired)
		if err := r.Status().Patch(ctx, s, patch); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Promote self.
	patch := client.MergeFrom(v.DeepCopy())
	v.Status.State = string(api.StateActive)
	if err := r.Status().Patch(ctx, v, patch); err != nil {
		return ctrl.Result{}, err
	}
	slog.Info("promoted to active", "dataset", v.Spec.Dataset, "version", v.Spec.VersionID)
	return ctrl.Result{}, nil
}

// reconcileRetired waits for the version's PV to have no remaining
// VolumeAttachments, then deletes the PV and the CR.
//
// VolumeAttachments are the K8s-native source of truth for "is this disk
// attached to a node": etcd-backed, kubelet-managed, and visible to every
// control-plane replica without depending on in-memory broker state. The
// reconciler watches them so retirement happens within milliseconds of the
// last detach instead of polling.
func (r *DatasetVersionReconciler) reconcileRetired(ctx context.Context, v *v1alpha1.DatasetVersion) (ctrl.Result, error) {
	if v.Status.PVName != "" {
		var vas storagev1.VolumeAttachmentList
		if err := r.List(ctx, &vas); err != nil {
			return ctrl.Result{}, err
		}
		for i := range vas.Items {
			src := vas.Items[i].Spec.Source.PersistentVolumeName
			if src != nil && *src == v.Status.PVName {
				// Still attached somewhere — wait for the watch event on
				// VolumeAttachment delete to wake us. The RequeueAfter is a
				// safety fallback for the watch missing an event.
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
		}
	}

	if v.Status.PVName != "" && r.Volume != nil {
		if err := r.Volume.DeletePV(ctx, v.Status.PVName); err != nil {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
	}

	if err := r.Delete(ctx, v); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	slog.Info("retired and deleted", "dataset", v.Spec.Dataset, "version", v.Spec.VersionID)
	return ctrl.Result{}, nil
}

// reconcileFailed cleans up build resources (Job, ConfigMap, PVC) left behind
// by a failed build using deterministic resource names, then deletes the CR.
// All deletions are idempotent — resources may already be gone.
func (r *DatasetVersionReconciler) reconcileFailed(ctx context.Context, v *v1alpha1.DatasetVersion) (ctrl.Result, error) {
	dataset := v.Spec.Dataset
	versionID := v.Spec.VersionID
	ns := v.Namespace

	// Cancel the build via the builder interface — this deletes the Job,
	// ConfigMap, and PVC. The builder derives resource names deterministically,
	// so it works even after a control-plane restart.
	handle := builder.Handle(builder.JobName(dataset, versionID))
	if err := r.Builder.Cancel(ctx, handle); err != nil {
		slog.Warn("reconcileFailed: builder cancel failed", "dataset", dataset, "version", versionID, "ns", ns, "err", err)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if err := r.Delete(ctx, v); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	slog.Info("failed build cleaned up and deleted", "dataset", dataset, "version", versionID)
	return ctrl.Result{}, nil
}

// markFailed transitions the version to failed with an error message.
// Always returns no requeue.
func (r *DatasetVersionReconciler) markFailed(ctx context.Context, v *v1alpha1.DatasetVersion, reason string) (ctrl.Result, error) {
	patch := client.MergeFrom(v.DeepCopy())
	v.Status.State = string(api.StateFailed)
	v.Status.Error = reason
	if err := r.Status().Patch(ctx, v, patch); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler with a controller-runtime manager.
//
// Watches VolumeAttachment as a secondary source so retirement (which gates
// on "no more attachments to this PV") wakes immediately when the last
// VolumeAttachment for a retired version's PV is deleted.
func (r *DatasetVersionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DatasetVersion{}).
		Named("datasetversion").
		Watches(
			&storagev1.VolumeAttachment{},
			handler.EnqueueRequestsFromMapFunc(r.mapVolumeAttachmentToRetired),
		).
		Complete(r)
}

// mapVolumeAttachmentToRetired returns reconcile requests for every retired
// DatasetVersion whose PV matches the VolumeAttachment's source. The mapping
// runs on every VA event (create/update/delete) but the reconciler itself is
// idempotent, so churn during normal attach/detach is harmless.
func (r *DatasetVersionReconciler) mapVolumeAttachmentToRetired(ctx context.Context, obj client.Object) []reconcile.Request {
	va, ok := obj.(*storagev1.VolumeAttachment)
	if !ok || va.Spec.Source.PersistentVolumeName == nil {
		return nil
	}
	pvName := *va.Spec.Source.PersistentVolumeName

	var versions v1alpha1.DatasetVersionList
	if err := r.List(ctx, &versions); err != nil {
		return nil
	}
	var out []reconcile.Request
	for i := range versions.Items {
		v := &versions.Items[i]
		if v.Status.State == string(api.StateRetired) && v.Status.PVName == pvName {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: v.Namespace, Name: v.Name,
			}})
		}
	}
	return out
}

// readDescriptorFromMeta reads the protobuf descriptor and message name from
// a snapshot's meta.json. Returns empty strings if the file is missing, the
// encoding section is absent, or any parse error occurs.
func readDescriptorFromMeta(snapshotPath string) (descriptor, messageName string) {
	data, err := os.ReadFile(filepath.Join(snapshotPath, "meta.json"))
	if err != nil {
		return "", ""
	}
	var meta struct {
		Encoding *struct {
			Protobuf *struct {
				Descriptor  string `json:"descriptor"`
				MessageName string `json:"message_name"`
			} `json:"protobuf"`
		} `json:"encoding"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return "", ""
	}
	if meta.Encoding == nil || meta.Encoding.Protobuf == nil {
		return "", ""
	}
	return meta.Encoding.Protobuf.Descriptor, meta.Encoding.Protobuf.MessageName
}
