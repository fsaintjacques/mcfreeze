package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/fsaintjacques/frostmap/go/api"
	v1alpha1 "github.com/fsaintjacques/frostmap/go/api/v1alpha1"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/builder"
)

// GroupVersionResource constants for the frostmap CRDs.
var (
	datasetGVR = schema.GroupVersionResource{
		Group:    v1alpha1.GroupVersion.Group,
		Version:  v1alpha1.GroupVersion.Version,
		Resource: "datasets",
	}
	datasetVersionGVR = schema.GroupVersionResource{
		Group:    v1alpha1.GroupVersion.Group,
		Version:  v1alpha1.GroupVersion.Version,
		Resource: "datasetversions",
	}
)

// CRDStore implements Store backed by Kubernetes CRDs for dataset specs and
// version records. Assignment, generation, notify-channel, and node-state
// fields are kept in memory because they are inherently ephemeral and
// re-derivable on restart from active DatasetVersion CRs and node-agent
// reconnections.
//
// All CRD-bound methods translate Store operations to dynamic client calls
// against the v1alpha1 CRDs. Status updates use the /status subresource.
type CRDStore struct {
	dyn       dynamic.Interface
	kube      kubernetes.Interface
	namespace string

	// createMu serializes CreateVersion's check-then-create sequence to
	// close the TOCTOU window between the building-version guard and the
	// CR Create call. This is correct for the current single-replica
	// control-plane Deployment; cluster-wide enforcement arrives in
	// Phase 5 with controller-runtime leader election.
	createMu sync.Mutex

	// Ephemeral per-node state (assignments, generation, notify, nodeStates,
	// registered nodes) lives in the embedded broker so the HTTP server and
	// the Phase 5 reconcilers can share it.
	*AssignmentBroker
}

// NewCRDStore constructs a CRDStore with a fresh in-memory AssignmentBroker.
// The CRDs must already be installed in the cluster; New does not register
// them.
func NewCRDStore(dyn dynamic.Interface, kube kubernetes.Interface, namespace string) *CRDStore {
	return NewCRDStoreWithBroker(dyn, kube, namespace, NewAssignmentBroker())
}

// NewCRDStoreWithBroker constructs a CRDStore that shares the given broker.
func NewCRDStoreWithBroker(dyn dynamic.Interface, kube kubernetes.Interface, namespace string, broker *AssignmentBroker) *CRDStore {
	return &CRDStore{
		dyn:              dyn,
		kube:             kube,
		namespace:        namespace,
		AssignmentBroker: broker,
	}
}

// Broker returns the underlying AssignmentBroker.
func (s *CRDStore) Broker() *AssignmentBroker { return s.AssignmentBroker }

// Compile-time check that CRDStore satisfies the Store interface.
var _ Store = (*CRDStore)(nil)

// ---------------------------------------------------------------------------
// Dataset CRD methods
// ---------------------------------------------------------------------------

// RegisterDataset creates or updates the Dataset CR. Idempotent.
//
// The Store interface returns no error here, so failures are logged at
// ERROR level. Subsequent operations on the dataset (CreateVersion,
// Promote) will fail loudly if the CR is missing, surfacing the problem
// to the orchestrator's retry loop.
func (s *CRDStore) RegisterDataset(spec api.DatasetSpec) {
	ctx := context.Background()
	cr := v1alpha1.FromAPIDatasetSpec(spec)
	cr.Name = spec.Name

	u, err := toUnstructured(cr, "Dataset")
	if err != nil {
		slog.Error("CRDStore.RegisterDataset: encode CR", "dataset", spec.Name, "err", err)
		return
	}

	client := s.dyn.Resource(datasetGVR).Namespace(s.namespace)
	existing, err := client.Get(ctx, spec.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := client.Create(ctx, u, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			slog.Error("CRDStore.RegisterDataset: create", "dataset", spec.Name, "err", err)
		}
		return
	}
	if err != nil {
		slog.Error("CRDStore.RegisterDataset: get", "dataset", spec.Name, "err", err)
		return
	}
	// Preserve resourceVersion + UID for the update.
	u.SetResourceVersion(existing.GetResourceVersion())
	u.SetUID(existing.GetUID())
	if _, err := client.Update(ctx, u, metav1.UpdateOptions{}); err != nil {
		slog.Error("CRDStore.RegisterDataset: update", "dataset", spec.Name, "err", err)
	}
}

func (s *CRDStore) GetDatasetSpec(name string) (api.DatasetSpec, bool) {
	ctx := context.Background()
	u, err := s.dyn.Resource(datasetGVR).Namespace(s.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return api.DatasetSpec{}, false
	}
	cr := &v1alpha1.Dataset{}
	if err := fromUnstructured(u, cr); err != nil {
		return api.DatasetSpec{}, false
	}
	return v1alpha1.ToAPIDatasetSpec(cr), true
}

// ---------------------------------------------------------------------------
// DatasetVersion CRD methods
// ---------------------------------------------------------------------------

func (s *CRDStore) CreateVersion(dataset, versionID string) error {
	ctx := context.Background()

	// Serialize the check+create across goroutines in this process to avoid
	// a TOCTOU race that would let two callers both pass the
	// duplicate-building guard. (Cross-process concurrency is out of scope:
	// only one control-plane writes to a given namespace.)
	s.createMu.Lock()
	defer s.createMu.Unlock()

	// Reject if a building version already exists.
	for _, v := range s.GetVersions(dataset) {
		if v.State == api.StateBuilding {
			return fmt.Errorf("dataset %q already has a building version %q", dataset, v.ID)
		}
	}

	// Look up parent Dataset for ownerRef and shardCount.
	var (
		ownerRefs  []metav1.OwnerReference
		shardCount int
	)
	parent, err := s.dyn.Resource(datasetGVR).Namespace(s.namespace).Get(ctx, dataset, metav1.GetOptions{})
	if err == nil {
		ownerRefs = []metav1.OwnerReference{{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "Dataset",
			Name:       parent.GetName(),
			UID:        parent.GetUID(),
		}}
		ds := &v1alpha1.Dataset{}
		if err := fromUnstructured(parent, ds); err == nil {
			shardCount = ds.Spec.ShardCount
		}
	}

	cr := &v1alpha1.DatasetVersion{
		Spec: v1alpha1.DatasetVersionSpec{
			Dataset:    dataset,
			VersionID:  versionID,
			ShardCount: shardCount,
		},
	}
	cr.Name = v1alpha1.VersionCRName(dataset, versionID)
	cr.Labels = map[string]string{v1alpha1.DatasetLabel: dataset}
	cr.OwnerReferences = ownerRefs

	u, err := toUnstructured(cr, "DatasetVersion")
	if err != nil {
		return fmt.Errorf("create version: encode CR: %w", err)
	}

	client := s.dyn.Resource(datasetVersionGVR).Namespace(s.namespace)
	created, err := client.Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create version: %w", err)
	}

	// Set initial status (state=building, createdAt) via status subresource.
	if err := s.patchVersionStatus(ctx, created.GetName(), map[string]interface{}{
		"state": string(api.StateBuilding),
	}); err != nil {
		return fmt.Errorf("create version: set status: %w", err)
	}
	return nil
}

func (s *CRDStore) MarkReady(dataset, versionID, snapshotPath, pvName string) error {
	ctx := context.Background()
	name := v1alpha1.VersionCRName(dataset, versionID)
	cur, err := s.getVersionCR(ctx, name)
	if err != nil {
		return err
	}
	if cur.Status.State != string(api.StateBuilding) {
		return fmt.Errorf("version %q is %q, expected building", versionID, cur.Status.State)
	}
	return s.patchVersionStatus(ctx, name, map[string]interface{}{
		"state":        string(api.StateReady),
		"pvName":       pvName,
		"snapshotPath": snapshotPath,
	})
}

func (s *CRDStore) SetDescriptor(dataset, versionID, descriptor, messageName string) error {
	if descriptor == "" && messageName == "" {
		return nil
	}
	ctx := context.Background()
	name := v1alpha1.VersionCRName(dataset, versionID)
	if _, err := s.getVersionCR(ctx, name); err != nil {
		return err
	}
	return s.patchVersionStatus(ctx, name, map[string]interface{}{
		"descriptor":  descriptor,
		"messageName": messageName,
	})
}

func (s *CRDStore) MarkFailed(dataset, versionID, reason string) error {
	ctx := context.Background()
	name := v1alpha1.VersionCRName(dataset, versionID)
	cur, err := s.getVersionCR(ctx, name)
	if err != nil {
		return err
	}
	if cur.Status.State != string(api.StateBuilding) {
		return fmt.Errorf("version %q is %q, expected building", versionID, cur.Status.State)
	}
	return s.patchVersionStatus(ctx, name, map[string]interface{}{
		"state": string(api.StateFailed),
		"error": reason,
	})
}

func (s *CRDStore) Promote(dataset, versionID string) error {
	ctx := context.Background()

	// Validate target is ready.
	target, err := s.getVersionCR(ctx, v1alpha1.VersionCRName(dataset, versionID))
	if err != nil {
		return err
	}
	if target.Status.State != string(api.StateReady) {
		return fmt.Errorf("version %q is %q, expected ready", versionID, target.Status.State)
	}

	// Look up dataset spec for the assignment KeyPrefix.
	spec, ok := s.GetDatasetSpec(dataset)
	if !ok {
		return fmt.Errorf("dataset %q not registered", dataset)
	}

	// Snapshot active versions before mutating, so a crash mid-sequence
	// leaves the dataset with at least one active version (the new one)
	// rather than zero.
	versions := s.GetVersions(dataset)

	// Promote target first.
	if err := s.patchVersionStatus(ctx, v1alpha1.VersionCRName(dataset, versionID), map[string]interface{}{
		"state": string(api.StateActive),
	}); err != nil {
		return fmt.Errorf("promote target: %w", err)
	}

	// Then retire any prior active version. Brief overlap (two active
	// versions) is preferable to a zero-active window for readers.
	for _, v := range versions {
		if v.State == api.StateActive && v.ID != versionID {
			if err := s.patchVersionStatus(ctx, v1alpha1.VersionCRName(dataset, v.ID), map[string]interface{}{
				"state": string(api.StateRetired),
			}); err != nil {
				return fmt.Errorf("retire prior active %q: %w", v.ID, err)
			}
		}
	}

	// Update Dataset.status.activeVersion.
	if err := s.patchDatasetStatus(ctx, dataset, map[string]interface{}{
		"activeVersion": versionID,
	}); err != nil {
		// Best-effort; not fatal.
	}

	// Build the in-memory assignment from the now-active version. Re-fetch
	// the CR so we observe the updated status (and any descriptor set
	// before promotion).
	updated, err := s.getVersionCR(ctx, v1alpha1.VersionCRName(dataset, versionID))
	if err != nil {
		return err
	}
	entry := versionEntryFromCR(updated)

	assignment := api.NodeAssignment{
		Dataset:   dataset,
		KeyPrefix: spec.KeyPrefix,
		Version:   entry.VersionRecord,
	}
	for _, nodeName := range s.AssignmentBroker.Nodes() {
		s.AssignmentBroker.MergeAssignment(nodeName, assignment)
	}

	return nil
}

func (s *CRDStore) GetVersions(dataset string) []VersionEntry {
	ctx := context.Background()
	list, err := s.dyn.Resource(datasetVersionGVR).Namespace(s.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", v1alpha1.DatasetLabel, dataset),
	})
	if err != nil {
		return nil
	}
	out := make([]VersionEntry, 0, len(list.Items))
	for i := range list.Items {
		cr := &v1alpha1.DatasetVersion{}
		if err := fromUnstructured(&list.Items[i], cr); err != nil {
			continue
		}
		out = append(out, versionEntryFromCR(cr))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (s *CRDStore) GetActiveVersion(dataset string) (VersionEntry, bool) {
	for _, v := range s.GetVersions(dataset) {
		if v.State == api.StateActive {
			return v, true
		}
	}
	return VersionEntry{}, false
}

// SetBuildHandle records the build job name in the version status and patches
// ownerReferences on the Job, ConfigMap, and PVC so they cascade-delete with
// the DatasetVersion CR.
func (s *CRDStore) SetBuildHandle(dataset, versionID string, handle builder.Handle) error {
	ctx := context.Background()
	name := v1alpha1.VersionCRName(dataset, versionID)
	cur, err := s.getVersionCR(ctx, name)
	if err != nil {
		return err
	}
	if cur.Status.State != string(api.StateBuilding) {
		return fmt.Errorf("version %q is %q, expected building", versionID, cur.Status.State)
	}
	if err := s.patchVersionStatus(ctx, name, map[string]interface{}{
		"buildJob": string(handle),
	}); err != nil {
		return err
	}

	if cur.UID != "" && s.kube != nil {
		s.patchBuildResourceOwnerRefs(ctx, dataset, versionID, cur.UID)
	}
	return nil
}

func (s *CRDStore) GetBuildingVersions() []VersionEntry {
	ctx := context.Background()
	list, err := s.dyn.Resource(datasetVersionGVR).Namespace(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	var out []VersionEntry
	for i := range list.Items {
		cr := &v1alpha1.DatasetVersion{}
		if err := fromUnstructured(&list.Items[i], cr); err != nil {
			continue
		}
		if cr.Status.State == string(api.StateBuilding) {
			out = append(out, versionEntryFromCR(cr))
		}
	}
	return out
}

func (s *CRDStore) DeleteVersion(dataset, versionID string) error {
	ctx := context.Background()
	name := v1alpha1.VersionCRName(dataset, versionID)
	cur, err := s.getVersionCR(ctx, name)
	if err != nil {
		return err
	}
	if cur.Status.State != string(api.StateRetired) {
		return fmt.Errorf("version %q is %q, expected retired", versionID, cur.Status.State)
	}
	return s.dyn.Resource(datasetVersionGVR).Namespace(s.namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

// ---------------------------------------------------------------------------
// Hybrid methods (CRD data + ephemeral node state)
// ---------------------------------------------------------------------------

func (s *CRDStore) RolloutStatus(dataset string) RolloutStatus {
	versions := s.GetVersions(dataset)
	status := RolloutStatus{
		Dataset:    dataset,
		NodeCounts: make(map[string]int),
	}
	for _, v := range versions {
		if v.State == api.StateActive {
			status.ActiveVersion = v.ID
			break
		}
	}

	nodes := s.AssignmentBroker.Nodes()
	nodeStates := s.AssignmentBroker.SnapshotNodeStates()

	for _, nodeName := range nodes {
		ns, ok := nodeStates[nodeName]
		if !ok {
			status.PendingNodes = append(status.PendingNodes, nodeName)
			continue
		}
		found := false
		for _, ds := range ns.Datasets {
			if ds.Dataset != dataset {
				continue
			}
			found = true
			if ds.Phase == api.PhaseError {
				status.ErrorNodes = append(status.ErrorNodes, nodeName)
			} else if ds.Phase == api.PhaseActive && ds.VersionID == status.ActiveVersion {
				status.ConvergedNodes = append(status.ConvergedNodes, nodeName)
			} else {
				status.PendingNodes = append(status.PendingNodes, nodeName)
			}
			status.NodeCounts[ds.VersionID]++
			break
		}
		if !found {
			status.PendingNodes = append(status.PendingNodes, nodeName)
		}
	}
	return status
}

func (s *CRDStore) CheckRetirement(dataset string) []VersionEntry {
	nodes := s.AssignmentBroker.Nodes()
	nodeStates := s.AssignmentBroker.SnapshotNodeStates()

	for _, nodeName := range nodes {
		if _, ok := nodeStates[nodeName]; !ok {
			return nil
		}
	}
	reportedVersions := make(map[string]bool)
	for _, ns := range nodeStates {
		for _, ds := range ns.Datasets {
			if ds.Dataset == dataset {
				reportedVersions[ds.VersionID] = true
			}
		}
	}

	versions := s.GetVersions(dataset)
	var eligible []VersionEntry
	for _, v := range versions {
		if v.State == api.StateRetired && !reportedVersions[v.ID] {
			eligible = append(eligible, v)
		}
	}
	return eligible
}

// ---------------------------------------------------------------------------
// CRD helpers
// ---------------------------------------------------------------------------

func (s *CRDStore) getVersionCR(ctx context.Context, name string) (*v1alpha1.DatasetVersion, error) {
	u, err := s.dyn.Resource(datasetVersionGVR).Namespace(s.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get version %q: %w", name, err)
	}
	cr := &v1alpha1.DatasetVersion{}
	if err := fromUnstructured(u, cr); err != nil {
		return nil, fmt.Errorf("decode version %q: %w", name, err)
	}
	return cr, nil
}

// patchVersionStatus merges fields into status via the /status subresource.
func (s *CRDStore) patchVersionStatus(ctx context.Context, name string, fields map[string]interface{}) error {
	patch, err := json.Marshal(map[string]interface{}{"status": fields})
	if err != nil {
		return err
	}
	_, err = s.dyn.Resource(datasetVersionGVR).Namespace(s.namespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status",
	)
	return err
}

func (s *CRDStore) patchDatasetStatus(ctx context.Context, name string, fields map[string]interface{}) error {
	patch, err := json.Marshal(map[string]interface{}{"status": fields})
	if err != nil {
		return err
	}
	_, err = s.dyn.Resource(datasetGVR).Namespace(s.namespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status",
	)
	return err
}

// patchBuildResourceOwnerRefs sets ownerReferences on the build Job,
// ConfigMap, and PVC pointing back to the DatasetVersion CR. Best-effort:
// errors are silently ignored (a missing resource just means cleanup will
// happen explicitly during retirement).
func (s *CRDStore) patchBuildResourceOwnerRefs(ctx context.Context, dataset, versionID string, versionUID types.UID) {
	owner := metav1.OwnerReference{
		APIVersion: v1alpha1.GroupVersion.String(),
		Kind:       "DatasetVersion",
		Name:       v1alpha1.VersionCRName(dataset, versionID),
		UID:        versionUID,
	}
	patch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"ownerReferences": []metav1.OwnerReference{owner},
		},
	})
	if err != nil {
		return
	}
	jobName := builder.JobName(dataset, versionID)
	cmName := builder.ConfigMapName(dataset, versionID)
	pvcName := builder.PVCName(dataset, versionID)

	_, _ = s.kube.BatchV1().Jobs(s.namespace).Patch(ctx, jobName, types.MergePatchType, patch, metav1.PatchOptions{})
	_, _ = s.kube.CoreV1().ConfigMaps(s.namespace).Patch(ctx, cmName, types.MergePatchType, patch, metav1.PatchOptions{})
	_, _ = s.kube.CoreV1().PersistentVolumeClaims(s.namespace).Patch(ctx, pvcName, types.MergePatchType, patch, metav1.PatchOptions{})
}

// versionEntryFromCR converts a DatasetVersion CR to a VersionEntry.
func versionEntryFromCR(cr *v1alpha1.DatasetVersion) VersionEntry {
	createdAt := cr.CreationTimestamp.Time
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	return VersionEntry{
		VersionRecord: api.VersionRecord{
			ID:          cr.Spec.VersionID,
			Dataset:     cr.Spec.Dataset,
			DiskURL:     cr.Status.DiskURL,
			PVName:      cr.Status.PVName,
			State:       api.VersionState(cr.Status.State),
			ShardCount:  cr.Spec.ShardCount,
			CreatedAt:   createdAt,
			Descriptor:  cr.Status.Descriptor,
			MessageName: cr.Status.MessageName,
		},
		SnapshotPath: cr.Status.SnapshotPath,
		BuildHandle:  builder.Handle(cr.Status.BuildJob),
	}
}

// toUnstructured converts a typed CRD object to *unstructured.Unstructured,
// stamping the GVK so the dynamic client routes correctly.
func toUnstructured(obj runtime.Object, kind string) (*unstructured.Unstructured, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind(kind))
	return u, nil
}

func fromUnstructured(u *unstructured.Unstructured, obj runtime.Object) error {
	return runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, obj)
}
