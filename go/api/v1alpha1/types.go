package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ---------------------------------------------------------------------------
// Dataset CRD
// ---------------------------------------------------------------------------

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=fmd
// +kubebuilder:printcolumn:name="KeyPrefix",type=string,JSONPath=`.spec.keyPrefix`
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.shardCount`
// +kubebuilder:printcolumn:name="Active",type=string,JSONPath=`.status.activeVersion`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Dataset describes an immutable KV dataset and its build configuration.
// The resource name (metadata.name) is the dataset name.
type Dataset struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DatasetSpec   `json:"spec"`
	Status            DatasetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:generate=true

// DatasetSpec defines the desired state of a Dataset.
type DatasetSpec struct {
	// KeyPrefix is the routing prefix the KV server uses for this dataset.
	// Must be unique across all datasets on a node.
	// +kubebuilder:validation:MinLength=1
	KeyPrefix string `json:"keyPrefix"`

	// Source describes how to produce key-value pairs from a tabular source.
	Source SourceSpec `json:"source"`

	// ShardCount is the number of hash partitions. Must be a power of 2.
	// +kubebuilder:validation:Minimum=1
	ShardCount int `json:"shardCount"`

	// Retention is the number of ready versions to keep before cleanup.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	Retention int `json:"retention"`

	// Trigger describes how new DatasetVersions are produced. If unset,
	// versions are only created by external triggers (HTTP build endpoint,
	// future webhooks, manual kubectl apply).
	// +optional
	Trigger *TriggerSpec `json:"trigger,omitempty"`

	// Storage configures the PersistentVolume backing this dataset.
	// When nil, cluster-wide defaults apply.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// BuilderResources overrides CPU/memory requests and limits for this
	// dataset's builder Jobs. When nil, cluster-wide defaults apply.
	// +optional
	BuilderResources *corev1.ResourceRequirements `json:"builderResources,omitempty"`
}

// +kubebuilder:object:generate=true

// StorageSpec configures the PersistentVolume for a dataset.
type StorageSpec struct {
	// DiskSizeGB is the PVC size in GiB.
	// +kubebuilder:validation:Minimum=1
	DiskSizeGB int64 `json:"diskSizeGB"`

	// ProvisionedThroughput is the throughput to provision for the disk,
	// passed directly as the CSI "provisioned-throughput-on-create" parameter.
	// Example: "1200Mi" for 1200 MiB/s.
	// +optional
	ProvisionedThroughput *resource.Quantity `json:"provisionedThroughput,omitempty"`
}

// +kubebuilder:object:generate=true

// TriggerSpec is a discriminated union describing a build trigger.
// Exactly one of Manual or Cron must be set.
type TriggerSpec struct {
	// Manual marks the dataset as build-on-demand only.
	// +optional
	Manual *ManualTrigger `json:"manual,omitempty"`

	// Cron schedules periodic builds.
	// +optional
	Cron *CronTrigger `json:"cron,omitempty"`
}

// ManualTrigger has no fields. Its presence indicates manual-only builds.
type ManualTrigger struct{}

// CronTrigger schedules periodic builds.
type CronTrigger struct {
	// Schedule is a standard cron expression (5-field, no seconds).
	// +kubebuilder:validation:MinLength=1
	Schedule string `json:"schedule"`
}

// +kubebuilder:object:generate=true

// DatasetStatus defines the observed state of a Dataset.
type DatasetStatus struct {
	// ActiveVersion is the version ID currently being served.
	ActiveVersion string `json:"activeVersion,omitempty"`

	// Conditions represent the latest observations of the Dataset's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

// DatasetList contains a list of Dataset resources.
type DatasetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Dataset `json:"items"`
}

// ---------------------------------------------------------------------------
// DatasetVersion CRD
// ---------------------------------------------------------------------------

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=fmv
// +kubebuilder:printcolumn:name="Dataset",type=string,JSONPath=`.spec.dataset`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.versionId`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="PV",type=string,JSONPath=`.status.pvName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DatasetVersion tracks a single snapshot version of a Dataset.
// The resource name is deterministic: <dataset>-<versionId>.
type DatasetVersion struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DatasetVersionSpec   `json:"spec"`
	Status            DatasetVersionStatus `json:"status,omitempty"`
}

// DatasetVersionSpec is immutable after creation.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="DatasetVersion spec is immutable"
type DatasetVersionSpec struct {
	// Dataset is the parent Dataset name.
	// +kubebuilder:validation:MinLength=1
	Dataset string `json:"dataset"`

	// VersionID is the version identifier.
	// +kubebuilder:validation:MinLength=1
	VersionID string `json:"versionId"`

	// ShardCount is the number of hash partitions (copied from the Dataset at creation time).
	// +kubebuilder:validation:Minimum=1
	ShardCount int `json:"shardCount"`
}

// +kubebuilder:object:generate=true

// DatasetVersionStatus defines the observed state of a DatasetVersion.
type DatasetVersionStatus struct {
	// State is the lifecycle state of this version.
	// +kubebuilder:validation:Enum=building;ready;active;retired;failed
	State string `json:"state,omitempty"`

	// PVName is the Kubernetes PersistentVolume name (set on transition to ready).
	PVName string `json:"pvName,omitempty"`

	// SnapshotPath is the on-disk location of the snapshot (local builds only).
	SnapshotPath string `json:"snapshotPath,omitempty"`

	// BuildJob is the builder handle (Kubernetes Job name).
	BuildJob string `json:"buildJob,omitempty"`

	// DiskURL is the cloud disk resource URL (set by the build job on completion).
	DiskURL string `json:"diskUrl,omitempty"`

	// Descriptor is a base64-encoded FileDescriptorSet for protobuf-encoded snapshots.
	Descriptor string `json:"descriptor,omitempty"`

	// MessageName is the fully-qualified protobuf message name.
	MessageName string `json:"messageName,omitempty"`

	// Error contains the failure reason when State is "failed".
	Error string `json:"error,omitempty"`

	// Rollout reports per-node convergence for this version while it is active.
	// +optional
	Rollout *RolloutStatus `json:"rollout,omitempty"`

	// Conditions represent the latest observations of the DatasetVersion's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:generate=true

// RolloutStatus aggregates per-node convergence for an active DatasetVersion.
type RolloutStatus struct {
	// TotalNodes is the count of nodes the version has been pushed to.
	TotalNodes int `json:"totalNodes"`

	// ConvergedNodes is the count of nodes reporting Active state for this version.
	ConvergedNodes int `json:"convergedNodes"`

	// PendingNodes is the count of nodes that have not yet reached Active.
	PendingNodes int `json:"pendingNodes"`

	// ErrorNodes is the count of nodes reporting Error for this version.
	ErrorNodes int `json:"errorNodes"`
}

// +kubebuilder:object:root=true

// DatasetVersionList contains a list of DatasetVersion resources.
type DatasetVersionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatasetVersion `json:"items"`
}

// ---------------------------------------------------------------------------
// Source types (duplicated from api.types with K8s-convention JSON tags)
// ---------------------------------------------------------------------------

// +kubebuilder:object:generate=true

// SourceSpec describes how to produce key-value pairs from a tabular source.
type SourceSpec struct {
	// KeyColumn is the Arrow column name whose bytes become the KV key.
	KeyColumn string `json:"keyColumn"`

	// ValueColumn is the Arrow column name whose bytes become the KV value (raw mode).
	// Required when Encoding is nil; must be empty when Encoding is set.
	ValueColumn string `json:"valueColumn,omitempty"`

	// IncludeKeyInValue includes the key column in the encoded value payload.
	// When true, all columns (including the key) are encoded into the value.
	// Default false: the key column is excluded from the value.
	// +optional
	IncludeKeyInValue bool `json:"includeKeyInValue,omitempty"`

	// Encoding specifies how to transcode non-key columns into the KV value.
	// +optional
	Encoding *EncodingSpec `json:"encoding,omitempty"`

	// BigQuery source configuration.
	// +optional
	BigQuery *BigQuerySource `json:"bigquery,omitempty"`

	// CSV source configuration.
	// +optional
	CSV *CsvSource `json:"csv,omitempty"`
}

// CsvSource describes a CSV data source.
type CsvSource struct {
	// Data is inline CSV content (including the header row).
	Data string `json:"data,omitempty"`

	// Path is a filesystem path to a CSV file.
	Path string `json:"path,omitempty"`
}

// +kubebuilder:object:generate=true

// BigQuerySource describes a BigQuery Storage Read API source.
type BigQuerySource struct {
	Project        string   `json:"project"`
	Table          string   `json:"table"`
	SelectedFields []string `json:"selectedFields,omitempty"`
	RowRestriction string   `json:"rowRestriction,omitempty"`
}

// +kubebuilder:object:generate=true

// EncodingSpec is a discriminated union for value encoding.
type EncodingSpec struct {
	// +optional
	Protobuf *ProtobufEncoding `json:"protobuf,omitempty"`
}

// ProtobufEncoding configures Arrow to protobuf transcoding.
type ProtobufEncoding struct {
	Descriptor    string `json:"descriptor,omitempty"`
	DescriptorURI string `json:"descriptorUri,omitempty"`
	Package       string `json:"package,omitempty"`
	MessageName   string `json:"messageName"`
}

// ---------------------------------------------------------------------------
// DatasetBinding CRD
// ---------------------------------------------------------------------------

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=fmb
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DatasetBinding selects a set of Datasets and a set of Kubernetes nodes,
// declaring that matched nodes should serve matched datasets.
// When no DatasetBinding matches a node, it receives all datasets (open-world default).
// When multiple DatasetBindings match a node, their dataset sets are unioned.
type DatasetBinding struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DatasetBindingSpec `json:"spec"`
}

// +kubebuilder:object:generate=true

// DatasetBindingSpec defines the node and dataset selectors for a binding.
type DatasetBindingSpec struct {
	// NodeSelector selects which Kubernetes nodes this binding applies to.
	// A nil or empty selector matches all nodes.
	// +optional
	NodeSelector *metav1.LabelSelector `json:"nodeSelector,omitempty"`

	// DatasetSelector selects which Datasets this binding includes,
	// matched against Dataset metadata.labels.
	// A nil or empty selector matches all datasets.
	// +optional
	DatasetSelector *metav1.LabelSelector `json:"datasetSelector,omitempty"`
}

// +kubebuilder:object:root=true

// DatasetBindingList contains a list of DatasetBinding resources.
type DatasetBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatasetBinding `json:"items"`
}
