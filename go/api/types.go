// Package api contains the canonical type definitions for fmtctl's wire
// contracts.  Both the control-plane and the node-agent import this package.
package api

import "time"

// DatasetSpec describes a dataset and its build configuration.
type DatasetSpec struct {
	Name string `json:"name"`
	// KeyPrefix is the routing prefix the KV server uses to match incoming
	// requests to this dataset, e.g. "users" routes "users:<key>" lookups.
	// Must be unique across all datasets on a node.
	KeyPrefix  string     `json:"key_prefix"`
	Source     SourceSpec `json:"source"`
	ShardCount int        `json:"shard_count"`
	Retention  int        `json:"retention"` // number of ready versions to keep
}

// SourceSpec describes how to produce key-value pairs from a tabular source.
// KeyColumn and ValueColumn/Encoding sit above the source-type discriminant
// because they apply regardless of where the data comes from.
//
// Validation: exactly one of ValueColumn or Encoding must be set.
// ValueColumn → raw encoding (take the column bytes as-is).
// Encoding    → transcode non-key columns (e.g. to protobuf).
type SourceSpec struct {
	// KeyColumn is the Arrow column name whose bytes become the KV key.
	KeyColumn string `json:"key_column"`
	// ValueColumn is the Arrow column name whose bytes become the KV value.
	// Required when Encoding is nil (raw mode); must be empty when Encoding is set.
	ValueColumn string `json:"value_column,omitempty"`
	// Encoding specifies how to transcode non-key columns into the KV value.
	// When nil, values are taken raw from ValueColumn.
	Encoding *EncodingSpec `json:"encoding,omitempty"`
	// Exactly one source type must be set.
	BigQuery *BigQuerySource `json:"bigquery,omitempty"`
}

// BigQuerySource describes a BigQuery Storage Read API source.
type BigQuerySource struct {
	Project        string   `json:"project"`
	Table          string   `json:"table"`
	SelectedFields []string `json:"selected_fields,omitempty"` // column projection; empty = all columns
	RowRestriction string   `json:"row_restriction,omitempty"` // SQL WHERE predicate pushed down to BQ
}

// EncodingSpec is a discriminated union for value encoding.
// When nil/absent, values are taken raw from a single column.
type EncodingSpec struct {
	Protobuf *ProtobufEncoding `json:"protobuf,omitempty"`
}

// ProtobufEncoding configures Arrow → protobuf transcoding via apb.
type ProtobufEncoding struct {
	// Descriptor is a base64-encoded FileDescriptorSet.
	// Mutually exclusive with DescriptorURI. When both are empty,
	// the descriptor is auto-generated from the Arrow schema.
	Descriptor string `json:"descriptor,omitempty"`
	// DescriptorURI is a GCS URI to a FileDescriptorSet binary
	// (e.g. "gs://bucket/schema.desc"). Mutually exclusive with Descriptor.
	DescriptorURI string `json:"descriptor_uri,omitempty"`
	// Package is the protobuf package name (e.g. "mypackage").
	// Required when auto-generating (no descriptor); ignored otherwise.
	Package string `json:"package,omitempty"`
	// MessageName is the protobuf message name.
	// When auto-generating: bare name (e.g. "MyMessage") — combined with
	// Package to form the FQN.
	// When a descriptor is provided: fully-qualified name
	// (e.g. "mypackage.MyMessage").
	MessageName string `json:"message_name"`
}

// VersionState is the lifecycle state of a VersionRecord.
type VersionState string

const (
	StateBuilding VersionState = "building"
	StateReady    VersionState = "ready"
	StateActive   VersionState = "active"
	StateRetired  VersionState = "retired"
	StateFailed   VersionState = "failed"
)

// VersionRecord tracks a single snapshot version.
type VersionRecord struct {
	ID         string       `json:"id"`
	Dataset    string       `json:"dataset"`
	DiskURL    string       `json:"disk_url"`          // cloud disk resource URL; set by the job on completion
	PVName     string       `json:"pv_name,omitempty"` // Kubernetes PersistentVolume name; set by the control-plane on transition to ready
	State      VersionState `json:"state"`
	ShardCount int          `json:"shard_count"`
	CreatedAt  time.Time    `json:"created_at"`
	// Descriptor is a base64-encoded FileDescriptorSet for protobuf-encoded
	// snapshots. Empty for raw-encoded snapshots or when unavailable.
	Descriptor string `json:"descriptor,omitempty"`
	// MessageName is the fully-qualified protobuf message name
	// (e.g. "mypackage.MyMessage"). Empty when Descriptor is empty.
	MessageName string `json:"message_name,omitempty"`
}

// CatalogEntry is one per-dataset entry written to catalog.json on the shared
// EmptyDir.  The KV server watches this file and swaps the active version when
// it changes.
type CatalogEntry struct {
	Dataset string `json:"dataset"`
	// KeyPrefix is the routing prefix the KV server registers for this dataset.
	// Requests whose key starts with "<KeyPrefix>:" are routed here.
	KeyPrefix string `json:"key_prefix"`
	VersionID string `json:"version_id"`
	MountPath string `json:"mount_path"` // e.g. /mnt/kv/<dataset>/v<N>
}

// CatalogFile is the top-level catalog document written to catalog.json.
// The KV server expects this format: {"entries":[...]}.
type CatalogFile struct {
	Entries []CatalogEntry `json:"entries"`
}

// DatasetPhase is the node-local lifecycle phase of one dataset.
type DatasetPhase string

const (
	// PhaseAttaching: disk is being attached and the block device is not yet visible.
	PhaseAttaching DatasetPhase = "attaching"
	// PhaseMounting: block device is present; mount syscall in progress.
	PhaseMounting DatasetPhase = "mounting"
	// PhaseActive: mounted and catalog.json written; KV server has acknowledged the version.
	PhaseActive DatasetPhase = "active"
	// PhaseUnmounting: previous version is being unmounted and its disk detached.
	PhaseUnmounting DatasetPhase = "unmounting"
	// PhaseError: an operation failed; Error field contains details.
	PhaseError DatasetPhase = "error"
)

// DatasetState is the node-local state of one dataset at a point in time.
type DatasetState struct {
	Dataset   string       `json:"dataset"`
	KeyPrefix string       `json:"key_prefix,omitempty"`
	VersionID string       `json:"version_id"`
	Phase     DatasetPhase `json:"phase"`
	PVName    string       `json:"pv_name,omitempty"`
	MountPath string       `json:"mount_path,omitempty"`
	Error     string       `json:"error,omitempty"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// NodeState is the complete state of all datasets on a node.  The node-agent
// reports this periodically; the control-plane diffs it against the desired
// assignments and drives convergence.  Reporting full state (rather than
// individual events) means a missed report never causes permanent divergence —
// the next report self-heals.
type NodeState struct {
	Node       string         `json:"node"`
	Datasets   []DatasetState `json:"datasets"`
	ReportedAt time.Time      `json:"reported_at"`
}

// NodeAssignment carries the active VersionRecord for one dataset as returned
// by the control-plane watched API.
type NodeAssignment struct {
	Dataset   string        `json:"dataset"`
	KeyPrefix string        `json:"key_prefix"`
	Version   VersionRecord `json:"version"`
}

// AssignmentsResponse is the body returned by
// GET /api/v1/node/{node-name}/assignments.
// Generation increments on every change; node-agent passes it back as
// ?generation=N to block until the next change.
type AssignmentsResponse struct {
	Generation  int64            `json:"generation"`
	Assignments []NodeAssignment `json:"assignments"`
}

// KVDatasetVersion is one entry in the response from GET /version on the KV
// server.  The node-agent polls this endpoint after writing catalog.json and
// waits until the reported version matches the desired one before detaching
// the old disk.  If the KV server is unreachable the call fails, which is
// itself a signal that the swap is not yet complete.
type KVDatasetVersion struct {
	Dataset   string    `json:"dataset"`
	VersionID string    `json:"version_id"`
	LoadedAt  time.Time `json:"loaded_at"`
}

// KVVersionResponse is the body returned by GET /version on the KV server.
type KVVersionResponse struct {
	Datasets []KVDatasetVersion `json:"datasets"`
}
