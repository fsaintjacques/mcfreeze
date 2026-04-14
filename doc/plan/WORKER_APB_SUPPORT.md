# Worker Multi-Source & Protobuf Encoding Support

## Goal

Evolve `DatasetSpec` from a BigQuery-only flat config into a multi-source,
multi-encoding system. Sources describe *where* data comes from (BigQuery,
CSV, future: GCS/S3). Encoding describes *how* values are produced — either
raw (single column as bytes) or protobuf (multiple Arrow columns transcoded
via `apb-core`). The two concerns are orthogonal: any source can pair with
any encoding.

## Existing foundation

The Rust loader already has a clean `KvSource` trait and two implementations:
`BqStreamSource` (BigQuery → Arrow → key/value pairs) and `CsvSource`
(base64 CSV → key/value pairs). The Go control-plane defines `DatasetSpec`
with a flat `SourceConfig` struct, but it is not consumed anywhere beyond
type definitions and test fixtures.

The `apb-core` library (sibling project) provides Arrow → protobuf
transcoding: `Transcoder::transcode_arrow(&RecordBatch) → BinaryArray`.
It can also auto-generate a proto descriptor from an Arrow schema via
`generate_file_descriptor()`.

## Design

### API types (`go/api/types.go`)

Replace the flat `SourceConfig` with discriminated unions using the K8s
"one-non-nil-pointer-field" pattern:

```go
type DatasetSpec struct {
    Name       string        `json:"name"`
    KeyPrefix  string        `json:"key_prefix"`
    Source     SourceSpec    `json:"source"`
    Encoding   *EncodingSpec `json:"encoding,omitempty"` // nil → raw
    ShardCount int           `json:"shard_count"`
    Retention  int           `json:"retention"`
}

type SourceSpec struct {
    BigQuery *BigQuerySource `json:"bigquery,omitempty"`
}

type BigQuerySource struct {
    Project        string `json:"project"`
    Table          string `json:"table"`
    KeyColumn      string `json:"key_column"`
    ValueColumn    string `json:"value_column,omitempty"`    // required when raw
    RowRestriction string `json:"row_restriction,omitempty"`
}

type EncodingSpec struct {
    Protobuf *ProtobufEncoding `json:"protobuf,omitempty"`
}

type ProtobufEncoding struct {
    Descriptor  string `json:"descriptor,omitempty"`     // base64 FileDescriptorSet; empty → auto-generate
    MessageName string `json:"message_name,omitempty"`   // required when Descriptor is set
}
```

YAML examples for CRD:

```yaml
# Raw — single column value (current behavior)
source:
  bigquery:
    project: my-project
    table: project.dataset.table
    keyColumn: user_id
    valueColumn: payload

# Protobuf — explicit descriptor
source:
  bigquery:
    project: my-project
    table: project.dataset.table
    keyColumn: user_id
encoding:
  protobuf:
    descriptor: "CgdmaXh0dXJl..."
    messageName: "mypackage.User"

# Protobuf — auto-generated from Arrow schema
source:
  bigquery:
    project: my-project
    table: project.dataset.table
    keyColumn: user_id
encoding:
  protobuf: {}
```

### Config passing: Go → Rust worker

The Go operator serializes `DatasetSpec` to JSON, creates a ConfigMap, and
mounts it as a volume in the worker Job at a well-known path
(`/etc/mcfreeze/config.json`). This is the standard K8s operator pattern
(Spark Operator, Flink Operator, Strimzi, Prometheus Operator all do this).

The Rust worker reads the config via `mcf load config --config <path>`.
Same binary, same flag for local dev — no K8s dependency.

### Rust pipeline architecture

`mcfreeze-loader` stays Arrow-free. A new `mcfreeze-encode` crate bridges
Arrow sources with protobuf encoding.

**Raw encoding (unchanged):**
```
BqReadSession → BqStreamSource (KvSource<ArrowBatch>) → SnapshotLoader
```

**Protobuf encoding (new):**
```
BqReadSession → BqRecordBatchSource → ProtobufEncodingSource (KvSource) → SnapshotLoader
                                              ↑
                                       apb Transcoder
```

Key new types:

- **`BqRecordBatchSource`** (in `mcfreeze-bq`): same gRPC streaming as
  `BqStreamSource` but yields full `RecordBatch` without key/value
  extraction. Add `BqReadSession::into_record_batch_sources()` and
  `schema()` accessor.

- **`ProtobufEncodingSource<S>`** (in `mcfreeze-encode`): wraps any source
  that yields `RecordBatch`es, extracts the key column, transcodes remaining
  columns via `apb Transcoder`, yields `(key_bytes, proto_bytes)` as a
  `KvSource`. One extra copy vs the zero-copy raw path, but unavoidable
  since protobuf bytes must be materialized.

- **Config serde types** (in `mcfreeze-encode`): `WorkerConfig`,
  `SourceSpec`, `EncodingSpec` — wire-compatible with Go's JSON serialization.

### Auto-generate flow

When `encoding.protobuf` is set but `descriptor` is empty:

1. `BqReadSession::open()` → get Arrow schema
2. Remove key column from schema
3. `apb_core::generate::generate_file_descriptor(&schema, "mcfreeze", "Value")`
4. Build `ProtoSchema` from generated descriptor
5. `apb_core::mapping::infer_mapping(&schema, &msg, &InferOptions::default())`
6. `Transcoder::new(&mapping)`

This requires the source to be opened before the transcoder is built —
fine because `BqReadSession::open()` already fetches the schema.

## Phases

### Phase 1 — Go API types

Restructure `go/api/types.go`: replace `SourceConfig` with `SourceSpec`,
`BigQuerySource`, `EncodingSpec`, `ProtobufEncoding`. Update test fixtures
that construct `DatasetSpec`. No behavioral change.

**Exit criterion:** All existing Go tests pass with the new type hierarchy.

### Phase 2 — `mcfreeze-bq` RecordBatch source

Add `BqRecordBatchSource` to `mcfreeze-bq`: same streaming logic as
`BqStreamSource` but yields `RecordBatch` directly. Add
`BqReadSession::into_record_batch_sources()` and `schema()`. Make
`BinaryCol` public for reuse.

**Exit criterion:** `BqRecordBatchSource` compiles, existing `BqStreamSource`
tests unchanged.

### Phase 3 — `mcfreeze-encode` crate

New crate with:
- `config.rs` — serde config types (wire-compatible with Go JSON)
- `adapter.rs` — `ProtobufEncodingSource` implementing `KvSource`
- `builder.rs` — factory: config + Arrow schema → `Transcoder`
  (handles both explicit descriptor and auto-generate paths)
- `error.rs` — wraps `apb TranscodeError` and Arrow errors

**Exit criterion:** Unit tests with synthetic Arrow batches: build a
`ProtobufEncodingSource` with auto-generated descriptor, verify output
key-value pairs contain valid protobuf.

### Phase 4 — CLI config-driven load

Add `Config` variant to the CLI `Source` enum in `mcfreeze-cli/src/load.rs`.
Reads JSON config file, dispatches to raw or protobuf pipeline based on
encoding spec.

**Exit criterion:** `mcf load config --config test.json --output /tmp/snap`
produces a valid snapshot from a BigQuery table with protobuf-encoded values.
Values decode correctly with `protoc --decode_raw`.
