use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use gcloud_sdk::{GoogleApi, GoogleAuthMiddleware};
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    big_query_read_client::BigQueryReadClient,
    read_session::{Schema as SessionSchema, TableReadOptions},
    arrow_serialization_options::CompressionCodec,
    ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadSession,
};

/// gRPC max decoding message size — BQ responses can reach 128 MiB.
const MAX_DECODING_BYTES: usize = 256 * 1024 * 1024;

use kv_loader::SourceMetadata;

use crate::{BqStreamSource, error::BqError};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

pub(crate) type BqApi = GoogleApi<BigQueryReadClient<GoogleAuthMiddleware>>;

// ---------------------------------------------------------------------------
// BqSourceConfig
// ---------------------------------------------------------------------------

pub struct BqSourceConfig {
    /// BigQuery project that owns the table (used for billing and parent path).
    pub project: String,

    /// Fully-qualified table resource:
    /// `"projects/{project}/datasets/{dataset}/tables/{table}"`.
    pub table: String,

    /// Arrow column name whose bytes become the KV key.
    pub key_column: String,

    /// Arrow column name whose bytes become the KV value.
    pub value_column: String,

    /// Hint for the number of parallel read streams.
    /// BQ may return fewer; call [`BqReadSession::n_streams`] for the actual count.
    pub n_streams: i32,

    /// Optional SQL `WHERE` predicate pushed down to BQ.
    pub row_restriction: Option<String>,
}

// ---------------------------------------------------------------------------
// BqReadSession
// ---------------------------------------------------------------------------

/// A BigQuery Storage Read session.
///
/// Created via [`BqReadSession::open`]; call [`into_sources`] to split into
/// one [`BqStreamSource`] per BQ read stream, then pass those to
/// [`SnapshotLoader::load_parallel`].
pub struct BqReadSession {
    pub(crate) client:       BqApi,
    pub(crate) streams:      Vec<String>,
    pub(crate) schema_bytes: Bytes,
    pub(crate) key_col_idx:  usize,
    pub(crate) val_col_idx:  usize,
    metadata:    SourceMetadata,
}

impl BqReadSession {
    /// Create a read session using Application Default Credentials.
    ///
    /// Discovers credentials from the environment in order:
    /// 1. `GOOGLE_APPLICATION_CREDENTIALS` service-account JSON
    /// 2. gcloud CLI credentials (`~/.config/gcloud/`)
    /// 3. GKE / GCE Workload Identity metadata server
    pub async fn open(config: BqSourceConfig) -> Result<Self, BqError> {
        let client: BqApi = GoogleApi::from_function(
            |ch| BigQueryReadClient::new(ch).max_decoding_message_size(MAX_DECODING_BYTES),
            "https://bigquerystorage.googleapis.com",
            None::<String>,
        )
        .await?;

        // Request LZ4_FRAME buffer compression: Arrow column buffers are
        // compressed per-buffer inside the IPC RecordBatch message.  This is
        // separate from gRPC-level compression; arrow-ipc decompresses
        // transparently via the `ipc_compression` feature.
        let arrow_serial_opts = ArrowSerializationOptions {
            buffer_compression: CompressionCodec::Lz4Frame as i32,
            ..Default::default()
        };

        let read_options = Some(TableReadOptions {
            row_restriction: config.row_restriction.unwrap_or_default(),
            output_format_serialization_options: Some(
                gcloud_sdk::google::cloud::bigquery::storage::v1::read_session::table_read_options
                    ::OutputFormatSerializationOptions::ArrowSerializationOptions(arrow_serial_opts),
            ),
            ..Default::default()
        });

        let request = CreateReadSessionRequest {
            parent: format!("projects/{}", config.project),
            read_session: Some(ReadSession {
                table:        config.table,
                data_format:  DataFormat::Arrow as i32,
                read_options,
                ..Default::default()
            }),
            max_stream_count:           config.n_streams,
            preferred_min_stream_count: config.n_streams,
        };

        let session = client.get().create_read_session(request).await?.into_inner();

        let schema_bytes = match session.schema {
            Some(SessionSchema::ArrowSchema(ref s)) => Bytes::copy_from_slice(&s.serialized_schema),
            Some(SessionSchema::AvroSchema(_)) => {
                return Err(BqError::Schema("session returned Avro schema; request must use DataFormat::Arrow".into()));
            }
            None => return Err(BqError::Schema("server did not return a schema".into())),
        };

        let arrow_schema = decode_schema(&schema_bytes)?;

        let key_col_idx = arrow_schema
            .index_of(&config.key_column)
            .map_err(|_| BqError::Schema(format!("key column {:?} not found", config.key_column)))?;
        let val_col_idx = arrow_schema
            .index_of(&config.value_column)
            .map_err(|_| BqError::Schema(format!("value column {:?} not found", config.value_column)))?;

        let streams: Vec<String> = session.streams.into_iter().map(|s| s.name).collect();

        let metadata = SourceMetadata {
            estimated_rows:  (session.estimated_row_count > 0)
                .then_some(session.estimated_row_count as u64),
            estimated_bytes: (session.estimated_total_bytes_scanned > 0)
                .then_some(session.estimated_total_bytes_scanned as u64),
        };

        Ok(Self { client, streams, schema_bytes, key_col_idx, val_col_idx, metadata })
    }

    /// Number of read streams returned by BQ (may differ from the requested hint).
    pub fn n_streams(&self) -> usize {
        self.streams.len()
    }

    pub fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Consume the session and return one [`BqStreamSource`] per stream.
    ///
    /// Pass the returned `Vec` directly to [`SnapshotLoader::load_parallel`].
    pub fn into_sources(self) -> Result<Vec<BqStreamSource>, BqError> {
        self.streams
            .into_iter()
            .map(|name| {
                BqStreamSource::new(
                    self.client.clone(),
                    name,
                    self.key_col_idx,
                    self.val_col_idx,
                    self.schema_bytes.clone(),
                )
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse the IPC schema message bytes from the BQ session into an Arrow schema.
///
/// `StreamReader::try_new` reads the schema message and stops; we never call
/// `.next()` on the reader so no data batches are required.
pub(crate) fn decode_schema(schema_bytes: &Bytes) -> Result<arrow::datatypes::Schema, BqError> {
    let reader = StreamReader::try_new(
        std::io::Cursor::new(schema_bytes.as_ref()),
        None,
    )?;
    Ok(reader.schema().as_ref().clone())
}

/// Initialise a `StreamDecoder` by feeding it the schema IPC message so it
/// can decode record batches without needing the schema in-band.
pub(crate) fn prime_decoder(schema_bytes: &Bytes) -> Result<arrow::ipc::reader::StreamDecoder, BqError> {
    let mut decoder = arrow::ipc::reader::StreamDecoder::new();
    let mut buf = Buffer::from(schema_bytes.as_ref());
    decoder.decode(&mut buf)?;
    Ok(decoder)
}
