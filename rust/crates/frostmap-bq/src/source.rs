use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use bytes::Bytes;
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    read_rows_response::Rows, ReadRowsRequest, ReadRowsResponse,
};
use tonic::Streaming;

use arrow::record_batch::RecordBatch;

use frostmap_loader::{KvSource, SourceMetadata};

use crate::{
    batch::ArrowBatch,
    error::BqError,
    session::{prime_decoder, BqApi},
};

// ---------------------------------------------------------------------------
// BqStreamSource
// ---------------------------------------------------------------------------

/// A single BigQuery read stream implementing [`KvSource`].
///
/// Created by [`BqReadSession::into_sources`]. Each source reads from its own
/// independent BQ stream; multiple sources can be driven concurrently via
/// [`SnapshotLoader::load_parallel`].
pub struct BqStreamSource {
    client: BqApi,
    stream_name: String,
    key_col_idx: usize,
    val_col_idx: usize,
    decoder: StreamDecoder,
    state: ReadState,
}

enum ReadState {
    NotStarted,
    Reading(Box<Streaming<ReadRowsResponse>>),
    Done,
}

// StreamDecoder holds Arrow buffers (Arc-backed, immutable after creation).
// tonic::Streaming<T> is Send when T: Send; ReadRowsResponse is a prost message (Send).
unsafe impl Send for BqStreamSource {}

impl BqStreamSource {
    pub(crate) fn new(
        client: BqApi,
        stream_name: String,
        key_col_idx: usize,
        val_col_idx: usize,
        schema_bytes: Bytes,
    ) -> Result<Self, BqError> {
        Ok(Self {
            client,
            stream_name,
            key_col_idx,
            val_col_idx,
            decoder: prime_decoder(&schema_bytes)?,
            state: ReadState::NotStarted,
        })
    }
}

impl KvSource for BqStreamSource {
    type Batch = ArrowBatch;
    type Error = BqError;

    async fn next_batch(&mut self) -> Result<Option<ArrowBatch>, BqError> {
        // On first call, start the BQ read-rows streaming RPC.
        if matches!(self.state, ReadState::NotStarted) {
            let stream = self
                .client
                .get()
                .read_rows(ReadRowsRequest {
                    read_stream: self.stream_name.clone(),
                    offset: 0,
                })
                .await?
                .into_inner();
            self.state = ReadState::Reading(Box::new(stream));
        }

        // `mem::replace` takes ownership of the stream without holding a
        // borrow of `self.state` across the `.await` on `stream.message()`.
        let prev = std::mem::replace(&mut self.state, ReadState::Done);
        let ReadState::Reading(mut stream) = prev else {
            return Ok(None); // Done state
        };

        match stream.message().await? {
            None => Ok(None), // stream exhausted; state remains Done

            Some(response) => {
                self.state = ReadState::Reading(stream);
                decode_batch(
                    response,
                    &mut self.decoder,
                    self.key_col_idx,
                    self.val_col_idx,
                )
                .map(Some)
            }
        }
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata::default()
    }
}

// ---------------------------------------------------------------------------
// BqRecordBatchSource
// ---------------------------------------------------------------------------

/// A single BigQuery read stream that yields full Arrow `RecordBatch`es.
///
/// Unlike [`BqStreamSource`], this does not extract key/value columns —
/// the caller receives the complete batch for downstream processing
/// (e.g. protobuf encoding via apb).
pub struct BqRecordBatchSource {
    client: BqApi,
    stream_name: String,
    decoder: StreamDecoder,
    state: ReadState,
}

unsafe impl Send for BqRecordBatchSource {}

impl BqRecordBatchSource {
    pub(crate) fn new(
        client: BqApi,
        stream_name: String,
        schema_bytes: Bytes,
    ) -> Result<Self, BqError> {
        Ok(Self {
            client,
            stream_name,
            decoder: prime_decoder(&schema_bytes)?,
            state: ReadState::NotStarted,
        })
    }

    /// Read the next Arrow `RecordBatch` from the stream.
    ///
    /// Returns `Ok(None)` when the stream is exhausted.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>, BqError> {
        if matches!(self.state, ReadState::NotStarted) {
            let stream = self
                .client
                .get()
                .read_rows(ReadRowsRequest {
                    read_stream: self.stream_name.clone(),
                    offset: 0,
                })
                .await?
                .into_inner();
            self.state = ReadState::Reading(Box::new(stream));
        }

        let prev = std::mem::replace(&mut self.state, ReadState::Done);
        let ReadState::Reading(mut stream) = prev else {
            return Ok(None);
        };

        match stream.message().await? {
            None => Ok(None),
            Some(response) => {
                self.state = ReadState::Reading(stream);
                decode_record_batch(response, &mut self.decoder).map(Some)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Decode a `ReadRowsResponse` into a raw Arrow `RecordBatch`.
fn decode_record_batch(
    response: ReadRowsResponse,
    decoder: &mut StreamDecoder,
) -> Result<RecordBatch, BqError> {
    let arrow_batch = match response.rows {
        Some(Rows::ArrowRecordBatch(b)) => b,
        Some(Rows::AvroRows(_)) => {
            return Err(BqError::Schema(
                "received Avro rows; session must be created with DataFormat::Arrow".into(),
            ))
        }
        None => return Err(BqError::Schema("ReadRowsResponse contained no rows".into())),
    };

    let mut buf = Buffer::from(arrow_batch.serialized_record_batch.as_slice());
    decoder.decode(&mut buf)?.ok_or_else(|| {
        BqError::Schema("IPC decoder returned no batch after consuming batch bytes".into())
    })
}

fn decode_batch(
    response: ReadRowsResponse,
    decoder: &mut StreamDecoder,
    key_col_idx: usize,
    val_col_idx: usize,
) -> Result<ArrowBatch, BqError> {
    let record_batch = decode_record_batch(response, decoder)?;
    ArrowBatch::from_record_batch(&record_batch, key_col_idx, val_col_idx)
}
