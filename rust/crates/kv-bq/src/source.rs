use std::future::Future;

use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use bytes::Bytes;
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    ReadRowsRequest, ReadRowsResponse,
    read_rows_response::Rows,
};
use tonic::Streaming;

use kv_loader::{KvSource, SourceMetadata};

use crate::{
    batch::ArrowBatch,
    error::BqError,
    session::{BqApi, prime_decoder},
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
    client:       BqApi,
    stream_name:  String,
    key_col_idx:  usize,
    val_col_idx:  usize,
    decoder:      StreamDecoder,
    state:        ReadState,
}

enum ReadState {
    NotStarted,
    Reading(Streaming<ReadRowsResponse>),
    Done,
}

// StreamDecoder holds Arrow buffers (Arc-backed, immutable after creation).
// tonic::Streaming<T> is Send when T: Send; ReadRowsResponse is a prost message (Send).
unsafe impl Send for BqStreamSource {}

impl BqStreamSource {
    pub(crate) fn new(
        client:       BqApi,
        stream_name:  String,
        key_col_idx:  usize,
        val_col_idx:  usize,
        schema_bytes: Bytes,
    ) -> Result<Self, BqError> {
        Ok(Self {
            client,
            stream_name,
            key_col_idx,
            val_col_idx,
            decoder: prime_decoder(&schema_bytes)?,
            state:   ReadState::NotStarted,
        })
    }
}

impl KvSource for BqStreamSource {
    type Batch = ArrowBatch;
    type Error = BqError;

    fn next_batch(&mut self) -> impl Future<Output = Result<Option<ArrowBatch>, BqError>> + Send {
        async move {
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
                self.state = ReadState::Reading(stream);
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
                    decode_batch(response, &mut self.decoder, self.key_col_idx, self.val_col_idx)
                        .map(Some)
                }
            }
        }
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata::default()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn decode_batch(
    response:    ReadRowsResponse,
    decoder:     &mut StreamDecoder,
    key_col_idx: usize,
    val_col_idx: usize,
) -> Result<ArrowBatch, BqError> {
    let arrow_batch = match response.rows {
        Some(Rows::ArrowRecordBatch(b)) => b,
        Some(Rows::AvroRows(_)) => {
            return Err(BqError::Schema(
                "received Avro rows; session must be created with DataFormat::Arrow".into(),
            ))
        }
        None => {
            return Err(BqError::Schema(
                "ReadRowsResponse contained no rows".into(),
            ))
        }
    };

    let mut buf = Buffer::from(arrow_batch.serialized_record_batch.as_slice());
    let record_batch = decoder
        .decode(&mut buf)?
        .ok_or_else(|| BqError::Schema(
            "IPC decoder returned no batch after consuming batch bytes".into(),
        ))?;

    ArrowBatch::from_record_batch(&record_batch, key_col_idx, val_col_idx)
}
