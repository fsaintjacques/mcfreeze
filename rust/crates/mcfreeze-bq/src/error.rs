// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BqError {
    #[error("gcloud-sdk error: {0}")]
    Sdk(#[from] gcloud_sdk::error::Error),

    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("schema error: {0}")]
    Schema(String),
}
