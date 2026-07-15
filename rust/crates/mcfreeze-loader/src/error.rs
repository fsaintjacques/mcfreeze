// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error("format error: {0}")]
    Format(#[from] mcfreeze_format::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("source error: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),

    #[error("thread pool error: {0}")]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error(
        "snapshot directory holds {found} scatter state but --format {requested} was requested; \
         delete the directory or rerun with --format {found}"
    )]
    FormatMismatch {
        requested: mcfreeze_format::FormatId,
        found: mcfreeze_format::FormatId,
    },

    #[error(
        "value for key {key:?} is {len} bytes, exceeding --max-value-bytes {max}; \
         raise the limit or fix the source column mapping"
    )]
    ValueTooLarge {
        /// Lossy UTF-8 preview of the key, truncated for display.
        key: String,
        len: usize,
        max: usize,
    },
}
