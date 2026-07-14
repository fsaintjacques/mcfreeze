// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("n_partitions must be a non-zero power of two, got {0}")]
    InvalidPartitionCount(u32),

    #[error("aligned offset {0} overflows u32 (256 GB per partition limit)")]
    OffsetOverflow(u64),

    #[error("PSL overflow at {psl}: hash table is full (n_keys={n_keys}, n_buckets={n_buckets}); reduce fill rate")]
    PslOverflow {
        psl: u8,
        n_keys: usize,
        n_buckets: usize,
    },

    #[error(
        "partition contains {max_count} records sharing a single 32-bit fingerprint \
         (max tolerated: {max_tolerated}); the key column is likely not unique"
    )]
    DuplicateFingerprints {
        max_count: usize,
        max_tolerated: usize,
    },

    #[error("unsupported hash algorithm: {0}")]
    UnsupportedHashAlgorithm(String),

    #[error("unsupported format version: {0}")]
    UnsupportedFormatVersion(u32),

    #[error("unknown format: {got:?} (expected one of: {expected})")]
    UnknownFormat { got: String, expected: String },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
