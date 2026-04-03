#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("n_partitions must be a non-zero power of two, got {0}")]
    InvalidPartitionCount(u32),

    #[error("aligned offset {0} overflows u32 (256 GB per partition limit)")]
    OffsetOverflow(u64),

    #[error("format version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u32, got: u32 },

    #[error("PSL overflow at {psl}: hash table is full (n_keys={n_keys}, n_buckets={n_buckets}); reduce fill rate")]
    PslOverflow {
        psl: u8,
        n_keys: usize,
        n_buckets: usize,
    },

    #[error("index metadata length mismatch: expected {expected} partitions, got {got_offsets} offsets and {got_buckets} bucket counts")]
    InvalidIndexMetadata {
        expected: usize,
        got_offsets: usize,
        got_buckets: usize,
    },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
