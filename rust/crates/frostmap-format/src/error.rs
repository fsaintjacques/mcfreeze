#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("n_partitions must be a non-zero power of two, got {0}")]
    InvalidPartitionCount(u32),

    #[error(
        "meta.json bit layout ({offset_bits}+{size_bits}+{psl_bits}) \
         does not match compiled constants"
    )]
    LayoutMismatch {
        offset_bits: u8,
        size_bits:   u8,
        psl_bits:    u8,
    },

    #[error("value too large: {size} bytes exceeds maximum")]
    ValueTooLarge { size: usize },

    #[error("aligned offset {0} overflows the offset bit field")]
    OffsetOverflow(u64),

    #[error("invalid magic bytes in index header")]
    InvalidMagic,

    #[error("format version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u32, got: u32 },

    #[error("PSL overflow at {psl}: hash table is full (n_keys={n_keys}, n_buckets={n_buckets}); reduce fill rate or increase PSL_BITS")]
    PslOverflow { psl: u8, n_keys: usize, n_buckets: usize },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
