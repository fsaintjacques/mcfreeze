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

    #[error("record encoding of {encoded} bytes exceeds block capacity {capacity}")]
    RecordTooLarge { encoded: usize, capacity: usize },

    #[error("value of {len} bytes exceeds the 30-bit length field (max {max})")]
    ValueTooLarge { len: u64, max: u32 },

    #[error("block assembler used after a sink error")]
    AssemblerPoisoned,

    #[error("block_size must be a power of two >= 4096, got {0}")]
    InvalidBlockSize(u32),

    #[error("n_blocks {0} overflows the addressable partition size")]
    InvalidBlockCount(u64),

    #[error("heap value checksum mismatch")]
    ValueChecksumMismatch,

    #[error("compressed record in a snapshot that declares no compression codec")]
    CompressedValueWithoutCodec,

    #[error("corrupt compressed value: {0}")]
    CorruptFrame(&'static str),

    #[error("compressed value claims {got} bytes, exceeding the value limit ({max})")]
    FrameContentTooLarge { got: u64, max: u32 },

    #[error("value decompression failed: {0}")]
    Decompress(#[source] std::io::Error),

    #[error(
        "compression dictionary checksum mismatch: got {got:#010x}, expected {expected:#010x}"
    )]
    DictChecksumMismatch { got: u32, expected: u32 },

    #[error("compression dictionary training failed: {0}")]
    DictTrain(#[source] std::io::Error),

    #[error(
        "v5.plan pins dictionary checksum {expected:#010x} but dict.bin is unreadable: {source} \
         (delete v5.plan and dict.bin to retrain from scratch)"
    )]
    DictMissing {
        expected: u32,
        source: std::io::Error,
    },

    #[error("zstd context setup failed: {0}")]
    Zstd(#[source] std::io::Error),

    #[error("unsupported compression codec: {0:?}")]
    UnsupportedCodec(String),

    #[error("invalid meta.compression: {0}")]
    InvalidCompressionMeta(&'static str),

    #[error("meta.compression requires dict.bin, which cannot be read: {0}")]
    DictUnreadable(#[source] std::io::Error),

    #[error("sketch construction failed: {0}")]
    SketchBuild(&'static str),

    #[error("corrupt sketch: {0}")]
    CorruptSketch(&'static str),

    #[error("unsupported sketch kind: {0:?}")]
    UnsupportedSketchKind(String),

    #[error("partition {partition}: {file} is {got} bytes, expected {expected}")]
    SnapshotFileSize {
        partition: usize,
        file: &'static str,
        got: u64,
        expected: u64,
    },

    #[error("partition {partition}: cannot read {file}: {source}")]
    SnapshotFileRead {
        partition: usize,
        file: &'static str,
        source: std::io::Error,
    },

    #[error("block checksum mismatch")]
    BlockChecksumMismatch,

    #[error("corrupt block: {0}")]
    CorruptBlock(&'static str),

    #[error("unsupported hash algorithm: {0}")]
    UnsupportedHashAlgorithm(String),

    #[error("unsupported format version: {0}")]
    UnsupportedFormatVersion(u32),

    #[error("unknown format: {got:?} (expected one of: {expected})")]
    UnknownFormat { got: String, expected: String },

    #[error("finalize called before build")]
    FinalizeBeforeBuild,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
