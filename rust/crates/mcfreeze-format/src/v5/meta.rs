// SPDX-License-Identifier: Apache-2.0

//! V5 `meta.json` payload. Written last by the builder as the snapshot
//! completeness sentinel; parsed by `SnapshotDesc::load` once the
//! version probe commits to V5.

use serde::{Deserialize, Serialize};

use crate::{
    meta::{Layout, Stats, HASH_ALGORITHM},
    Error, Result,
};

pub const FORMAT_VERSION: u32 = 5;

/// Smallest legal `block_size`: the physical I/O floor (page cache
/// granularity, NVMe sector). The auto-tune clamp's lower edge.
pub const MIN_BLOCK_SIZE: u32 = 4096;
/// Auto-tune clamp's upper edge. `--block-size` may exceed it.
pub const MAX_AUTO_BLOCK_SIZE: u32 = 64 * 1024;

/// Optional per-partition filter over key fingerprints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SketchMeta {
    pub kind: String,
}

/// The only compression codec (`meta.compression.codec`).
pub const CODEC_ZSTD: &str = "zstd";

/// Transparent value compression (`doc/plan/V5_COMPRESSION.md`).
/// Absent = values stored verbatim. Declares the snapshot's codec and
/// dictionary once; the per-record bit only selects between this codec
/// and the raw fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionMeta {
    /// Codec name; unknown codecs fail at `SnapshotDesc::load` (same
    /// posture as sketch kinds).
    pub codec: String,
    /// True when decoding requires the learned dictionary (`dict.bin`).
    #[serde(default)]
    pub dict: bool,
    /// `checksum32(dict.bin)`, required when `dict`: a corrupt
    /// dictionary decompresses cleanly into wrong values, so its
    /// integrity is anchored here — keeping `dict.bin` raw ZDICT output
    /// that standard zstd tooling can use directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dict_checksum: Option<u32>,
    /// Writer-side knob, recorded for provenance only.
    #[serde(default)]
    pub min_compress_len: u32,
    /// The vendored libzstd that trained the dictionary and compressed
    /// the values — provenance for ratio drift across toolchain bumps
    /// (decoding is version-compatible; trained bytes are not).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zstd_version: Option<String>,
}

/// Per-partition control data. `n_blocks` is derivable from
/// `len(fences.bin) / 4`; kept explicit to catch truncation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    pub n_blocks: u64,
    /// `checksum32(sketch.bin)` for this partition; `Some` iff a sketch
    /// was written — the filter is enabled and the partition is
    /// non-empty (`n_blocks > 0`). The filter bytes carry no trailer of
    /// their own: a flipped fingerprint is a silent false negative, so
    /// the integrity anchor lives here in the manifest, exactly like
    /// [`CompressionMeta::dict_checksum`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sketch_checksum: Option<u32>,
}

/// Contents of a V5 `meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub format_version: u32,
    pub hash_algorithm: String,
    pub verify_seed: u64,
    /// Power-of-two multiple of 4 KiB. One block = one `pread`.
    pub block_size: u32,
    /// Values ≤ threshold are inline; larger ones live in `heap.bin`
    /// behind a stub. Defaults to `block_size / 2` at build time.
    pub inline_threshold: u32,
    /// `None` when the filter is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sketch: Option<SketchMeta>,
    /// `None` when values are stored verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<CompressionMeta>,
    pub partitions: Vec<PartitionMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<Stats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<serde_json::Value>,
}

impl Meta {
    /// Validate hash algorithm, block size, and partition count; return
    /// the derived [`Layout`]. Version dispatch is not re-checked here —
    /// it has a single owner, the probe in `SnapshotDesc::load`.
    pub fn layout(&self) -> Result<Layout> {
        if self.hash_algorithm != HASH_ALGORITHM {
            return Err(Error::UnsupportedHashAlgorithm(self.hash_algorithm.clone()));
        }
        validate_block_size(self.block_size)?;
        if let Some(s) = &self.sketch {
            if s.kind != crate::v5::sketch::KIND {
                return Err(Error::UnsupportedSketchKind(s.kind.clone()));
            }
        }
        // Coherence both ways: the reader loads (and must verify) a
        // sketch exactly when the filter is enabled and the partition is
        // non-empty, so `sketch_checksum` must be present there and
        // absent everywhere else — a stray or missing anchor fails the
        // open, never a silently unverified filter.
        for pm in &self.partitions {
            let sketch_written = self.sketch.is_some() && pm.n_blocks > 0;
            match (sketch_written, pm.sketch_checksum.is_some()) {
                (true, false) => {
                    return Err(Error::InvalidSketchMeta(
                        "non-empty partition with the filter enabled requires sketch_checksum",
                    ))
                }
                (false, true) => {
                    return Err(Error::InvalidSketchMeta(
                        "sketch_checksum present without a written sketch",
                    ))
                }
                _ => {}
            }
        }
        if let Some(c) = &self.compression {
            if c.codec != CODEC_ZSTD {
                return Err(Error::UnsupportedCodec(c.codec.clone()));
            }
            // Coherence both ways: the reader keys the dictionary load
            // on `dict`, so a checksum without the flag (or vice versa)
            // must fail here, not silently half-apply.
            if c.dict && c.dict_checksum.is_none() {
                return Err(Error::InvalidCompressionMeta(
                    "dict: true requires dict_checksum",
                ));
            }
            if !c.dict && c.dict_checksum.is_some() {
                return Err(Error::InvalidCompressionMeta(
                    "dict_checksum requires dict: true",
                ));
            }
        }
        // Bound n_blocks so `n_blocks × block_size` (and a fortiori
        // `n_blocks × 4` for fences.bin) cannot overflow u64: a corrupt
        // meta.json must fail here with a typed error, not wrap the
        // reader's open-time size checks into accepting garbage.
        for pm in &self.partitions {
            if pm.n_blocks.checked_mul(self.block_size as u64).is_none() {
                return Err(Error::InvalidBlockCount(pm.n_blocks));
            }
        }
        Layout::new(self.partitions.len() as u32)
    }
}

/// `block_size` contract shared by meta validation and the `--block-size`
/// build override: a power of two, at least [`MIN_BLOCK_SIZE`].
pub fn validate_block_size(block_size: u32) -> Result<()> {
    if !block_size.is_power_of_two() || block_size < MIN_BLOCK_SIZE {
        return Err(Error::InvalidBlockSize(block_size));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::DEFAULT_VERIFY_SEED;

    fn test_meta(n_partitions: u32) -> Meta {
        Meta {
            format_version: FORMAT_VERSION,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            verify_seed: DEFAULT_VERIFY_SEED,
            block_size: 4096,
            inline_threshold: 2048,
            sketch: None,
            compression: None,
            partitions: (0..n_partitions)
                .map(|_| PartitionMeta {
                    n_blocks: 0,
                    sketch_checksum: None,
                })
                .collect(),
            stats: None,
            encoding: None,
        }
    }

    #[test]
    fn layout_roundtrip() {
        assert_eq!(test_meta(8).layout().unwrap().n_partitions, 8);
    }

    #[test]
    fn rejects_bad_hash_algorithm() {
        let mut m = test_meta(1);
        m.hash_algorithm = "sha256".into();
        assert!(m.layout().is_err());
    }

    #[test]
    fn rejects_overflowing_n_blocks() {
        // A corrupt meta.json with a huge n_blocks must fail at load
        // with a typed error, not wrap the reader's size arithmetic.
        let mut m = test_meta(1);
        m.partitions[0].n_blocks = u64::MAX / 2;
        assert!(matches!(
            m.layout(),
            Err(Error::InvalidBlockCount(n)) if n == u64::MAX / 2
        ));
    }

    #[test]
    fn rejects_bad_block_size() {
        for bad in [0u32, 512, 4095, 5000, 6144] {
            assert!(
                matches!(validate_block_size(bad), Err(Error::InvalidBlockSize(b)) if b == bad),
                "{bad} must be rejected"
            );
        }
        for good in [4096u32, 8192, 65536, 1 << 20] {
            validate_block_size(good).unwrap();
        }
    }

    #[test]
    fn sketch_field_optional_in_json() {
        // Older/sketchless snapshots omit the field entirely.
        let m = test_meta(1);
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("sketch"));
        let back: Meta = serde_json::from_str(&json).unwrap();
        assert!(back.sketch.is_none());
    }

    #[test]
    fn compression_field_optional_in_json() {
        // Uncompressed snapshots omit the section entirely.
        let m = test_meta(1);
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("compression"));
        let back: Meta = serde_json::from_str(&json).unwrap();
        assert!(back.compression.is_none());
    }

    #[test]
    fn rejects_unknown_codec() {
        let mut m = test_meta(1);
        m.compression = Some(CompressionMeta {
            codec: "lz4".into(),
            dict: false,
            dict_checksum: None,
            min_compress_len: 64,
            zstd_version: None,
        });
        assert!(matches!(
            m.layout(),
            Err(Error::UnsupportedCodec(c)) if c == "lz4"
        ));
    }

    #[test]
    fn rejects_incoherent_dict_flag_and_checksum() {
        // dict without a checksum: nothing anchors a dictionary that
        // would decompress cleanly into wrong values. A checksum
        // without the flag: the reader keys the load on `dict`, so the
        // pin would silently not apply. Both fail the load.
        for (dict, dict_checksum) in [(true, None), (false, Some(7u32))] {
            let mut m = test_meta(1);
            m.compression = Some(CompressionMeta {
                codec: CODEC_ZSTD.into(),
                dict,
                dict_checksum,
                min_compress_len: 64,
                zstd_version: None,
            });
            assert!(
                matches!(m.layout(), Err(Error::InvalidCompressionMeta(_))),
                "dict={dict}, checksum={dict_checksum:?} must be rejected"
            );
        }
    }
}
