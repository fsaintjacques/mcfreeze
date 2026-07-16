// SPDX-License-Identifier: Apache-2.0

//! V5 transparent value compression (`doc/plan/V5_COMPRESSION.md`).
//!
//! Values are compressed independently as **bare zstd frames**:
//! magicless, with the frame content size pinned, so a stored frame
//! self-describes its decompressed size and no side-channel raw length
//! exists anywhere. The record's `length` field is always the stored
//! byte count; bit 30 ([`super::block`]) selects between "the stored
//! bytes are a frame" and the raw fallback.
//!
//! Writer side ([`Compressor`]): a frame is kept only when strictly
//! smaller than the raw value — incompressible and sub-`min_compress_len`
//! values store raw, so storage never expands.
//!
//! Reader side ([`Decompressor`]): the pinned content size is
//! attacker-influenceable bytes, bounds-checked against
//! [`MAX_VALUE_LEN`] before allocation; a decode failure or
//! content-size mismatch after a clean checksum is corruption — an
//! error, never a miss. Checksums verify stored bytes, so a corrupt
//! *dictionary* is the one input that could decompress cleanly into
//! wrong values; [`verify_dict`] must pass at open before any frame is
//! decoded.

use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Mutex;

use crate::v5::block::{checksum32, MAX_VALUE_LEN};
use crate::{Error, Result};

/// Default zstd compression level for both zstd modes.
pub const DEFAULT_LEVEL: i32 = 3;
/// Values shorter than this skip the compression attempt entirely
/// (writer-side knob, recorded in `meta.json` for provenance only).
pub const DEFAULT_MIN_COMPRESS_LEN: usize = 64;

/// Snapshot compression mode — a plan-time decision, pinned in
/// `v5.plan` and declared once by `meta.compression`. The per-record
/// bit only selects between the snapshot's codec and the raw fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Bytes stored verbatim; on-disk output byte-identical to
    /// compression-less V5.
    #[default]
    None,
    /// Each value zstd-compressed independently.
    Zstd,
    /// zstd with a per-snapshot learned dictionary (`dict.bin`).
    ZstdDict,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::None => "none",
            Mode::Zstd => "zstd",
            Mode::ZstdDict => "zstd-dict",
        }
    }
}

impl FromStr for Mode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "none" => Ok(Mode::None),
            "zstd" => Ok(Mode::Zstd),
            "zstd-dict" => Ok(Mode::ZstdDict),
            other => Err(format!(
                "unknown compression mode {other:?} (expected none, zstd, or zstd-dict)"
            )),
        }
    }
}

/// The vendored libzstd version, recorded in `meta.json` as provenance:
/// trained-dictionary bytes are not reproducible across versions.
pub fn zstd_version() -> String {
    let n = zstd::zstd_safe::version_number();
    format!("{}.{}.{}", n / 10000, n / 100 % 100, n % 100)
}

// ---------------------------------------------------------------------------
// Dictionary lifecycle
// ---------------------------------------------------------------------------

/// Train a zstd dictionary (ZDICT/COVER) of at most `max_size` bytes.
/// The output is raw ZDICT bytes, written to `dict.bin` unframed so
/// standard zstd tooling can use it directly; its integrity is anchored
/// by a checksum in `meta.json`, not in the file.
pub fn train_dict<S: AsRef<[u8]>>(samples: &[S], max_size: usize) -> Result<Vec<u8>> {
    zstd::dict::from_samples(samples, max_size).map_err(Error::DictTrain)
}

/// Verify `dict.bin` bytes against the checksum recorded in
/// `meta.compression.dict_checksum`. Must pass at open: value checksums
/// cover stored bytes, so a corrupt dictionary would decompress cleanly
/// into wrong values — the one failure mode they cannot catch.
pub fn verify_dict(dict: &[u8], expected: u32) -> Result<()> {
    let got = checksum32(dict);
    if got != expected {
        return Err(Error::DictChecksumMismatch { got, expected });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Compressor (writer side)
// ---------------------------------------------------------------------------

/// Per-thread compression context: bare-frame zstd with an optional
/// dictionary and the raw-fallback rules. Not thread-safe (`&mut`);
/// the builder holds one per partition build.
pub struct Compressor {
    ctx: zstd::bulk::Compressor<'static>,
    min_compress_len: usize,
}

impl Compressor {
    /// `dict` is the raw `dict.bin` bytes (loaded by copy — the borrow
    /// ends here). `level` is the zstd compression level.
    pub fn new(level: i32, dict: Option<&[u8]>, min_compress_len: usize) -> Result<Self> {
        let mut ctx = match dict {
            Some(d) => zstd::bulk::Compressor::with_dictionary(level, d),
            None => zstd::bulk::Compressor::new(level),
        }
        .map_err(Error::Zstd)?;
        // Bare frame: no magic, no per-frame dictionary id; the content
        // size stays pinned so the frame self-describes its raw length.
        ctx.include_magicbytes(false).map_err(Error::Zstd)?;
        ctx.include_dictid(false).map_err(Error::Zstd)?;
        ctx.include_contentsize(true).map_err(Error::Zstd)?;
        Ok(Self {
            ctx,
            min_compress_len,
        })
    }

    /// Compress `value` into a bare frame, or `None` when the value
    /// must store raw: shorter than `min_compress_len`, or the frame
    /// is not strictly smaller than the value.
    pub fn compress(&mut self, value: &[u8]) -> Result<Option<Vec<u8>>> {
        if value.len() < self.min_compress_len {
            return Ok(None);
        }
        let frame = self.ctx.compress(value).map_err(Error::Zstd)?;
        Ok((frame.len() < value.len()).then_some(frame))
    }
}

// ---------------------------------------------------------------------------
// Decompressor (reader side)
// ---------------------------------------------------------------------------

/// Per-snapshot decompression state: the dictionary plus a pool of
/// reusable contexts. Sharable across reader threads (`&self`).
///
/// Contexts are pooled because a `DCtx` is not thread-safe and costs
/// ~µs to create — plus dictionary digestion — while a reused context
/// decompresses typical values in ~200–500 ns; per-call creation would
/// dominate page-cache-hot gets. The pool grows to the number of
/// concurrently decompressing threads.
pub struct Decompressor {
    /// Raw `dict.bin` bytes; `None` for plain zstd. Each pooled context
    /// digests its own copy once at creation.
    dict: Option<Vec<u8>>,
    pool: Mutex<Vec<zstd::bulk::Decompressor<'static>>>,
}

impl Decompressor {
    /// `dict` must already be verified against `meta.compression`
    /// (see [`verify_dict`]).
    pub fn new(dict: Option<Vec<u8>>) -> Result<Self> {
        let this = Self {
            dict,
            pool: Mutex::new(Vec::new()),
        };
        // Fail bad dictionary bytes at construction, not on first get.
        let ctx = this.new_ctx()?;
        this.pool.lock().unwrap().push(ctx);
        Ok(this)
    }

    fn new_ctx(&self) -> Result<zstd::bulk::Decompressor<'static>> {
        let mut ctx = match &self.dict {
            Some(d) => zstd::bulk::Decompressor::with_dictionary(d),
            None => zstd::bulk::Decompressor::new(),
        }
        .map_err(Error::Zstd)?;
        ctx.include_magicbytes(false).map_err(Error::Zstd)?;
        Ok(ctx)
    }

    /// Decompress one stored frame back into value bytes. Every failure
    /// is an error, never a miss: the block/heap checksum already
    /// passed, so a bad frame is corruption (or a format bug), not
    /// media damage.
    pub fn decompress(&self, frame: &[u8]) -> Result<Vec<u8>> {
        let content_size = frame_content_size(frame)?;
        if content_size > MAX_VALUE_LEN as u64 {
            return Err(Error::FrameContentTooLarge {
                got: content_size,
                max: MAX_VALUE_LEN,
            });
        }
        let mut ctx = match self.pool.lock().unwrap().pop() {
            Some(ctx) => ctx,
            None => self.new_ctx()?,
        };
        let res = ctx.decompress(frame, content_size as usize);
        // A context is reusable after an error (the next call resets
        // the session), so recycle it unconditionally.
        self.pool.lock().unwrap().push(ctx);
        let value = res.map_err(Error::Decompress)?;
        if value.len() as u64 != content_size {
            return Err(Error::CorruptFrame(
                "decompressed size differs from pinned content size",
            ));
        }
        Ok(value)
    }
}

/// Resolve a record's stored bytes into value bytes: `compressed` (bit
/// 30) selects between the snapshot's codec and the raw passthrough.
/// `codec` is `Some` iff `meta.compression` declares one — a compressed
/// record in a codec-less snapshot is a stray or future-foreign record,
/// and fails loudly as corruption.
pub fn decode<'a>(
    stored: &'a [u8],
    compressed: bool,
    codec: Option<&Decompressor>,
) -> Result<Cow<'a, [u8]>> {
    match (compressed, codec) {
        (false, _) => Ok(Cow::Borrowed(stored)),
        (true, Some(d)) => d.decompress(stored).map(Cow::Owned),
        (true, None) => Err(Error::CompressedValueWithoutCodec),
    }
}

/// Extract the pinned content size from a magicless zstd frame header
/// (RFC 8878 §3.1.1.1). The writer always pins it, so a frame without
/// one — like a truncated or reserved-bit header — is corruption.
fn frame_content_size(frame: &[u8]) -> Result<u64> {
    let &fhd = frame.first().ok_or(Error::CorruptFrame("empty frame"))?;
    if fhd & 0x08 != 0 {
        return Err(Error::CorruptFrame("reserved frame header bit set"));
    }
    let single_segment = fhd & 0x20 != 0;
    let fcs_len: usize = match fhd >> 6 {
        0 if single_segment => 1,
        0 => return Err(Error::CorruptFrame("frame content size not pinned")),
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let start = 1
        + usize::from(!single_segment) // window descriptor
        + [0, 1, 2, 4][(fhd & 0x03) as usize]; // dictionary id
    let bytes = frame
        .get(start..start + fcs_len)
        .ok_or(Error::CorruptFrame("truncated frame header"))?;
    let mut le = [0u8; 8];
    le[..fcs_len].copy_from_slice(bytes);
    let size = u64::from_le_bytes(le);
    // The 2-byte field trades its redundant low range for reach.
    Ok(if fcs_len == 2 { size + 256 } else { size })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh64::xxh64;

    /// Small structured records sharing shape but not bytes: the
    /// dictionary-mode workload.
    fn samples() -> Vec<Vec<u8>> {
        (0..2000u64)
            .map(|i| {
                format!(
                    r#"{{"user_id":{i},"display_name":"user-{i}","reputation":{},"badges":["gold","silver","bronze"],"location":"city-{}"}}"#,
                    i * 37,
                    i % 50
                )
                .into_bytes()
            })
            .collect()
    }

    /// Deterministic incompressible bytes without a rand dependency.
    fn noise(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut h = 0x9E37_79B9_7F4A_7C15u64;
        while out.len() < len {
            h = xxh64(&h.to_le_bytes(), 0);
            out.extend_from_slice(&h.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    #[test]
    fn decompressor_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Decompressor>();
    }

    #[test]
    fn zstd_roundtrip() {
        let value = b"a fairly repetitive value ".repeat(40);
        let mut c = Compressor::new(DEFAULT_LEVEL, None, DEFAULT_MIN_COMPRESS_LEN).unwrap();
        let frame = c.compress(&value).unwrap().expect("compressible");
        assert!(frame.len() < value.len());

        let d = Decompressor::new(None).unwrap();
        assert_eq!(d.decompress(&frame).unwrap(), value);
        // Second call exercises the pooled-context path.
        assert_eq!(d.decompress(&frame).unwrap(), value);
    }

    #[test]
    fn zstd_dict_roundtrip_wins_on_small_values() {
        let samples = samples();
        let dict = train_dict(&samples, 64 * 1024).unwrap();
        let mut c = Compressor::new(DEFAULT_LEVEL, Some(&dict), DEFAULT_MIN_COMPRESS_LEN).unwrap();
        let d = Decompressor::new(Some(dict)).unwrap();

        let mut wins = 0;
        for v in &samples {
            if let Some(frame) = c.compress(v).unwrap() {
                assert_eq!(d.decompress(&frame).unwrap(), *v);
                wins += 1;
            }
        }
        // The point of the dictionary: values this small (~100 B) must
        // compress, which plain per-value zstd cannot deliver.
        assert!(
            wins > samples.len() / 2,
            "dictionary won on only {wins}/{} small values",
            samples.len()
        );
    }

    #[test]
    fn frames_roundtrip_across_content_size_widths() {
        // 100 → 1-byte FCS (single segment), 300 → 2-byte, 70 000 →
        // 4-byte (past the 2-byte field's 65 791 ceiling).
        for len in [100usize, 300, 70_000] {
            let value = b"ab".repeat(len / 2);
            let mut c = Compressor::new(DEFAULT_LEVEL, None, 1).unwrap();
            let frame = c.compress(&value).unwrap().expect("compressible");
            assert_eq!(frame_content_size(&frame).unwrap(), value.len() as u64);
            let d = Decompressor::new(None).unwrap();
            assert_eq!(d.decompress(&frame).unwrap(), value);
        }
    }

    #[test]
    fn sub_threshold_value_stores_raw() {
        let mut c = Compressor::new(DEFAULT_LEVEL, None, 64).unwrap();
        let value = b"aaaaaaaa".repeat(7); // 56 B, compressible but short
        assert_eq!(c.compress(&value).unwrap(), None);
    }

    #[test]
    fn incompressible_value_stores_raw() {
        let mut c = Compressor::new(DEFAULT_LEVEL, None, 64).unwrap();
        assert_eq!(c.compress(&noise(256)).unwrap(), None);
    }

    #[test]
    fn empty_value_stores_raw_even_with_zero_threshold() {
        let mut c = Compressor::new(DEFAULT_LEVEL, None, 0).unwrap();
        // Any frame is larger than zero bytes; strictly-smaller says no.
        assert_eq!(c.compress(b"").unwrap(), None);
    }

    #[test]
    fn corrupt_dict_is_rejected() {
        let mut dict = train_dict(&samples(), 16 * 1024).unwrap();
        let expected = checksum32(&dict);
        verify_dict(&dict, expected).unwrap();

        let mid = dict.len() / 2;
        dict[mid] ^= 0xFF;
        assert!(matches!(
            verify_dict(&dict, expected),
            Err(Error::DictChecksumMismatch { .. })
        ));
    }

    #[test]
    fn dict_frame_without_dict_is_error_not_wrong_bytes() {
        let samples = samples();
        let dict = train_dict(&samples, 16 * 1024).unwrap();
        let mut c = Compressor::new(DEFAULT_LEVEL, Some(&dict), 1).unwrap();
        let frame = c.compress(&samples[7]).unwrap().expect("compressible");

        let d = Decompressor::new(None).unwrap();
        match d.decompress(&frame) {
            Err(Error::Decompress(_)) | Err(Error::CorruptFrame(_)) => {}
            other => panic!("expected decode error, got {other:?}"),
        }
    }

    /// Locate the 1-byte FCS field of a small single-segment dict-less
    /// frame, asserting the header shape first so a libzstd behavior
    /// change fails loudly here instead of misleading the test below.
    fn fcs1_offset(frame: &[u8]) -> usize {
        let fhd = frame[0];
        assert_eq!(fhd >> 6, 0, "expected 1-byte FCS");
        assert_ne!(fhd & 0x20, 0, "expected single-segment frame");
        assert_eq!(fhd & 0x03, 0, "expected no dictionary id");
        1
    }

    #[test]
    fn content_size_mismatch_is_error() {
        let value = b"abcdefgh".repeat(20); // 160 B < 256: 1-byte FCS
        let mut c = Compressor::new(DEFAULT_LEVEL, None, 1).unwrap();
        let frame = c.compress(&value).unwrap().expect("compressible");
        let at = fcs1_offset(&frame);
        let d = Decompressor::new(None).unwrap();

        // Claim larger than the frame regenerates.
        let mut inflated = frame.clone();
        inflated[at] = (value.len() + 40) as u8;
        assert!(d.decompress(&inflated).is_err());

        // Claim smaller: regeneration overruns the claimed capacity.
        let mut deflated = frame;
        deflated[at] = (value.len() - 40) as u8;
        assert!(d.decompress(&deflated).is_err());
    }

    #[test]
    fn oversized_content_size_is_rejected_before_allocation() {
        // Hand-built header: FCS flag 3 (8-byte field) + single segment,
        // claiming 1 TiB. Must fail the bounds check, not allocate.
        let mut frame = vec![0xE0u8];
        frame.extend_from_slice(&(1u64 << 40).to_le_bytes());
        frame.extend_from_slice(b"garbage");
        assert!(matches!(
            Decompressor::new(None).unwrap().decompress(&frame),
            Err(Error::FrameContentTooLarge { got, max })
                if got == 1 << 40 && max == MAX_VALUE_LEN
        ));
    }

    #[test]
    fn truncated_or_unpinned_frame_header_is_corrupt() {
        let d = Decompressor::new(None).unwrap();
        // Empty frame.
        assert!(matches!(
            d.decompress(b""),
            Err(Error::CorruptFrame("empty frame"))
        ));
        // Reserved FHD bit set.
        assert!(matches!(
            d.decompress(&[0x08, 0, 0]),
            Err(Error::CorruptFrame("reserved frame header bit set"))
        ));
        // FCS flag 0 without single-segment: content size not pinned.
        assert!(matches!(
            d.decompress(&[0x00, 0, 0]),
            Err(Error::CorruptFrame("frame content size not pinned"))
        ));
        // Single-segment header cut off before its 1-byte FCS.
        assert!(matches!(
            d.decompress(&[0x20]),
            Err(Error::CorruptFrame("truncated frame header"))
        ));
    }

    #[test]
    fn decode_applies_bit30_against_snapshot_codec() {
        let value = b"repetitive repetitive repetitive ".repeat(8);
        let mut c = Compressor::new(DEFAULT_LEVEL, None, 1).unwrap();
        let frame = c.compress(&value).unwrap().unwrap();
        let d = Decompressor::new(None).unwrap();

        // Raw passthrough borrows, with or without a codec.
        assert!(matches!(
            decode(b"raw", false, None).unwrap(),
            Cow::Borrowed(b"raw")
        ));
        assert!(matches!(
            decode(b"raw", false, Some(&d)).unwrap(),
            Cow::Borrowed(b"raw")
        ));
        // Compressed record decodes through the snapshot codec.
        assert_eq!(decode(&frame, true, Some(&d)).unwrap().as_ref(), &value[..]);
        // Bit 30 in a codec-less snapshot is corruption, loudly.
        assert!(matches!(
            decode(&frame, true, None),
            Err(Error::CompressedValueWithoutCodec)
        ));
    }

    #[test]
    fn mode_parses_and_prints() {
        for (s, m) in [
            ("none", Mode::None),
            ("zstd", Mode::Zstd),
            ("zstd-dict", Mode::ZstdDict),
        ] {
            assert_eq!(s.parse::<Mode>().unwrap(), m);
            assert_eq!(m.as_str(), s);
        }
        assert!("lz4".parse::<Mode>().is_err());
    }
}
