// SPDX-License-Identifier: Apache-2.0

//! V5 sketch (`sketch.bin`): per-partition binary-fuse-8 filter over
//! full 64-bit key fingerprints, queried before the fence search to
//! restore V4's zero-I/O misses. ~9 bits/key, < 0.4% false-positive
//! rate; a false positive costs one wasted `pread`.
//!
//! On-disk layout (stable; xorf's documented DMA form):
//!
//! ```text
//! [20B descriptor][fingerprint bytes][4B checksum32 of all prior bytes]
//! ```
//!
//! The checksum is not optional hygiene: a flipped fingerprint byte
//! makes `contains` return false for a present key — a false negative,
//! i.e. silent data loss. A sketch that fails verification must fail
//! the open, never degrade.

use xorf::{BinaryFuse8, BinaryFuse8Ref, DmaSerializable, Filter, FilterRef};

use crate::{v5::block::checksum32, Error, Result};

const DESCRIPTOR_LEN: usize = <BinaryFuse8 as DmaSerializable>::DESCRIPTOR_LEN;
const CHECKSUM_LEN: usize = 4;

/// Theoretical false-positive rate of an 8-bit binary fuse filter.
pub const FALSE_POSITIVE_RATE: f64 = 1.0 / 256.0;

/// The `meta.json` sketch kind string for this implementation.
pub const KIND: &str = "binary_fuse8";

/// Serialize a filter over a partition's key fingerprints.
///
/// `fps` must be duplicate-free; the builder feeds fp-sorted records,
/// so adjacent `dedup` upstream suffices. Construction is randomized
/// internally by xorf (seed retries) but deterministic in failure:
/// an error here means the key set is pathological (or duplicated).
pub fn build(fps: &[u64]) -> Result<Vec<u8>> {
    let filter = BinaryFuse8::try_from(fps).map_err(Error::SketchBuild)?;
    let mut out = vec![0u8; DESCRIPTOR_LEN];
    filter.dma_copy_descriptor_to(&mut out[..DESCRIPTOR_LEN]);
    out.extend_from_slice(filter.dma_fingerprints());
    let ck = checksum32(&out);
    out.extend_from_slice(&ck.to_le_bytes());
    Ok(out)
}

/// A verified sketch owning its serialized bytes.
pub struct Sketch {
    buf: Vec<u8>,
}

impl Sketch {
    /// Verify the checksum and the descriptor's internal consistency,
    /// then adopt the buffer. Corruption is an error at open — never a
    /// silently degraded filter, and never a panic: `contains` indexes
    /// the fingerprint slice with values derived purely from the
    /// descriptor, so a checksum-valid but inconsistent file (writer
    /// bug, crafted input) must be rejected here, not out-of-bounds on
    /// the lookup hot path.
    pub fn parse(buf: Vec<u8>) -> Result<Self> {
        if buf.len() < DESCRIPTOR_LEN + CHECKSUM_LEN {
            return Err(Error::CorruptSketch("sketch shorter than its header"));
        }
        let body = buf.len() - CHECKSUM_LEN;
        let stored = u32::from_le_bytes(buf[body..].try_into().unwrap());
        if checksum32(&buf[..body]) != stored {
            return Err(Error::CorruptSketch("sketch checksum mismatch"));
        }

        // Descriptor layout (xorf DMA form): [8B seed][4B segment_length]
        // [4B segment_length_mask][4B segment_count_length]. The bounds
        // proof for `contains`'s three indices requires: segment_length
        // a non-zero power of two, mask = segment_length − 1,
        // segment_count_length a multiple of segment_length, and
        // fingerprints.len() == segment_count_length + 2 × segment_length
        // (xorf builds arrays as (segment_count + arity − 1) × length,
        // arity 3).
        let sl = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let mask = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let scl = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let n_fingerprints = (body - DESCRIPTOR_LEN) as u64;
        if sl == 0 || !sl.is_power_of_two() || mask != sl - 1 {
            return Err(Error::CorruptSketch("invalid sketch segment descriptor"));
        }
        if scl % sl != 0 || n_fingerprints != scl as u64 + 2 * sl as u64 {
            return Err(Error::CorruptSketch(
                "sketch descriptor/fingerprint length mismatch",
            ));
        }
        Ok(Self { buf })
    }

    /// May the snapshot contain this fingerprint? No false negatives.
    pub fn contains(&self, fp: u64) -> bool {
        let body = self.buf.len() - CHECKSUM_LEN;
        // Descriptor parse is four integer reads — negligible per query.
        let filter =
            BinaryFuse8Ref::from_dma(&self.buf[..DESCRIPTOR_LEN], &self.buf[DESCRIPTOR_LEN..body]);
        filter.contains(&fp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fps(n: u64) -> Vec<u64> {
        // Spread-out, duplicate-free synthetic fingerprints.
        (0..n)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .collect()
    }

    #[test]
    fn roundtrip_no_false_negatives() {
        let keys = fps(10_000);
        let sketch = Sketch::parse(build(&keys).unwrap()).unwrap();
        for &k in &keys {
            assert!(sketch.contains(k), "false negative for {k:#x}");
        }
    }

    #[test]
    fn false_positive_rate_in_bounds() {
        let keys = fps(10_000);
        let sketch = Sketch::parse(build(&keys).unwrap()).unwrap();
        let absent = (0..100_000u64).map(|i| i.wrapping_mul(0xD1B5_4A32_D192_ED03) | 1);
        let fp = absent.filter(|&k| sketch.contains(k)).count();
        // ε ≈ 0.39%; 100K probes → ~390 expected. Generous bound.
        assert!(fp < 800, "false positive rate too high: {fp}/100000");
    }

    #[test]
    fn size_is_about_9_bits_per_key() {
        let bytes = build(&fps(100_000)).unwrap().len();
        let bits_per_key = bytes as f64 * 8.0 / 100_000.0;
        assert!(
            (8.0..11.5).contains(&bits_per_key),
            "unexpected size: {bits_per_key} bits/key"
        );
    }

    #[test]
    fn checksum_valid_but_inconsistent_descriptor_is_rejected() {
        // A crafted (or writer-bug) sketch whose checksum passes but
        // whose descriptor disagrees with the fingerprint length must
        // fail parse, not index out of bounds inside contains().
        let good = build(&fps(1000)).unwrap();

        // Truncate one fingerprint byte and re-seal the checksum.
        let mut short = good.clone();
        short.truncate(good.len() - CHECKSUM_LEN - 1);
        let ck = checksum32(&short);
        short.extend_from_slice(&ck.to_le_bytes());
        assert!(matches!(
            Sketch::parse(short),
            Err(Error::CorruptSketch(
                "sketch descriptor/fingerprint length mismatch"
            ))
        ));

        // Zero segment_length with a re-sealed checksum.
        let mut zeroed = good.clone();
        zeroed[8..12].copy_from_slice(&0u32.to_le_bytes());
        let body = zeroed.len() - CHECKSUM_LEN;
        let ck = checksum32(&zeroed[..body]);
        let n = zeroed.len();
        zeroed[n - CHECKSUM_LEN..].copy_from_slice(&ck.to_le_bytes());
        assert!(matches!(
            Sketch::parse(zeroed),
            Err(Error::CorruptSketch("invalid sketch segment descriptor"))
        ));
    }

    #[test]
    fn corruption_is_error_not_degradation() {
        let mut buf = build(&fps(100)).unwrap();
        let mid = buf.len() / 2;
        buf[mid] ^= 0xFF;
        assert!(matches!(Sketch::parse(buf), Err(Error::CorruptSketch(_))));
        assert!(matches!(
            Sketch::parse(vec![0u8; 10]),
            Err(Error::CorruptSketch(_))
        ));
    }
}
