// SPDX-License-Identifier: Apache-2.0

//! V5 sketch (`sketch.bin`): per-partition binary-fuse-8 filter over
//! full 64-bit key fingerprints, queried before the fence search.

use self_cell::self_cell;
use xorf::{BinaryFuse8, BinaryFuse8Ref, DmaSerializable, Filter, FilterRef};

use crate::{Error, Result};

const DESCRIPTOR_LEN: usize = <BinaryFuse8 as DmaSerializable>::DESCRIPTOR_LEN;

/// The `meta.json` sketch kind string for this implementation.
pub const KIND: &str = "binary_fuse8";

/// The `from_dma` view named for `self_cell`. `BinaryFuse8Ref` borrows
/// its fingerprint bytes, so it can only live as a dependent of the
/// buffer it reads — never a free-standing field.
type FilterView<'a> = BinaryFuse8Ref<'a>;

self_cell!(
    /// The sketch bytes (`owner`) plus the `BinaryFuse8Ref`
    /// parsed from them once at construction (`dependent`).
    struct SketchCell {
        owner: Box<[u8]>,
        #[covariant]
        dependent: FilterView,
    }
);

/// A BinaryFuse8 sketch: the serialized filter bytes and the `from_dma`
/// view over them, parsed once at open. `contains` reuses the stored
/// view — no per-query re-parse.
pub struct Sketch(SketchCell);

impl Sketch {
    /// Validate the descriptor's internal consistency, then parse the
    /// `from_dma` view once and store it beside the bytes. Integrity
    /// against the manifest anchor is the caller's job ([`verify_sketch`],
    /// run first by the reader); this owns only structural validity — an
    /// intrinsic property of the bytes. It is never a panic: `from_dma`'s
    /// three index computations trust the descriptor, so an inconsistent
    /// file (writer bug, crafted input) must be rejected here, not
    /// out-of-bounds on the lookup hot path.
    pub fn parse(buf: Vec<u8>) -> Result<Self> {
        if buf.len() < DESCRIPTOR_LEN {
            return Err(Error::CorruptSketch("sketch shorter than its descriptor"));
        }

        // Descriptor layout (xorf DMA form): [8B seed][4B segment_length]
        // [4B segment_length_mask][4B segment_count_length]. The bounds
        // proof for `from_dma`'s three indices requires: segment_length
        // a non-zero power of two, mask = segment_length − 1,
        // segment_count_length a multiple of segment_length, and
        // fingerprints.len() == segment_count_length + 2 × segment_length
        // (xorf builds arrays as (segment_count + arity − 1) × length,
        // arity 3).
        let sl = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let mask = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let scl = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let n_fingerprints = (buf.len() - DESCRIPTOR_LEN) as u64;
        if sl == 0 || !sl.is_power_of_two() || mask != sl - 1 {
            return Err(Error::CorruptSketch("invalid sketch segment descriptor"));
        }
        if scl % sl != 0 || n_fingerprints != scl as u64 + 2 * sl as u64 {
            return Err(Error::CorruptSketch(
                "sketch descriptor/fingerprint length mismatch",
            ));
        }

        // Validated above: `from_dma` here is total, so `new` (infallible).
        Ok(Self(SketchCell::new(buf.into(), |b| {
            BinaryFuse8Ref::from_dma(&b[..DESCRIPTOR_LEN], &b[DESCRIPTOR_LEN..])
        })))
    }

    /// May the snapshot contain this fingerprint? No false negatives.
    pub fn contains(&self, fp: u64) -> bool {
        self.0.borrow_dependent().contains(&fp)
    }
}

/// Serialize a filter over a partition's key fingerprints into the bare
/// DMA form — no checksum trailer; the caller anchors `checksum32` of
/// these bytes in `PartitionMeta.sketch_checksum`.
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
    Ok(out)
}

/// Reconcile `sketch.bin` against its manifest anchor
/// (`PartitionMeta.sketch_checksum`). The reader calls this before
/// [`Sketch::parse`] — integrity is a cross-file concern owned by the
/// caller that holds both the bytes and the meta, exactly like
/// [`super::compress::verify_dict`]. A mismatch means a flipped
/// fingerprint (silent false negatives) and must fail the open.
pub fn verify_sketch(sketch: &[u8], expected: u32) -> Result<()> {
    let got = crate::v5::block::checksum32(sketch);
    if got != expected {
        return Err(Error::SketchChecksumMismatch { got, expected });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v5::block::checksum32;

    fn fps(n: u64) -> Vec<u64> {
        // Spread-out, duplicate-free synthetic fingerprints.
        (0..n)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .collect()
    }

    #[test]
    fn roundtrip_no_false_negatives() {
        let keys = fps(10_000);
        let bytes = build(&keys).unwrap();
        verify_sketch(&bytes, checksum32(&bytes)).unwrap();
        let sketch = Sketch::parse(bytes).unwrap();
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
        // No trailer now — the bytes are the bare DMA form.
        let bytes = build(&fps(100_000)).unwrap().len();
        let bits_per_key = bytes as f64 * 8.0 / 100_000.0;
        assert!(
            (8.0..11.5).contains(&bits_per_key),
            "unexpected size: {bits_per_key} bits/key"
        );
    }

    #[test]
    fn verify_sketch_catches_flipped_byte() {
        // Integrity check (owned by the caller): any bit-flip breaks the
        // match against the manifest anchor, before the structural parse.
        let mut bytes = build(&fps(1000)).unwrap();
        let anchor = checksum32(&bytes);
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        assert!(matches!(
            verify_sketch(&bytes, anchor),
            Err(Error::SketchChecksumMismatch { .. })
        ));
    }

    #[test]
    fn inconsistent_descriptor_is_rejected() {
        // A crafted (or writer-bug) sketch that passes integrity but
        // whose descriptor disagrees with the fingerprint length must
        // fail parse, not index out of bounds in from_dma. (parse owns
        // structure only, so these feed it directly.)
        let good = build(&fps(1000)).unwrap();

        // Truncate one fingerprint byte.
        let mut short = good.clone();
        short.truncate(good.len() - 1);
        assert!(matches!(
            Sketch::parse(short),
            Err(Error::CorruptSketch(
                "sketch descriptor/fingerprint length mismatch"
            ))
        ));

        // Zero segment_length.
        let mut zeroed = good.clone();
        zeroed[8..12].copy_from_slice(&0u32.to_le_bytes());
        assert!(matches!(
            Sketch::parse(zeroed),
            Err(Error::CorruptSketch("invalid sketch segment descriptor"))
        ));

        // Runt input shorter than the descriptor.
        assert!(matches!(
            Sketch::parse(vec![0u8; 10]),
            Err(Error::CorruptSketch("sketch shorter than its descriptor"))
        ));
    }
}
