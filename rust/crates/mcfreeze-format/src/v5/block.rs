// SPDX-License-Identifier: Apache-2.0

//! V5 block and record encoding (`blocks.bin`).
//!
//! A block is `block_size` bytes: densely packed records, zero padding,
//! and a trailing 4-byte checksum over everything before it. Records are
//! 8-byte aligned and never straddle a block boundary; when the next
//! record does not fit, the block is zero-padded, sealed, and the record
//! starts the next block.
//!
//! Record layout (8-byte aligned within the block):
//!
//! ```text
//! [8B verify_fingerprint] [4B length|flags] [payload] [pad to 8B]
//! ```
//!
//! - `verify_fingerprint` is non-zero by construction (biased 0 → 1);
//!   the scanner stops at vfp == 0, the padding sentinel.
//! - `length|flags`: bit 31 = out-of-line, bit 30 = compressed (the
//!   stored bytes are a bare zstd frame, see [`super::compress`]). The
//!   low 30 bits are ALWAYS the stored byte count at the value's
//!   location — inline payload bytes, or the heap extent a stub preads;
//!   the compressed length when bit 30 is set. Inline payload is the
//!   stored value; out-of-line payload is a 12-byte stub
//!   `[8B heap_offset] [4B value_checksum]`.
//! - The decoder masks `length` with `MAX_VALUE_LEN` unconditionally:
//!   bit 30 is always decoded as a flag, never folded into length.
//!
//! The checksum is verified after every block `pread`, *before* the scan
//! trusts any `length` field. A mismatch is an error, never a miss.

use xxhash_rust::xxh64::xxh64;

use crate::{Error, Result};

/// Trailing per-block checksum width.
pub const CHECKSUM_LEN: usize = 4;
/// Fixed record header: 8-byte vfp + 4-byte length|flags.
pub const HEADER_LEN: usize = 12;
/// Records are aligned to this within a block.
pub const RECORD_ALIGN: usize = 8;
/// Out-of-line payload: 8-byte heap offset + 4-byte value checksum.
pub const STUB_PAYLOAD_LEN: usize = 12;
/// Maximum stored byte length representable in the 30-bit length field.
pub const MAX_VALUE_LEN: u32 = (1 << 30) - 1;

const FLAG_OUT_OF_LINE: u32 = 1 << 31;
const FLAG_COMPRESSED: u32 = 1 << 30;

#[inline]
fn align8(n: usize) -> usize {
    (n + (RECORD_ALIGN - 1)) & !(RECORD_ALIGN - 1)
}

/// Encoded size of a record with `payload_len` payload bytes.
#[inline]
pub fn encoded_len(payload_len: usize) -> usize {
    align8(HEADER_LEN + payload_len)
}

/// `xxhash64` truncated to its low 32 bits: the block and heap-value
/// checksum function.
#[inline]
pub fn checksum32(bytes: &[u8]) -> u32 {
    xxh64(bytes, 0) as u32
}

/// Out-of-line value locator, stored as a record's payload. The stored
/// extent's byte length lives in the record's `length` field, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stub {
    pub heap_offset: u64,
    /// Byte count of the stored heap extent — the compressed length
    /// when the record's compressed flag is set.
    pub stored_len: u32,
    /// Checksum of the stored heap extent (before any decompression).
    pub value_checksum: u32,
}

/// One decoded record, borrowing from the block buffer. `compressed`
/// means the stored bytes (inline payload or heap extent) are a bare
/// zstd frame.
#[derive(Debug, PartialEq, Eq)]
pub enum Record<'a> {
    Inline {
        vfp: u64,
        value: &'a [u8],
        compressed: bool,
    },
    Stub {
        vfp: u64,
        stub: Stub,
        compressed: bool,
    },
}

impl Record<'_> {
    pub fn vfp(&self) -> u64 {
        match self {
            Record::Inline { vfp, .. } | Record::Stub { vfp, .. } => *vfp,
        }
    }
}

// ---------------------------------------------------------------------------
// Sealing and verification
// ---------------------------------------------------------------------------

/// Verify a sealed block's trailing checksum. Must pass before any
/// `length` field in the block is trusted. A block too short to carry
/// a checksum (a truncated `pread`) is corruption, not a panic.
pub fn verify(block: &[u8]) -> Result<()> {
    let Some(body) = block.len().checked_sub(CHECKSUM_LEN) else {
        return Err(Error::CorruptBlock("block shorter than its checksum"));
    };
    let stored = u32::from_le_bytes(block[body..].try_into().unwrap());
    if checksum32(&block[..body]) != stored {
        return Err(Error::BlockChecksumMismatch);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// BlockAssembler
// ---------------------------------------------------------------------------

/// Packs fp-sorted records into sealed `block_size` blocks, emitting each
/// finished block to `sink` and one fence (high 32 bits of the block's
/// first record fp) per block.
pub struct BlockAssembler<F: FnMut(&[u8]) -> Result<()>> {
    block_size: usize,
    /// Current block fill; `len()` < `block_size - CHECKSUM_LEN`.
    buf: Vec<u8>,
    fences: Vec<u32>,
    /// Set when the sink errors: the failed block is lost, so any
    /// further output would be missing a block. All subsequent
    /// operations fail with [`Error::AssemblerPoisoned`].
    failed: bool,
    #[cfg(debug_assertions)]
    last_fp: u64,
    sink: F,
}

impl<F: FnMut(&[u8]) -> Result<()>> BlockAssembler<F> {
    pub fn new(block_size: usize, sink: F) -> Self {
        assert!(
            block_size.is_multiple_of(RECORD_ALIGN)
                && block_size >= CHECKSUM_LEN + encoded_len(STUB_PAYLOAD_LEN),
            "block_size {block_size} too small or misaligned"
        );
        Self {
            block_size,
            buf: Vec::with_capacity(block_size),
            fences: Vec::new(),
            failed: false,
            #[cfg(debug_assertions)]
            last_fp: 0,
            sink,
        }
    }

    /// Append an inline record; `stored` is what lands in the block —
    /// the compressed frame when `compressed`, the raw value otherwise.
    /// Records must arrive sorted by `fp`.
    pub fn push_inline(
        &mut self,
        fp: u64,
        vfp: u64,
        stored: &[u8],
        compressed: bool,
    ) -> Result<()> {
        if stored.len() as u64 > MAX_VALUE_LEN as u64 {
            return Err(Error::ValueTooLarge {
                len: stored.len() as u64,
                max: MAX_VALUE_LEN,
            });
        }
        let mut lf = stored.len() as u32;
        if compressed {
            lf |= FLAG_COMPRESSED;
        }
        self.push_header(fp, vfp, stored.len(), lf)?;
        self.push_payload(stored);
        Ok(())
    }

    /// Append an out-of-line record. `stub.stored_len` is the heap
    /// extent's byte length, carried in the record's `length` field.
    pub fn push_stub(&mut self, fp: u64, vfp: u64, stub: Stub, compressed: bool) -> Result<()> {
        if stub.stored_len > MAX_VALUE_LEN {
            return Err(Error::ValueTooLarge {
                len: stub.stored_len as u64,
                max: MAX_VALUE_LEN,
            });
        }
        let mut lf = stub.stored_len | FLAG_OUT_OF_LINE;
        if compressed {
            lf |= FLAG_COMPRESSED;
        }
        self.push_header(fp, vfp, STUB_PAYLOAD_LEN, lf)?;
        let mut payload = [0u8; STUB_PAYLOAD_LEN];
        payload[..8].copy_from_slice(&stub.heap_offset.to_le_bytes());
        payload[8..].copy_from_slice(&stub.value_checksum.to_le_bytes());
        self.push_payload(&payload);
        Ok(())
    }

    /// Seal the trailing partial block and return the fence array.
    pub fn finish(mut self) -> Result<Vec<u32>> {
        if self.failed {
            return Err(Error::AssemblerPoisoned);
        }
        if !self.buf.is_empty() {
            self.cut()?;
        }
        Ok(self.fences)
    }

    fn push_header(
        &mut self,
        fp: u64,
        vfp: u64,
        payload_len: usize,
        length_flags: u32,
    ) -> Result<()> {
        if self.failed {
            return Err(Error::AssemblerPoisoned);
        }
        assert_ne!(vfp, 0, "vfp 0 is the padding sentinel; bias it to 1");
        #[cfg(debug_assertions)]
        {
            debug_assert!(fp >= self.last_fp, "records must arrive fp-sorted");
            self.last_fp = fp;
        }

        let enc = encoded_len(payload_len);
        let capacity = self.block_size - CHECKSUM_LEN;
        if enc > capacity {
            return Err(Error::RecordTooLarge {
                encoded: enc,
                capacity,
            });
        }
        if self.buf.len() + enc > capacity {
            self.cut()?;
        }
        if self.buf.is_empty() {
            self.fences.push(super::fence::fence_of(fp));
        }
        self.buf.extend_from_slice(&vfp.to_le_bytes());
        self.buf.extend_from_slice(&length_flags.to_le_bytes());
        Ok(())
    }

    fn push_payload(&mut self, payload: &[u8]) {
        self.buf.extend_from_slice(payload);
        self.buf.resize(align8(self.buf.len()), 0);
    }

    fn cut(&mut self) -> Result<()> {
        debug_assert!(!self.buf.is_empty());
        let body = self.block_size - CHECKSUM_LEN;
        self.buf.resize(body, 0);
        let cksum = checksum32(&self.buf);
        self.buf.extend_from_slice(&cksum.to_le_bytes());
        let res = (self.sink)(&self.buf);
        self.buf.clear();
        if res.is_err() {
            self.failed = true;
        }
        res
    }
}

// ---------------------------------------------------------------------------
// Scanning
// ---------------------------------------------------------------------------

/// Iterate a sealed block's records. Assumes [`verify`] already passed;
/// still bounds-checks every header and yields `Err` (then fuses) on a
/// structurally impossible layout.
pub fn records(block: &[u8]) -> Records<'_> {
    Records {
        block,
        // Saturating: a block too short to carry a checksum yields no
        // records. `find` rejects it via `verify` before scanning.
        usable: block.len().saturating_sub(CHECKSUM_LEN),
        pos: 0,
        failed: false,
    }
}

pub struct Records<'a> {
    block: &'a [u8],
    usable: usize,
    pos: usize,
    failed: bool,
}

impl<'a> Iterator for Records<'a> {
    type Item = Result<Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.pos + HEADER_LEN > self.usable {
            return None;
        }
        let vfp = u64::from_le_bytes(self.block[self.pos..self.pos + 8].try_into().unwrap());
        if vfp == 0 {
            return None; // padding sentinel
        }
        let lf = u32::from_le_bytes(
            self.block[self.pos + 8..self.pos + HEADER_LEN]
                .try_into()
                .unwrap(),
        );
        // Unconditional 30-bit mask: bits 30/31 are always flags, never
        // part of length — a foreign flag can inflate a length otherwise.
        let length = lf & MAX_VALUE_LEN;
        let out_of_line = lf & FLAG_OUT_OF_LINE != 0;
        let compressed = lf & FLAG_COMPRESSED != 0;
        let payload_len = if out_of_line {
            STUB_PAYLOAD_LEN
        } else {
            length as usize
        };

        let start = self.pos + HEADER_LEN;
        let end = start + payload_len;
        if end > self.usable {
            self.failed = true;
            return Some(Err(Error::CorruptBlock("record overruns block")));
        }
        self.pos = align8(end);

        let payload = &self.block[start..end];
        Some(Ok(if out_of_line {
            Record::Stub {
                vfp,
                stub: Stub {
                    heap_offset: u64::from_le_bytes(payload[..8].try_into().unwrap()),
                    stored_len: length,
                    value_checksum: u32::from_le_bytes(payload[8..].try_into().unwrap()),
                },
                compressed,
            }
        } else {
            Record::Inline {
                vfp,
                value: payload,
                compressed,
            }
        }))
    }
}

/// Verify `block`'s checksum, then scan for the first record with `vfp`.
/// Corruption is an error, never a miss.
pub fn find(block: &[u8], vfp: u64) -> Result<Option<Record<'_>>> {
    verify(block)?;
    for rec in records(block) {
        let rec = rec?;
        if rec.vfp() == vfp {
            return Ok(Some(rec));
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn assemble(block_size: usize, recs: &[(u64, u64, &[u8])]) -> (Vec<Vec<u8>>, Vec<u32>) {
        let mut blocks = Vec::new();
        let mut asm = BlockAssembler::new(block_size, |b: &[u8]| {
            blocks.push(b.to_vec());
            Ok(())
        });
        for &(fp, vfp, value) in recs {
            asm.push_inline(fp, vfp, value, false).unwrap();
        }
        let fences = asm.finish().unwrap();
        (blocks, fences)
    }

    #[test]
    fn inline_roundtrip() {
        let recs: &[(u64, u64, &[u8])] = &[
            (1 << 32, 11, b"hello"),
            (2 << 32, 22, b""),
            (3 << 32, 33, b"a longer value with some bytes in it"),
        ];
        let (blocks, fences) = assemble(4096, recs);
        assert_eq!(blocks.len(), 1);
        assert_eq!(fences, vec![1]);
        verify(&blocks[0]).unwrap();

        let got: Vec<_> = records(&blocks[0]).map(|r| r.unwrap()).collect();
        assert_eq!(got.len(), 3);
        for (rec, &(_, vfp, value)) in got.iter().zip(recs) {
            match rec {
                Record::Inline {
                    vfp: v,
                    value: val,
                    compressed,
                } => {
                    assert_eq!(*v, vfp);
                    assert_eq!(*val, value);
                    assert!(!compressed);
                }
                other => panic!("expected inline, got {other:?}"),
            }
        }
    }

    #[test]
    fn stub_roundtrip_and_size() {
        assert_eq!(
            encoded_len(STUB_PAYLOAD_LEN),
            24,
            "stub must encode to 24 bytes"
        );
        let stub = Stub {
            heap_offset: u64::MAX - 7,
            stored_len: MAX_VALUE_LEN,
            value_checksum: 0xCAFE_F00D,
        };
        let mut blocks = Vec::new();
        let mut asm = BlockAssembler::new(4096, |b: &[u8]| {
            blocks.push(b.to_vec());
            Ok(())
        });
        asm.push_stub(7 << 32, 77, stub, false).unwrap();
        asm.finish().unwrap();

        match find(&blocks[0], 77).unwrap() {
            Some(Record::Stub {
                vfp,
                stub: s,
                compressed,
            }) => {
                assert_eq!(vfp, 77);
                assert_eq!(s, stub);
                assert!(!compressed);
            }
            other => panic!("expected stub, got {other:?}"),
        }
    }

    #[test]
    fn compressed_flag_roundtrips_and_never_inflates_length() {
        // The stored bytes of a compressed record are opaque here; the
        // block layer must carry them verbatim, expose the flag, and
        // decode `length` as the stored count with bit 30 masked out —
        // including on a stub, where bits 30 and 31 are both set.
        let frame = b"\x28\xb5\x2f\xfdnot really a frame";
        let stub = Stub {
            heap_offset: 4096,
            stored_len: 999,
            value_checksum: 0xBEEF,
        };
        let mut blocks = Vec::new();
        let mut asm = BlockAssembler::new(4096, |b: &[u8]| {
            blocks.push(b.to_vec());
            Ok(())
        });
        asm.push_inline(1 << 32, 11, frame, true).unwrap();
        asm.push_stub(2 << 32, 22, stub, true).unwrap();
        asm.push_inline(3 << 32, 33, b"raw", false).unwrap();
        asm.finish().unwrap();

        let got: Vec<_> = records(&blocks[0]).map(|r| r.unwrap()).collect();
        assert_eq!(
            got,
            vec![
                Record::Inline {
                    vfp: 11,
                    value: frame,
                    compressed: true,
                },
                Record::Stub {
                    vfp: 22,
                    stub,
                    compressed: true,
                },
                Record::Inline {
                    vfp: 33,
                    value: b"raw",
                    compressed: false,
                },
            ]
        );
    }

    #[test]
    fn boundary_padding_never_straddles() {
        // 64-byte blocks: usable 60, two 24-byte records fit, a third
        // (48 + 24 > 60) starts the next block.
        let recs: Vec<(u64, u64, Vec<u8>)> = (1..=5u64)
            .map(|i| (i << 32, i, i.to_le_bytes().to_vec()))
            .collect();
        let mut blocks = Vec::new();
        let mut asm = BlockAssembler::new(64, |b: &[u8]| {
            blocks.push(b.to_vec());
            Ok(())
        });
        for (fp, vfp, v) in &recs {
            asm.push_inline(*fp, *vfp, v, false).unwrap();
        }
        let fences = asm.finish().unwrap();

        assert_eq!(blocks.len(), 3);
        assert_eq!(fences, vec![1, 3, 5]);
        for block in &blocks {
            assert_eq!(block.len(), 64);
            verify(block).unwrap();
        }
        // Each record is wholly recoverable from its own block.
        let counts: Vec<usize> = blocks.iter().map(|b| records(b).count()).collect();
        assert_eq!(counts, vec![2, 2, 1]);
        // Zero padding between last record and checksum.
        assert!(blocks[2][24..60].iter().all(|&b| b == 0));
    }

    #[test]
    fn records_are_8_byte_aligned() {
        let recs: &[(u64, u64, &[u8])] = &[
            (1 << 32, 1, b"x"),     // 12 + 1 -> 16
            (2 << 32, 2, b"yyyyy"), // 12 + 5 -> 24
            (3 << 32, 3, b"zz"),    // 12 + 2 -> 16
        ];
        let (blocks, _) = assemble(4096, recs);
        // Re-scan, checking each record header lands on an 8-byte offset
        // by decoding successfully and by construction of encoded_len.
        let offsets = [0usize, 16, 40];
        for (i, &(_, vfp, _)) in recs.iter().enumerate() {
            let at = offsets[i];
            assert_eq!(
                u64::from_le_bytes(blocks[0][at..at + 8].try_into().unwrap()),
                vfp
            );
        }
    }

    #[test]
    fn oversized_record_is_an_error() {
        let mut asm = BlockAssembler::new(64, |_: &[u8]| Ok(()));
        let too_big = vec![0u8; 64];
        match asm.push_inline(1 << 32, 1, &too_big, false) {
            Err(Error::RecordTooLarge { encoded, capacity }) => {
                assert_eq!(encoded, encoded_len(64));
                assert_eq!(capacity, 60);
            }
            other => panic!("expected RecordTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn corrupted_block_is_error_not_miss() {
        let (mut blocks, _) = assemble(64, &[(1 << 32, 42, b"v")]);
        blocks[0][13] ^= 0xFF; // flip a bit in the record body
        assert!(matches!(
            find(&blocks[0], 42),
            Err(Error::BlockChecksumMismatch)
        ));
        // Absent key too: corruption must never read as a miss.
        assert!(matches!(
            find(&blocks[0], 999),
            Err(Error::BlockChecksumMismatch)
        ));
    }

    #[test]
    fn corrupt_length_with_valid_checksum_is_error() {
        // A hostile block: length field overruns the block, checksum
        // recomputed to match. The scan's bounds check must reject it.
        let (mut blocks, _) = assemble(64, &[(1 << 32, 42, b"v")]);
        blocks[0][8..12].copy_from_slice(&1000u32.to_le_bytes());
        let cksum = checksum32(&blocks[0][..60]);
        blocks[0][60..].copy_from_slice(&cksum.to_le_bytes());
        assert!(matches!(find(&blocks[0], 42), Err(Error::CorruptBlock(_))));
    }

    #[test]
    fn short_block_is_error_not_panic() {
        // Truncated pread / corrupt file: shorter than the checksum.
        assert!(matches!(verify(&[]), Err(Error::CorruptBlock(_))));
        assert!(matches!(verify(&[0u8; 3]), Err(Error::CorruptBlock(_))));
        assert!(matches!(find(&[0u8; 3], 1), Err(Error::CorruptBlock(_))));
        assert_eq!(records(&[0u8; 3]).count(), 0);
    }

    #[test]
    fn record_exactly_filling_usable_space() {
        // encoded_len(44) == 56 == block_size - 8: the largest 8-aligned
        // fit under capacity 60. The next record must start a new block.
        assert_eq!(encoded_len(44), 64 - 8);
        let v = vec![0xA5u8; 44];
        let mut blocks = Vec::new();
        let mut asm = BlockAssembler::new(64, |b: &[u8]| {
            blocks.push(b.to_vec());
            Ok(())
        });
        asm.push_inline(1 << 32, 1, &v, false).unwrap();
        asm.push_inline(2 << 32, 2, b"x", false).unwrap();
        let fences = asm.finish().unwrap();

        assert_eq!(blocks.len(), 2);
        assert_eq!(fences, vec![1, 2]);
        assert_eq!(records(&blocks[0]).count(), 1);
        match find(&blocks[0], 1).unwrap() {
            Some(Record::Inline { value, .. }) => assert_eq!(value, &v[..]),
            other => panic!("expected inline, got {other:?}"),
        }
        match find(&blocks[1], 2).unwrap() {
            Some(Record::Inline { value, .. }) => assert_eq!(value, b"x"),
            other => panic!("expected inline, got {other:?}"),
        }
    }

    #[test]
    fn oversized_value_length_is_an_error() {
        let mut asm = BlockAssembler::new(64, |_: &[u8]| Ok(()));
        let stub = Stub {
            heap_offset: 0,
            stored_len: MAX_VALUE_LEN + 1,
            value_checksum: 0,
        };
        assert!(matches!(
            asm.push_stub(1 << 32, 1, stub, false),
            Err(Error::ValueTooLarge { .. })
        ));
    }

    #[test]
    fn sink_error_poisons_assembler() {
        let mut asm = BlockAssembler::new(64, |_: &[u8]| Err(Error::CorruptBlock("sink failure")));
        // Fills the block exactly; no cut yet.
        asm.push_inline(1 << 32, 1, &[0u8; 44], false).unwrap();
        // Forces a cut: the sink error surfaces here...
        assert!(matches!(
            asm.push_inline(2 << 32, 2, b"x", false),
            Err(Error::CorruptBlock(_))
        ));
        // ...and every subsequent operation fails instead of silently
        // emitting a block sequence with a hole in it.
        assert!(matches!(
            asm.push_inline(3 << 32, 3, b"y", false),
            Err(Error::AssemblerPoisoned)
        ));
        assert!(matches!(asm.finish(), Err(Error::AssemblerPoisoned)));
    }

    #[test]
    #[should_panic(expected = "padding sentinel")]
    fn zero_vfp_is_rejected() {
        let mut asm = BlockAssembler::new(64, |_: &[u8]| Ok(()));
        let _ = asm.push_inline(1 << 32, 0, b"v", false);
    }

    #[test]
    fn checksum32_detects_change() {
        let a = checksum32(b"some value bytes");
        let mut v = b"some value bytes".to_vec();
        v[0] ^= 1;
        assert_ne!(a, checksum32(&v));
    }
}
