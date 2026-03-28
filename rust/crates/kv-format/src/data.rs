use std::fs::File;
use std::io::Write;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use crate::{
    index::aligned_size,
    meta::VALUE_ALIGNMENT,
    Result,
};

// Zero buffer used for padding writes; sized to one alignment unit.
const PAD: [u8; VALUE_ALIGNMENT as usize] = [0u8; VALUE_ALIGNMENT as usize];

// ---------------------------------------------------------------------------
// AlignedWriter
// ---------------------------------------------------------------------------

/// Sequential writer for `data.bin`.
///
/// Every value is written at a `VALUE_ALIGNMENT`-byte aligned offset.
/// After each value the writer emits zero-padding to restore alignment
/// before the next write.
///
/// The inner writer `W` is typically a `File` or a `BufWriter<File>`.
/// Callers that need large write buffers (e.g. 8 MiB per partition) should
/// wrap the file in `BufWriter::with_capacity(...)` before constructing.
pub struct AlignedWriter<W: Write> {
    inner:       W,
    byte_offset: u64,
}

impl<W: Write> AlignedWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, byte_offset: 0 }
    }

    /// Current byte position in the file (always a multiple of `VALUE_ALIGNMENT`).
    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    /// Write `value`, pad to `VALUE_ALIGNMENT`, and return the `aligned_offset`
    /// (i.e. `byte_offset / VALUE_ALIGNMENT` _before_ the write).
    ///
    /// The returned value is what should be stored in the `loc` field of the
    /// corresponding index bucket.
    pub fn write_value(&mut self, value: &[u8]) -> Result<u64> {
        debug_assert_eq!(
            self.byte_offset % VALUE_ALIGNMENT,
            0,
            "invariant: byte_offset is always aligned"
        );

        let aligned_offset = self.byte_offset / VALUE_ALIGNMENT;
        let padded          = aligned_size(value.len() as u32);
        let pad_len         = (padded - value.len() as u64) as usize;

        self.inner.write_all(value)?;
        if pad_len > 0 {
            self.inner.write_all(&PAD[..pad_len])?;
        }

        self.byte_offset += padded;
        Ok(aligned_offset)
    }

    /// Flush and return the inner writer.
    pub fn finish(mut self) -> Result<W> {
        self.inner.flush()?;
        Ok(self.inner)
    }
}

// ---------------------------------------------------------------------------
// pread
// ---------------------------------------------------------------------------

/// Read exactly `size` bytes from `file` at `byte_offset`.
///
/// Wraps `pread(2)` via [`FileExt::read_exact_at`]; a single syscall
/// regardless of whether the range crosses a page boundary.
#[cfg(unix)]
pub fn pread(file: &File, byte_offset: u64, size: u32) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; size as usize];
    file.read_exact_at(&mut buf, byte_offset)?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufWriter;
    use tempfile::tempfile;

    fn writer() -> AlignedWriter<File> {
        AlignedWriter::new(tempfile().unwrap())
    }

    fn buffered_writer() -> AlignedWriter<BufWriter<File>> {
        AlignedWriter::new(BufWriter::new(tempfile().unwrap()))
    }

    // --- AlignedWriter<File> ---

    #[test]
    fn write_single_value_aligned() {
        let mut w = writer();
        // Exactly 64 bytes: no padding needed.
        let data = vec![0xAAu8; 64];
        let off = w.write_value(&data).unwrap();
        assert_eq!(off, 0);
        assert_eq!(w.byte_offset(), 64);
    }

    #[test]
    fn write_single_value_needs_padding() {
        let mut w = writer();
        let data = vec![0xBBu8; 1];
        let off = w.write_value(&data).unwrap();
        assert_eq!(off, 0);
        // Padded to 64 bytes.
        assert_eq!(w.byte_offset(), 64);
    }

    #[test]
    fn write_multiple_values_offsets() {
        let mut w = writer();

        let off0 = w.write_value(&vec![1u8; 1]).unwrap();   // 1 byte → 64 on disk
        let off1 = w.write_value(&vec![2u8; 64]).unwrap();  // 64 bytes → 64 on disk
        let off2 = w.write_value(&vec![3u8; 65]).unwrap();  // 65 bytes → 128 on disk

        assert_eq!(off0, 0);   // byte 0  / 64 = 0
        assert_eq!(off1, 1);   // byte 64 / 64 = 1
        assert_eq!(off2, 2);   // byte 128 / 64 = 2

        assert_eq!(w.byte_offset(), 64 + 64 + 128);
    }

    #[test]
    fn write_empty_value() {
        let mut w = writer();
        let off = w.write_value(&[]).unwrap();
        assert_eq!(off, 0);
        // Zero bytes, no padding needed.
        assert_eq!(w.byte_offset(), 0);
    }

    // --- AlignedWriter<BufWriter<File>> ---

    #[test]
    fn buffered_writer_offsets() {
        let mut w = buffered_writer();
        let off0 = w.write_value(b"hello").unwrap();
        let off1 = w.write_value(b"world").unwrap();
        assert_eq!(off0, 0);
        assert_eq!(off1, 1);
        assert_eq!(w.byte_offset(), 128);
    }

    // --- pread roundtrip ---

    #[cfg(unix)]
    #[test]
    fn pread_roundtrip_single() {
        let mut w = writer();
        let payload = b"hello, world";
        let aligned_offset = w.write_value(payload).unwrap();
        let file = w.finish().unwrap();

        let got = pread(&file, aligned_offset * VALUE_ALIGNMENT, payload.len() as u32).unwrap();
        assert_eq!(got, payload);
    }

    #[cfg(unix)]
    #[test]
    fn pread_roundtrip_multiple() {
        let mut w = writer();

        let values: &[&[u8]] = &[b"alpha", b"beta", b"gamma gamma gamma"];
        let mut offsets = Vec::new();
        for v in values {
            offsets.push(w.write_value(v).unwrap());
        }
        let file = w.finish().unwrap();

        for (&v, &off) in values.iter().zip(offsets.iter()) {
            let got = pread(&file, off * VALUE_ALIGNMENT, v.len() as u32).unwrap();
            assert_eq!(got.as_slice(), v);
        }
    }

    #[cfg(unix)]
    #[test]
    fn pread_roundtrip_buffered() {
        let mut w = buffered_writer();
        let payload = b"via bufwriter";
        let aligned_offset = w.write_value(payload).unwrap();
        let buf_writer = w.finish().unwrap();
        let file = buf_writer.into_inner().unwrap();

        let got = pread(&file, aligned_offset * VALUE_ALIGNMENT, payload.len() as u32).unwrap();
        assert_eq!(got, payload);
    }

    #[cfg(unix)]
    #[test]
    fn padding_bytes_do_not_corrupt_neighbours() {
        let mut w = writer();
        // Write a 1-byte value; the remaining 63 bytes in that slot are pad.
        // The next value must start cleanly at offset 64.
        let off0 = w.write_value(b"X").unwrap();
        let off1 = w.write_value(b"Y").unwrap();
        assert_eq!(off0, 0);
        assert_eq!(off1, 1);

        let file = w.finish().unwrap();
        assert_eq!(pread(&file, 0,  1).unwrap(), b"X");
        assert_eq!(pread(&file, 64, 1).unwrap(), b"Y");
    }
}
