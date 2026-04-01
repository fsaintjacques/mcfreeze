use std::fs::File;
use std::io::Write;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use crate::{
    index::{aligned_size, verify_fingerprint},
    meta::{VALUE_ALIGNMENT, VALUE_HEADER_SIZE},
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
    inner:        W,
    byte_offset:  u64,
    verify_seed:  u64,
}

impl<W: Write> AlignedWriter<W> {
    pub fn new(inner: W, verify_seed: u64) -> Self {
        assert!(verify_seed != 0, "verify_seed must be non-zero");
        Self { inner, byte_offset: 0, verify_seed }
    }

    /// Current byte position in the file (always a multiple of `VALUE_ALIGNMENT`).
    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    /// Write a value with its 12-byte header, pad to `VALUE_ALIGNMENT`, and
    /// return `(aligned_offset, on_disk_size)`.
    ///
    /// On-disk layout: `[8B verify_fp][4B byte_length][value bytes][padding to 64B]`
    ///
    /// `on_disk_size` includes the header and is what should be stored in the
    /// `loc` field of the index bucket.
    pub fn write_value(&mut self, key: &[u8], value: &[u8]) -> Result<(u64, u32)> {
        debug_assert_eq!(
            self.byte_offset % VALUE_ALIGNMENT,
            0,
            "invariant: byte_offset is always aligned"
        );

        let aligned_offset = self.byte_offset / VALUE_ALIGNMENT;

        // 12-byte header: 8B verify fingerprint + 4B value length.
        let vfp = verify_fingerprint(key, self.verify_seed);
        self.inner.write_all(&vfp.to_le_bytes())?;
        self.inner.write_all(&(value.len() as u32).to_le_bytes())?;
        self.inner.write_all(value)?;
        let on_disk_size = VALUE_HEADER_SIZE as u32 + value.len() as u32;

        let padded  = aligned_size(on_disk_size);
        let pad_len = (padded - on_disk_size as u64) as usize;
        if pad_len > 0 {
            self.inner.write_all(&PAD[..pad_len])?;
        }

        self.byte_offset += padded;
        Ok((aligned_offset, on_disk_size))
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

    const TEST_SEED: u64 = 42;

    fn writer() -> AlignedWriter<File> {
        AlignedWriter::new(tempfile().unwrap(), TEST_SEED)
    }

    fn buffered_writer() -> AlignedWriter<BufWriter<File>> {
        AlignedWriter::new(BufWriter::new(tempfile().unwrap()), TEST_SEED)
    }

    // --- AlignedWriter<File> ---

    #[test]
    fn write_single_value_aligned() {
        let mut w = writer();
        // 52 bytes value + 12 byte header = 64 bytes: no padding needed.
        let data = vec![0xAAu8; 52];
        let (off, size) = w.write_value(b"key", &data).unwrap();
        assert_eq!(off, 0);
        assert_eq!(size, 12 + 52);
        assert_eq!(w.byte_offset(), 64);
    }

    #[test]
    fn write_single_value_needs_padding() {
        let mut w = writer();
        let data = vec![0xBBu8; 1];
        // 1 byte value + 12 byte header = 13 → padded to 64.
        let (off, size) = w.write_value(b"key", &data).unwrap();
        assert_eq!(off, 0);
        assert_eq!(size, 13);
        assert_eq!(w.byte_offset(), 64);
    }

    #[test]
    fn write_multiple_values_offsets() {
        let mut w = writer();

        // value 1B + header 12B = 13B → padded to 64B
        let (off0, _) = w.write_value(b"k1", &vec![1u8; 1]).unwrap();
        // value 52B + header 12B = 64B → no padding
        let (off1, _) = w.write_value(b"k2", &vec![2u8; 52]).unwrap();
        // value 53B + header 12B = 65B → padded to 128B
        let (off2, _) = w.write_value(b"k3", &vec![3u8; 53]).unwrap();

        assert_eq!(off0, 0);   // byte 0  / 64 = 0
        assert_eq!(off1, 1);   // byte 64 / 64 = 1
        assert_eq!(off2, 2);   // byte 128 / 64 = 2

        assert_eq!(w.byte_offset(), 64 + 64 + 128);
    }

    #[test]
    fn write_empty_value() {
        let mut w = writer();
        // 0 byte value + 12 byte header = 12 → padded to 64.
        let (off, size) = w.write_value(b"key", &[]).unwrap();
        assert_eq!(off, 0);
        assert_eq!(size, 12);
        assert_eq!(w.byte_offset(), 64);
    }

    // --- AlignedWriter<BufWriter<File>> ---

    #[test]
    fn buffered_writer_offsets() {
        let mut w = buffered_writer();
        let (off0, _) = w.write_value(b"k1", b"hello").unwrap();
        let (off1, _) = w.write_value(b"k2", b"world").unwrap();
        assert_eq!(off0, 0);
        assert_eq!(off1, 1);
        // Each: 12 header + 5 value = 17 → padded to 64. Total: 128.
        assert_eq!(w.byte_offset(), 128);
    }

    // --- pread roundtrip (reads raw on-disk bytes including header) ---

    #[cfg(unix)]
    #[test]
    fn pread_roundtrip_single() {
        let mut w = writer();
        let payload = b"hello, world";
        let (aligned_offset, on_disk_size) = w.write_value(b"key", payload).unwrap();
        let file = w.finish().unwrap();

        let got = pread(&file, aligned_offset * VALUE_ALIGNMENT, on_disk_size).unwrap();
        // Raw bytes include the 12-byte header.
        assert_eq!(got.len(), VALUE_HEADER_SIZE + payload.len());
        assert_eq!(&got[VALUE_HEADER_SIZE..], payload);
    }

    #[cfg(unix)]
    #[test]
    fn pread_roundtrip_multiple() {
        let mut w = writer();

        let keys: &[&[u8]] = &[b"k1", b"k2", b"k3"];
        let values: &[&[u8]] = &[b"alpha", b"beta", b"gamma gamma gamma"];
        let mut entries = Vec::new();
        for (&k, &v) in keys.iter().zip(values.iter()) {
            entries.push(w.write_value(k, v).unwrap());
        }
        let file = w.finish().unwrap();

        for (&v, &(off, size)) in values.iter().zip(entries.iter()) {
            let got = pread(&file, off * VALUE_ALIGNMENT, size).unwrap();
            assert_eq!(&got[VALUE_HEADER_SIZE..], v);
        }
    }

    #[cfg(unix)]
    #[test]
    fn pread_roundtrip_buffered() {
        let mut w = buffered_writer();
        let payload = b"via bufwriter";
        let (aligned_offset, on_disk_size) = w.write_value(b"key", payload).unwrap();
        let buf_writer = w.finish().unwrap();
        let file = buf_writer.into_inner().unwrap();

        let got = pread(&file, aligned_offset * VALUE_ALIGNMENT, on_disk_size).unwrap();
        assert_eq!(&got[VALUE_HEADER_SIZE..], payload);
    }

    #[cfg(unix)]
    #[test]
    fn padding_bytes_do_not_corrupt_neighbours() {
        let mut w = writer();
        // Write two 1-byte values; each with 12-byte header = 13 bytes, padded to 64.
        let (off0, _) = w.write_value(b"k1", b"X").unwrap();
        let (off1, _) = w.write_value(b"k2", b"Y").unwrap();
        assert_eq!(off0, 0);
        assert_eq!(off1, 1);

        let file = w.finish().unwrap();
        // Read raw on-disk: header(12) + value(1) = 13 bytes.
        let raw0 = pread(&file, 0,  13).unwrap();
        let raw1 = pread(&file, 64, 13).unwrap();
        assert_eq!(&raw0[VALUE_HEADER_SIZE..], b"X");
        assert_eq!(&raw1[VALUE_HEADER_SIZE..], b"Y");
    }
}
