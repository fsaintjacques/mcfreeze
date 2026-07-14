// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::index::Bucket;
use crate::{Error, Result};

// ---------------------------------------------------------------------------
// Spill file format
// ---------------------------------------------------------------------------

/// Magic bumped across two format revisions:
///
/// - `KVSPILL\n` (legacy): 24-byte record with full u64 fingerprint, u64
///   offset, and a dead `size: u32` + `_pad: u32`.
/// - `KVSPIL2\n`: 8-byte record, but `fingerprint` was the **low** 32 bits
///   of the xxhash64. Those low bits are also used by `Layout::partition_of`
///   for routing, so within a partition every compact fingerprint shared
///   the same low `log2(n_partitions)` bits. `cfp % n_buckets` collapsed
///   onto a small subset of the table and Robin Hood insertion looped
///   forever in PSL overflow retries.
/// - `KVSPIL3\n` (current): 8-byte record where `fingerprint` is the
///   **high** 32 bits of the xxhash64 via `compact_fingerprint`. Decouples
///   partition routing (low bits) from home-position computation (high
///   bits). Each record is a `Bucket` (8 bytes, `#[repr(C)]`).
pub const SPILL_MAGIC: [u8; 8] = *b"KVSPIL3\n";
pub const SPILL_HEADER_SIZE: usize = 16; // magic(8) + count(8)
pub const SPILL_RECORD_SIZE: usize = std::mem::size_of::<Bucket>();

const _: () = assert!(SPILL_RECORD_SIZE == 8);

// ---------------------------------------------------------------------------
// SpillWriter
// ---------------------------------------------------------------------------

pub struct SpillWriter {
    writer: BufWriter<File>,
    count: u64,
}

impl SpillWriter {
    pub fn create(path: &Path, buf_bytes: usize) -> Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(buf_bytes, file);
        // Write placeholder header; count is back-filled in finish().
        writer.write_all(&SPILL_MAGIC)?;
        writer.write_all(&0u64.to_le_bytes())?;
        Ok(Self { writer, count: 0 })
    }

    pub fn push(&mut self, record: Bucket) -> Result<()> {
        self.writer.write_all(bytemuck::bytes_of(&record))?;
        self.count += 1;
        Ok(())
    }

    /// Flush, seek back to offset 8, overwrite the count field, return count.
    pub fn finish(mut self) -> Result<u64> {
        self.writer.flush()?;
        let mut file = self
            .writer
            .into_inner()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        file.seek(SeekFrom::Start(8))?;
        file.write_all(&self.count.to_le_bytes())?;
        Ok(self.count)
    }
}

// ---------------------------------------------------------------------------
// SpillReader / SpillIter
// ---------------------------------------------------------------------------

pub struct SpillReader {
    file: File,
    count: u64,
}

impl SpillReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut hdr = [0u8; SPILL_HEADER_SIZE];
        file.read_exact(&mut hdr)?;

        if hdr[..8] != SPILL_MAGIC {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid magic bytes in spill header",
            )));
        }
        let count = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
        Ok(Self { file, count })
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn records(self) -> SpillIter {
        SpillIter {
            reader: BufReader::with_capacity(256 * 1024, self.file),
            remaining: self.count,
        }
    }
}

pub struct SpillIter {
    reader: BufReader<File>,
    remaining: u64,
}

impl Iterator for SpillIter {
    type Item = Result<Bucket>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut buf = [0u8; SPILL_RECORD_SIZE];
        match self.reader.read_exact(&mut buf) {
            Err(e) => Some(Err(Error::Io(e))),
            Ok(()) => {
                self.remaining -= 1;
                Some(Ok(*bytemuck::from_bytes(&buf)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.remaining as usize;
        (n, Some(n))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn round_trip(records: &[Bucket]) -> Vec<Bucket> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("spill.bin");

        let mut w = SpillWriter::create(&path, 4096).unwrap();
        for &r in records {
            w.push(r).unwrap();
        }
        let count = w.finish().unwrap();
        assert_eq!(count, records.len() as u64);

        let reader = SpillReader::open(&path).unwrap();
        assert_eq!(reader.count(), records.len() as u64);
        reader.records().map(|r| r.unwrap()).collect()
    }

    #[test]
    fn spill_record_size() {
        assert_eq!(std::mem::size_of::<Bucket>(), SPILL_RECORD_SIZE);
    }

    #[test]
    fn empty_spill() {
        let got = round_trip(&[]);
        assert!(got.is_empty());
    }

    #[test]
    fn single_record() {
        let r = Bucket {
            fingerprint: 0xDEAD,
            offset: 42,
        };
        let got = round_trip(&[r]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].fingerprint, r.fingerprint);
        assert_eq!(got[0].offset, r.offset);
    }

    #[test]
    fn many_records() {
        let records: Vec<Bucket> = (0u32..1000)
            .map(|i| Bucket {
                fingerprint: i + 1, // avoid zero-fingerprint sentinel
                offset: i * 2,
            })
            .collect();
        let got = round_trip(&records);
        assert_eq!(got.len(), records.len());
        for (a, b) in got.iter().zip(records.iter()) {
            assert_eq!(a.fingerprint, b.fingerprint);
            assert_eq!(a.offset, b.offset);
        }
    }

    #[test]
    fn spill_iter_size_hint() {
        let records: Vec<Bucket> = (0u32..5)
            .map(|i| Bucket {
                fingerprint: i + 1,
                offset: 0,
            })
            .collect();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("spill.bin");
        let mut w = SpillWriter::create(&path, 4096).unwrap();
        for &r in &records {
            w.push(r).unwrap();
        }
        w.finish().unwrap();

        let mut iter = SpillReader::open(&path).unwrap().records();
        assert_eq!(iter.size_hint(), (5, Some(5)));
        iter.next();
        assert_eq!(iter.size_hint(), (4, Some(4)));
    }
}
