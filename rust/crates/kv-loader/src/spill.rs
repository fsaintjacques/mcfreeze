use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use bytemuck::{Pod, Zeroable};

use crate::error::LoaderError;

// ---------------------------------------------------------------------------
// SpillRecord — 24-byte fixed-size on-disk entry
// ---------------------------------------------------------------------------

pub const SPILL_MAGIC:       [u8; 8] = *b"KVSPILL\n";
pub const SPILL_HEADER_SIZE: usize   = 16; // magic(8) + count(8)
pub const SPILL_RECORD_SIZE: usize   = 24;

/// On-disk representation of one index entry accumulated during scatter.
///
/// 24 bytes (4-byte explicit pad after `size`) so that the struct is
/// 8-byte aligned and safe to cast via `bytemuck`.
#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
pub struct SpillRecord {
    pub fingerprint:    u64,  //  8
    pub aligned_offset: u64,  //  8
    pub size:           u32,  //  4
    pub _pad:           u32,  //  4  → total 24
}

const _: () = assert!(std::mem::size_of::<SpillRecord>() == SPILL_RECORD_SIZE);

// ---------------------------------------------------------------------------
// SpillWriter
// ---------------------------------------------------------------------------

pub struct SpillWriter {
    writer: BufWriter<File>,
    count:  u64,
}

impl SpillWriter {
    pub fn create(path: &Path, buf_bytes: usize) -> Result<Self, LoaderError> {
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(buf_bytes, file);
        // Write placeholder header; count is back-filled in finish().
        writer.write_all(&SPILL_MAGIC)?;
        writer.write_all(&0u64.to_le_bytes())?;
        Ok(Self { writer, count: 0 })
    }

    pub fn push(&mut self, record: SpillRecord) -> Result<(), LoaderError> {
        self.writer.write_all(bytemuck::bytes_of(&record))?;
        self.count += 1;
        Ok(())
    }

    /// Flush, seek back to offset 8, overwrite the count field, return count.
    pub fn finish(mut self) -> Result<u64, LoaderError> {
        self.writer.flush()?;
        let mut file = self.writer.into_inner()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        file.seek(SeekFrom::Start(8))?;
        file.write_all(&self.count.to_le_bytes())?;
        Ok(self.count)
    }
}

// ---------------------------------------------------------------------------
// SpillReader / SpillIter
// ---------------------------------------------------------------------------

pub struct SpillReader {
    file:  File,
    count: u64,
}

impl SpillReader {
    pub fn open(path: &Path) -> Result<Self, LoaderError> {
        let mut file = File::open(path)?;
        let mut hdr  = [0u8; SPILL_HEADER_SIZE];
        file.read_exact(&mut hdr)?;

        if hdr[..8] != SPILL_MAGIC {
            return Err(kv_format::Error::InvalidMagic.into());
        }
        let count = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
        Ok(Self { file, count })
    }

    pub fn count(&self) -> u64 { self.count }

    pub fn records(self) -> SpillIter {
        SpillIter {
            reader:    BufReader::with_capacity(256 * 1024, self.file),
            remaining: self.count,
        }
    }
}

pub struct SpillIter {
    reader:    BufReader<File>,
    remaining: u64,
}

impl Iterator for SpillIter {
    type Item = Result<SpillRecord, LoaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut buf = [0u8; SPILL_RECORD_SIZE];
        match self.reader.read_exact(&mut buf) {
            Err(e) => Some(Err(LoaderError::Io(e))),
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

    fn round_trip(records: &[SpillRecord]) -> Vec<SpillRecord> {
        let dir  = TempDir::new().unwrap();
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
        assert_eq!(std::mem::size_of::<SpillRecord>(), SPILL_RECORD_SIZE);
    }

    #[test]
    fn empty_spill() {
        let got = round_trip(&[]);
        assert!(got.is_empty());
    }

    #[test]
    fn single_record() {
        let r   = SpillRecord { fingerprint: 0xDEAD, aligned_offset: 42, size: 100, _pad: 0 };
        let got = round_trip(&[r]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].fingerprint,    r.fingerprint);
        assert_eq!(got[0].aligned_offset, r.aligned_offset);
        assert_eq!(got[0].size,           r.size);
    }

    #[test]
    fn many_records() {
        let records: Vec<SpillRecord> = (0u64..1000)
            .map(|i| SpillRecord { fingerprint: i, aligned_offset: i * 2, size: i as u32, _pad: 0 })
            .collect();
        let got = round_trip(&records);
        assert_eq!(got.len(), records.len());
        for (a, b) in got.iter().zip(records.iter()) {
            assert_eq!(a.fingerprint,    b.fingerprint);
            assert_eq!(a.aligned_offset, b.aligned_offset);
            assert_eq!(a.size,           b.size);
        }
    }

    #[test]
    fn spill_iter_size_hint() {
        let records: Vec<SpillRecord> = (0..5)
            .map(|i| SpillRecord { fingerprint: i, ..Default::default() })
            .collect();
        let dir  = TempDir::new().unwrap();
        let path = dir.path().join("spill.bin");
        let mut w = SpillWriter::create(&path, 4096).unwrap();
        for &r in &records { w.push(r).unwrap(); }
        w.finish().unwrap();

        let mut iter = SpillReader::open(&path).unwrap().records();
        assert_eq!(iter.size_hint(), (5, Some(5)));
        iter.next();
        assert_eq!(iter.size_hint(), (4, Some(4)));
    }
}
