use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;

use kv_format::{
    data::AlignedWriter,
    index::fingerprint,
    meta::Layout,
};

use crate::{
    error::LoaderError,
    spill::{SpillRecord, SpillWriter},
};

// ---------------------------------------------------------------------------
// PartitionScatter
// ---------------------------------------------------------------------------

/// Writes one partition's `data.bin` and `spill.bin` during the scatter phase.
struct PartitionScatter {
    data:  AlignedWriter<BufWriter<File>>,
    spill: SpillWriter,
}

impl PartitionScatter {
    fn new(dir: &Path, data_buf_bytes: usize, spill_buf_bytes: usize) -> Result<Self, LoaderError> {
        fs::create_dir_all(dir)?;
        let file = File::create(dir.join("data.bin"))?;
        let data = AlignedWriter::new(BufWriter::with_capacity(data_buf_bytes, file));
        let spill = SpillWriter::create(&dir.join("spill.bin"), spill_buf_bytes)?;
        Ok(Self { data, spill })
    }

    fn write(&mut self, fp: u64, value: &[u8]) -> Result<(), LoaderError> {
        let aligned_offset = self.data.write_value(value)?;
        self.spill.push(SpillRecord {
            fingerprint:    fp,
            aligned_offset,
            size:           value.len() as u32,
            _pad:           0,
        })?;
        Ok(())
    }

    /// Flush `data.bin`, finalize `spill.bin`. Returns entry count for this partition.
    fn finish(self) -> Result<u64, LoaderError> {
        self.data.finish()?;
        self.spill.finish()
    }
}

// ---------------------------------------------------------------------------
// ScatterPhase
// ---------------------------------------------------------------------------

pub struct ScatterStats {
    pub n_keys:      u64,
    pub data_bytes:  u64,   // unpadded value bytes
}

pub struct ScatterPhase {
    layout:     Layout,
    partitions: Vec<PartitionScatter>,
    n_keys:     u64,
    data_bytes: u64,
}

impl ScatterPhase {
    pub fn new(
        root:            &Path,
        layout:          Layout,
        data_buf_bytes:  usize,
        spill_buf_bytes: usize,
    ) -> Result<Self, LoaderError> {
        let n = layout.n_partitions as usize;
        let width = format!("{}", layout.n_partitions - 1).len();
        let partitions = (0..n)
            .map(|i| {
                let dir = root.join(format!("part-{:0>width$}", i, width = width));
                PartitionScatter::new(&dir, data_buf_bytes, spill_buf_bytes)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { layout, partitions, n_keys: 0, data_bytes: 0 })
    }

    /// Route one key-value pair to its partition and write it.
    pub fn scatter(&mut self, key: &[u8], value: &[u8]) -> Result<(), LoaderError> {
        let fp  = fingerprint(key);
        let idx = self.layout.partition_of(fp);
        self.partitions[idx].write(fp, value)?;
        self.n_keys    += 1;
        self.data_bytes += value.len() as u64;
        Ok(())
    }

    /// Current `(n_keys, data_bytes)` counters for progress reporting.
    pub fn counters(&self) -> (u64, u64) {
        (self.n_keys, self.data_bytes)
    }

    /// Flush all partitions. Returns per-partition entry counts and aggregate stats.
    pub fn finish(self) -> Result<(Vec<u64>, ScatterStats), LoaderError> {
        let stats = ScatterStats { n_keys: self.n_keys, data_bytes: self.data_bytes };
        let counts = self.partitions
            .into_iter()
            .map(|p| p.finish())
            .collect::<Result<Vec<_>, _>>()?;
        Ok((counts, stats))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spill::SpillReader;
    use kv_format::{data::pread, meta::VALUE_ALIGNMENT};
    use tempfile::TempDir;

    fn layout(n: u32) -> Layout { Layout::new(n).unwrap() }

    fn scatter(pairs: &[(&[u8], &[u8])], n: u32) -> (TempDir, Vec<u64>) {
        let dir = TempDir::new().unwrap();
        let mut phase = ScatterPhase::new(dir.path(), layout(n), 1024 * 1024, 4096).unwrap();
        for &(k, v) in pairs {
            phase.scatter(k, v).unwrap();
        }
        let (counts, _) = phase.finish().unwrap();
        (dir, counts)
    }

    #[test]
    fn counts_sum_to_total() {
        let pairs: &[(&[u8], &[u8])] = &[
            (b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4"),
        ];
        let (_dir, counts) = scatter(pairs, 4);
        assert_eq!(counts.iter().sum::<u64>(), pairs.len() as u64);
    }

    #[test]
    fn spill_entries_match_values() {
        let pairs: &[(&[u8], &[u8])] = &[(b"hello", b"world"), (b"foo", b"bar")];
        let (dir, _) = scatter(pairs, 1);

        let spill_path = dir.path().join("part-0").join("spill.bin");
        let reader = SpillReader::open(&spill_path).unwrap();
        assert_eq!(reader.count(), 2);

        // Each spill record must have the right size field.
        let data_file = File::open(dir.path().join("part-0").join("data.bin")).unwrap();
        for rec in reader.records() {
            let rec = rec.unwrap();
            let val = pread(&data_file, rec.aligned_offset * VALUE_ALIGNMENT, rec.size).unwrap();
            // The value must be one of our input values.
            assert!(
                val == b"world" || val == b"bar",
                "unexpected value: {val:?}"
            );
        }
    }

    #[test]
    fn data_bin_alignment() {
        let (dir, _) = scatter(&[(b"k", b"v")], 1);
        let meta = std::fs::metadata(dir.path().join("part-0").join("data.bin")).unwrap();
        // File size must be a multiple of VALUE_ALIGNMENT (1-byte value → 64 bytes on disk).
        assert_eq!(meta.len() % VALUE_ALIGNMENT, 0);
        assert_eq!(meta.len(), 64);
    }

    #[test]
    fn scatter_stats() {
        let pairs: &[(&[u8], &[u8])] = &[(b"k1", b"hello"), (b"k2", b"world")];
        let dir = TempDir::new().unwrap();
        let mut phase = ScatterPhase::new(dir.path(), layout(1), 1024 * 1024, 4096).unwrap();
        for &(k, v) in pairs {
            phase.scatter(k, v).unwrap();
        }
        let (_, stats) = phase.finish().unwrap();
        assert_eq!(stats.n_keys,     2);
        assert_eq!(stats.data_bytes, 10); // "hello" + "world"
    }

    #[test]
    fn empty_scatter() {
        let (_dir, counts) = scatter(&[], 4);
        assert!(counts.iter().all(|&c| c == 0));
    }
}
