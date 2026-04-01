use std::fs::{self, File};
use std::path::Path;

use chrono::Utc;

use crate::{
    data::AlignedWriter,
    index::{self, Bucket, RawEntry, fingerprint},
    meta::{DEFAULT_VERIFY_SEED, FORMAT_VERSION, HASH_ALGORITHM,
           Layout, Meta, index_path, partition_dir},
    Result,
};

// ---------------------------------------------------------------------------
// PartitionWriter
// ---------------------------------------------------------------------------

/// Writes one partition's `data.bin` and accumulates index entries.
///
/// Phase 1 (`write`): stream key-value pairs → `data.bin`.
/// Phase 2 (`build_index`): flush data, build Robin Hood table in memory.
struct PartitionWriter {
    data:    AlignedWriter<File>,
    entries: Vec<RawEntry>,
}

impl PartitionWriter {
    fn new(dir: &Path, verify_seed: u64) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let data_file = File::create(dir.join("data.bin"))?;
        Ok(Self {
            data: AlignedWriter::new(data_file, verify_seed),
            entries: Vec::new(),
        })
    }

    /// Record one key-value pair into this partition.
    fn write(&mut self, key: &[u8], fp: u64, value: &[u8]) -> Result<()> {
        let (aligned_offset, _on_disk_size) = self.data.write_value(key, value)?;
        self.entries.push(RawEntry::new(fp, aligned_offset)?);
        Ok(())
    }

    /// Flush `data.bin` and build the Robin Hood index in memory.
    fn build_index(self) -> Result<(Vec<Bucket>, u64)> {
        self.data.finish()?;
        let n_keys     = self.entries.len() as u64;
        let (table, _) = index::build(&self.entries)?;
        Ok((table, n_keys))
    }
}

// ---------------------------------------------------------------------------
// SnapshotWriter
// ---------------------------------------------------------------------------

/// Writes a complete snapshot directory.
///
/// ```text
/// let mut w = SnapshotWriter::new("/snapshots/v42", 64)?;
/// for (key, value) in source {
///     w.write(key, value)?;
/// }
/// w.finish()?;
/// ```
pub struct SnapshotWriter {
    layout:      Layout,
    verify_seed: u64,
    partitions:  Vec<PartitionWriter>,
}

impl SnapshotWriter {
    /// Create the snapshot directory tree and open all partition writers.
    pub fn new(root: impl AsRef<Path>, n_partitions: u32) -> Result<Self> {
        let root        = root.as_ref();
        let layout      = Layout::new(n_partitions)?;
        let verify_seed = DEFAULT_VERIFY_SEED;

        fs::create_dir_all(root)?;

        let partitions = (0..n_partitions as usize)
            .map(|i| PartitionWriter::new(&partition_dir(root, n_partitions, i), verify_seed))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { layout, verify_seed, partitions })
    }

    /// Route a key-value pair to the correct partition and write it.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let fp  = fingerprint(key);
        let idx = self.layout.partition_of(fp);
        self.partitions[idx].write(key, fp, value)
    }

    /// Build all partition indexes, write `index.all`, then write `meta.json`.
    pub fn finish(self, root: impl AsRef<Path>) -> Result<()> {
        let root = root.as_ref();
        let n_partitions = self.layout.n_partitions;

        // Build all partition tables in memory.
        let mut tables       = Vec::with_capacity(n_partitions as usize);
        let mut n_keys_total = 0u64;
        for pw in self.partitions {
            let (table, n_keys) = pw.build_index()?;
            n_keys_total += n_keys;
            tables.push(table);
        }

        // Write unified index.all with 2MB-aligned partitions.
        let info = index::write_unified_index(&index_path(root), &tables)?;

        let meta = Meta {
            format_version:  FORMAT_VERSION,
            n_partitions,
            hash_algorithm:  HASH_ALGORITHM.to_string(),
            n_keys:          n_keys_total,
            verify_seed:     self.verify_seed,
            created_at:      Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            index_offsets:   info.offsets,
            index_n_buckets: info.n_buckets,
            scatter:         None,
            index:           None,
        };

        let json = serde_json::to_string_pretty(&meta)?;
        fs::write(root.join("meta.json"), json)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{DEFAULT_VERIFY_SEED, FILL_RATE};
    use tempfile::TempDir;

    fn temp_snapshot(n_partitions: u32) -> (TempDir, SnapshotWriter) {
        let dir = TempDir::new().unwrap();
        let w   = SnapshotWriter::new(dir.path(), n_partitions).unwrap();
        (dir, w)
    }

    // --- partition_dir naming ---

    #[test]
    fn partition_dir_padding_n64() {
        let root = Path::new("/snap");
        assert_eq!(partition_dir(root, 64, 0),  Path::new("/snap/data/part-00"));
        assert_eq!(partition_dir(root, 64, 7),  Path::new("/snap/data/part-07"));
        assert_eq!(partition_dir(root, 64, 63), Path::new("/snap/data/part-63"));
    }

    #[test]
    fn partition_dir_padding_n1() {
        let root = Path::new("/snap");
        assert_eq!(partition_dir(root, 1, 0), Path::new("/snap/data/part-0"));
    }

    // --- SnapshotWriter: directory creation ---

    #[test]
    fn creates_partition_dirs() {
        let (dir, _w) = temp_snapshot(4);
        for i in 0..4 {
            let p = partition_dir(dir.path(), 4, i);
            assert!(p.exists(), "{p:?} should exist");
        }
    }

    // --- SnapshotWriter: write + finish ---

    #[test]
    fn write_and_finish_produces_files() {
        let (dir, mut w) = temp_snapshot(4);
        w.write(b"hello", b"world").unwrap();
        w.write(b"foo",   b"bar").unwrap();
        w.finish(dir.path()).unwrap();

        assert!(dir.path().join("meta.json").exists());
        assert!(index_path(dir.path()).exists(), "index.all missing");
        for i in 0..4 {
            let p = partition_dir(dir.path(), 4, i);
            assert!(p.join("data.bin").exists(), "data.bin missing for part-{i}");
        }
    }

    #[test]
    fn meta_json_is_valid() {
        let (dir, mut w) = temp_snapshot(4);
        w.write(b"k1", b"v1").unwrap();
        w.finish(dir.path()).unwrap();

        let raw  = fs::read_to_string(dir.path().join("meta.json")).unwrap();
        let meta: Meta = serde_json::from_str(&raw).unwrap();
        assert_eq!(meta.format_version, FORMAT_VERSION);
        assert_eq!(meta.n_partitions,   4);
        assert_eq!(meta.n_keys,         1);
        assert_eq!(meta.verify_seed,    DEFAULT_VERIFY_SEED);
        assert_eq!(meta.index_offsets.len(),   4);
        assert_eq!(meta.index_n_buckets.len(), 4);
    }

    #[test]
    fn n_keys_counts_across_partitions() {
        let (dir, mut w) = temp_snapshot(4);
        let n = 200usize;
        for i in 0..n {
            let key = format!("key-{i}");
            w.write(key.as_bytes(), b"v").unwrap();
        }
        w.finish(dir.path()).unwrap();

        let raw  = fs::read_to_string(dir.path().join("meta.json")).unwrap();
        let meta: Meta = serde_json::from_str(&raw).unwrap();
        assert_eq!(meta.n_keys, n as u64);
    }

    #[test]
    fn index_bucket_count_respects_fill_rate() {
        let (dir, mut w) = temp_snapshot(1);
        let n = 100usize;
        for i in 0..n {
            w.write(format!("k{i}").as_bytes(), b"x").unwrap();
        }
        w.finish(dir.path()).unwrap();

        let raw  = fs::read_to_string(dir.path().join("meta.json")).unwrap();
        let meta: Meta = serde_json::from_str(&raw).unwrap();

        let expected = ((n as f64) / FILL_RATE).ceil() as u64;
        assert_eq!(meta.index_n_buckets[0], expected);
    }
}
