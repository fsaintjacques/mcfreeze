use std::fs::{self, File};
use std::path::{Path, PathBuf};

use chrono::Utc;

use crate::{
    data::AlignedWriter,
    index::{self, IndexHeader, RawEntry, fingerprint},
    meta::{FORMAT_VERSION, HASH_ALGORITHM, OFFSET_BITS, PSL_BITS, SIZE_BITS, Layout, Meta, partition_dir},
    Result,
};

// ---------------------------------------------------------------------------
// PartitionWriter
// ---------------------------------------------------------------------------

/// Writes one partition's `data.bin` and `index.idx`.
///
/// Phase 1 (`write`): stream key-value pairs → `data.bin`.
/// Phase 2 (`finish`): build Robin Hood index → `index.idx`.
pub struct PartitionWriter {
    dir:     PathBuf,
    data:    AlignedWriter<File>,
    entries: Vec<RawEntry>,
}

impl PartitionWriter {
    fn new(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        let data_file = File::create(dir.join("data.bin"))?;
        Ok(Self {
            dir,
            data: AlignedWriter::new(data_file),
            entries: Vec::new(),
        })
    }

    /// Record one key-value pair into this partition.
    ///
    /// The caller must have already verified that this key belongs to this
    /// partition (i.e. `fingerprint & (N-1) == partition_index`).
    pub fn write(&mut self, fp: u64, value: &[u8]) -> Result<()> {
        let aligned_offset = self.data.write_value(value)?;
        self.entries.push(RawEntry::new(fp, aligned_offset, value.len() as u32)?);
        Ok(())
    }

    /// Build the Robin Hood index and write `index.idx`.
    pub fn finish(self) -> Result<u64> {
        self.data.finish()?;

        let n_keys        = self.entries.len() as u64;
        let (table, _)    = index::build(&self.entries)?;
        let n_buckets     = table.len() as u64;

        let header = IndexHeader { n_buckets, n_keys };
        let mut idx = File::create(self.dir.join("index.idx"))?;

        use std::io::Write;
        idx.write_all(&header.to_bytes())?;

        // SAFETY: Bucket is Pod (bytemuck) so the slice can be viewed as bytes.
        let bytes = bytemuck::cast_slice::<_, u8>(&table);
        idx.write_all(bytes)?;
        idx.flush()?;

        Ok(n_keys)
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
    layout:     Layout,
    partitions: Vec<PartitionWriter>,
}

impl SnapshotWriter {
    /// Create the snapshot directory tree and open all partition writers.
    pub fn new(root: impl AsRef<Path>, n_partitions: u32) -> Result<Self> {
        let root   = root.as_ref();
        let layout = Layout::new(n_partitions)?;

        fs::create_dir_all(root)?;

        let partitions = (0..n_partitions as usize)
            .map(|i| PartitionWriter::new(partition_dir(root, n_partitions, i)))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { layout, partitions })
    }

    /// Route a key-value pair to the correct partition and write it.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let fp  = fingerprint(key);
        let idx = self.layout.partition_of(fp);
        self.partitions[idx].write(fp, value)
    }

    /// Finish all partitions, then write `meta.json` as the completion signal.
    pub fn finish(self, root: impl AsRef<Path>) -> Result<()> {
        let root = root.as_ref();
        let n_partitions = self.layout.n_partitions;

        let mut n_keys_total = 0u64;
        for pw in self.partitions {
            n_keys_total += pw.finish()?;
        }

        let meta = Meta {
            format_version: FORMAT_VERSION,
            n_partitions,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            offset_bits:    OFFSET_BITS,
            size_bits:      SIZE_BITS,
            psl_bits:       PSL_BITS,
            n_keys:         n_keys_total,
            created_at:     Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            scatter:        None,
            index:          None,
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
    use crate::meta::FILL_RATE;
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
        for i in 0..4 {
            let p = partition_dir(dir.path(), 4, i);
            assert!(p.join("data.bin").exists(),  "data.bin missing for part-{i}");
            assert!(p.join("index.idx").exists(), "index.idx missing for part-{i}");
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
        assert_eq!(meta.offset_bits,    OFFSET_BITS);
        assert_eq!(meta.size_bits,      SIZE_BITS);
        assert_eq!(meta.psl_bits,       PSL_BITS);
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
        use crate::index::IndexHeader;
        use std::io::Read;

        let (dir, mut w) = temp_snapshot(1);
        let n = 100usize;
        for i in 0..n {
            w.write(format!("k{i}").as_bytes(), b"x").unwrap();
        }
        w.finish(dir.path()).unwrap();

        let idx_path = partition_dir(dir.path(), 1, 0).join("index.idx");
        let mut f    = File::open(&idx_path).unwrap();
        let mut hdr  = [0u8; 64];
        f.read_exact(&mut hdr).unwrap();
        let header   = IndexHeader::from_bytes(&hdr).unwrap();

        let expected = ((n as f64) / FILL_RATE).ceil() as u64;
        assert_eq!(header.n_buckets, expected);
        assert_eq!(header.n_keys,    n as u64);
    }
}
