// SPDX-License-Identifier: Apache-2.0

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;

use crate::{
    data::AlignedWriter,
    index::{self, fingerprint, Bucket},
    meta::{
        index_path, partition_dir, Layout, Meta, PartitionMeta, Stats, DEFAULT_VERIFY_SEED,
        FORMAT_VERSION, HASH_ALGORITHM,
    },
    spill::{SpillReader, SpillWriter},
    Error, Result,
};

// ---------------------------------------------------------------------------
// PartitionWriter
// ---------------------------------------------------------------------------

/// Writes one partition's `data.bin` and spills index entries to `spill.bin`.
///
/// Phase 1 (`write`): stream key-value pairs → `data.bin` + `spill.bin`.
/// Phase 2 (`finish_data`): flush both files, return a [`PartitionBuildReady`]
///         that can build the Robin Hood index on demand.
///
/// Generic over the `data.bin` writer: use `File` for the simple path
/// (SnapshotWriter) or `BufWriter<File>` for the parallel loader path
/// where larger I/O buffers matter.
pub struct PartitionWriter<W: Write> {
    dir: PathBuf,
    data: AlignedWriter<W>,
    spill: SpillWriter,
}

impl PartitionWriter<File> {
    /// Create a partition writer that writes directly to unbuffered `File`.
    ///
    /// Suitable for the single-threaded `SnapshotWriter` path where the
    /// OS page cache is sufficient.
    pub fn new(dir: &Path, verify_seed: u64, spill_buf_bytes: usize) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let data_file = File::create(dir.join("data.bin"))?;
        let spill = SpillWriter::create(&dir.join("spill.bin"), spill_buf_bytes)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            data: AlignedWriter::new(data_file, verify_seed),
            spill,
        })
    }
}

impl PartitionWriter<BufWriter<File>> {
    /// Create a partition writer with a buffered `data.bin` writer.
    ///
    /// Suitable for the parallel loader path where each partition writer
    /// runs in its own thread and benefits from larger I/O buffers.
    pub fn new_buffered(
        dir: &Path,
        verify_seed: u64,
        data_buf_bytes: usize,
        spill_buf_bytes: usize,
    ) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let data_file =
            BufWriter::with_capacity(data_buf_bytes, File::create(dir.join("data.bin"))?);
        let spill = SpillWriter::create(&dir.join("spill.bin"), spill_buf_bytes)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            data: AlignedWriter::new(data_file, verify_seed),
            spill,
        })
    }
}

impl<W: Write> PartitionWriter<W> {
    /// Record one key-value pair into this partition.
    ///
    /// The caller provides the pre-computed full 64-bit fingerprint; the
    /// compact 32-bit form is derived internally.
    pub fn write(&mut self, key: &[u8], fp: u64, value: &[u8]) -> Result<()> {
        let (aligned_offset, _on_disk_size) = self.data.write_value(key, value)?;
        let bucket = Bucket::new(index::compact_fingerprint(fp), aligned_offset)?;
        self.spill.push(bucket)?;
        Ok(())
    }

    /// Flush `data.bin` and `spill.bin`, returning a handle that can build
    /// the index.
    pub fn finish_data(self) -> Result<PartitionBuildReady> {
        self.data.finish()?;
        self.spill.finish()?;
        Ok(PartitionBuildReady { dir: self.dir })
    }
}

// ---------------------------------------------------------------------------
// PartitionBuildReady
// ---------------------------------------------------------------------------

/// Per-partition statistics returned by [`PartitionBuildReady::build_index`].
pub struct PartitionIndexStats {
    pub n_keys: u64,
    pub n_buckets: u64,
    pub fill_rate: f64,
    pub retries: usize,
}

/// A partition whose data and spill phases are complete. Call
/// [`build_index`](Self::build_index) to read `spill.bin`, construct
/// the Robin Hood hash table, and clean up the spill file.
pub struct PartitionBuildReady {
    dir: PathBuf,
}

impl PartitionBuildReady {
    /// Resume from an existing partition directory that already has
    /// `data.bin` and `spill.bin` on disk (scatter was completed in a
    /// previous run).
    pub fn from_existing(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// Read `spill.bin` and build the Robin Hood table.
    ///
    /// Returns the bucket table and per-partition stats. The spill file is
    /// left on disk; call [`remove_spill`](Self::remove_spill) after the
    /// index has been durably written if crash-recovery semantics require it.
    pub fn build_index(&self) -> Result<(Vec<Bucket>, PartitionIndexStats)> {
        let spill_path = self.dir.join("spill.bin");

        if !spill_path.exists() {
            return Ok((
                Vec::new(),
                PartitionIndexStats {
                    n_keys: 0,
                    n_buckets: 0,
                    fill_rate: 0.0,
                    retries: 0,
                },
            ));
        }

        let spill = SpillReader::open(&spill_path)?;
        let n = spill.count() as usize;
        let entries: Vec<Bucket> = spill.records().collect::<Result<_>>()?;

        // Preflight: Robin Hood insertion caps PSL at u8::MAX. A set of
        // records sharing the same compact fingerprint forms a contiguous
        // chain whose length is a property of the input, not the table
        // size. Detect this up-front instead of looping forever.
        check_no_pathological_duplicates(&entries)?;

        let (table, retries) = index::build(&entries)?;
        let n_buckets = table.len();
        let fill_rate = if n_buckets > 0 {
            n as f64 / n_buckets as f64
        } else {
            0.0
        };

        Ok((
            table,
            PartitionIndexStats {
                n_keys: n as u64,
                n_buckets: n_buckets as u64,
                fill_rate,
                retries,
            },
        ))
    }

    /// Remove the `spill.bin` file after the index has been durably written.
    pub fn remove_spill(&self) -> Result<()> {
        let spill_path = self.dir.join("spill.bin");
        if spill_path.exists() {
            fs::remove_file(&spill_path)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SnapshotWriter
// ---------------------------------------------------------------------------

/// Default spill buffer size for the simple (single-threaded) path.
const DEFAULT_SPILL_BUF_BYTES: usize = 256 * 1024;

/// Writes a complete snapshot directory.
///
/// ```text
/// let mut w = SnapshotWriter::new("/snapshots/v42", 64)?;
/// for (key, value) in source {
///     w.write(key, value)?;
/// }
/// w.finish()?;
/// ```
///
/// For parallel pipelines, call [`into_partition_writers`](Self::into_partition_writers)
/// to decompose into per-partition writers that can be distributed across threads,
/// and a [`SnapshotFinalizer`] that reassembles the results.
pub struct SnapshotWriter {
    root: PathBuf,
    layout: Layout,
    verify_seed: u64,
    partitions: Vec<PartitionWriter<File>>,
}

impl SnapshotWriter {
    /// Create the snapshot directory tree and open all partition writers.
    pub fn new(root: impl AsRef<Path>, n_partitions: u32) -> Result<Self> {
        let root = root.as_ref();
        let layout = Layout::new(n_partitions)?;
        let verify_seed = DEFAULT_VERIFY_SEED;

        fs::create_dir_all(root)?;

        let partitions = (0..n_partitions as usize)
            .map(|i| {
                PartitionWriter::new(
                    &partition_dir(root, n_partitions, i),
                    verify_seed,
                    DEFAULT_SPILL_BUF_BYTES,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            root: root.to_path_buf(),
            layout,
            verify_seed,
            partitions,
        })
    }

    /// Route a key-value pair to the correct partition and write it.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let fp = fingerprint(key);
        let idx = self.layout.partition_of(fp);
        self.partitions[idx].write(key, fp, value)
    }

    /// Build all partition indexes sequentially, write `index.all`, then
    /// write `meta.json`.
    ///
    /// For parallel index builds, use [`into_partition_writers`](Self::into_partition_writers)
    /// with a [`SnapshotFinalizer`] instead.
    pub fn finish(self) -> Result<()> {
        let (partitions, finalizer) = self.into_partition_writers();

        let ready: Vec<PartitionBuildReady> = partitions
            .into_iter()
            .map(|pw| pw.finish_data())
            .collect::<Result<_>>()?;

        finalizer.finish(&ready)?;
        Ok(())
    }

    /// Decompose into per-partition writers and a finalizer.
    ///
    /// Use this for parallel pipelines where each `PartitionWriter` is
    /// driven by its own thread/task. Once all writers are done, pass the
    /// resulting [`PartitionBuildReady`] handles to
    /// [`SnapshotFinalizer::finish`].
    pub fn into_partition_writers(self) -> (Vec<PartitionWriter<File>>, SnapshotFinalizer) {
        let finalizer = SnapshotFinalizer {
            root: self.root,
            layout: self.layout,
            verify_seed: self.verify_seed,
        };
        (self.partitions, finalizer)
    }
}

// ---------------------------------------------------------------------------
// SnapshotFinalizer
// ---------------------------------------------------------------------------

/// Finalizes a snapshot after all partitions have been written.
///
/// Builds per-partition Robin Hood indexes, writes the unified `index.all`,
/// and writes `meta.json`.
pub struct SnapshotFinalizer {
    root: PathBuf,
    layout: Layout,
    verify_seed: u64,
}

impl SnapshotFinalizer {
    /// Create a finalizer for an existing snapshot directory (e.g. to resume
    /// after a crash where scatter completed but index build did not).
    pub fn from_existing(root: PathBuf, layout: Layout, verify_seed: u64) -> Self {
        Self {
            root,
            layout,
            verify_seed,
        }
    }

    /// The layout of the snapshot being finalized.
    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// The root directory of the snapshot.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Build all partition indexes and write `index.all` + `meta.json`.
    ///
    /// Each [`PartitionBuildReady`] reads its `spill.bin`, builds the Robin
    /// Hood table, and removes the spill file.
    pub fn finish(self, partitions: &[PartitionBuildReady]) -> Result<()> {
        let n = partitions.len();
        let mut tables = Vec::with_capacity(n);
        let mut n_keys_total = 0u64;

        for p in partitions {
            let (table, stats) = p.build_index()?;
            n_keys_total += stats.n_keys;
            tables.push(table);
        }

        let info = self.write_index(&tables)?;
        let stats = Stats {
            n_keys: n_keys_total,
            created_at: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            scatter: None,
            index: None,
        };
        self.write_meta(&info, Some(stats), None)?;

        for p in partitions {
            p.remove_spill()?;
        }

        Ok(())
    }

    /// Write only `index.all` from pre-built tables and return the per-partition
    /// offsets and bucket counts. Does **not** write `meta.json`.
    pub fn write_index(&self, tables: &[Vec<Bucket>]) -> Result<index::UnifiedIndexInfo> {
        index::write_unified_index(&index_path(&self.root), tables)
    }

    /// Write `meta.json` from index info and caller-provided stats.
    ///
    /// The caller supplies the [`UnifiedIndexInfo`](index::UnifiedIndexInfo)
    /// returned by [`write_index`](Self::write_index) and an optional
    /// [`Stats`] block. This method owns the `Meta` / `PartitionMeta`
    /// construction and serialization.
    pub fn write_meta(
        &self,
        info: &index::UnifiedIndexInfo,
        stats: Option<Stats>,
        encoding: Option<serde_json::Value>,
    ) -> Result<()> {
        let partitions = info
            .offsets
            .iter()
            .zip(&info.n_buckets)
            .map(|(&offset, &n_buckets)| PartitionMeta {
                index_offset: offset,
                index_n_buckets: n_buckets,
            })
            .collect();

        let meta = Meta {
            format_version: FORMAT_VERSION,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            verify_seed: self.verify_seed,
            partitions,
            stats,
            encoding,
        };

        let json = serde_json::to_string_pretty(&meta)?;
        fs::write(self.root.join("meta.json"), json)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Duplicate-fingerprint preflight
// ---------------------------------------------------------------------------

/// Maximum tolerated duplicate-fingerprint chain length.
///
/// Robin Hood insertion caps probe sequence length at `MAX_PSL = u8::MAX`.
/// A set of records all sharing the same compact fingerprint forms a
/// contiguous chain at a single home position; record `k` of that chain
/// ends up at PSL `k - 1`. Once the chain exceeds this limit, insertion
/// fails and growing the table has no effect — the chain length is a
/// property of the input, not the table size.
const MAX_DUPLICATE_FINGERPRINT_CHAIN: usize = 255;

fn check_no_pathological_duplicates(entries: &[Bucket]) -> Result<()> {
    use std::collections::HashMap;

    let mut counts: HashMap<u32, u32> = HashMap::with_capacity(entries.len());
    let mut worst: u32 = 0;
    for e in entries {
        let c = counts.entry(e.fingerprint).or_insert(0);
        *c += 1;
        if *c > worst {
            worst = *c;
            if worst as usize > MAX_DUPLICATE_FINGERPRINT_CHAIN {
                return Err(Error::DuplicateFingerprints {
                    max_count: worst as usize,
                    max_tolerated: MAX_DUPLICATE_FINGERPRINT_CHAIN,
                });
            }
        }
    }
    Ok(())
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
        let w = SnapshotWriter::new(dir.path(), n_partitions).unwrap();
        (dir, w)
    }

    // --- partition_dir naming ---

    #[test]
    fn partition_dir_padding_n64() {
        let root = Path::new("/snap");
        assert_eq!(partition_dir(root, 64, 0), Path::new("/snap/data/part-00"));
        assert_eq!(partition_dir(root, 64, 7), Path::new("/snap/data/part-07"));
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
        w.write(b"foo", b"bar").unwrap();
        w.finish().unwrap();

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
        w.finish().unwrap();

        let raw = fs::read_to_string(dir.path().join("meta.json")).unwrap();
        let meta: Meta = serde_json::from_str(&raw).unwrap();
        assert_eq!(meta.format_version, FORMAT_VERSION);
        assert_eq!(meta.partitions.len(), 4);
        assert_eq!(meta.verify_seed, DEFAULT_VERIFY_SEED);
        let stats = meta.stats.unwrap();
        assert_eq!(stats.n_keys, 1);
    }

    #[test]
    fn n_keys_counts_across_partitions() {
        let (dir, mut w) = temp_snapshot(4);
        let n = 200usize;
        for i in 0..n {
            let key = format!("key-{i}");
            w.write(key.as_bytes(), b"v").unwrap();
        }
        w.finish().unwrap();

        let raw = fs::read_to_string(dir.path().join("meta.json")).unwrap();
        let meta: Meta = serde_json::from_str(&raw).unwrap();
        assert_eq!(meta.stats.unwrap().n_keys, n as u64);
    }

    #[test]
    fn index_bucket_count_respects_fill_rate() {
        let (dir, mut w) = temp_snapshot(1);
        let n = 100usize;
        for i in 0..n {
            w.write(format!("k{i}").as_bytes(), b"x").unwrap();
        }
        w.finish().unwrap();

        let raw = fs::read_to_string(dir.path().join("meta.json")).unwrap();
        let meta: Meta = serde_json::from_str(&raw).unwrap();

        let expected = ((n as f64) / FILL_RATE).ceil() as u64;
        assert_eq!(meta.partitions[0].index_n_buckets, expected);
    }
}
