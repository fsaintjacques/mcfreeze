use std::fs::File;
use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use bytemuck::cast_slice;
use serde::{Deserialize, Serialize};
use tracing::info;
use rayon::prelude::*;

use frostmap_format::{
    index::{Bucket, IndexHeader, RawEntry, INDEX_HEADER_SIZE},
    meta::Layout,
};

use crate::{
    error::LoaderError,
    spill::SpillReader,
};

// ---------------------------------------------------------------------------
// index.done JSON
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionIndexDone {
    pub n_keys:      u64,
    pub n_buckets:   u64,
    pub fill_rate:   f64,
    pub retries:     u64,
    pub index_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexDone {
    pub n_keys:                 u64,
    pub n_buckets:              u64,
    pub fill_rate_min:          f64,
    pub fill_rate_max:          f64,
    pub fill_rate_mean:         f64,
    pub n_overflow_partitions:  u64,
    pub total_retries:          u64,
    pub index_bytes:            u64,
    pub wall_secs:              Option<f64>,
    pub partitions_per_sec:     Option<f64>,
    pub partitions:             Vec<PartitionIndexDone>,
}

// ---------------------------------------------------------------------------
// IndexBuildPhase
// ---------------------------------------------------------------------------

pub struct IndexBuildPhase {
    root:        std::path::PathBuf,
    layout:      Layout,
    parallelism: usize,
    progress_fn: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
}

impl IndexBuildPhase {
    pub fn new(
        root:        &Path,
        layout:      Layout,
        parallelism: usize,
        progress_fn: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
    ) -> Self {
        Self {
            root: root.to_path_buf(),
            layout,
            parallelism: parallelism.max(1),
            progress_fn,
        }
    }

    /// Build all partition indexes in parallel.
    ///
    /// Idempotent at the phase level: if `index.done` already exists the phase
    /// is skipped entirely and its stats are returned from the file.
    pub fn run(self) -> Result<IndexDone, LoaderError> {
        let sentinel = self.root.join("index.done");
        if sentinel.exists() {
            let json = std::fs::read_to_string(&sentinel)?;
            let done: IndexDone = serde_json::from_str(&json)
                .map_err(|e| LoaderError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
            info!(n_keys = done.n_keys, "index already complete, skipping");
            return Ok(done);
        }

        let start = Instant::now();
        let n    = self.layout.n_partitions as usize;
        let root = &self.root;
        let cb   = &self.progress_fn;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.parallelism)
            .build()?;

        let results: Vec<Result<PartitionIndexDone, LoaderError>> = pool.install(|| {
            (0..n)
                .into_par_iter()
                .map(|i| {
                    let dir = frostmap_format::meta::partition_dir(root, self.layout.n_partitions, i);
                    let part   = build_partition(&dir)?;
                    if let Some(ref f) = cb {
                        f(1, 0);
                    }
                    Ok(part)
                })
                .collect()
        });

        let partitions: Vec<PartitionIndexDone> = results.into_iter().collect::<Result<_, _>>()?;

        let wall_secs = start.elapsed().as_secs_f64();

        // Aggregate stats.
        let n_keys                = partitions.iter().map(|p| p.n_keys).sum();
        let n_buckets             = partitions.iter().map(|p| p.n_buckets).sum();
        let index_bytes           = partitions.iter().map(|p| p.index_bytes).sum();
        let total_retries         = partitions.iter().map(|p| p.retries).sum();
        let n_overflow_partitions = partitions.iter().filter(|p| p.retries > 0).count() as u64;

        let fill_rates: Vec<f64> = partitions.iter().map(|p| p.fill_rate).collect();
        let fill_rate_min  = fill_rates.iter().cloned().fold(f64::INFINITY,  f64::min);
        let fill_rate_max  = fill_rates.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let fill_rate_mean = if partitions.is_empty() { 0.0 }
                             else { fill_rates.iter().sum::<f64>() / partitions.len() as f64 };

        let partitions_per_sec = if wall_secs > 0.0 {
            Some(partitions.len() as f64 / wall_secs)
        } else {
            None
        };

        let done = IndexDone {
            n_keys,
            n_buckets,
            fill_rate_min:         if fill_rate_min.is_infinite() { 0.0 } else { fill_rate_min },
            fill_rate_max:         if fill_rate_max.is_infinite() { 0.0 } else { fill_rate_max },
            fill_rate_mean,
            n_overflow_partitions,
            total_retries,
            index_bytes,
            wall_secs:             Some(wall_secs),
            partitions_per_sec,
            partitions,
        };

        let json = serde_json::to_string_pretty(&done)
            .map_err(|e| LoaderError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        std::fs::write(&sentinel, json)?;

        Ok(done)
    }
}

// ---------------------------------------------------------------------------
// Per-partition index build
// ---------------------------------------------------------------------------

/// Load `spill.bin`, build Robin Hood table, write `index.idx`.
///
/// Idempotent: if `index.idx` already exists (and `spill.bin` is absent) the
/// partition was successfully indexed in a previous run — skip it and return
/// the key count from the header.
///
/// To avoid leaving a partial index visible on failure, the file is written to
/// `index.idx.tmp` and renamed to `index.idx` only after a successful flush.
/// `spill.bin` is removed only after the rename succeeds.
fn build_partition(dir: &Path) -> Result<PartitionIndexDone, LoaderError> {
    let idx_path   = dir.join("index.idx");
    let spill_path = dir.join("spill.bin");

    // --- Skip if already indexed ---
    if idx_path.exists() && !spill_path.exists() {
        use std::io::Read;
        let mut f   = File::open(&idx_path)?;
        let mut buf = [0u8; INDEX_HEADER_SIZE];
        f.read_exact(&mut buf)?;
        let hdr         = IndexHeader::from_bytes(&buf)?;
        let index_bytes = INDEX_HEADER_SIZE as u64
            + hdr.n_buckets * size_of::<Bucket>() as u64;
        let fill_rate   = if hdr.n_buckets > 0 {
            hdr.n_keys as f64 / hdr.n_buckets as f64
        } else {
            0.0
        };
        return Ok(PartitionIndexDone {
            n_keys:      hdr.n_keys,
            n_buckets:   hdr.n_buckets,
            fill_rate,
            retries:     0,
            index_bytes,
        });
    }

    let spill = SpillReader::open(&spill_path)?;
    let n     = spill.count() as usize;

    let entries: Vec<RawEntry> = spill.records()
        .map(|r| -> Result<RawEntry, LoaderError> {
            let r = r?;
            Ok(RawEntry::new(r.fingerprint, r.aligned_offset, r.size)?)
        })
        .collect::<Result<_, _>>()?;

    let (table, retries) = frostmap_format::index::build(&entries)?;
    let n_buckets        = table.len() as u64;
    let index_bytes      = INDEX_HEADER_SIZE as u64 + n_buckets * size_of::<Bucket>() as u64;
    let fill_rate        = if n_buckets > 0 { n as f64 / n_buckets as f64 } else { 0.0 };

    if retries > 0 {
        info!(
            partition = %dir.display(),
            retries,
            n_buckets,
            n_keys = n,
            "PSL overflow: index rebuilt",
        );
    }

    // Write to a tmp file, then atomically rename.
    let tmp_path = dir.join("index.idx.tmp");
    {
        let mut f = BufWriter::new(File::create(&tmp_path)?);
        let header = IndexHeader { n_buckets, n_keys: n as u64 };
        f.write_all(&header.to_bytes())?;
        f.write_all(cast_slice::<Bucket, u8>(&table))?;
        f.flush()?;
    }
    std::fs::rename(&tmp_path, &idx_path)?;

    // Remove spill only after the index is durably in place.
    std::fs::remove_file(&spill_path)?;

    Ok(PartitionIndexDone { n_keys: n as u64, n_buckets, fill_rate, retries: retries as u64, index_bytes })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scatter::ScatterPhase;
    use crate::source::VecBatch;
    use frostmap_format::index::IndexHeader;
    use tempfile::TempDir;

    fn layout(n: u32) -> Layout { Layout::new(n).unwrap() }

    fn part_dir(root: &std::path::Path, n: u32, i: usize) -> std::path::PathBuf {
        frostmap_format::meta::partition_dir(root, n, i)
    }

    async fn scatter_and_build(pairs: &[(&[u8], &[u8])], n: u32) -> TempDir {
        let dir   = TempDir::new().unwrap();
        let batch = VecBatch(pairs.iter().map(|&(k, v)| (k.to_vec(), v.to_vec())).collect());
        let phase = ScatterPhase::new(dir.path(), layout(n), 4, 1024 * 1024, 4096).unwrap();
        let mut fanout = phase.fanout();
        fanout.scatter_batch(&batch).await.unwrap();
        phase.finish(vec![fanout], std::time::Instant::now()).await.unwrap();
        IndexBuildPhase::new(dir.path(), layout(n), 2, None).run().unwrap();
        dir
    }

    #[tokio::test]
    async fn build_produces_index_files() {
        let dir = scatter_and_build(&[(b"k", b"v")], 4).await;
        for i in 0..4 {
            let idx = part_dir(dir.path(), 4, i).join("index.idx");
            assert!(idx.exists(), "{idx:?}");
        }
    }

    #[tokio::test]
    async fn spill_files_removed_after_build() {
        let dir = scatter_and_build(&[(b"k", b"v")], 4).await;
        for i in 0..4 {
            let spill = part_dir(dir.path(), 4, i).join("spill.bin");
            assert!(!spill.exists(), "spill.bin should be removed: {spill:?}");
        }
    }

    #[tokio::test]
    async fn index_key_count() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        let dir = scatter_and_build(pairs, 1).await;

        let hdr_bytes = {
            use std::io::Read;
            let mut f   = File::open(part_dir(dir.path(), 1, 0).join("index.idx")).unwrap();
            let mut buf = [0u8; 64];
            f.read_exact(&mut buf).unwrap();
            buf
        };
        let hdr = IndexHeader::from_bytes(&hdr_bytes).unwrap();
        assert_eq!(hdr.n_keys, 3);
    }

    #[tokio::test]
    async fn empty_partition_builds_ok() {
        let dir = scatter_and_build(&[], 4).await;
        for i in 0..4 {
            let idx = part_dir(dir.path(), 4, i).join("index.idx");
            assert!(idx.exists());
        }
    }

    #[tokio::test]
    async fn index_build_is_idempotent() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2")];
        let dir = scatter_and_build(pairs, 1).await;

        let idx      = part_dir(dir.path(), 1, 0).join("index.idx");
        let tmp      = part_dir(dir.path(), 1, 0).join("index.idx.tmp");
        let snapshot = std::fs::read(&idx).unwrap();

        // Second run: spill.bin is gone, index.idx exists → skip.
        let done = IndexBuildPhase::new(dir.path(), layout(1), 1, None).run().unwrap();
        assert_eq!(done.n_keys, 2, "key count must be preserved on skip");
        assert_eq!(std::fs::read(&idx).unwrap(), snapshot, "index.idx must be unchanged");
        assert!(!tmp.exists(), "skip path must not leave a tmp file");
    }

    #[tokio::test]
    async fn tmp_file_removed_on_success() {
        let dir = scatter_and_build(&[(b"k", b"v")], 1).await;
        let tmp = part_dir(dir.path(), 1, 0).join("index.idx.tmp");
        assert!(!tmp.exists(), "index.idx.tmp should be gone after successful build");
    }
}
