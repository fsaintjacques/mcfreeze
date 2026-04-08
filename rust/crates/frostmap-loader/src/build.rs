use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;

use frostmap_format::{
    index::{self, Bucket, RawEntry},
    meta::{index_path, Layout},
};

use crate::{error::LoaderError, spill::SpillReader};

// ---------------------------------------------------------------------------
// index.done JSON
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionIndexDone {
    pub n_keys: u64,
    pub n_buckets: u64,
    pub fill_rate: f64,
    pub retries: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexDone {
    pub n_keys: u64,
    pub n_buckets: u64,
    pub fill_rate_min: f64,
    pub fill_rate_max: f64,
    pub fill_rate_mean: f64,
    pub n_overflow_partitions: u64,
    pub total_retries: u64,
    pub index_bytes: u64,
    pub wall_secs: Option<f64>,
    pub partitions_per_sec: Option<f64>,
    pub partitions: Vec<PartitionIndexDone>,
    pub index_offsets: Vec<u64>,
    pub index_n_buckets: Vec<u64>,
}

// ---------------------------------------------------------------------------
// IndexBuildPhase
// ---------------------------------------------------------------------------

pub struct IndexBuildPhase {
    root: std::path::PathBuf,
    layout: Layout,
    parallelism: usize,
    progress_fn: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
}

impl IndexBuildPhase {
    pub fn new(
        root: &Path,
        layout: Layout,
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

    /// Build all partition indexes in parallel, write unified `index.all`.
    ///
    /// Idempotent at the phase level: if `index.done` already exists the phase
    /// is skipped entirely and its stats are returned from the file.
    pub fn run(self) -> Result<IndexDone, LoaderError> {
        let sentinel = self.root.join("index.done");
        if sentinel.exists() {
            let json = std::fs::read_to_string(&sentinel)?;
            let done: IndexDone = serde_json::from_str(&json).map_err(|e| {
                LoaderError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            info!(n_keys = done.n_keys, "index already complete, skipping");
            return Ok(done);
        }

        let start = Instant::now();
        let n = self.layout.n_partitions as usize;
        let root = &self.root;
        let cb = &self.progress_fn;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.parallelism)
            .build()?;

        // Build all partition tables in parallel.
        let results: Vec<Result<(Vec<Bucket>, PartitionIndexDone), LoaderError>> =
            pool.install(|| {
                (0..n)
                    .into_par_iter()
                    .map(|i| {
                        let dir =
                            frostmap_format::meta::partition_dir(root, self.layout.n_partitions, i);
                        let result = build_partition_table(&dir)?;
                        if let Some(ref f) = cb {
                            f(1, 0);
                        }
                        Ok(result)
                    })
                    .collect()
            });

        let mut tables = Vec::with_capacity(n);
        let mut partitions = Vec::with_capacity(n);
        for r in results {
            let (table, part) = r?;
            tables.push(table);
            partitions.push(part);
        }

        // Write unified index.all.
        let info = index::write_unified_index(&index_path(root), &tables)?;

        let wall_secs = start.elapsed().as_secs_f64();

        // Aggregate stats.
        let n_keys = partitions.iter().map(|p| p.n_keys).sum();
        let n_buckets = partitions.iter().map(|p| p.n_buckets).sum();
        let total_retries = partitions.iter().map(|p| p.retries).sum();
        let n_overflow_partitions = partitions.iter().filter(|p| p.retries > 0).count() as u64;
        let index_bytes = std::fs::metadata(index_path(root))
            .map(|m| m.len())
            .unwrap_or(0);

        let fill_rates: Vec<f64> = partitions.iter().map(|p| p.fill_rate).collect();
        let fill_rate_min = fill_rates.iter().cloned().fold(f64::INFINITY, f64::min);
        let fill_rate_max = fill_rates.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let fill_rate_mean = if partitions.is_empty() {
            0.0
        } else {
            fill_rates.iter().sum::<f64>() / partitions.len() as f64
        };

        let partitions_per_sec = if wall_secs > 0.0 {
            Some(partitions.len() as f64 / wall_secs)
        } else {
            None
        };

        let done = IndexDone {
            n_keys,
            n_buckets,
            fill_rate_min: if fill_rate_min.is_infinite() {
                0.0
            } else {
                fill_rate_min
            },
            fill_rate_max: if fill_rate_max.is_infinite() {
                0.0
            } else {
                fill_rate_max
            },
            fill_rate_mean,
            n_overflow_partitions,
            total_retries,
            index_bytes,
            wall_secs: Some(wall_secs),
            partitions_per_sec,
            partitions,
            index_offsets: info.offsets,
            index_n_buckets: info.n_buckets,
        };

        let json = serde_json::to_string_pretty(&done)
            .map_err(|e| LoaderError::Io(std::io::Error::other(e)))?;
        std::fs::write(&sentinel, json)?;

        // Remove spill files only after index.done is durably written.
        // A crash before this point leaves spills intact for a full retry.
        for i in 0..n {
            let spill = frostmap_format::meta::partition_dir(root, self.layout.n_partitions, i)
                .join("spill.bin");
            if spill.exists() {
                std::fs::remove_file(&spill)?;
            }
        }

        Ok(done)
    }
}

// ---------------------------------------------------------------------------
// Per-partition table build (no I/O except reading spill.bin)
// ---------------------------------------------------------------------------

/// Read `spill.bin` and build the Robin Hood table in memory.
///
/// Returns the bucket table and per-partition stats.
fn build_partition_table(dir: &Path) -> Result<(Vec<Bucket>, PartitionIndexDone), LoaderError> {
    let spill_path = dir.join("spill.bin");

    if !spill_path.exists() {
        // Empty partition (no keys routed here).
        return Ok((
            Vec::new(),
            PartitionIndexDone {
                n_keys: 0,
                n_buckets: 0,
                fill_rate: 0.0,
                retries: 0,
            },
        ));
    }

    let spill = SpillReader::open(&spill_path)?;
    let n = spill.count() as usize;

    let entries: Vec<RawEntry> = spill
        .records()
        .map(|r| -> Result<RawEntry, LoaderError> {
            let r = r?;
            // Spill already holds compact fp + u32 offset; widen for the
            // current RawEntry API. insert() will re-narrow via
            // compact_fingerprint (idempotent) and cast offset back to u32.
            Ok(RawEntry::new(r.fingerprint as u64, r.offset as u64)?)
        })
        .collect::<Result<_, _>>()?;

    let (table, retries) = frostmap_format::index::build(&entries)?;
    let n_buckets = table.len() as u64;
    let fill_rate = if n_buckets > 0 {
        n as f64 / n_buckets as f64
    } else {
        0.0
    };

    if retries > 0 {
        info!(
            partition = %dir.display(),
            retries,
            n_buckets,
            n_keys = n,
            "PSL overflow: index rebuilt",
        );
    }

    Ok((
        table,
        PartitionIndexDone {
            n_keys: n as u64,
            n_buckets,
            fill_rate,
            retries: retries as u64,
        },
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scatter::ScatterPhase;
    use crate::source::VecBatch;
    use tempfile::TempDir;

    fn layout(n: u32) -> Layout {
        Layout::new(n).unwrap()
    }

    async fn scatter_and_build(pairs: &[(&[u8], &[u8])], n: u32) -> TempDir {
        let dir = TempDir::new().unwrap();
        let batch = VecBatch(
            pairs
                .iter()
                .map(|&(k, v)| (k.to_vec(), v.to_vec()))
                .collect(),
        );
        let phase = ScatterPhase::new(dir.path(), layout(n), 4, 1024 * 1024, 4096).unwrap();
        let mut fanout = phase.fanout();
        fanout.scatter_batch(&batch).await.unwrap();
        phase
            .finish(vec![fanout], std::time::Instant::now())
            .await
            .unwrap();
        IndexBuildPhase::new(dir.path(), layout(n), 2, None)
            .run()
            .unwrap();
        dir
    }

    #[tokio::test]
    async fn build_produces_index_all() {
        let dir = scatter_and_build(&[(b"k", b"v")], 4).await;
        assert!(index_path(dir.path()).exists(), "index.all should exist");
    }

    #[tokio::test]
    async fn spill_files_removed_after_build() {
        let dir = scatter_and_build(&[(b"k", b"v")], 4).await;
        for i in 0..4 {
            let spill = frostmap_format::meta::partition_dir(dir.path(), 4, i).join("spill.bin");
            assert!(!spill.exists(), "spill.bin should be removed: {spill:?}");
        }
    }

    #[tokio::test]
    async fn index_key_count() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        let dir = scatter_and_build(pairs, 1).await;

        let done_json = std::fs::read_to_string(dir.path().join("index.done")).unwrap();
        let done: IndexDone = serde_json::from_str(&done_json).unwrap();
        assert_eq!(done.n_keys, 3);
    }

    #[tokio::test]
    async fn empty_partition_builds_ok() {
        let dir = scatter_and_build(&[], 4).await;
        assert!(index_path(dir.path()).exists());
    }

    #[tokio::test]
    async fn index_build_is_idempotent() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2")];
        let dir = scatter_and_build(pairs, 1).await;

        let idx = index_path(dir.path());
        let snapshot = std::fs::read(&idx).unwrap();

        // Second run: index.done exists → skip.
        let done = IndexBuildPhase::new(dir.path(), layout(1), 1, None)
            .run()
            .unwrap();
        assert_eq!(done.n_keys, 2, "key count must be preserved on skip");
        assert_eq!(
            std::fs::read(&idx).unwrap(),
            snapshot,
            "index.all must be unchanged"
        );
    }
}
