// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;

use mcfreeze_format::{
    index::Bucket,
    meta::index_path,
    writer::{PartitionBuildReady, SnapshotFinalizer},
};

use crate::error::LoaderError;

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
    finalizer: SnapshotFinalizer,
    partitions: Vec<PartitionBuildReady>,
    parallelism: usize,
    progress_fn: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
}

impl IndexBuildPhase {
    pub fn new(
        finalizer: SnapshotFinalizer,
        partitions: Vec<PartitionBuildReady>,
        parallelism: usize,
        progress_fn: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
    ) -> Self {
        Self {
            finalizer,
            partitions,
            parallelism: parallelism.max(1),
            progress_fn,
        }
    }

    /// Build all partition indexes in parallel, write unified `index.all`.
    ///
    /// Idempotent at the phase level: if `index.done` already exists the phase
    /// is skipped entirely and its stats are returned from the file.
    pub fn run(self) -> Result<(IndexDone, SnapshotFinalizer), LoaderError> {
        let root = self.finalizer.root();
        let sentinel = root.join("index.done");
        if sentinel.exists() {
            let json = std::fs::read_to_string(&sentinel)?;
            let done: IndexDone = serde_json::from_str(&json).map_err(|e| {
                LoaderError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            info!(n_keys = done.n_keys, "index already complete, skipping");
            return Ok((done, self.finalizer));
        }

        let start = Instant::now();
        let cb = &self.progress_fn;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.parallelism)
            .build()?;

        // Build all partition tables in parallel.
        let results: Vec<Result<(Vec<Bucket>, PartitionIndexDone), LoaderError>> =
            pool.install(|| {
                self.partitions
                    .par_iter()
                    .map(|p| {
                        let (table, stats) = p.build_index()?;
                        let part = PartitionIndexDone {
                            n_keys: stats.n_keys,
                            n_buckets: stats.n_buckets,
                            fill_rate: stats.fill_rate,
                            retries: stats.retries as u64,
                        };
                        if stats.retries > 0 {
                            info!(
                                retries = stats.retries,
                                n_buckets = stats.n_buckets,
                                n_keys = stats.n_keys,
                                "PSL overflow: index rebuilt",
                            );
                        }
                        if let Some(ref f) = cb {
                            f(1, 0);
                        }
                        Ok((table, part))
                    })
                    .collect()
            });

        let mut tables = Vec::with_capacity(results.len());
        let mut partitions = Vec::with_capacity(results.len());
        for r in results {
            let (table, part) = r?;
            tables.push(table);
            partitions.push(part);
        }

        // Write unified index.all.
        let info = self.finalizer.write_index(&tables)?;

        let wall_secs = start.elapsed().as_secs_f64();

        // Aggregate stats.
        let n_keys = partitions.iter().map(|p| p.n_keys).sum();
        let n_buckets = partitions.iter().map(|p| p.n_buckets).sum();
        let total_retries = partitions.iter().map(|p| p.retries).sum();
        let n_overflow_partitions = partitions.iter().filter(|p| p.retries > 0).count() as u64;
        let index_bytes = std::fs::metadata(index_path(self.finalizer.root()))
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
        for p in &self.partitions {
            p.remove_spill()?;
        }

        Ok((done, self.finalizer))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scatter::ScatterPhase;
    use crate::source::VecBatch;
    use mcfreeze_format::meta::{Layout, DEFAULT_VERIFY_SEED};
    use mcfreeze_format::writer::PartitionWriter;
    use std::io::BufWriter;
    use tempfile::TempDir;

    fn layout(n: u32) -> Layout {
        Layout::new(n).unwrap()
    }

    fn make_writers(
        root: &std::path::Path,
        n: u32,
    ) -> Vec<PartitionWriter<BufWriter<std::fs::File>>> {
        (0..n as usize)
            .map(|i| {
                let dir = mcfreeze_format::meta::partition_dir(root, n, i);
                PartitionWriter::new_buffered(&dir, DEFAULT_VERIFY_SEED, 1024 * 1024, 4096).unwrap()
            })
            .collect()
    }

    async fn scatter_and_build(pairs: &[(&[u8], &[u8])], n: u32) -> TempDir {
        let dir = TempDir::new().unwrap();
        let batch = VecBatch(
            pairs
                .iter()
                .map(|&(k, v)| (k.to_vec(), v.to_vec()))
                .collect(),
        );
        let writers = make_writers(dir.path(), n);
        let phase = ScatterPhase::new(dir.path(), layout(n), writers, 4).unwrap();
        let mut fanout = phase.fanout();
        fanout.scatter_batch(&batch).await.unwrap();
        let stats = phase
            .finish(vec![fanout], std::time::Instant::now())
            .await
            .unwrap();
        let finalizer = SnapshotFinalizer::from_existing(
            dir.path().to_path_buf(),
            layout(n),
            DEFAULT_VERIFY_SEED,
        );
        let (_done, _finalizer) = IndexBuildPhase::new(finalizer, stats.partitions_ready, 2, None)
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
            let spill = mcfreeze_format::meta::partition_dir(dir.path(), 4, i).join("spill.bin");
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
        let finalizer = SnapshotFinalizer::from_existing(
            dir.path().to_path_buf(),
            layout(1),
            DEFAULT_VERIFY_SEED,
        );
        let (done, _finalizer) = IndexBuildPhase::new(finalizer, Vec::new(), 1, None)
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
