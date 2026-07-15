// SPDX-License-Identifier: Apache-2.0

//! V4 [`FormatBuilder`]: Robin Hood index build behind the format seam.
//!
//! The index-phase logic (per-partition table builds, unified `index.all`,
//! the `index.done` sentinel and its statistics schema) moved here verbatim
//! from the loader's `IndexBuildPhase` — the sentinel JSON is a stable
//! schema embedded into `meta.json` stats and must not drift.

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    builder::{BuildDone, BuilderConfig, FormatBuilder, PartitionAppender, ScatterProbe},
    desc::FormatId,
    index::{Bucket, UnifiedIndexInfo},
    meta::{index_path, partition_dir, Layout, Stats},
    v4::{
        spill::SpillReader,
        writer::{PartitionBuildReady, PartitionWriter, SnapshotFinalizer},
    },
    Error, Result,
};

// ---------------------------------------------------------------------------
// index.done JSON (stable schema; embedded into meta.json stats)
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
// V4Appender
// ---------------------------------------------------------------------------

struct V4Appender {
    writer: PartitionWriter<BufWriter<File>>,
    /// Deposits the sealed partition's build handle for the build phase.
    slot: Arc<Mutex<Option<PartitionBuildReady>>>,
}

impl PartitionAppender for V4Appender {
    fn append(&mut self, key: &[u8], fp: u64, value: &[u8]) -> Result<()> {
        self.writer.write(key, fp, value)
    }

    fn finish(self: Box<Self>) -> Result<()> {
        let ready = self.writer.finish_data()?;
        *self.slot.lock().expect("appender slot poisoned") = Some(ready);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// V4Builder
// ---------------------------------------------------------------------------

pub struct V4Builder {
    finalizer: SnapshotFinalizer,
    layout: Layout,
    verify_seed: u64,
    data_buf_bytes: usize,
    spill_buf_bytes: usize,
    /// Filled by appenders at scatter finish; missing entries are
    /// reconstructed with `PartitionBuildReady::from_existing` on resume.
    ready: Vec<Arc<Mutex<Option<PartitionBuildReady>>>>,
    /// Set by `build` (both live and sentinel-skip paths); consumed by
    /// `finalize` for `meta.json`'s partition table.
    index_info: Mutex<Option<UnifiedIndexInfo>>,
}

impl V4Builder {
    pub fn new(config: BuilderConfig) -> Result<Self> {
        let layout = Layout::new(config.n_partitions)?;
        let finalizer =
            SnapshotFinalizer::from_existing(config.root.clone(), layout, config.verify_seed);
        let n = config.n_partitions as usize;
        Ok(Self {
            finalizer,
            layout,
            verify_seed: config.verify_seed,
            data_buf_bytes: config.data_buf_bytes,
            spill_buf_bytes: config.spill_buf_bytes,
            ready: (0..n).map(|_| Arc::new(Mutex::new(None))).collect(),
            index_info: Mutex::new(None),
        })
    }

    fn partition_dir(&self, p: usize) -> PathBuf {
        partition_dir(self.finalizer.root(), self.layout.n_partitions, p)
    }

    /// Build handles for every partition: appender-deposited when this
    /// process ran scatter, reconstructed from disk on resume.
    fn take_ready(&self) -> Vec<PartitionBuildReady> {
        self.ready
            .iter()
            .enumerate()
            .map(|(p, slot)| {
                slot.lock()
                    .expect("appender slot poisoned")
                    .take()
                    .unwrap_or_else(|| PartitionBuildReady::from_existing(self.partition_dir(p)))
            })
            .collect()
    }
}

impl FormatBuilder for V4Builder {
    fn format(&self) -> FormatId {
        FormatId::V4
    }

    fn appender(&self, p: usize) -> Result<Box<dyn PartitionAppender>> {
        let writer = PartitionWriter::new_buffered(
            &self.partition_dir(p),
            self.verify_seed,
            self.data_buf_bytes,
            self.spill_buf_bytes,
        )?;
        Ok(Box::new(V4Appender {
            writer,
            slot: Arc::clone(&self.ready[p]),
        }))
    }

    fn scatter_probe(&self, p: usize) -> Result<Option<ScatterProbe>> {
        let dir = self.partition_dir(p);
        let data_path = dir.join("data.bin");
        let spill_path = dir.join("spill.bin");

        if !data_path.exists() {
            return Ok(None);
        }
        let data_bytes = std::fs::metadata(&data_path)?.len();

        if spill_path.exists() {
            let n_keys = SpillReader::open(&spill_path)?.count();
            Ok(Some(ScatterProbe { n_keys, data_bytes }))
        } else if index_path(self.finalizer.root()).exists() {
            // index.all exists but no spill — partition already indexed.
            // Per-partition key counts are unknowable without the phase
            // sentinel.
            Ok(Some(ScatterProbe {
                n_keys: 0,
                data_bytes,
            }))
        } else {
            Ok(None)
        }
    }

    fn plan(&self) -> Result<()> {
        Ok(()) // V4 has no global scatter→build decisions.
    }

    /// Build all partition indexes in parallel, write unified `index.all`.
    ///
    /// Idempotent at the phase level: if `index.done` already exists the
    /// phase is skipped entirely and its stats are returned from the file.
    fn build(
        &self,
        parallelism: usize,
        progress: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
    ) -> Result<BuildDone> {
        let root = self.finalizer.root();
        let sentinel = root.join("index.done");
        if sentinel.exists() {
            let json = std::fs::read_to_string(&sentinel)?;
            let done: IndexDone = serde_json::from_str(&json)?;
            tracing::info!(n_keys = done.n_keys, "index already complete, skipping");
            *self.index_info.lock().expect("index_info poisoned") = Some(UnifiedIndexInfo {
                offsets: done.index_offsets.clone(),
                n_buckets: done.index_n_buckets.clone(),
            });
            // A crash between writing index.done and the spill sweep
            // leaves spills behind, and every later run takes this path —
            // sweep them here (idempotent, best-effort).
            for p in 0..self.layout.n_partitions as usize {
                let spill = self.partition_dir(p).join("spill.bin");
                if spill.exists() {
                    let _ = std::fs::remove_file(&spill);
                }
            }
            return Ok(BuildDone {
                n_keys: done.n_keys,
            });
        }

        let start = Instant::now();
        let cb = &progress;
        let ready = self.take_ready();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism.max(1))
            .build()
            .map_err(|e| Error::Io(std::io::Error::other(e)))?;

        // Build all partition tables in parallel.
        let results: Vec<Result<(Vec<Bucket>, PartitionIndexDone)>> = pool.install(|| {
            ready
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
                        tracing::info!(
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
            index_offsets: info.offsets.clone(),
            index_n_buckets: info.n_buckets.clone(),
        };

        let json = serde_json::to_string_pretty(&done)?;
        std::fs::write(&sentinel, json)?;

        // Remove spill files only after index.done is durably written.
        // A crash before this point leaves spills intact for a full retry.
        for p in &ready {
            p.remove_spill()?;
        }

        *self.index_info.lock().expect("index_info poisoned") = Some(info);
        Ok(BuildDone { n_keys })
    }

    fn finalize(&self, stats: Stats, encoding: Option<serde_json::Value>) -> Result<()> {
        let guard = self.index_info.lock().expect("index_info poisoned");
        let info = guard.as_ref().ok_or(Error::FinalizeBeforeBuild)?;
        self.finalizer.write_meta(info, Some(stats), encoding)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fingerprint;
    use crate::meta::DEFAULT_VERIFY_SEED;
    use tempfile::TempDir;

    fn make_builder(root: &std::path::Path, n: u32) -> V4Builder {
        V4Builder::new(BuilderConfig {
            root: root.to_path_buf(),
            n_partitions: n,
            verify_seed: DEFAULT_VERIFY_SEED,
            data_buf_bytes: 1024 * 1024,
            spill_buf_bytes: 4096,
            v5: Default::default(),
        })
        .unwrap()
    }

    /// Scatter pairs through appenders (loader-style routing) and build.
    fn scatter_and_build(pairs: &[(&[u8], &[u8])], n: u32) -> (TempDir, V4Builder) {
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), n);
        let layout = Layout::new(n).unwrap();

        let mut apps: Vec<_> = (0..n as usize)
            .map(|p| builder.appender(p).unwrap())
            .collect();
        for &(k, v) in pairs {
            let fp = fingerprint(k);
            apps[layout.partition_of(fp)].append(k, fp, v).unwrap();
        }
        for a in apps {
            a.finish().unwrap();
        }

        builder.build(2, None).unwrap();
        (dir, builder)
    }

    #[test]
    fn scatter_probe_branches() {
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 1);

        // Fresh dir: no data.bin yet.
        assert!(builder.scatter_probe(0).unwrap().is_none());

        // Scattered: spill present, keys countable.
        let mut app = builder.appender(0).unwrap();
        let fp = fingerprint(b"k");
        app.append(b"k", fp, b"v").unwrap();
        app.finish().unwrap();
        let probe = builder.scatter_probe(0).unwrap().unwrap();
        assert_eq!(probe.n_keys, 1);
        assert!(probe.data_bytes > 0);

        // Built: spill gone, index.all present -> completed, count unknown.
        builder.build(1, None).unwrap();
        let probe = builder.scatter_probe(0).unwrap().unwrap();
        assert_eq!(probe.n_keys, 0);
    }

    #[test]
    fn build_resumes_from_existing_scatter_state() {
        // Scatter with one builder; build with a FRESH builder whose
        // appender slots are empty. take_ready must fall back to
        // PartitionBuildReady::from_existing.
        let dir = TempDir::new().unwrap();
        {
            let b1 = make_builder(dir.path(), 1);
            let mut app = b1.appender(0).unwrap();
            let fp = fingerprint(b"key");
            app.append(b"key", fp, b"val").unwrap();
            app.finish().unwrap();
        } // b1 dropped without building; no index.done exists.

        let b2 = make_builder(dir.path(), 1);
        let done = b2.build(1, None).unwrap();
        assert_eq!(done.n_keys, 1);
    }

    #[test]
    fn sentinel_skip_sweeps_leftover_spills() {
        // Simulate a crash after index.done was written but before the
        // spill sweep: the skip path must clean up.
        let (dir, _) = scatter_and_build(&[(b"a", b"1")], 1);
        let spill = partition_dir(dir.path(), 1, 0).join("spill.bin");
        std::fs::write(&spill, b"leftover").unwrap();

        let b = make_builder(dir.path(), 1);
        b.build(1, None).unwrap(); // sentinel-skip path
        assert!(!spill.exists(), "skip path must sweep leftover spills");
    }

    #[test]
    fn build_produces_index_all() {
        let (dir, _) = scatter_and_build(&[(b"k", b"v")], 4);
        assert!(index_path(dir.path()).exists(), "index.all should exist");
    }

    #[test]
    fn spill_files_removed_after_build() {
        let (dir, _) = scatter_and_build(&[(b"k", b"v")], 4);
        for i in 0..4 {
            let spill = partition_dir(dir.path(), 4, i).join("spill.bin");
            assert!(!spill.exists(), "spill.bin should be removed: {spill:?}");
        }
    }

    #[test]
    fn index_key_count() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        let (dir, _) = scatter_and_build(pairs, 1);
        let done_json = std::fs::read_to_string(dir.path().join("index.done")).unwrap();
        let done: IndexDone = serde_json::from_str(&done_json).unwrap();
        assert_eq!(done.n_keys, 3);
    }

    #[test]
    fn empty_partition_builds_ok() {
        let (dir, _) = scatter_and_build(&[], 4);
        assert!(index_path(dir.path()).exists());
    }

    #[test]
    fn build_is_idempotent_and_finalize_works_after_skip() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2")];
        let (dir, _) = scatter_and_build(pairs, 1);

        let idx = index_path(dir.path());
        let snapshot_bytes = std::fs::read(&idx).unwrap();

        // Fresh builder on the same dir: index.done exists → skip, but
        // finalize must still work (index_info restored from sentinel).
        let builder = make_builder(dir.path(), 1);
        let done = builder.build(1, None).unwrap();
        assert_eq!(done.n_keys, 2, "key count must be preserved on skip");
        assert_eq!(
            std::fs::read(&idx).unwrap(),
            snapshot_bytes,
            "index.all must be unchanged"
        );

        let stats = Stats {
            n_keys: done.n_keys,
            created_at: "2026-01-01T00:00:00Z".into(),
            scatter: None,
            index: None,
        };
        builder.finalize(stats, None).unwrap();

        // End-to-end: the built snapshot is readable through the facade.
        let snap = crate::Snapshot::open_path(dir.path()).unwrap();
        for &(k, v) in pairs {
            match snap.get(k).unwrap() {
                crate::GetOutcome::Hit(got) => assert_eq!(got, v),
                other => panic!("expected hit for {k:?}, got {other:?}"),
            }
        }
    }
}
