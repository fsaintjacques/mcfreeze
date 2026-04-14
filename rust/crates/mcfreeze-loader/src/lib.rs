pub mod source;

mod build;
mod error;
mod scatter;

pub use error::LoaderError;
pub use source::{
    for_each_key, key_column_size, ArrowKvBatch, BoxedRecordBatchSource, CsvSource, KvBatch,
    KvSource, RawEncodingSource, RecordBatchSource, SourceMetadata, VecBatch,
};

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;

use frostmap_format::meta::{Layout, Stats, DEFAULT_VERIFY_SEED};
use frostmap_format::spill::SpillReader;
use frostmap_format::writer::{PartitionBuildReady, PartitionWriter, SnapshotFinalizer};

use build::IndexBuildPhase;
use scatter::{Fanout, PartitionDone, ScatterDone, ScatterPhase};

// ---------------------------------------------------------------------------
// Scatter completion detection
// ---------------------------------------------------------------------------

/// Check whether the scatter phase has already completed for `root`.
///
/// Two paths:
/// 1. `scatter.done` exists and parses as JSON  →  fast path.
/// 2. Fallback (no sentinel): every partition must have `data.bin` plus either
///    `spill.bin` or `index.idx`.  Key counts and any available value sizes are
///    collected from those files, a `scatter.done` JSON is written, and scatter
///    is skipped.
///
/// Lightweight info returned when scatter was already completed.
struct ScatterDoneInfo {
    n_keys: u64,
    data_bytes: u64,
}

/// Returns `Some(ScatterDoneInfo)` when scatter can be skipped, `None` otherwise.
async fn check_scatter_done(
    root: &Path,
    layout: Layout,
) -> Result<Option<ScatterDoneInfo>, LoaderError> {
    let sentinel = root.join("scatter.done");

    // Fast path: sentinel already written.
    if sentinel.exists() {
        let json = tokio::fs::read_to_string(&sentinel).await?;
        let done: ScatterDone = serde_json::from_str(&json).map_err(|e| {
            LoaderError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        return Ok(Some(ScatterDoneInfo {
            n_keys: done.n_keys,
            data_bytes: 0,
        }));
    }

    // Fallback: derive completion from per-partition files.
    let n = layout.n_partitions as usize;
    let mut partitions = Vec::with_capacity(n);
    let mut data_bytes = 0u64;

    for i in 0..n {
        let dir = frostmap_format::meta::partition_dir(root, layout.n_partitions, i);
        let data_path = dir.join("data.bin");
        let spill_path = dir.join("spill.bin");

        if !data_path.exists() {
            return Ok(None);
        }
        data_bytes += std::fs::metadata(&data_path)?.len();

        if spill_path.exists() {
            // Spill no longer carries value sizes (they live in each value's
            // 12-byte header in data.bin). On this crash-recovery fallback
            // we record key counts only; the histogram is left as None. The
            // happy path still gets the full histogram via scatter.done.
            // TODO: re-introduce histogram reconstruction (by walking
            // data.bin headers or via a per-partition sidecar).
            let n_part = SpillReader::open(&spill_path)?.count();
            partitions.push(PartitionDone {
                n_keys: n_part,
                value_sizes: None,
            });
        } else if frostmap_format::meta::index_path(root).exists() {
            // index.all exists but no spill — partition was already indexed.
            // We can't know per-partition key counts without index.done, so
            // read it from the phase sentinel if available.
            partitions.push(PartitionDone {
                n_keys: 0,
                value_sizes: None,
            });
        } else {
            return Ok(None);
        }
    }

    let n_keys = partitions.iter().map(|p| p.n_keys).sum();

    let done = ScatterDone {
        n_keys,
        n_partitions: layout.n_partitions,
        data_bytes,
        wall_secs: None,
        bytes_per_sec: None,
        value_sizes: None,
        partitions,
    };
    let json = serde_json::to_string_pretty(&done)
        .map_err(|e| LoaderError::Io(std::io::Error::other(e)))?;
    tokio::fs::write(&sentinel, json).await?;

    Ok(Some(ScatterDoneInfo {
        n_keys,
        data_bytes: 0,
    }))
}

// ---------------------------------------------------------------------------
// LoaderConfig
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct LoaderConfig {
    /// Number of partitions. Must be a power of two. Default: 64.
    pub n_partitions: u32,

    /// `BufWriter` capacity for each partition's `data.bin`. Default: 8 MiB.
    pub data_buf_bytes: usize,

    /// `BufWriter` capacity for each partition's `spill.bin`. Default: 256 KiB.
    pub spill_buf_bytes: usize,

    /// Bounded channel capacity between the fan-out and each partition writer.
    /// Controls peak in-flight memory: `n_partitions × channel_capacity × avg_batch_bytes`.
    /// Default: 8.
    pub channel_capacity: usize,

    /// Number of partitions to build indexes for in parallel. Default: 2.
    pub index_parallelism: usize,

    /// Invoke the progress callback every N keys. Default: 100_000.
    pub progress_interval: u64,

    /// Optional progress callback: `fn(keys_processed, unpadded_bytes_written)`.
    pub progress_fn: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
}

impl std::fmt::Debug for LoaderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoaderConfig")
            .field("n_partitions", &self.n_partitions)
            .field("data_buf_bytes", &self.data_buf_bytes)
            .field("spill_buf_bytes", &self.spill_buf_bytes)
            .field("channel_capacity", &self.channel_capacity)
            .field("index_parallelism", &self.index_parallelism)
            .field("progress_interval", &self.progress_interval)
            .field("progress_fn", &self.progress_fn.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self {
            n_partitions: 64,
            data_buf_bytes: 8 * 1024 * 1024,
            spill_buf_bytes: 256 * 1024,
            channel_capacity: 8,
            index_parallelism: 2,
            progress_interval: 100_000,
            progress_fn: None,
        }
    }
}

// ---------------------------------------------------------------------------
// LoadStats
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct LoadStats {
    pub n_keys: u64,
    pub data_bytes: u64,
    pub scatter_duration: Duration,
    pub index_duration: Duration,
}

// ---------------------------------------------------------------------------
// ScatterResult
// ---------------------------------------------------------------------------

/// Opaque result of a completed scatter phase, consumed by [`SnapshotLoader::finalize`].
pub struct ScatterResult {
    finalizer: SnapshotFinalizer,
    partitions_ready: Vec<PartitionBuildReady>,
    n_keys: u64,
    data_bytes: u64,
    duration: Duration,
}

// ---------------------------------------------------------------------------
// SnapshotLoader
// ---------------------------------------------------------------------------

/// Builds a snapshot directory from any [`KvSource`].
///
/// `load` is async: the source is awaited for each batch (enabling async
/// sources such as BigQuery gRPC streams), while the CPU/I/O heavy phases
/// (scatter flush, index build) are dispatched to `spawn_blocking` to avoid
/// blocking the async executor for extended periods.
pub struct SnapshotLoader {
    root: std::path::PathBuf,
    config: LoaderConfig,
}

impl SnapshotLoader {
    pub fn new(root: impl AsRef<Path>, config: LoaderConfig) -> Result<Self, LoaderError> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, config })
    }

    /// Scatter sources into partition files, returning a [`ScatterResult`] that
    /// can be passed to [`finalize`] to build indexes and write `meta.json`.
    ///
    /// Splitting the two phases lets callers create phase-specific resources
    /// (e.g. a progress reporter) at exactly the right time.
    pub async fn scatter_parallel<S>(&self, sources: Vec<S>) -> Result<ScatterResult, LoaderError>
    where
        S: KvSource + Send + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let layout = Layout::new(self.config.n_partitions)?;
        let finalizer =
            SnapshotFinalizer::from_existing(self.root.clone(), layout, DEFAULT_VERIFY_SEED);
        let scatter_start = Instant::now();

        if let Some(info) = check_scatter_done(&self.root, layout).await? {
            tracing::info!(n_keys = info.n_keys, "scatter already complete, skipping");
            let n = layout.n_partitions as usize;
            let partitions_ready = (0..n)
                .map(|i| {
                    let dir =
                        frostmap_format::meta::partition_dir(&self.root, layout.n_partitions, i);
                    PartitionBuildReady::from_existing(dir)
                })
                .collect();
            Ok(ScatterResult {
                finalizer,
                partitions_ready,
                n_keys: info.n_keys,
                data_bytes: info.data_bytes,
                duration: Duration::ZERO,
            })
        } else {
            let stats = self
                .run_scatter_sources(layout, sources.into_iter(), scatter_start)
                .await?;
            Ok(ScatterResult {
                finalizer,
                partitions_ready: stats.partitions_ready,
                n_keys: stats.n_keys,
                data_bytes: stats.data_bytes,
                duration: scatter_start.elapsed(),
            })
        }
    }

    /// Build a [`ScatterResult`] from an existing `scatter.done`, without
    /// opening any source. Returns `Ok(None)` if scatter has not yet
    /// completed for this snapshot root.
    ///
    /// Use this on the resume path to run the index phase without
    /// constructing (and holding) source pipelines. Opening a BigQuery
    /// read session or similar source costs hundreds of MB to multiple
    /// GB of resident state — none of which is needed once scatter has
    /// already spilled every partition to disk.
    pub async fn scatter_result_from_done(&self) -> Result<Option<ScatterResult>, LoaderError> {
        let layout = Layout::new(self.config.n_partitions)?;
        let Some(stats) = check_scatter_done(&self.root, layout).await? else {
            return Ok(None);
        };
        let finalizer =
            SnapshotFinalizer::from_existing(self.root.clone(), layout, DEFAULT_VERIFY_SEED);
        let n = layout.n_partitions as usize;
        let partitions_ready: Vec<PartitionBuildReady> = (0..n)
            .map(|i| {
                let dir = frostmap_format::meta::partition_dir(&self.root, layout.n_partitions, i);
                PartitionBuildReady::from_existing(dir)
            })
            .collect();
        Ok(Some(ScatterResult {
            finalizer,
            partitions_ready,
            n_keys: stats.n_keys,
            data_bytes: stats.data_bytes,
            duration: Duration::ZERO,
        }))
    }

    /// Build indexes and write `meta.json` from a completed [`ScatterResult`].
    ///
    /// `index_progress_fn` is called with `(1, 0)` after each partition is
    /// indexed.  Pass `None` if progress reporting is not needed.
    pub async fn finalize(
        &self,
        scatter_result: ScatterResult,
        index_progress_fn: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
    ) -> Result<LoadStats, LoaderError> {
        let ScatterResult {
            finalizer,
            partitions_ready,
            n_keys: _scatter_n_keys,
            data_bytes,
            duration: scatter_duration,
        } = scatter_result;

        let index_start = Instant::now();
        let parallelism = self.config.index_parallelism;

        let (index_done, finalizer) = tokio::task::spawn_blocking(move || {
            IndexBuildPhase::new(finalizer, partitions_ready, parallelism, index_progress_fn).run()
        })
        .await??;

        let index_duration = index_start.elapsed();

        // Read the .done sentinels for embedding into meta.json stats.
        let scatter_path = self.root.join("scatter.done");
        let index_path = self.root.join("index.done");

        let scatter_val: Option<serde_json::Value> = tokio::fs::read_to_string(&scatter_path)
            .await
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());
        let index_val: Option<serde_json::Value> = tokio::fs::read_to_string(&index_path)
            .await
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .map(|mut v: serde_json::Value| {
                // Strip control data already present in meta.partitions.
                if let Some(obj) = v.as_object_mut() {
                    obj.remove("index_offsets");
                    obj.remove("index_n_buckets");
                }
                v
            });

        let info = frostmap_format::index::UnifiedIndexInfo {
            offsets: index_done.index_offsets,
            n_buckets: index_done.index_n_buckets,
        };
        let stats = Stats {
            n_keys: index_done.n_keys,
            created_at: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            scatter: scatter_val,
            index: index_val,
        };
        finalizer.write_meta(&info, Some(stats), None)?;

        // Remove sentinels — their data is now in meta.json.
        let _ = tokio::fs::remove_file(&scatter_path).await;
        let _ = tokio::fs::remove_file(&index_path).await;

        Ok(LoadStats {
            n_keys: index_done.n_keys,
            data_bytes,
            scatter_duration,
            index_duration,
        })
    }

    /// Convenience method: scatter + finalize with no index progress reporting.
    pub async fn load_parallel<S>(&self, sources: Vec<S>) -> Result<LoadStats, LoaderError>
    where
        S: KvSource + Send + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let sr = self.scatter_parallel(sources).await?;
        self.finalize(sr, None).await
    }

    pub async fn load<S>(&self, source: &mut S) -> Result<LoadStats, LoaderError>
    where
        S: KvSource + Send,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let layout = Layout::new(self.config.n_partitions)?;
        let finalizer =
            SnapshotFinalizer::from_existing(self.root.clone(), layout, DEFAULT_VERIFY_SEED);
        let scatter_start = Instant::now();

        let (n_keys, data_bytes, partitions_ready, duration) = if let Some(stats) =
            check_scatter_done(&self.root, layout).await?
        {
            tracing::info!(n_keys = stats.n_keys, "scatter already complete, skipping");
            let n = layout.n_partitions as usize;
            let partitions_ready: Vec<PartitionBuildReady> = (0..n)
                .map(|i| {
                    let dir =
                        frostmap_format::meta::partition_dir(&self.root, layout.n_partitions, i);
                    PartitionBuildReady::from_existing(dir)
                })
                .collect();
            (
                stats.n_keys,
                stats.data_bytes,
                partitions_ready,
                Duration::ZERO,
            )
        } else {
            let writers = self.create_partition_writers(layout)?;
            let phase =
                ScatterPhase::new(&self.root, layout, writers, self.config.channel_capacity)?;
            let mut fanout = phase.fanout();
            let progress_fn = self.config.progress_fn.clone();
            let interval = self.config.progress_interval;
            let mut last_reported_keys = 0u64;
            let mut last_reported_bytes = 0u64;

            while let Some(batch) = source
                .next_batch()
                .await
                .map_err(|e| LoaderError::Source(Box::new(e)))?
            {
                fanout.scatter_batch(&batch).await?;
                if let Some(ref cb) = progress_fn {
                    let (n, b) = fanout.counters();
                    if n - last_reported_keys >= interval {
                        cb(n - last_reported_keys, b - last_reported_bytes);
                        last_reported_keys = n;
                        last_reported_bytes = b;
                    }
                }
            }

            let stats = phase.finish(vec![fanout], scatter_start).await?;
            let pr = stats.partitions_ready;
            (stats.n_keys, stats.data_bytes, pr, scatter_start.elapsed())
        };

        self.finalize(
            ScatterResult {
                finalizer,
                partitions_ready,
                n_keys,
                data_bytes,
                duration,
            },
            None,
        )
        .await
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn create_partition_writers(
        &self,
        layout: Layout,
    ) -> Result<Vec<PartitionWriter<std::io::BufWriter<std::fs::File>>>, LoaderError> {
        let n = layout.n_partitions as usize;
        (0..n)
            .map(|i| {
                let dir = frostmap_format::meta::partition_dir(&self.root, layout.n_partitions, i);
                Ok(PartitionWriter::new_buffered(
                    &dir,
                    DEFAULT_VERIFY_SEED,
                    self.config.data_buf_bytes,
                    self.config.spill_buf_bytes,
                )?)
            })
            .collect()
    }

    async fn run_scatter_sources<S, I>(
        &self,
        layout: Layout,
        sources: I,
        start: Instant,
    ) -> Result<scatter::ScatterStats, LoaderError>
    where
        S: KvSource + Send + 'static,
        I: IntoIterator<Item = S>,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let writers = self.create_partition_writers(layout)?;
        let phase = ScatterPhase::new(&self.root, layout, writers, self.config.channel_capacity)?;

        let interval = self.config.progress_interval;
        let progress_fn = self.config.progress_fn.clone();

        let tasks: Vec<_> = sources
            .into_iter()
            .map(|mut src| {
                let mut fanout = phase.fanout();
                let progress_fn = progress_fn.clone();
                tokio::spawn(async move {
                    let mut last_reported_keys = 0u64;
                    let mut last_reported_bytes = 0u64;
                    while let Some(batch) = src
                        .next_batch()
                        .await
                        .map_err(|e| LoaderError::Source(Box::new(e)))?
                    {
                        fanout.scatter_batch(&batch).await?;
                        if let Some(ref cb) = progress_fn {
                            let (n, b) = fanout.counters();
                            if n - last_reported_keys >= interval {
                                cb(n - last_reported_keys, b - last_reported_bytes);
                                last_reported_keys = n;
                                last_reported_bytes = b;
                            }
                        }
                    }
                    Ok::<Fanout, LoaderError>(fanout)
                })
            })
            .collect();

        let mut fanouts = Vec::with_capacity(tasks.len());
        for task in tasks {
            fanouts.push(task.await??);
        }

        phase.finish(fanouts, start).await
    }
}
