pub mod source;

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

use mcfreeze_format::builder::{builder_for, BuilderConfig, FormatBuilder, PartitionAppender};
pub use mcfreeze_format::builder::{MarkerMode, V5Options};
use mcfreeze_format::meta::{Layout, Stats, DEFAULT_VERIFY_SEED};
use mcfreeze_format::FormatId;

use scatter::{write_sentinel, Fanout, PartitionDone, ScatterDone, ScatterPhase};

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
    builder: &Arc<dyn FormatBuilder>,
    marker_mode: MarkerMode,
) -> Result<Option<ScatterDoneInfo>, LoaderError> {
    let sentinel = root.join("scatter.done");

    // Fast path: sentinel already written. An unparseable sentinel (a
    // torn write from before it was published atomically) must not wedge
    // resume: fall through to the probe-based re-derivation, which
    // rewrites it from per-partition state — the correct self-healing.
    if sentinel.exists() {
        let json = tokio::fs::read_to_string(&sentinel).await?;
        match serde_json::from_str::<ScatterDone>(&json) {
            Ok(done) => {
                // Resuming with a different --format would re-scatter
                // new-format state into a directory still holding the
                // old format's artifacts. Fail fast instead.
                if done.format != builder.format() {
                    return Err(LoaderError::FormatMismatch {
                        requested: builder.format(),
                        found: done.format,
                    });
                }
                return Ok(Some(ScatterDoneInfo {
                    n_keys: done.n_keys,
                    data_bytes: 0,
                }));
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "scatter.done unparseable (torn write?); re-deriving from partition state"
                );
            }
        }
    }

    // Fallback: derive completion from the format's transient state.
    // Value-size histograms are unavailable on this crash-recovery path
    // (the happy path gets them via scatter.done); key counts may also be
    // unknowable for already-built partitions.
    let n = layout.n_partitions as usize;
    let mut partitions = Vec::with_capacity(n);
    let mut data_bytes = 0u64;

    for i in 0..n {
        match builder.scatter_probe(i)? {
            None => return Ok(None),
            Some(probe) => {
                data_bytes += probe.data_bytes;
                partitions.push(PartitionDone {
                    n_keys: probe.n_keys,
                    value_sizes: None,
                });
            }
        }
    }

    let n_keys = partitions.iter().map(|p| p.n_keys).sum();

    let done = ScatterDone {
        format: builder.format(),
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
    write_sentinel(&sentinel, &json, marker_mode)?;

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
    /// On-disk format to write. Default: [`FormatId::DEFAULT`].
    pub format: FormatId,

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

    /// Reject any value larger than this many bytes at scatter time.
    /// Operational policy, not a format limit: a serving cache has no
    /// business holding values ~1000× the read-path latency budget, and
    /// an oversized value usually means a mis-mapped source column.
    /// Default: [`DEFAULT_MAX_VALUE_BYTES`] (16 MiB).
    pub max_value_bytes: usize,

    /// V5-specific build knobs (block size override, sketch, radix
    /// bucket target); ignored by other formats.
    pub v5: V5Options,
}

/// Default for [`LoaderConfig::max_value_bytes`]: 16 MiB — well above
/// any sane cache value (memcached's own default cap is 1 MiB), low
/// enough to catch a mis-mapped blob column before hours of scatter.
pub const DEFAULT_MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;

impl std::fmt::Debug for LoaderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoaderConfig")
            .field("format", &self.format)
            .field("n_partitions", &self.n_partitions)
            .field("data_buf_bytes", &self.data_buf_bytes)
            .field("spill_buf_bytes", &self.spill_buf_bytes)
            .field("channel_capacity", &self.channel_capacity)
            .field("index_parallelism", &self.index_parallelism)
            .field("progress_interval", &self.progress_interval)
            .field("progress_fn", &self.progress_fn.as_ref().map(|_| "<fn>"))
            .field("max_value_bytes", &self.max_value_bytes)
            .field("v5", &self.v5)
            .finish()
    }
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self {
            format: FormatId::DEFAULT,
            n_partitions: 64,
            data_buf_bytes: 8 * 1024 * 1024,
            spill_buf_bytes: 256 * 1024,
            channel_capacity: 8,
            index_parallelism: 2,
            progress_interval: 100_000,
            progress_fn: None,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            v5: V5Options::default(),
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
/// The format's per-partition build state lives inside the loader's
/// [`FormatBuilder`], not here.
pub struct ScatterResult {
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
    /// Format-erased write path, selected by `config.format`. The loader
    /// owns orchestration (sources, fingerprint routing, concurrency,
    /// sentinels); the builder owns what bytes hit disk.
    builder: Arc<dyn FormatBuilder>,
}

impl SnapshotLoader {
    pub fn new(root: impl AsRef<Path>, config: LoaderConfig) -> Result<Self, LoaderError> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        let builder = builder_for(
            config.format,
            BuilderConfig {
                root: root.clone(),
                n_partitions: config.n_partitions,
                verify_seed: DEFAULT_VERIFY_SEED,
                data_buf_bytes: config.data_buf_bytes,
                spill_buf_bytes: config.spill_buf_bytes,
                v5: config.v5.clone(),
            },
        )?;
        Ok(Self {
            root,
            config,
            builder,
        })
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
        let scatter_start = Instant::now();

        if let Some(info) = check_scatter_done(
            &self.root,
            layout,
            &self.builder,
            self.config.v5.marker_mode,
        )
        .await?
        {
            tracing::info!(n_keys = info.n_keys, "scatter already complete, skipping");
            Ok(ScatterResult {
                n_keys: info.n_keys,
                data_bytes: info.data_bytes,
                duration: Duration::ZERO,
            })
        } else {
            let stats = self
                .run_scatter_sources(layout, sources, scatter_start)
                .await?;
            Ok(ScatterResult {
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
        let Some(stats) = check_scatter_done(
            &self.root,
            layout,
            &self.builder,
            self.config.v5.marker_mode,
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(ScatterResult {
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
            n_keys: _scatter_n_keys,
            data_bytes,
            duration: scatter_duration,
        } = scatter_result;

        let index_start = Instant::now();
        let parallelism = self.config.index_parallelism;

        // Barrier between scatter and build: global, whole-snapshot
        // decisions (no-op for V4).
        self.builder.plan()?;

        let builder = Arc::clone(&self.builder);
        let build_done =
            tokio::task::spawn_blocking(move || builder.build(parallelism, index_progress_fn))
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

        let stats = Stats {
            n_keys: build_done.n_keys,
            created_at: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            scatter: scatter_val,
            index: index_val,
        };
        self.builder.finalize(stats, None)?;

        // Remove sentinels — their data is now in meta.json.
        let _ = tokio::fs::remove_file(&scatter_path).await;
        let _ = tokio::fs::remove_file(&index_path).await;

        Ok(LoadStats {
            n_keys: build_done.n_keys,
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
        let scatter_start = Instant::now();

        let (n_keys, data_bytes, duration) = if let Some(stats) = check_scatter_done(
            &self.root,
            layout,
            &self.builder,
            self.config.v5.marker_mode,
        )
        .await?
        {
            tracing::info!(n_keys = stats.n_keys, "scatter already complete, skipping");
            (stats.n_keys, stats.data_bytes, Duration::ZERO)
        } else {
            let appenders = self.create_appenders(layout)?;
            let phase = ScatterPhase::new(
                &self.root,
                layout,
                self.builder.format(),
                appenders,
                self.config.channel_capacity,
                self.config.max_value_bytes,
                self.config.v5.marker_mode,
            )?;
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
            (stats.n_keys, stats.data_bytes, scatter_start.elapsed())
        };

        self.finalize(
            ScatterResult {
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

    fn create_appenders(
        &self,
        layout: Layout,
    ) -> Result<Vec<Box<dyn PartitionAppender>>, LoaderError> {
        let n = layout.n_partitions as usize;
        (0..n).map(|p| Ok(self.builder.appender(p)?)).collect()
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
        let appenders = self.create_appenders(layout)?;
        let phase = ScatterPhase::new(
            &self.root,
            layout,
            self.builder.format(),
            appenders,
            self.config.channel_capacity,
            self.config.max_value_bytes,
            self.config.v5.marker_mode,
        )?;

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
