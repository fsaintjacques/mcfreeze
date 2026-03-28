pub mod source;

mod build;
mod error;
mod scatter;
mod spill;

pub use error::LoaderError;
pub use source::{KvBatch, KvSource, SourceMetadata, VecBatch};

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;

use kv_format::meta::{
    FORMAT_VERSION, HASH_ALGORITHM, OFFSET_BITS, PSL_BITS, SIZE_BITS, Layout, Meta,
};

use build::IndexBuildPhase;
use scatter::{Fanout, ScatterPhase};

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
            .field("n_partitions",      &self.n_partitions)
            .field("data_buf_bytes",    &self.data_buf_bytes)
            .field("spill_buf_bytes",   &self.spill_buf_bytes)
            .field("channel_capacity",  &self.channel_capacity)
            .field("index_parallelism", &self.index_parallelism)
            .field("progress_interval", &self.progress_interval)
            .field("progress_fn",       &self.progress_fn.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self {
            n_partitions:      64,
            data_buf_bytes:    8 * 1024 * 1024,
            spill_buf_bytes:   256 * 1024,
            channel_capacity:  8,
            index_parallelism: 2,
            progress_interval: 100_000,
            progress_fn:       None,
        }
    }
}

// ---------------------------------------------------------------------------
// LoadStats
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct LoadStats {
    pub n_keys:           u64,
    pub data_bytes:       u64,
    pub scatter_duration: Duration,
    pub index_duration:   Duration,
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
    root:   std::path::PathBuf,
    config: LoaderConfig,
}

impl SnapshotLoader {
    pub fn new(root: impl AsRef<Path>, config: LoaderConfig) -> Result<Self, LoaderError> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, config })
    }

    /// Load from multiple concurrent sources (e.g. BigQuery parallel read streams).
    ///
    /// Each source is driven by its own `tokio::spawn` task, each with its own
    /// [`Fanout`].  All fanouts write to the same set of partition writers via
    /// cloned channel senders — no locking, no serialization.
    pub async fn load_parallel<S>(&self, sources: Vec<S>) -> Result<LoadStats, LoaderError>
    where
        S: KvSource + Send + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let layout = Layout::new(self.config.n_partitions)?;

        let scatter_start = Instant::now();
        let scatter_stats = self.scatter_sources(layout, sources.into_iter()).await?;
        let scatter_duration = scatter_start.elapsed();

        self.build_and_finalize(layout, scatter_stats, scatter_duration).await
    }

    pub async fn load<S>(&self, source: &mut S) -> Result<LoadStats, LoaderError>
    where
        S: KvSource + Send,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let layout = Layout::new(self.config.n_partitions)?;
        let phase  = ScatterPhase::new(
            &self.root, layout,
            self.config.channel_capacity,
            self.config.data_buf_bytes,
            self.config.spill_buf_bytes,
        )?;

        let scatter_start    = Instant::now();
        let mut fanout       = phase.fanout();
        let progress_fn      = self.config.progress_fn.clone();
        let interval         = self.config.progress_interval;
        let mut last_reported = 0u64;

        while let Some(batch) = source.next_batch().await.map_err(|e| LoaderError::Source(Box::new(e)))? {
            fanout.scatter_batch(&batch).await?;
            if let Some(ref cb) = progress_fn {
                let (n, b) = fanout.counters();
                if n - last_reported >= interval {
                    cb(n, b);
                    last_reported = n;
                }
            }
        }

        let scatter_stats    = phase.finish(vec![fanout]).await?;
        let scatter_duration = scatter_start.elapsed();
        self.build_and_finalize(layout, scatter_stats, scatter_duration).await
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Drive `sources` concurrently through the fan-out, then join writers.
    ///
    /// Each source gets its own [`Fanout`] and runs in a `tokio::spawn` task.
    /// For the single-source `load` path this degenerates to one task with
    /// zero extra overhead.
    async fn scatter_sources<S, I>(
        &self,
        layout:  Layout,
        sources: I,
    ) -> Result<scatter::ScatterStats, LoaderError>
    where
        S: KvSource + Send + 'static,
        I: IntoIterator<Item = S>,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let phase = ScatterPhase::new(
            &self.root,
            layout,
            self.config.channel_capacity,
            self.config.data_buf_bytes,
            self.config.spill_buf_bytes,
        )?;

        let interval    = self.config.progress_interval;
        let progress_fn = self.config.progress_fn.clone();

        let tasks: Vec<_> = sources
            .into_iter()
            .map(|mut src| {
                let mut fanout  = phase.fanout();
                let progress_fn = progress_fn.clone();
                tokio::spawn(async move {
                    let mut last_reported = 0u64;
                    while let Some(batch) = src.next_batch().await.map_err(|e| LoaderError::Source(Box::new(e)))? {
                        fanout.scatter_batch(&batch).await?;
                        if let Some(ref cb) = progress_fn {
                            let (n, b) = fanout.counters();
                            if n - last_reported >= interval {
                                cb(n, b);
                                last_reported = n;
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

        phase.finish(fanouts).await
    }

    /// Index build + meta.json write, shared by `load` and `load_parallel`.
    async fn build_and_finalize(
        &self,
        layout:           Layout,
        scatter_stats:    scatter::ScatterStats,
        scatter_duration: Duration,
    ) -> Result<LoadStats, LoaderError> {
        let index_start = Instant::now();
        let root2       = self.root.clone();
        let parallelism = self.config.index_parallelism;

        tokio::task::spawn_blocking(move || {
            IndexBuildPhase::new(&root2, layout, parallelism).run()
        })
        .await??;

        let index_duration = index_start.elapsed();

        let meta = Meta {
            format_version: FORMAT_VERSION,
            n_partitions:   self.config.n_partitions,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            offset_bits:    OFFSET_BITS,
            size_bits:      SIZE_BITS,
            psl_bits:       PSL_BITS,
            n_keys:         scatter_stats.n_keys,
            created_at:     Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        };
        let json = serde_json::to_string_pretty(&meta).map_err(kv_format::Error::from)?;
        tokio::fs::write(self.root.join("meta.json"), json).await?;

        Ok(LoadStats {
            n_keys:           scatter_stats.n_keys,
            data_bytes:       scatter_stats.data_bytes,
            scatter_duration,
            index_duration,
        })
    }
}
