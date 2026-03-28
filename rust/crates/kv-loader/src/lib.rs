pub mod source;

mod build;
mod error;
mod scatter;
mod spill;

pub use error::LoaderError;
pub use source::{KvBatch, KvSource, VecBatch};

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;

use kv_format::meta::{
    FORMAT_VERSION, HASH_ALGORITHM, OFFSET_BITS, PSL_BITS, SIZE_BITS, Layout, Meta,
};

use build::IndexBuildPhase;
use scatter::ScatterPhase;

// ---------------------------------------------------------------------------
// LoaderConfig
// ---------------------------------------------------------------------------

/// Configuration for [`SnapshotLoader`].
#[derive(Clone)]
pub struct LoaderConfig {
    /// Number of partitions. Must be a power of two. Default: 64.
    pub n_partitions: u32,

    /// `BufWriter` capacity for each partition's `data.bin`. Default: 8 MiB.
    pub data_buf_bytes: usize,

    /// `BufWriter` capacity for each partition's `spill.bin`. Default: 256 KiB.
    pub spill_buf_bytes: usize,

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
/// ```text
/// let loader = SnapshotLoader::new("/snapshots/v42", LoaderConfig::default())?;
/// loader.load(&mut CsvSource::from_path("data.csv", 1000)?)?;
/// ```
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

    pub fn load<S>(&self, source: &mut S) -> Result<LoadStats, LoaderError>
    where
        S: KvSource,
        LoaderError: From<S::Error>,
    {
        let layout = Layout::new(self.config.n_partitions)?;

        // --- Phase 1: scatter ---
        let scatter_start = Instant::now();
        let mut phase = ScatterPhase::new(
            &self.root,
            layout,
            self.config.data_buf_bytes,
            self.config.spill_buf_bytes,
        )?;

        let interval = self.config.progress_interval;
        let mut last_reported = 0u64;

        while let Some(batch) = source.next_batch().map_err(LoaderError::from)? {
            for (key, value) in batch.iter() {
                phase.scatter(key, value)?;
            }
            if let Some(ref cb) = self.config.progress_fn {
                let (n, b) = phase.counters();
                if n - last_reported >= interval {
                    cb(n, b);
                    last_reported = n;
                }
            }
        }

        let (_, scatter_stats) = phase.finish()?;
        let scatter_duration = scatter_start.elapsed();

        // --- Phase 2: index build ---
        let index_start = Instant::now();
        IndexBuildPhase::new(&self.root, layout, self.config.index_parallelism).run()?;
        let index_duration = index_start.elapsed();

        // --- Phase 3: meta.json (completion sentinel) ---
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
        let json = serde_json::to_string_pretty(&meta)
            .map_err(kv_format::Error::from)?;
        std::fs::write(self.root.join("meta.json"), json)?;

        Ok(LoadStats {
            n_keys:           scatter_stats.n_keys,
            data_bytes:       scatter_stats.data_bytes,
            scatter_duration,
            index_duration,
        })
    }
}
