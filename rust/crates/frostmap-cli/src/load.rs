use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use tracing::info;

use frostmap_bq::{BqReadSession, BqSourceConfig};
use frostmap_loader::source::CsvSource;
use frostmap_loader::{KvBatch, KvSource, LoaderConfig, SnapshotLoader};

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Args)]
pub struct LoadArgs {
    /// Output snapshot directory (created if absent).
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Number of hash partitions — must be a power of two
    #[arg(long, default_value = "64")]
    pub partitions: u32,

    /// Rayon threads used for the parallel index build phase
    #[arg(long, default_value = "2")]
    pub index_parallelism: usize,

    /// Validate the configuration without writing any data
    #[arg(long)]
    pub dry_run: bool,

    /// Progress report interval in seconds.
    #[arg(long, default_value = "5")]
    pub progress_secs: u64,

    #[command(subcommand)]
    pub source: Source,
}

#[derive(Subcommand)]
pub enum Source {
    /// Load from the BigQuery Storage Read API
    Bq {
        /// GCP project used for billing.
        /// Defaults to the project embedded in --table when omitted.
        #[arg(long)]
        project: Option<String>,

        /// Table or view in dotted notation: project.dataset.table
        #[arg(long)]
        table: String,

        /// Arrow column name to use as the KV key
        #[arg(long, default_value = "key")]
        key_column: String,

        /// Arrow column name to use as the KV value
        #[arg(long, default_value = "value")]
        value_column: String,

        /// Number of parallel BQ read streams
        #[arg(long, default_value = "8")]
        streams: i32,

        /// Optional SQL WHERE predicate pushed down to BigQuery
        #[arg(long)]
        row_restriction: Option<String>,

        /// Disable LZ4 buffer compression.
        /// Recommended when values are high-entropy (hashes, ciphertext).
        #[arg(long)]
        no_compression: bool,

        /// Download all batches from the source and discard them, reporting
        /// throughput.  No data is written to --output.
        #[arg(long)]
        download_benchmark: bool,
    },
    /// Load from a two-column CSV (base64-encoded key, base64-encoded value).
    /// Reads from stdin if --file is not provided.
    Csv {
        /// Path to the CSV file. Reads from stdin when omitted.
        #[arg(long)]
        file: Option<PathBuf>,

        /// Number of rows per internal batch
        #[arg(long, default_value = "1024")]
        batch_size: usize,
    },
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run(args: LoadArgs) -> Result<()> {
    match args.source {
        Source::Bq {
            project,
            table,
            key_column,
            value_column,
            streams,
            row_restriction,
            no_compression,
            download_benchmark,
        } => {
            // Open BQ session even on dry-run to validate credentials and table.
            let session = open_bq_session(
                project,
                &table,
                &key_column,
                &value_column,
                streams,
                row_restriction.as_deref(),
                no_compression,
            )
            .await?;

            if args.dry_run {
                info!("dry-run: source validated, no data written");
                return Ok(());
            }

            let meta = session.metadata();
            let estimated_rows = meta.estimated_rows;

            let sources = session
                .into_sources()
                .context("failed to split session into sources")?;

            if download_benchmark {
                return benchmark_download(sources, estimated_rows, args.progress_secs).await;
            }

            let output = args.output.context("--output is required")?;
            load_sources(
                &output,
                args.partitions,
                args.index_parallelism,
                args.progress_secs,
                estimated_rows,
                sources,
            )
            .await
        }
        Source::Csv { file, batch_size } => {
            // Validate the source is readable even on dry-run.
            let source = open_csv_source(file, batch_size)?;

            if args.dry_run {
                info!("dry-run: source validated, no data written");
                return Ok(());
            }

            let output = args.output.context("--output is required")?;
            load_sources(
                &output,
                args.partitions,
                args.index_parallelism,
                args.progress_secs,
                None,
                vec![source],
            )
            .await
        }
    }
}

// ---------------------------------------------------------------------------
// Shared load orchestration
// ---------------------------------------------------------------------------

/// Load from one or more sources. All sources run in parallel via
/// `scatter_parallel`; a single-element vec works fine.
async fn load_sources<S>(
    output: &Path,
    partitions: u32,
    index_parallelism: usize,
    progress_secs: u64,
    estimated_rows: Option<u64>,
    sources: Vec<S>,
) -> Result<()>
where
    S: KvSource + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let scatter_reporter = ProgressReporter::new("scatter", estimated_rows, progress_secs);

    let loader_config = LoaderConfig {
        n_partitions: partitions,
        index_parallelism,
        progress_fn: Some(scatter_reporter.updater()),
        progress_interval: 0,
        ..LoaderConfig::default()
    };

    info!(output = %output.display(), partitions, n_sources = sources.len(), "loading snapshot");

    let loader =
        SnapshotLoader::new(output, loader_config).context("failed to create SnapshotLoader")?;

    let scatter_result = loader
        .scatter_parallel(sources)
        .await
        .context("scatter failed")?;

    scatter_reporter.stop();

    let index_reporter = ProgressReporter::new("index", Some(partitions as u64), progress_secs);
    let stats = loader
        .finalize(scatter_result, Some(index_reporter.updater()))
        .await
        .context("index/finalize failed")?;

    index_reporter.stop();
    log_stats(&stats);
    Ok(())
}

fn log_stats(stats: &frostmap_loader::LoadStats) {
    info!(
        n_keys = stats.n_keys,
        data_bytes = stats.data_bytes,
        scatter_secs = stats.scatter_duration.as_secs_f64(),
        index_secs = stats.index_duration.as_secs_f64(),
        "load complete",
    );
}

// ---------------------------------------------------------------------------
// Source constructors
// ---------------------------------------------------------------------------

async fn open_bq_session(
    project: Option<String>,
    table: &str,
    key_column: &str,
    value_column: &str,
    streams: i32,
    row_restriction: Option<&str>,
    no_compression: bool,
) -> Result<BqReadSession> {
    let (billing_project, table_resource) = parse_table(table, project.as_deref())?;

    let config = BqSourceConfig {
        project: billing_project.clone(),
        table: table_resource,
        key_column: key_column.to_owned(),
        value_column: value_column.to_owned(),
        n_streams: streams,
        row_restriction: row_restriction.map(|s| s.to_owned()),
        disable_compression: no_compression,
    };

    info!(
        billing_project = %billing_project,
        table,
        key_column,
        value_column,
        compression = if no_compression { "off" } else { "LZ4_FRAME" },
        row_restriction = row_restriction.unwrap_or(""),
        "opening BigQuery read session",
    );

    let session = BqReadSession::open(config)
        .await
        .context("failed to open BigQuery read session")?;

    let meta = session.metadata();
    info!(
        n_streams = session.n_streams(),
        estimated_rows = meta.estimated_rows,
        estimated_bytes = meta.estimated_bytes,
        "BigQuery read session opened",
    );

    Ok(session)
}

fn open_csv_source(
    file: Option<PathBuf>,
    batch_size: usize,
) -> Result<CsvSource<Box<dyn std::io::Read + Send>>> {
    match &file {
        Some(path) => {
            info!(file = %path.display(), "loading CSV from file");
            let f = std::fs::File::open(path)
                .with_context(|| format!("failed to open {}", path.display()))?;
            Ok(CsvSource::new(Box::new(f), batch_size))
        }
        None => {
            info!("loading CSV from stdin");
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut std::io::stdin().lock(), &mut buf)
                .context("failed to read stdin")?;
            Ok(CsvSource::new(
                Box::new(std::io::Cursor::new(buf)),
                batch_size,
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Download benchmark (BQ-specific)
// ---------------------------------------------------------------------------

async fn benchmark_download<S>(
    sources: Vec<S>,
    estimated_rows: Option<u64>,
    progress_secs: u64,
) -> Result<()>
where
    S: KvSource + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    info!(n_sources = sources.len(), "download benchmark started");
    let start = Instant::now();
    let reporter = ProgressReporter::new("download", estimated_rows, progress_secs);
    let updater = reporter.updater();

    let tasks: Vec<_> = sources
        .into_iter()
        .enumerate()
        .map(|(idx, mut src)| {
            let updater = updater.clone();
            tokio::spawn(async move {
                let mut n_keys: u64 = 0;
                let mut payload_bytes: u64 = 0;
                while let Some(batch) = src
                    .next_batch()
                    .await
                    .map_err(|e| anyhow::anyhow!("stream {idx}: {e}"))?
                {
                    let batch_keys = batch.len() as u64;
                    let batch_bytes = batch.total_bytes();
                    updater(batch_keys, batch_bytes);
                    n_keys += batch_keys;
                    payload_bytes += batch_bytes;
                }
                Ok::<(u64, u64), anyhow::Error>((n_keys, payload_bytes))
            })
        })
        .collect();

    let mut total_keys = 0u64;
    let mut total_bytes = 0u64;
    for task in tasks {
        let (keys, bytes) = task.await??;
        total_keys += keys;
        total_bytes += bytes;
    }

    reporter.stop();

    let elapsed = start.elapsed().as_secs_f64();
    let bytes_sec = if elapsed > 0.0 {
        (total_bytes as f64 / elapsed) as u64
    } else {
        0
    };

    info!(
        n_keys        = total_keys,
        payload_bytes = total_bytes,
        elapsed_secs  = elapsed,
        bytes_per_sec = bytes_sec,
        throughput    = %human_bandwidth(bytes_sec),
        "download benchmark complete",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_table(table: &str, project_override: Option<&str>) -> Result<(String, String)> {
    let (project, resource) = if table.starts_with("projects/") {
        let project = table
            .split('/')
            .nth(1)
            .context("malformed resource name, expected projects/P/datasets/D/tables/T")?
            .to_string();
        (project, table.to_string())
    } else {
        let parts: Vec<&str> = table.splitn(3, '.').collect();
        if parts.len() != 3 {
            bail!("--table must be in dotted notation project.dataset.table or full resource name projects/P/datasets/D/tables/T");
        }
        let resource = format!(
            "projects/{}/datasets/{}/tables/{}",
            parts[0], parts[1], parts[2]
        );
        (parts[0].to_string(), resource)
    };

    let billing = project_override.unwrap_or(&project).to_string();
    Ok((billing, resource))
}

pub fn human_bandwidth(bytes_per_sec: u64) -> String {
    const UNITS: &[&str] = &["B/s", "KB/s", "MB/s", "GB/s", "TB/s"];
    let mut value = bytes_per_sec as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit + 1 < UNITS.len() {
        value /= 1000.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

// ---------------------------------------------------------------------------
// Progress reporter
// ---------------------------------------------------------------------------

pub struct ProgressReporter {
    total_keys: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl ProgressReporter {
    pub fn new(phase: &'static str, estimated: Option<u64>, interval_secs: u64) -> Self {
        let total_keys = Arc::new(AtomicU64::new(0));
        let total_bytes = Arc::new(AtomicU64::new(0));
        let interval = Duration::from_secs(interval_secs.max(1));

        let task = tokio::spawn({
            let keys = total_keys.clone();
            let bytes = total_bytes.clone();
            async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.tick().await;
                let start = Instant::now();
                let mut prev_keys = 0u64;
                let mut prev_bytes = 0u64;
                let dt = interval.as_secs_f64();

                loop {
                    ticker.tick().await;
                    let cur_keys = keys.load(Ordering::Relaxed);
                    let cur_bytes = bytes.load(Ordering::Relaxed);
                    let elapsed = start.elapsed().as_secs_f64();
                    let recs_sec = ((cur_keys - prev_keys) as f64 / dt) as u64;
                    let bytes_sec = ((cur_bytes - prev_bytes) as f64 / dt) as u64;

                    let progress = match estimated {
                        Some(n) if n > 0 => format!(
                            "{}/{} ({:.1}%)",
                            cur_keys,
                            n,
                            cur_keys as f64 / n as f64 * 100.0,
                        ),
                        _ => format!("{cur_keys}"),
                    };

                    info!(
                        phase,
                        items         = %progress,
                        recs_sec,
                        throughput    = %human_bandwidth(bytes_sec),
                        elapsed_secs  = format!("{elapsed:.1}"),
                        "progress",
                    );

                    prev_keys = cur_keys;
                    prev_bytes = cur_bytes;
                }
            }
        });

        Self {
            total_keys,
            total_bytes,
            task,
        }
    }

    pub fn updater(&self) -> Arc<dyn Fn(u64, u64) + Send + Sync> {
        let keys = self.total_keys.clone();
        let bytes = self.total_bytes.clone();
        Arc::new(move |delta_keys, delta_bytes| {
            keys.fetch_add(delta_keys, Ordering::Relaxed);
            bytes.fetch_add(delta_bytes, Ordering::Relaxed);
        })
    }

    pub fn stop(self) {
        self.task.abort();
    }
}
