use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use tracing::info;

use base64::{engine::general_purpose::STANDARD, Engine as _};

use frostmap_bq::{BqReadSession, BqSourceConfig};
use frostmap_encode::config::{ProtobufEncoding, WorkerConfig};
use frostmap_loader::{
    BoxedRecordBatchSource, CsvSource, KvBatch, KvSource, LoaderConfig, RawEncodingSource,
    SnapshotLoader,
};

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

    /// Download all batches from the source and discard them, reporting
    /// throughput.  No data is written to --output.
    #[arg(long)]
    pub download_benchmark: bool,

    // -- Column mapping (shared across all sources) -------------------------
    /// Arrow column name to use as the KV key
    #[arg(long, default_value = "key")]
    pub key_column: String,

    /// Arrow column name to use as the KV value (raw encoding).
    /// Mutually exclusive with --protobuf-message.
    #[arg(long, default_value = "value")]
    pub value_column: String,

    // -- Protobuf encoding (optional) ---------------------------------------
    /// Protobuf message name — enables protobuf encoding.
    /// When set, all non-key columns are transcoded into a protobuf message.
    #[arg(long)]
    pub protobuf_message: Option<String>,

    /// Protobuf package name (required for auto-generated descriptors).
    #[arg(long)]
    pub protobuf_package: Option<String>,

    /// Base64-encoded FileDescriptorSet, or path to a .desc file.
    #[arg(long)]
    pub protobuf_descriptor: Option<String>,

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

        /// Column projection — only read these columns.
        #[arg(long)]
        selected_fields: Vec<String>,
    },
    /// Load from a CSV file with headers. Reads from stdin if --file is omitted or "-".
    Csv {
        /// Path to the CSV file. Omit or pass "-" to read from stdin.
        #[arg(long)]
        file: Option<PathBuf>,

        /// Number of rows per internal batch
        #[arg(long, default_value = "1024")]
        batch_size: usize,
    },
    /// Load from a JSON config file (for worker / K8s Job use).
    Config {
        /// Path to the JSON config file.
        #[arg(long)]
        config: PathBuf,
    },
}

// ---------------------------------------------------------------------------
// Pipeline — unified source + encoding + action
// ---------------------------------------------------------------------------

/// Descriptor metadata from protobuf encoding, for patching into meta.json.
struct DescriptorMeta {
    descriptor_bytes: Vec<u8>,
    message_fqn: String,
}

/// Everything needed to run a load or benchmark after sources are opened.
struct Pipeline {
    sources: Vec<BoxedRecordBatchSource>,
    schema: arrow::datatypes::Schema,
    estimated_rows: Option<u64>,
    key_column: String,
    value_column: String,
    encoding: Option<ProtobufEncoding>,
    progress_secs: u64,
}

impl Pipeline {
    /// Run the full load pipeline: encode → scatter → index → meta.json.
    async fn load(self, output: &Path, partitions: u32, index_parallelism: usize) -> Result<()> {
        let Self {
            sources,
            schema,
            estimated_rows,
            key_column,
            value_column,
            encoding,
            progress_secs,
        } = self;
        let key_col_idx = schema
            .index_of(&key_column)
            .with_context(|| format!("key column {:?} not found in schema", key_column))?;

        match encoding {
            Some(proto_config) => {
                let (sources, desc) =
                    apply_protobuf_encoding(sources, &schema, key_col_idx, &proto_config)?;
                load_sources(
                    output,
                    partitions,
                    index_parallelism,
                    progress_secs,
                    estimated_rows,
                    sources,
                )
                .await?;
                patch_meta_descriptor(output, &desc.descriptor_bytes, &desc.message_fqn).await
            }
            None => {
                let val_col_idx = schema.index_of(&value_column).with_context(|| {
                    format!("value column {:?} not found in schema", value_column)
                })?;
                let sources: Vec<_> = sources
                    .into_iter()
                    .map(|s| RawEncodingSource::new(s, key_col_idx, val_col_idx))
                    .collect();
                load_sources(
                    output,
                    partitions,
                    index_parallelism,
                    progress_secs,
                    estimated_rows,
                    sources,
                )
                .await
            }
        }
    }

    /// Download benchmark: encode + discard, reporting throughput.
    async fn benchmark(self) -> Result<()> {
        let Self {
            sources,
            schema,
            estimated_rows,
            key_column,
            value_column,
            encoding,
            progress_secs,
        } = self;
        let key_col_idx = schema
            .index_of(&key_column)
            .with_context(|| format!("key column {:?} not found in schema", key_column))?;

        match encoding {
            Some(proto_config) => {
                let (sources, _) =
                    apply_protobuf_encoding(sources, &schema, key_col_idx, &proto_config)?;
                benchmark_download(sources, estimated_rows, progress_secs).await
            }
            None => {
                let val_col_idx = schema.index_of(&value_column).with_context(|| {
                    format!("value column {:?} not found in schema", value_column)
                })?;
                let sources: Vec<_> = sources
                    .into_iter()
                    .map(|s| RawEncodingSource::new(s, key_col_idx, val_col_idx))
                    .collect();
                benchmark_download(sources, estimated_rows, progress_secs).await
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run(args: LoadArgs) -> Result<()> {
    let pipeline = build_pipeline(&args).await?;

    if args.dry_run {
        info!("dry-run: source validated, no data written");
        return Ok(());
    }

    if args.download_benchmark {
        pipeline.benchmark().await
    } else {
        let output = args.output.context("--output is required")?;
        pipeline
            .load(&output, args.partitions, args.index_parallelism)
            .await
    }
}

// ---------------------------------------------------------------------------
// Source construction — single function for all source types
// ---------------------------------------------------------------------------

async fn build_pipeline(args: &LoadArgs) -> Result<Pipeline> {
    let encoding = if args.protobuf_message.is_some() {
        Some(build_protobuf_config(args)?)
    } else {
        None
    };

    match &args.source {
        Source::Bq {
            project,
            table,
            streams,
            row_restriction,
            no_compression,
            selected_fields,
        } => {
            let session = open_bq_session(
                project.clone(),
                table,
                *streams,
                row_restriction.as_deref(),
                *no_compression,
                selected_fields.clone(),
            )
            .await?;

            let schema = session.schema().context("failed to get Arrow schema")?;
            let estimated_rows = session.metadata().estimated_rows;
            let sources: Vec<_> = session
                .into_record_batch_sources()
                .context("failed to create record batch sources")?
                .into_iter()
                .map(BoxedRecordBatchSource::new)
                .collect();

            Ok(Pipeline {
                sources,
                schema,
                estimated_rows,
                key_column: args.key_column.clone(),
                value_column: args.value_column.clone(),
                encoding,
                progress_secs: args.progress_secs,
            })
        }
        Source::Csv { file, batch_size } => {
            let use_stdin = file.as_ref().is_none_or(|p| p.as_os_str() == "-");

            let csv_source = if use_stdin {
                info!(key_column = %args.key_column, "loading CSV from stdin");
                CsvSource::from_reader(std::io::stdin().lock(), *batch_size)
                    .context("failed to read CSV from stdin")?
            } else {
                let path = file.as_ref().unwrap();
                info!(file = %path.display(), key_column = %args.key_column, "loading CSV from file");
                CsvSource::from_reader(
                    std::fs::File::open(path)
                        .with_context(|| format!("failed to open {}", path.display()))?,
                    *batch_size,
                )
                .context("failed to read CSV")?
            };

            let schema = csv_source.schema().as_ref().clone();

            Ok(Pipeline {
                sources: vec![BoxedRecordBatchSource::new(csv_source)],
                schema,
                estimated_rows: None,
                key_column: args.key_column.clone(),
                value_column: args.value_column.clone(),
                encoding,
                progress_secs: args.progress_secs,
            })
        }
        Source::Config { config } => {
            let config_bytes = std::fs::read(config)
                .with_context(|| format!("failed to read config file {}", config.display()))?;
            let wc: WorkerConfig = serde_json::from_slice(&config_bytes)
                .with_context(|| format!("failed to parse config file {}", config.display()))?;

            let bq = wc
                .source
                .bigquery
                .as_ref()
                .context("only bigquery source is currently supported in config mode")?;

            let encoding = wc.source.encoding.as_ref().and_then(|e| e.protobuf.clone());

            let (billing_project, table_resource) = parse_table(&bq.table, Some(&bq.project))?;
            let bq_config = BqSourceConfig {
                project: billing_project,
                table: table_resource,
                selected_fields: bq.selected_fields.clone(),
                n_streams: bq.streams,
                row_restriction: bq.row_restriction.clone(),
                disable_compression: bq.no_compression,
            };

            let session = BqReadSession::open(bq_config)
                .await
                .context("failed to open BigQuery read session")?;

            let schema = session.schema().context("failed to get Arrow schema")?;
            let estimated_rows = session.metadata().estimated_rows;
            let sources: Vec<_> = session
                .into_record_batch_sources()
                .context("failed to create record batch sources")?
                .into_iter()
                .map(BoxedRecordBatchSource::new)
                .collect();

            Ok(Pipeline {
                sources,
                schema,
                estimated_rows,
                key_column: wc.source.key_column,
                value_column: wc.source.value_column.unwrap_or_else(|| "value".into()),
                encoding,
                progress_secs: args.progress_secs,
            })
        }
    }
}

fn apply_protobuf_encoding(
    sources: Vec<BoxedRecordBatchSource>,
    schema: &arrow::datatypes::Schema,
    key_col_idx: usize,
    proto_config: &ProtobufEncoding,
) -> Result<(
    Vec<frostmap_encode::ProtobufEncodingSource<BoxedRecordBatchSource>>,
    DescriptorMeta,
)> {
    let value_fields: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|&(i, _)| i != key_col_idx)
        .map(|(_, f)| f.clone())
        .collect();
    let value_schema = arrow::datatypes::Schema::new(value_fields);
    let tc_output = frostmap_encode::build_transcoder(proto_config, &value_schema)
        .context("failed to build protobuf transcoder")?;

    info!(
        message_fields = value_schema.fields().len(),
        message_name = %tc_output.message_fqn,
        "protobuf transcoder ready"
    );

    let desc = DescriptorMeta {
        descriptor_bytes: tc_output.descriptor_bytes,
        message_fqn: tc_output.message_fqn,
    };

    let transcoder = Arc::new(tc_output.transcoder);
    let sources = sources
        .into_iter()
        .map(|s| frostmap_encode::ProtobufEncodingSource::new(s, key_col_idx, transcoder.clone()))
        .collect();
    Ok((sources, desc))
}

// ---------------------------------------------------------------------------
// Shared load orchestration
// ---------------------------------------------------------------------------

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

    info!(
        n_keys = stats.n_keys,
        data_bytes = stats.data_bytes,
        scatter_secs = stats.scatter_duration.as_secs_f64(),
        index_secs = stats.index_duration.as_secs_f64(),
        "load complete",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Download benchmark
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

/// Patch the snapshot's `meta.json` to include the protobuf descriptor and
/// message name so downstream tools can decode values without the original
/// build config.
async fn patch_meta_descriptor(
    output: &Path,
    descriptor_bytes: &[u8],
    message_fqn: &str,
) -> Result<()> {
    let meta_path = output.join("meta.json");
    let raw = tokio::fs::read_to_string(&meta_path)
        .await
        .context("failed to read meta.json for descriptor patching")?;
    let mut meta: serde_json::Value =
        serde_json::from_str(&raw).context("failed to parse meta.json")?;

    meta["encoding"] = serde_json::json!({
        "protobuf": {
            "descriptor": STANDARD.encode(descriptor_bytes),
            "message_name": message_fqn,
        }
    });

    let patched = serde_json::to_string_pretty(&meta).context("failed to serialize meta.json")?;
    tokio::fs::write(&meta_path, patched)
        .await
        .context("failed to write patched meta.json")?;

    info!(
        message_name = message_fqn,
        "descriptor embedded in meta.json"
    );
    Ok(())
}

async fn open_bq_session(
    project: Option<String>,
    table: &str,
    streams: i32,
    row_restriction: Option<&str>,
    no_compression: bool,
    selected_fields: Vec<String>,
) -> Result<BqReadSession> {
    let (billing_project, table_resource) = parse_table(table, project.as_deref())?;

    let config = BqSourceConfig {
        project: billing_project.clone(),
        table: table_resource,
        selected_fields,
        n_streams: streams,
        row_restriction: row_restriction.map(|s| s.to_owned()),
        disable_compression: no_compression,
    };

    info!(
        billing_project = %billing_project,
        table,
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

fn build_protobuf_config(args: &LoadArgs) -> Result<ProtobufEncoding> {
    let message_name = args
        .protobuf_message
        .as_ref()
        .context("--protobuf-message is required for protobuf encoding")?
        .clone();
    Ok(ProtobufEncoding {
        descriptor: args.protobuf_descriptor.clone(),
        descriptor_uri: None,
        package: args.protobuf_package.clone(),
        message_name,
    })
}

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
