use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

use kv_bq::{BqReadSession, BqSourceConfig};
use kv_loader::{LoaderConfig, SnapshotLoader};

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name    = "kv-load",
    about   = "Load key-value pairs into a read-only snapshot directory",
    version,
)]
struct Cli {
    /// Output snapshot directory (created if absent)
    #[arg(short, long)]
    output: PathBuf,

    /// Number of hash partitions — must be a power of two
    #[arg(long, default_value = "64")]
    partitions: u32,

    /// Rayon threads used for the parallel index build phase
    #[arg(long, default_value = "2")]
    index_parallelism: usize,

    /// Set log level to DEBUG (default: INFO). Overridden by RUST_LOG.
    #[arg(short, long)]
    verbose: bool,

    /// Validate and print the load plan without writing any data
    #[arg(long)]
    dry_run: bool,

    #[command(subcommand)]
    source: Source,
}

#[derive(Subcommand)]
enum Source {
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
    },
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let default_level = if cli.verbose { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // Bridge `log` crate calls (kv-format) into tracing.
    tracing_log::LogTracer::init().ok();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    if let Err(e) = run(cli).await {
        tracing::error!(error = format!("{e:#}"), "fatal");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.source {
        Source::Bq {
            project,
            table,
            key_column,
            value_column,
            streams,
            row_restriction,
            no_compression,
        } => {
            run_bq(
                cli.output,
                cli.partitions,
                cli.index_parallelism,
                cli.dry_run,
                project,
                table,
                key_column,
                value_column,
                streams,
                row_restriction,
                no_compression,
            )
            .await
        }
    }
}

// ---------------------------------------------------------------------------
// BigQuery load
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_bq(
    output:            PathBuf,
    partitions:        u32,
    index_parallelism: usize,
    dry_run:           bool,
    project:           Option<String>,
    table:             String,
    key_column:        String,
    value_column:      String,
    streams:           i32,
    row_restriction:   Option<String>,
    no_compression:    bool,
) -> Result<()> {
    let (billing_project, table_resource) = parse_table(&table, project.as_deref())?;

    let config = BqSourceConfig {
        project:             billing_project.clone(),
        table:               table_resource,
        key_column:          key_column.clone(),
        value_column:        value_column.clone(),
        n_streams:           streams,
        row_restriction:     row_restriction.clone(),
        disable_compression: no_compression,
    };

    info!(
        billing_project = %billing_project,
        table           = %table,
        key_column      = %key_column,
        value_column    = %value_column,
        compression     = if no_compression { "off" } else { "LZ4_FRAME" },
        row_restriction = row_restriction.as_deref().unwrap_or(""),
        "opening BigQuery read session",
    );

    let session = BqReadSession::open(config)
        .await
        .context("failed to open BigQuery read session")?;

    let meta = session.metadata();
    info!(
        n_streams       = session.n_streams(),
        estimated_rows  = meta.estimated_rows,
        estimated_bytes = meta.estimated_bytes,
        "BigQuery read session opened",
    );

    if dry_run {
        info!("dry-run: skipping load");
        return Ok(());
    }

    let loader_config = LoaderConfig {
        n_partitions:      partitions,
        index_parallelism,
        progress_fn:       Some(make_progress_fn()),
        ..LoaderConfig::default()
    };

    info!(output = %output.display(), "loading snapshot");

    let loader = SnapshotLoader::new(&output, loader_config)
        .context("failed to create SnapshotLoader")?;

    let sources = session.into_sources()
        .context("failed to split session into sources")?;

    let stats = loader
        .load_parallel(sources)
        .await
        .context("load failed")?;

    info!(
        n_keys           = stats.n_keys,
        data_bytes       = stats.data_bytes,
        scatter_secs     = stats.scatter_duration.as_secs_f64(),
        index_secs       = stats.index_duration.as_secs_f64(),
        "load complete",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Accept `project.dataset.table` (dotted) or
/// `projects/P/datasets/D/tables/T` (resource name) and return
/// `(billing_project, full_resource_name)`.
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

fn make_progress_fn() -> Arc<dyn Fn(u64, u64) + Send + Sync> {
    Arc::new(|n_keys, data_bytes| {
        debug!(n_keys, data_bytes, "scatter progress");
    })
}
