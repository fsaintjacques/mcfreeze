use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

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

    /// Report progress to stderr every 100k keys
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
    if let Err(e) = run(Cli::parse()).await {
        eprintln!("error: {e:#}");
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
                cli.verbose,
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
    verbose:           bool,
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

    eprintln!("Opening BigQuery read session…");
    eprintln!("  billing project : {billing_project}");
    eprintln!("  table           : {table}");
    eprintln!("  key / value     : {key_column} / {value_column}");
    if let Some(ref r) = row_restriction {
        eprintln!("  row restriction : {r}");
    }
    eprintln!("  compression     : {}", if no_compression { "off" } else { "LZ4_FRAME" });

    let session = BqReadSession::open(config)
        .await
        .context("failed to open BigQuery read session")?;

    let meta = session.metadata();
    eprintln!("  streams         : {}", session.n_streams());
    if let Some(rows) = meta.estimated_rows {
        eprintln!("  estimated rows  : {rows}");
    }
    if let Some(bytes) = meta.estimated_bytes {
        eprintln!("  estimated bytes : {}", human_bytes(bytes));
    }

    if dry_run {
        eprintln!("dry-run: skipping load.");
        return Ok(());
    }

    let loader_config = LoaderConfig {
        n_partitions:      partitions,
        index_parallelism,
        progress_fn:       verbose.then(|| make_progress_fn()),
        ..LoaderConfig::default()
    };

    eprintln!("\nLoading into {}…", output.display());
    let loader = SnapshotLoader::new(&output, loader_config)
        .context("failed to create SnapshotLoader")?;

    let sources = session.into_sources()
        .context("failed to split session into sources")?;

    let t0    = Instant::now();
    let stats = loader
        .load_parallel(sources)
        .await
        .context("load failed")?;

    let elapsed = t0.elapsed();
    eprintln!("\nDone.");
    eprintln!("  keys written    : {}", stats.n_keys);
    eprintln!("  data bytes      : {}", human_bytes(stats.data_bytes));
    eprintln!("  scatter         : {:.1}s", stats.scatter_duration.as_secs_f64());
    eprintln!("  index build     : {:.1}s", stats.index_duration.as_secs_f64());
    eprintln!("  total           : {:.1}s", elapsed.as_secs_f64());
    if elapsed.as_secs_f64() > 0.0 {
        let throughput = stats.n_keys as f64 / elapsed.as_secs_f64();
        eprintln!("  throughput      : {:.0} keys/s", throughput);
    }

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
        // Already a resource name.
        let project = table
            .split('/')
            .nth(1)
            .context("malformed resource name, expected projects/P/datasets/D/tables/T")?
            .to_string();
        (project, table.to_string())
    } else {
        // Dotted notation: project.dataset.table
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
    Arc::new(|keys, bytes| {
        eprintln!("  progress: {} keys  {}  written", keys, human_bytes(bytes));
    })
}

fn human_bytes(b: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 { format!("{b} B") } else { format!("{v:.1} {}", UNITS[i]) }
}
