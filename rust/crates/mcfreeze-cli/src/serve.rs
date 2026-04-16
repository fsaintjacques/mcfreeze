use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use mcfreeze_server::modes::catalog_mode::{run as run_catalog, CatalogConfig};
use mcfreeze_server::modes::snapshot_mode::{run as run_snapshot, SnapshotConfig};

/// Default idle-connection timeout. Bounds how long a silent client may pin
/// a stale catalog generation's `index.all` mmap in memory. Configurable
/// via `--idle-timeout-secs`; `0` disables the timeout.
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300;

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
pub struct ServeArgs {
    #[command(subcommand)]
    pub mode: ServeMode,
}

#[derive(Subcommand)]
pub enum ServeMode {
    /// Serve a single static snapshot directory (no catalog, no hot-swap)
    Snapshot(SnapshotArgs),
    /// Serve datasets described by a catalog.json, with live hot-swap
    Catalog(CatalogArgs),
}

#[derive(Parser)]
pub struct SnapshotArgs {
    /// Snapshot directory containing index.bin and data/
    #[arg(long)]
    pub dir: PathBuf,

    /// Unix-domain socket path to bind
    #[arg(long)]
    pub uds: Option<PathBuf>,

    /// TCP address to bind (e.g. 0.0.0.0:7777)
    #[arg(long)]
    pub tcp: Option<SocketAddr>,

    /// Address to expose Prometheus /metrics on (e.g. 0.0.0.0:9090)
    #[arg(long)]
    pub metrics: Option<SocketAddr>,

    /// Close connections idle (no client bytes) for longer than this many
    /// seconds. 0 disables the timeout.
    #[arg(long, default_value_t = DEFAULT_IDLE_TIMEOUT_SECS)]
    pub idle_timeout_secs: u64,
}

#[derive(Parser)]
pub struct CatalogArgs {
    /// Path to catalog.json watched for atomic renames
    #[arg(long)]
    pub catalog: PathBuf,

    /// Unix-domain socket path to bind
    #[arg(long)]
    pub uds: Option<PathBuf>,

    /// TCP address to bind (e.g. 0.0.0.0:7777)
    #[arg(long)]
    pub tcp: Option<SocketAddr>,

    /// Address to expose Prometheus /metrics on (e.g. 0.0.0.0:9090)
    #[arg(long)]
    pub metrics: Option<SocketAddr>,

    /// Close connections idle (no client bytes) for longer than this many
    /// seconds. Bounds how long a silent client may pin a stale catalog
    /// generation's `index.all` mmap in memory. 0 disables the timeout.
    #[arg(long, default_value_t = DEFAULT_IDLE_TIMEOUT_SECS)]
    pub idle_timeout_secs: u64,
}

/// Translate a seconds-based CLI knob into an `Option<Duration>` where 0
/// means "no timeout".
fn idle_timeout(secs: u64) -> Option<Duration> {
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run(args: ServeArgs) -> Result<()> {
    match args.mode {
        ServeMode::Snapshot(a) => {
            if a.uds.is_none() && a.tcp.is_none() {
                anyhow::bail!("at least one of --uds or --tcp must be specified");
            }
            let cfg = SnapshotConfig {
                dir: a.dir,
                uds_path: a.uds,
                tcp_addr: a.tcp,
                semver: env!("CARGO_PKG_VERSION").to_owned(),
                metrics_addr: a.metrics,
                idle_timeout: idle_timeout(a.idle_timeout_secs),
            };
            run_snapshot(cfg).await?;
            Ok(())
        }
        ServeMode::Catalog(a) => {
            if a.uds.is_none() && a.tcp.is_none() {
                anyhow::bail!("at least one of --uds or --tcp must be specified");
            }
            let cfg = CatalogConfig {
                catalog_path: a.catalog,
                uds_path: a.uds,
                tcp_addr: a.tcp,
                semver: env!("CARGO_PKG_VERSION").to_owned(),
                metrics_addr: a.metrics,
                idle_timeout: idle_timeout(a.idle_timeout_secs),
            };
            run_catalog(cfg).await?;
            Ok(())
        }
    }
}
