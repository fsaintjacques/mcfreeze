use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use frostmap_server::modes::snapshot_mode::{SnapshotConfig, run as run_snapshot};

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
                dir:      a.dir,
                uds_path: a.uds,
                tcp_addr: a.tcp,
                semver:   env!("CARGO_PKG_VERSION").to_owned(),
            };
            run_snapshot(cfg).await?;
            Ok(())
        }
    }
}
