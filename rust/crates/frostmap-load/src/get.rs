use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use frostmap_format::reader::SnapshotReader;

#[derive(Parser)]
#[command(
    name  = "kv-get",
    about = "Look up a key in a snapshot directory",
    version,
)]
struct Cli {
    /// Snapshot directory produced by kv-load
    #[arg(short, long)]
    snapshot: PathBuf,

    /// Key to look up (UTF-8 string)
    key: String,

    /// Print raw bytes as hex instead of interpreting as UTF-8
    #[arg(long)]
    hex: bool,
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    let reader = SnapshotReader::open(&cli.snapshot)
        .context("failed to open snapshot")?;

    match reader.get(cli.key.as_bytes()).context("lookup failed")? {
        None => {
            eprintln!("not found");
            std::process::exit(1);
        }
        Some(bytes) => {
            if cli.hex {
                println!("{}", hex(&bytes));
            } else {
                match std::str::from_utf8(&bytes) {
                    Ok(s)  => println!("{s}"),
                    Err(_) => println!("{}", hex(&bytes)),
                }
            }
        }
    }

    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
