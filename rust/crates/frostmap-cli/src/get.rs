use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use frostmap_format::reader::SnapshotReader;

#[derive(Args)]
pub struct GetArgs {
    /// Snapshot directory produced by `fm load`
    #[arg(short, long)]
    snapshot: PathBuf,

    /// Key to look up (UTF-8 string)
    key: String,

    /// Print raw bytes as hex instead of interpreting as UTF-8
    #[arg(long)]
    hex: bool,
}

pub fn run(args: GetArgs) -> Result<()> {
    let reader = SnapshotReader::open(&args.snapshot).context("failed to open snapshot")?;

    match reader.get(args.key.as_bytes()).context("lookup failed")? {
        None => {
            eprintln!("not found");
            std::process::exit(1);
        }
        Some(bytes) => {
            if args.hex {
                println!("{}", hex(&bytes));
            } else {
                match std::str::from_utf8(&bytes) {
                    Ok(s) => println!("{s}"),
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
