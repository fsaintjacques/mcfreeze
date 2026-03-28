use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod get;
mod load;

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name    = "fm",
    about   = "Frostmap CLI",
    version,
)]
struct Cli {
    /// Set log level to DEBUG (default: INFO). Overridden by RUST_LOG.
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Load key-value pairs into a read-only snapshot directory
    Load(load::LoadArgs),
    /// Look up a key in a snapshot directory
    Get(get::GetArgs),
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

    // Bridge `log` crate calls (frostmap-format) into tracing.
    tracing_log::LogTracer::init().ok();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let result: Result<()> = match cli.command {
        Command::Load(args) => load::run(args).await,
        Command::Get(args)  => get::run(args).map_err(Into::into),
    };

    if let Err(e) = result {
        tracing::error!(error = format!("{e:#}"), "fatal");
        std::process::exit(1);
    }
}
