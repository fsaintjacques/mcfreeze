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

    /// Decode value as protobuf and print JSON.
    /// Requires the snapshot to have an embedded descriptor in meta.json.
    #[arg(long)]
    json: bool,
}

pub fn run(args: GetArgs) -> Result<()> {
    let reader = SnapshotReader::open(&args.snapshot).context("failed to open snapshot")?;

    match reader.get(args.key.as_bytes()).context("lookup failed")? {
        None => {
            eprintln!("not found");
            std::process::exit(1);
        }
        Some(bytes) => {
            if args.json {
                print_json(&args.snapshot, &bytes)?;
            } else if args.hex {
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

/// Read the descriptor from meta.json, decode the protobuf value, and print
/// pretty-printed JSON to stdout.
fn print_json(snapshot: &std::path::Path, value_bytes: &[u8]) -> Result<()> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use prost_reflect::{DynamicMessage, SerializeOptions};

    let meta_raw =
        std::fs::read_to_string(snapshot.join("meta.json")).context("failed to read meta.json")?;
    let meta: serde_json::Value =
        serde_json::from_str(&meta_raw).context("failed to parse meta.json")?;

    let encoding = meta
        .get("encoding")
        .and_then(|e| e.get("protobuf"))
        .context(
            "no encoding.protobuf section in meta.json — snapshot may not use protobuf encoding",
        )?;

    let desc_b64 = encoding
        .get("descriptor")
        .and_then(|v| v.as_str())
        .context("missing encoding.protobuf.descriptor in meta.json")?;
    let message_name = encoding
        .get("message_name")
        .and_then(|v| v.as_str())
        .context("missing encoding.protobuf.message_name in meta.json")?;

    let desc_bytes = STANDARD
        .decode(desc_b64)
        .context("failed to decode base64 descriptor")?;

    let pool = {
        let mut pool = prost_reflect::DescriptorPool::global();
        pool.decode_file_descriptor_set(&desc_bytes[..])
            .context("failed to parse FileDescriptorSet")?;
        pool
    };

    let msg_desc = pool
        .get_message_by_name(message_name)
        .with_context(|| format!("message {message_name:?} not found in descriptor"))?;

    let msg =
        DynamicMessage::decode(msg_desc, value_bytes).context("failed to decode protobuf value")?;

    let opts = SerializeOptions::new().stringify_64_bit_integers(false);
    let mut ser = serde_json::Serializer::pretty(Vec::new());
    msg.serialize_with_options(&mut ser, &opts)
        .context("failed to serialize to JSON")?;
    let json = String::from_utf8(ser.into_inner()).context("invalid UTF-8 in JSON output")?;
    println!("{json}");

    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
