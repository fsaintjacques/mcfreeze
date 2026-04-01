# frostmap

Immutable key-value snapshots built from BigQuery, served at sub-millisecond latency over the memcache meta protocol.

## Quick start

```bash
# Build
make all

# Load a snapshot from CSV
echo "aGVsbG8=,d29ybGQ=" | fm load csv --output /tmp/snap --partitions 4

# Look up a key
fm get --snapshot /tmp/snap hello

# Serve it
fm serve snapshot --dir /tmp/snap --tcp 0.0.0.0:7777
```

## Usage

### `fm load` — build a snapshot

From BigQuery:

```bash
fm load bq \
  --table myproject.dataset.table \
  --key-column key --value-column value \
  --output /data/snap \
  --partitions 64 \
  --streams 8
```

From CSV (two columns: base64-encoded key, base64-encoded value):

```bash
fm load csv --file data.csv --output /data/snap
cat data.csv | fm load csv --output /data/snap
```

Common flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--output` | required | Snapshot directory |
| `--partitions` | 64 | Hash partitions (power of 2) |
| `--index-parallelism` | 2 | Parallel index build threads |
| `-v` | off | Debug logging (overridden by `RUST_LOG`) |

### `fm get` — point lookup

```bash
fm get --snapshot /data/snap my-key
fm get --snapshot /data/snap my-key --hex   # binary values
```

### `fm serve` — start the server

Single snapshot (development):

```bash
fm serve snapshot --dir /data/snap --tcp 0.0.0.0:7777
```

Catalog mode (production) — serves multiple datasets with atomic hot-swap:

```bash
fm serve catalog --catalog /run/kv/catalog.json \
  --uds /run/kv/kv.sock \
  --tcp 0.0.0.0:7777 \
  --metrics 0.0.0.0:9090
```

Catalog JSON format:

```json
{
  "entries": [
    { "dataset": "users", "version": "v42", "snapshot_dir": "/mnt/kv/users/v42" }
  ]
}
```

In catalog mode, keys are prefixed with the dataset name: `users:alice`.

### Protocol

The server speaks memcache meta commands (memcached 1.6+):

```
mg users:alice v\r\n        # meta-get with value flag
VA 5\r\n                    # hit: 5-byte value
hello\r\n

mg users:missing v\r\n
EN\r\n                      # miss
```

Write commands (`ms`, `md`, `ma`) are rejected — snapshots are immutable.

## Library API

### Writing a snapshot

```rust
use frostmap_format::writer::SnapshotWriter;

let mut w = SnapshotWriter::new("/data/snap", 64)?;
w.write(b"hello", b"world")?;
w.write(b"foo", b"bar")?;
w.finish("/data/snap")?;
```

### Reading a snapshot

```rust
use frostmap_format::reader::SnapshotReader;

let r = SnapshotReader::open("/data/snap")?;
if let Some(value) = r.get(b"hello")? {
    println!("{}", String::from_utf8_lossy(&value));
}
```

### Parallel loading from a custom source

```rust
use frostmap_loader::{SnapshotLoader, LoaderConfig, KvSource, KvBatch};

let config = LoaderConfig { n_partitions: 64, ..Default::default() };
let loader = SnapshotLoader::new("/data/snap", config)?;
let stats = loader.load(&mut my_source).await?;
println!("loaded {} keys in {:?}", stats.n_keys, stats.scatter_duration + stats.index_duration);
```

Implement `KvSource` + `KvBatch` to plug in any data source. Built-in sources: `CsvSource`, `BqReadSession`.

## On-disk format

```
<snapshot>/
  meta.json           Partition count, key stats, index offsets
  index.all           Robin Hood hash tables (2 MiB-aligned per partition)
  data/
    part-00/data.bin  Values with 12-byte headers, 64-byte aligned
    part-01/data.bin
    ...
```

- **Hash**: xxhash64, partition routing via `hash & (n_partitions - 1)`
- **Index**: 8-byte compact buckets (32-bit fingerprint + 32-bit offset), Robin Hood probing, SIMD group compare (AVX2/NEON)
- **Values**: 12-byte header (8-byte verify fingerprint + 4-byte length), padded to 64-byte alignment
- **Capacity**: 256 GB per partition, up to 64 TB total (256 partitions)

Read path: partition lookup + SIMD fingerprint compare in mmap'd RAM, then a single `pread` for the value. A fingerprint miss avoids disk entirely.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  fm load    │────▶│  Snapshot    │◀────│  fm serve    │
│  (builder)  │     │  (on disk)   │     │  (server)    │
└─────────────┘     └──────────────┘     └──────────────┘
```

**Crates:**

| Crate | Purpose |
|-------|---------|
| `frostmap-format` | On-disk format: Robin Hood index, aligned values, reader/writer |
| `frostmap-loader` | Parallel scatter/gather build pipeline |
| `frostmap-bq` | BigQuery Storage Read API adapter |
| `frostmap-server` | Memcache meta protocol server (snapshot + catalog modes) |
| `frostmap-cli` | CLI (`fm load`, `fm get`, `fm serve`) |

**Orchestration** (Go, `fmtctl`): node-agent DaemonSet attaches Hyperdisk ML volumes, writes `catalog.json`, the Rust server hot-swaps via inotify.

## Contributing

```bash
# Build everything
make all

# Run all tests (unit + integration + Go)
make test

# Unit tests only (no binary build)
make test-unit

# Just the format crate
cd rust && cargo test -p frostmap-format
```

The project is a Rust workspace (`rust/`) + Go modules (`go/`). Rust builds produce the `fm` binary; Go builds produce `fmtctl`.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
