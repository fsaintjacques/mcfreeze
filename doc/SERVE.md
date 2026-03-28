# KV Server Architecture

## Overview

`fm serve` is a stateless, read-only key-value server. It serves the Memcache meta
protocol over a Unix domain socket (UDS) and TCP. It is designed to run as an
unprivileged container in a DaemonSet pod alongside the Lifecycle Manager.

The server never writes or builds snapshot data. It only:
1. Opens `SnapshotReader` handles against mounted snapshot directories
2. Serves key lookups by probing the Robin Hood index and `pread`-ing from `data.bin`
3. In `catalog` mode: watches `catalog.json` for version changes via inotify and
   atomically hot-swaps the active snapshot set

---

## Crate Layout

The server logic lives in a dedicated library crate `frostmap-server`. The CLI crate
(`frostmap-cli`) contains only a thin `serve.rs` — argument parsing and a call to
`frostmap_server::run(config)`.

Keeping the library separate makes every internal module independently testable without
spinning up a full binary.

```
rust/crates/
  frostmap-format/          (existing — read path)
  frostmap-loader/          (existing — write path, NOT used by server)
  frostmap-bq/              (existing — BQ source, NOT used by server)
  frostmap-server/          (NEW — lib crate)
    src/
      lib.rs
      error.rs
      lookup.rs             (Lookup trait + SnapshotLookup + CatalogLookup)
      registry.rs           (DatasetHandle, ActiveCatalog, Registry — catalog mode only)
      catalog.rs            (catalog.json schema + inotify watcher — catalog mode only)
      protocol/
        mod.rs
        meta.rs             (Memcache meta protocol — parser + framer)
        commands.rs         (mg dispatch through Lookup)
      listener.rs           (UDS + TCP accept loops)
      metrics.rs            (Prometheus metric definitions + /metrics handler)
      modes/
        mod.rs
        snapshot_mode.rs    (serve a single static snapshot, no inotify)
        catalog_mode.rs     (production: inotify-driven hot-swap, multi-dataset)
    Cargo.toml
  frostmap-cli/             (existing)
    src/
      serve.rs              (NEW — ServeArgs + call frostmap_server::run())
      main.rs               (add Command::Serve)
```

---

## Modes

The two modes differ only in how the `Lookup` implementation is constructed and
whether the catalog-watcher task is spawned. The protocol, listener, and metrics
layers are identical in both modes — they work against `Arc<dyn Lookup>` and have
no awareness of which mode is active.

`snapshot` mode is implemented first. It exercises the full serving stack
(listener, protocol, metrics, connection handling) against real snapshot data
without requiring a Lifecycle Manager or catalog file.

### `snapshot` (first to implement)

```
fm serve snapshot --dir /mnt/snapshots/v42 --uds /run/kv/kv.sock --tcp 0.0.0.0:7777
```

- Opens a single `SnapshotReader` at startup against `--dir`; never changes
- No inotify, no catalog file, no dataset routing, no hot-swap
- Keys are passed to the snapshot as-is — no prefix stripping
- Useful for development, integration tests, and manual benchmarking against
  a real snapshot without any infrastructure

### `catalog` (production)

```
fm serve catalog --catalog /run/kv/catalog.json --uds /run/kv/kv.sock --tcp 0.0.0.0:7777
```

- Watches `catalog.json` via inotify (`IN_MOVED_TO` on the parent directory,
  to catch the atomic `rename(2)` the Lifecycle Manager uses)
- On change: opens new `SnapshotReader`s, constructs a new `ActiveCatalog`,
  atomically swaps the registry, writes an acknowledgement file
- Keys are routed by `<dataset>:<actual-key>` prefix
- Full production path; requires the shared EmptyDir to be mounted

---

## The `Lookup` Abstraction

The `Lookup` trait is the single interface between the protocol/connection layer
and the storage layer. It hides all mode-specific concerns (routing, hot-swap,
single vs. multi-dataset) from the serving path.

```rust
// lookup.rs
pub trait Lookup: Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError>;
}
```

The connection handler receives an `Arc<dyn Lookup>` and calls `get` for each
key in a request. It has no knowledge of modes, datasets, or routing.

### `SnapshotLookup` (snapshot mode)

The single implementation of the Robin Hood probe + `pread` path. Both modes
use it — `CatalogLookup` delegates to it rather than touching `SnapshotReader`
directly.

```rust
pub struct SnapshotLookup {
    reader: SnapshotReader,
}

impl Lookup for SnapshotLookup {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError> {
        self.reader.get(key).map(|v| v.map(Bytes::from)).map_err(Into::into)
    }
}
```

No routing. The key is used verbatim.

### `CatalogLookup` (catalog mode)

`split_prefix` returns two sub-slices of the original `key` byte slice — no
allocation, analogous to C++ `string_view` splitting on `':'`.

```rust
/// Splits `b"<dataset>:<actual-key>"` on the first `b':'`.
/// Both returned slices borrow from `key`; no heap allocation.
fn split_prefix(key: &[u8]) -> Result<(&[u8], &[u8]), ServeError> {
    let pos = key.iter().position(|&b| b == b':')
        .ok_or(ServeError::MissingDatasetPrefix)?;
    Ok((&key[..pos], &key[pos + 1..]))
}

pub struct CatalogLookup {
    registry: Arc<Registry>,
}

impl Lookup for CatalogLookup {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError> {
        let (dataset, actual_key) = split_prefix(key)?;  // zero-copy slices
        let catalog = self.registry.load();
        catalog.get(dataset, actual_key)   // delegates into SnapshotLookup
    }
}
```

Routing and key splitting are entirely contained here; the protocol layer is
unaware of them.

---

## Key Data Structures

### `CatalogEntry` / `CatalogFile` (catalog mode)

Deserialised from `catalog.json` (written by the Lifecycle Manager):

```rust
pub struct CatalogEntry {
    pub dataset:      String,   // e.g. "my-ds"
    pub version:      String,   // opaque version ID, e.g. "v42"
    pub snapshot_dir: PathBuf,  // e.g. /mnt/kv/my-ds/v42
}

pub struct CatalogFile {
    pub entries: Vec<CatalogEntry>,
}
```

### `DatasetHandle` (catalog mode)

An opened, named snapshot. Wraps `SnapshotLookup` — not the raw
`SnapshotReader` — so that all `pread` paths go through the same implementation:

```rust
pub struct DatasetHandle {
    pub name:    String,
    pub version: String,
    lookup:      SnapshotLookup,   // single pread path; shared with snapshot mode
}
```

### `ActiveCatalog` (catalog mode)

Immutable after construction. Holds all currently-active datasets and is stored
inside the RCU cell.

```rust
pub struct ActiveCatalog {
    datasets:       HashMap<String, DatasetHandle>,
    pub generation: u64,   // incremented on each swap; exposed in metrics + version command
}

impl ActiveCatalog {
    pub fn get(&self, dataset: &str, key: &[u8])
        -> Result<Option<Bytes>, ServeError>
    {
        match self.datasets.get(dataset) {
            None    => Ok(None),               // dataset unknown → miss
            Some(h) => h.lookup.get(key),      // delegates to SnapshotLookup
        }
    }
}
```

### `Registry` (catalog mode)

The RCU cell. `CatalogLookup` holds an `Arc<Registry>`:

```rust
pub struct Registry {
    inner: ArcSwap<ActiveCatalog>,
}

impl Registry {
    pub fn new(initial: ActiveCatalog) -> Arc<Self> { ... }

    // Hot path — seqlock-pinned guard, no atomic increment.
    pub fn load(&self) -> arc_swap::Guard<Arc<ActiveCatalog>> {
        self.inner.load()
    }

    // Called by the catalog-watcher task only.
    pub fn swap(&self, new: Arc<ActiveCatalog>) -> Arc<ActiveCatalog> {
        self.inner.swap(new)
    }
}
```

---

## Concurrency Model

```
tokio multi-thread runtime
│
├── catalog-watcher task (catalog mode only)
│     inotify loop → parse catalog.json → open SnapshotReaders
│     → Registry::swap(new) → write ack file
│     → old Arc<ActiveCatalog> dropped (mmaps + fds released)
│
├── UDS accept-loop task
│     tokio::net::UnixListener::accept()
│     → clone Arc<dyn Lookup>, spawn connection-handler task
│
├── TCP accept-loop task
│     tokio::net::TcpListener::accept()
│     → clone Arc<dyn Lookup>, spawn connection-handler task
│
├── connection-handler tasks (×N, one per open connection)
│     hold Arc<dyn Lookup> for the connection lifetime
│     per request:
│       block_in_place(|| lookup.get(key))
│         // Robin Hood probe (mmap, instant) + pread (blocking syscall)
│         // block_in_place tells tokio this worker will block briefly;
│         // multi-thread runtime shifts other tasks to remaining workers.
│
└── metrics task (single)
      axum handler on :9090/metrics
```

**Invariants:**

- No `Mutex` or `RwLock` on the hot path.
- `SnapshotReader::get` is `&self` — the mmap is read-only, `pread` is
  thread-safe, the Robin Hood probe reads immutable memory.
- `pread` is a blocking syscall; connection handlers must call it inside
  `tokio::task::block_in_place`. The `Lookup` trait stays synchronous —
  `block_in_place` is the adapter at the call site. `spawn_blocking` is not
  used because ownership does not need to move and the runtime is always
  multi-threaded.
- **Epoch / munmap — owned by `catalog_mode`:**
  After `Registry::swap(new)` the returned old `Arc<ActiveCatalog>` is held
  by the catalog-watcher task. `arc-swap` Guards are per-request and
  extremely short-lived (one `pread` duration); they do not hold the Arc's
  strong count, only an internal epoch pin that is released on Guard drop.
  The catalog-watcher writes the ack file immediately after `swap()` returns.
  The Lifecycle Manager acts on the ack and begins unmounting, but the actual
  `umount(2)` call takes at least one RPC round-trip — far longer than any
  in-flight Guard. When the catalog-watcher task drops the old Arc, all mmaps
  and file descriptors are released. No explicit drain loop is required.
  _Invariant_: ack written ⟹ `swap()` complete ⟹ no new request can pin the
  old catalog. Old Guards drain passively before unmount completes.

---

## Memcache Meta Protocol

The server implements the [Memcache meta protocol][meta-proto] (introduced in
memcached 1.6), not the classic text or binary protocols.

Advantages over the classic text protocol:
- Commands and responses are uniform and machine-parseable without special-casing
- `mg` (meta get) supports optional flag tokens for richer semantics (TTL,
  CAS, hit-before flag) without changing the response shape
- More compact miss response (`EN` vs `END`)
- Retains full debuggability with `nc` / `telnet`

[meta-proto]: https://github.com/memcached/memcached/blob/master/doc/protocol.txt

### Commands

| Command | Wire | Notes |
|---|---|---|
| `mg` | `mg <key> [flags]\r\n` | Meta get — one key per command; batch via pipelining |
| `version` | `version\r\n` | Returns `VERSION <semver> gen/<generation>\r\n` |
| `quit` | `quit\r\n` | Clean connection teardown |

The server is read-only. Write commands (`ms`, `md`, `ma`) return
`SERVER_ERROR read-only\r\n`.

### `mg` response format

```
# Hit:
VA <size> [echo-flags]\r\n
<value bytes>\r\n

# Miss:
EN\r\n

# Error:
SERVER_ERROR <message>\r\n
```

### Supported `mg` flags

| Flag | Meaning | Server behaviour |
|---|---|---|
| `v` | Return value | Always honoured (value is always returned on hit) |
| `t` | Return TTL remaining | Returns `t-1` (no expiry in frostmap) |
| `h` | Return hit-before flag | Not tracked; omitted from response |
| `k` | Return key in response | Echoed as `k<key>` token in the `VA` line |

Unrecognised flags are silently ignored, preserving forward compatibility.

### Batching

The meta protocol batches via pipelining — clients send multiple `mg` commands
back-to-back without waiting for individual responses. The connection handler
reads and dispatches commands in a loop, flushing the write buffer at the end of
each pipeline batch (when the read buffer is drained). This achieves multi-get
semantics without a dedicated multi-key command.

### Key routing (catalog mode only)

In `catalog` mode keys carry a dataset prefix: `<dataset>:<actual-key>`.
`CatalogLookup::get` splits on the first `:` before dispatching.

In `snapshot` mode keys are passed to the reader verbatim. No prefix convention
is enforced, making it easy to test with raw keys from the original dataset.

---

## Observability

All metrics are registered in a single `prometheus_client::Registry` shared across
tasks. Served at `:9090/metrics`.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fm_request_duration_seconds` | Histogram | `result` | End-to-end request latency; `result` ∈ {hit, miss, error} |
| `fm_keys_requested_total` | Counter | — | Total individual key lookups |
| `fm_keys_hit_total` | Counter | — | Individual key hits |
| `fm_keys_miss_total` | Counter | — | Individual key misses |
| `fm_response_bytes_total` | Counter | — | Total value bytes sent to clients |
| `fm_catalog_generation` | Gauge | — | Current generation; 0 in snapshot mode |
| `fm_catalog_swap_total` | Counter | `result` | Catalog swaps; `result` ∈ {ok, error}; 0 in snapshot mode |
| `fm_catalog_swap_duration_seconds` | Histogram | — | inotify event → swap() complete |
| `fm_active_datasets` | Gauge | — | Dataset count; always 1 in snapshot mode |
| `fm_connections_active` | Gauge | `transport` | Open connections; `transport` ∈ {uds, tcp} |
| `fm_connections_total` | Counter | `transport` | Total connections accepted |

Histogram buckets for `fm_request_duration_seconds`:
`[50µs, 100µs, 200µs, 500µs, 1ms, 2ms, 5ms, 10ms, 50ms, 100ms]`

---

## New Dependencies (`frostmap-server`)

| Crate | Purpose |
|---|---|
| `arc-swap = "1"` | Lock-free RCU cell for `ActiveCatalog` (catalog mode) |
| `inotify = "0.10"` | Linux inotify bindings via `spawn_blocking` (catalog mode) |
| `prometheus-client = "0.22"` | Typed Prometheus metrics with label API |
| `axum = "0.7"` | Minimal HTTP server for `/metrics` endpoint |
| `bytes = "1"` | `Bytes` for zero-copy value returns (already in workspace) |
| `tokio` | `net`, `io-util`, `fs`, `sync`, `time` features |
| `serde` + `serde_json` | `catalog.json` parsing (already in workspace) |
| `thiserror = "2"` | `ServeError` (already in workspace) |
| `tracing = "0.1"` | Structured logging (already in workspace) |

inotify is Linux-only, matching the DaemonSet deployment target. The watch is placed
on the **parent directory** of `catalog.json` for `IN_MOVED_TO` events, catching the
atomic `rename(2)` the Lifecycle Manager uses to update the file.

---

## Implementation Sequence

Each step produces independently testable code before the next begins.
`snapshot` mode is completed end-to-end before catalog work begins.

1. `frostmap-server` crate scaffold — `lib.rs`, `error.rs`, `Cargo.toml`
2. `lookup.rs` — `Lookup` trait + `SnapshotLookup`
3. `protocol/meta.rs` — Memcache meta protocol framing, testable with a trivial `Lookup` stub
4. `protocol/commands.rs` — `mg` dispatch through `Lookup`
5. `listener.rs` — UDS + TCP accept loops, connection-task spawning
6. `modes/snapshot_mode.rs` + wire into `frostmap-cli` — **snapshot mode complete**
7. `metrics.rs` — Prometheus definitions + axum `/metrics` handler
8. `registry.rs` — `DatasetHandle`, `ActiveCatalog`, `Registry`
9. `lookup.rs` — add `CatalogLookup`
10. `catalog.rs` + `modes/catalog_mode.rs` — inotify watcher, hot-swap, ack file
11. `serve.rs` in `frostmap-cli` — `ServeArgs` + `Command::Serve` wired into `main.rs`

---

## Async I/O Strategy for `pread`

### Storage targets

The server runs against two distinct storage backends with very different latency profiles:

| Backend | pread latency | Bottleneck |
|---|---|---|
| Local NVMe (warm page cache) | 1–20 µs | CPU / memory bandwidth |
| Hyperdisk ML (network block device) | 500–2000 µs | Network round-trip to storage node |

The gap is two orders of magnitude. The I/O strategy must be correct for both.

### Why `block_in_place` is insufficient for Hyperdisk ML

`block_in_place(|| lookup.get(key))` stalls the calling tokio worker thread for the full
pread duration. At 1 ms × 100 concurrent requests the multi-thread scheduler runs out of
workers; tasks queue behind I/O waits. `spawn_blocking` (tokio's separate blocking pool,
default 512 threads) avoids stalling workers but still allocates one OS thread per
in-flight read — 100 concurrent Hyperdisk reads = 100 blocked OS threads.

### Decision: `Lookup::get` is `async` from day one

`frostmap-format` stays a zero-runtime sync library. Async lives in `frostmap-server`,
which already depends on tokio. The `Lookup` trait becomes:

```rust
// lookup.rs
#[async_trait::async_trait]
pub trait Lookup: Send + Sync + 'static {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError>;
}
```

`async-trait` is used for `dyn`-safety — AFIT (`async fn` in traits, stable since 1.75)
is not yet object-safe without a shim, and `Arc<dyn Lookup>` is required by the
connection handler. The boxing cost (~40 ns per call) is negligible against pread latency.

`SnapshotLookup` implements it via `spawn_blocking` for phase 1:

```rust
#[async_trait::async_trait]
impl Lookup for SnapshotLookup {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError> {
        // Key must be owned to cross the 'static boundary of spawn_blocking.
        let key   = Bytes::copy_from_slice(key);
        let inner = self.inner.clone();   // Arc<SnapshotReaderInner>
        tokio::task::spawn_blocking(move || {
            inner.get(&key)
                 .map(|v| v.map(Bytes::from))
                 .map_err(Into::into)
        })
        .await
        .map_err(|_| ServeError::BlockingTaskPanicked)?
    }
}
```

`SnapshotReader` must be wrapped in an `Arc` internally so it is `Clone + Send + 'static`.
The connection handler simply `.await`s — no `block_in_place`.

### Upgrade path: io_uring for Hyperdisk ML

`spawn_blocking` is the correct initial implementation. When Hyperdisk ML becomes the
primary target, `SnapshotLookup::get` is replaced with a true async pread via io_uring —
nothing above it changes.

```
Connection handler  ──await──▶  SnapshotLookup::get
                                    │
                              (phase 1) spawn_blocking ──▶ pread(2)
                                    │
                              (phase 2) io_uring channel ──▶ IORING_OP_READ
```

The io_uring implementation runs a dedicated pool of threads each driving a
`tokio_uring::start` loop. Connection handlers send `(fd, offset, len, oneshot_tx)`
over a channel; the uring thread submits the read and sends the completed `Bytes` back.
`tokio-uring` is Linux-only and uses per-thread rings that do not compose with tokio's
multi-thread scheduler directly — hence the channel-based isolation.

### On `preadv` and batching

`preadv(2)` scatters one read at one offset into multiple buffers — not useful for
serving multiple keys at different offsets. The equivalent for pipeline batching is
**io_uring batch submission**: N `IORING_OP_READ` entries submitted in a single
`io_uring_enter(2)` call. This amortises the syscall cost across a full pipeline
batch and is the primary Hyperdisk ML throughput lever. It requires io_uring (phase 2).

### `frostmap-format` is unaffected

`SnapshotReader::get` remains `&self -> Result<Option<Vec<u8>>>` — synchronous,
no runtime dependency, independently usable and testable. Async is an adapter concern
that belongs in `frostmap-server`.
