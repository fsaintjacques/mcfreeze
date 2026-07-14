<!-- SPDX-License-Identifier: Apache-2.0 -->

# Format interface: multi-format read/write seam

## Motivation

Today the format is a hard-coded assumption: `SnapshotReader` *is* V4, the
loader *writes* V4, and the server is compiled against both facts. Two
forces break this:

1. **V5 (sparse index)** must roll out against a fleet still serving V4
   snapshots. See `FORMAT_V5_SPARSE_INDEX.md`.
2. **Formats are dataset-dependent, permanently.** The V5 analysis showed
   the right layout varies with the data: tiny values favor sparse
   fences, fat values shift block size, compressible values may justify
   an offset-indexed compressed variant, and a hash-table format (V4)
   remains best when RAM is plentiful and misses must be free. We should
   expect to *keep* several formats, chosen per dataset and per
   infrastructure — not migrate through them serially.

This plan introduces the seam that makes formats pluggable: a read facade
the server depends on, a build interface the loader depends on, and the
contracts both sides must honor. It deliberately lands **before** V5, as
a pure refactor over V4 — so V5 (and every later format) arrives as "one
more implementor" rather than a fork of the reader and loader.

## Non-goals

- No on-disk change of any kind. A V4 snapshot built before this refactor
  is byte-identical to one built after.
- No async / batched lookup interface (`get_many`, io_uring dispatch).
  Discussed and deliberately deferred; nothing below precludes it.
- No auto-selection of format from dataset statistics (see Open
  questions); selection is explicit with a static default.

## Read side

### The facade

```rust
// mcfreeze-format
pub enum Snapshot {
    V4(v4::SnapshotReader),
    // V5(v5::SnapshotReader),   // added by FORMAT_V5_SPARSE_INDEX
}

/// Parsed, validated snapshot description. Cheap: reads and parses
/// meta.json only — no data fds, no mmaps, no residency work. This is
/// where format metadata parsing lives; `open` consumes the result and
/// cannot fail on syntax.
pub struct SnapshotDesc {
    root: PathBuf,
    meta: Meta,                    // per-version payload, private
}

impl SnapshotDesc {
    pub fn load(root: impl Into<PathBuf>) -> Result<Self>;
    pub fn format(&self) -> FormatId;
    pub fn stats(&self) -> &Stats;         // n_keys, created_at, ...
    pub fn encoding(&self) -> Option<&Encoding>;
}

/// Runtime knobs — how to open, as opposed to what/where (the desc).
/// Starts empty; the extension slot for future options (huge page
/// policy, mlock, derived search structures) so they never end up in
/// meta.json, which describes the artifact, not the process.
#[non_exhaustive]
#[derive(Default)]
pub struct OpenOptions {}

impl Snapshot {
    /// Performs ALL residency work (V4: mmap + MADV_POPULATE_READ;
    /// V5: load fences and sketch). Blocking and heavy — call from a
    /// blocking context. A snapshot that cannot reach steady-state
    /// serving latency fails here, loudly, before it can be swapped in.
    pub fn open(desc: SnapshotDesc, opts: &OpenOptions) -> Result<Self>;

    /// Convenience: SnapshotDesc::load + default options.
    pub fn open_path(root: impl Into<PathBuf>) -> Result<Self>;

    /// Sync, may block on pread — callers offload (spawn_blocking).
    pub fn get(&self, key: &[u8]) -> Result<GetOutcome>;

    pub fn desc(&self) -> &SnapshotDesc;
}
```

An enum, not `dyn Trait`: the set of formats is small and closed per
release, exhaustive matches catch missed arms at compile time, and the
facade stays object-safe-agnostic. Version dispatch happens exactly once
— the probe in `SnapshotDesc::load` selects the `Meta` variant, and
`open` matches on it.

The desc/open split earns its keep beyond tidiness:

- **Inspection without loading.** `/version` endpoints, `mcf inspect`,
  and controller-side checks read format, key count, and encoding from
  the desc without paying gigabytes of residency I/O.
- **A decision point.** The caller sees the parsed desc *before*
  committing to the heavy open — reject an unsupported
  `format_version`, or admission-check expected resident bytes against
  available RAM, without having half-opened anything.
- **Completeness checking is cheap.** `meta.json` is the build
  sentinel, so `SnapshotDesc::load` failing *is* the "snapshot not
  ready / corrupt" signal — the registry's polling can use it without
  touching data files.
- **What vs how.** The desc carries what's on disk; `OpenOptions`
  carries process-local choices. `open(path)` conflated the two.

### Contracts

Every implementor must honor these; they are what the server is allowed
to assume:

1. **`get` is sync and thread-safe** (`&self`, `Send + Sync`), may block
   on I/O; the caller owns offloading. No interior async, no runtime
   dependency in `mcfreeze-format`.
2. **Two stages, each total**: `SnapshotDesc::load` performs *only*
   metadata I/O (one small file) and all parsing/validation of it;
   `open` performs *all* residency work and returns a reader at
   steady-state latency (nothing left to fault in or load lazily).
   There is no separate warm-up step and no lazy loading — anything
   that can fail or stall does so in one of these two calls, before the
   registry can swap the snapshot into service. `open` never re-parses;
   it may cross-check the desc against file sizes where cheap
   (snapshots are immutable, so the desc cannot go stale).
3. **`Drop` releases everything** — mmaps, owned memory, file
   descriptors. The hot-swap lifecycle (registry swaps the
   `Arc<Snapshot>`, node-agent `MNT_DETACH` waits on references) depends
   on this and must not know what "everything" is.
4. **`GetOutcome` semantics are cost-defined** (below), so metrics mean
   the same thing on a mixed-format fleet.

### GetOutcome: cost-defined, not mechanism-defined

```rust
pub enum GetOutcome {
    Hit(Vec<u8>),
    Miss { io: bool },   // io: ≥1 pread was paid to conclude absence
}
```

`Miss { io }` replaces V4's `Collision` variant. Rationale: "collision"
names V4's mechanism (a 32-bit fingerprint collision) and carries V4's
interpretation (rare; a rise signals a hashing anomaly). Other formats
reach the same observable state — paid I/O, found nothing — through
mechanisms that are neither collisions nor anomalies (a filter false
positive at a *configured* rate; a format with no free-miss path at
all). Naming the observable cost is the only semantics every format can
implement honestly.

| Format | Path | Outcome |
|---|---|---|
| V4 | index probe, no candidate | `Miss { io: false }` |
| V4 | compact-fp collision, verify fails after pread | `Miss { io: true }` |
| V5 | sketch rejects | `Miss { io: false }` |
| V5 | block scanned, no match (sketch fp, or no sketch) | `Miss { io: true }` |

Metrics: `result ∈ {hit, miss, miss_io, error}`; the `collision` label is
retired (the word stays as internal V4 vocabulary). Each snapshot also
exports its **expected** paid-miss rate as a gauge
(`mcf_expected_miss_io_rate`: ≈0 for V4, the sketch ε for V5, 1.0 for a
sketch-less V5), so alerting compares observed vs expected instead of
hard-coding per-format thresholds — the V4 rule "any collision is weird"
falls out as the ε≈0 special case.

Finer cost (preads per lookup, heap follow-ups, fence ties) is
measurement, not semantics: histograms, never new enum variants.

### meta.json: probe, then commit

All parsing lives in `SnapshotDesc::load`, in two stages:

```rust
#[derive(Deserialize)]
struct VersionProbe { format_version: u32 }
```

then a full typed parse into the version's own `Meta` struct. Shared
envelope fields — `format_version`, `hash_algorithm`, `verify_seed`,
`stats`, `encoding` — live in a common struct each version embeds. No
union-of-all-versions `Meta` with `Option` fields.

The per-version payloads are **private to `mcfreeze-format`**: consumers
see only the desc's accessors (`format()`, `stats()`, `encoding()`), so
format-internal fields (bucket counts, block sizes, sketch parameters)
never leak into server or CLI code.

`meta.json` written last remains the completeness sentinel for every
format; this is an interface contract, not a format choice.

## Write side

### FormatId and the default

```rust
pub enum FormatId { V4 }        // V5 added later

impl FormatId {
    pub const DEFAULT: FormatId = FormatId::V4;   // flips when V5 is stable
}
```

The default lives in exactly one place in `mcfreeze-format`. The CLI
exposes it as `mcf load --format <id>` (default: `FormatId::DEFAULT`),
parsed and validated before any source is opened. `--format` is how a
dataset opts into a non-default layout (e.g. keep a RAM-rich dataset on
the hash index while tiny-KV datasets move to sparse).

### FormatBuilder

The loader's orchestration — sources, partition routing, concurrency,
progress, phase sentinels — is format-independent. What varies is what
gets written per record and what "build a partition" means. That is the
trait:

```rust
pub trait FormatBuilder: Send + Sync {
    /// Called concurrently from scatter workers. Appends one record to
    /// partition `p`'s transient state.
    ///   V4: data.bin append + spill record
    ///   V5: arrival.bin append + size histogram
    fn append(&self, p: usize, fp: u64, vfp: u64, value: &[u8]) -> Result<()>;

    /// Barrier between scatter and build. Global decisions go here.
    ///   V4: no-op
    ///   V5: derive block_size from the merged histogram
    fn plan(&mut self) -> Result<()>;

    /// Build partition `p`'s final artifacts from its transient state,
    /// then delete the transient state. Called from the build pool
    /// (`--index-parallelism`); must be independent across partitions.
    ///   V4: Robin Hood table
    ///   V5: radix buckets → sort → blocks/fences/sketch
    fn build_partition(&self, p: usize) -> Result<PartitionStats>;

    /// Write meta.json (last) from accumulated stats.
    fn finalize(self, stats: BuildStats) -> Result<()>;
}
```

The loader keeps owning the phase protocol (scatter → `scatter.done` →
plan → per-partition build + done markers → finalize/meta.json) and its
crash-recovery rules; the *sentinel protocol* is part of the interface,
the transient files behind it belong to the format. Recovery contract:
a partition with a done marker is skipped; one without is rebuilt from
its transient state; `meta.json` present ⇒ everything is skipped.

Key derivation (`fp`, `vfp`, partition routing by `fp & (N-1)`) stays in
the loader: it is shared by every format and pinning it above the trait
guarantees a key routes identically regardless of format.

## Module layout

```
mcfreeze-format/src/
  lib.rs            Snapshot facade, GetOutcome, FormatId, contracts
  meta.rs           envelope + version probe + per-version payloads
  data.rs           pread helpers (shared)
  v4/               index.rs, reader.rs, spill.rs, writer.rs, builder.rs
                    (moved verbatim; frozen except bug fixes)
```

`mcfreeze-loader` gains the `FormatBuilder` dispatch (`--format` →
constructor) and loses direct knowledge of spill/index internals.
`mcfreeze-server` swaps `SnapshotReader` for `Snapshot`, deletes the
separate `prewarm_index()` call in the registry (`open` subsumes it),
and renames the metric label.

## Conformance tests

The existing `reader.rs` tests are a format-independent behavioral spec
in disguise. Lift them into a generic harness:

```rust
fn conformance(build: impl Fn(&[(&[u8], &[u8])]) -> TempDir) {
    // roundtrip, empty value, large value, 10K keys, single partition,
    // missing key → Miss, corrupted/truncated data → error not miss,
    // outcome cost semantics (Miss{io:false} pays no syscalls)
}
```

Every format registers its builder once. This suite is the definition of
"is a valid format" — a new format is done when it passes, and deleting
an old format is safe because nothing else encodes its behavior.

The `Miss { io: false } == zero syscalls` assertion deserves a real
check (e.g. counting preads through a test shim on the data fd), since
it is the contract dashboards depend on.

## Rollout

Pure-refactor PR sequence, V4-only, no on-disk change:

1. Move V4 code under `v4/`; introduce `Snapshot` facade + two-stage
   meta parse. Server compiles against the facade.
2. `GetOutcome::Miss { io }` + metric label rename (`collision` →
   `miss_io`) + expected-rate gauge. Update dashboards/alerts once.
3. `FormatBuilder` trait; port the V4 loader path onto it; `--format`
   flag with `v4` as the only value.
4. Conformance harness; port reader tests onto it.
5. Verify: `mcf load` output byte-identical to pre-refactor (hash the
   snapshot dir), serve benchmark unchanged.

`FORMAT_V5_SPARSE_INDEX.md` then lands entirely *inside* this seam: a
`v5/` module, a `FormatId::V5` arm, a conformance registration — and no
server changes at all.

## Open questions

- **Auto-selection** (`--format auto`): choosing a format from dataset
  statistics requires knowing sizes/counts before scatter begins, i.e.
  source metadata (BQ session stats) or a sampling pre-pass — scatter
  output is format-specific, so switching after scatter is not an
  option. Defer until at least two formats coexist in production and
  the choice rule is validated by hand first.
- Error taxonomy: one shared `Error` enum with format-specific variants
  boxed, or per-format errors unified at the facade? Lean shared —
  the server matches on error kinds for metrics.
- Should `SnapshotDesc` grow a stable derived-facts schema beyond raw
  build stats — notably `expected_resident_bytes()` (V4: index size;
  V5: fences + sketch) — so the registry can admission-check a
  generation against available RAM before `open`, and `/version` /
  capacity dashboards get format-independent numbers?
