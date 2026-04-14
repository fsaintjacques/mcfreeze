# mmap-backed per-partition index build

## Motivation

`mcf load` on a 14.24M-key / 64-partition snapshot is killed (OOM) during
the index phase. Root cause is in
`rust/crates/mcfreeze-loader/src/build.rs`: `IndexBuildPhase::run` builds
every partition's bucket table via rayon `par_iter` and collects them into
a `Vec<Vec<Bucket>>` before calling `write_unified_index`. Peak RSS holds
all 64 tables plus the per-partition `Vec<RawEntry>` that each build
materializes from `spill.bin`.

For this dataset:

- 14.24M keys, uniform across 64 partitions → ~222K keys/partition.
- Per partition during build: `Vec<RawEntry>` ~3.5 MB + `Vec<Bucket>` ~1.8
  MB + psls ~230 KB.
- All 64 `Vec<Bucket>` (~115 MB) plus transient entry vecs, plus whatever
  scatter-time residuals (tokio runtime, BQ buffers, `data.bin` mmaps)
  are still resident. Enough to trip the kernel on a modestly-sized box.

We want peak memory that is **bounded by `index_parallelism`**, not by
`n_partitions` or `n_keys`, and that is evictable under memory pressure
rather than living on the heap.

## Non-goals

- Truly input-size-independent memory (would require external sort of
  `spill.bin` by home position — meaningful work, deferred).
- Pipelining index build with scatter. Fingerprint-shuffle means no
  partition is "done" until every BQ stream finishes, so all partitions
  unblock together. Nothing to pipeline.
- Changing the on-disk format of `index.all`, `data.bin`, `spill.bin`,
  `meta.json`, or `scatter.done`.
- Changing the reader. `index.all` is still a concatenation of per-
  partition bucket tables with 2 MiB alignment padding between them, and
  `meta.json` still records `(offset, n_buckets)` per partition.

## Prerequisite: shrink `SpillRecord` from 24 B to 8 B

`SpillRecord` today is 24 B (`u64` fingerprint + `u64` aligned_offset +
`u32` size + `u32` _pad). Only 8 B of that survives into the final
`Bucket`:

- `fingerprint: u64` → narrowed to `u32` via `compact_fingerprint` during
  insert. The high bits only exist so `Layout::partition_of` can route
  to a partition; once a record is in `part-NN/spill.bin`, the partition
  is implicit and the high bits are dead.
- `aligned_offset: u64` → validated to fit in `u32` by `RawEntry::new`.
- `size: u32` → explicitly unused in V3 (value size is in the value
  header). The struct comment calls it out.
- `_pad: u32` → alignment filler for the dead `size` field.

So the "right" `SpillRecord` is literally `Bucket`:

```rust
#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
pub struct SpillRecord {
    pub fingerprint: u32, // compact (low 32 bits of the u64 fp)
    pub offset: u32,      // aligned_offset in 64 B units
}
```

Wins (independent of the mmap refactor, but strictly helpful to it):

- **3× less spill I/O.** For the `gh-files` snapshot: 346 MB → 115 MB.
- **Faster scatter.** `partition_writer`'s disk throughput per key drops
  by 3×.
- **Cleaner build loop.** The build phase consumes `(u32, u32)` pairs
  that are already in the final bucket representation. No
  `compact_fingerprint` call, no `RawEntry::new` validation in the hot
  loop, no u64→u32 narrowing — just read and Robin-Hood-insert.
- **Conceptual clarity.** `SpillRecord == Bucket`: spill becomes "an
  unsorted multiset of buckets," index build becomes "hash-place them."

Pre-audit required before the change:

1. Confirm `Layout::partition_of` uses high bits of the `u64`
   fingerprint and `compact_fingerprint` uses the low 32 bits, so that
   dropping the high bits at scatter time is lossless for the in-
   partition hash. 5-minute read of `mcfreeze-format::index::fingerprint`
   + `Layout::partition_of`.
2. Grep for any reader of `SpillRecord::size`. Expected: none.

Migration:

- Spill is transient state between scatter and index. A completed
  snapshot has no spills, so shipped snapshots are unaffected.
- The only on-disk hazard is "crashed mid-load, upgrade binary, resume."
  Mitigate by bumping the magic: `KVSPILL\n` → `KVSPIL2\n` (or similar).
  The new reader rejects old magic with a clear error instead of casting
  into a wrongly-sized struct. On mismatch the user deletes the snapshot
  dir and re-runs; we document this in the release notes.
- `SPILL_RECORD_SIZE` constant updates from 24 to 8.
- Update `spill.rs` tests (record size assertion, round-trips).
- Update `scatter.rs` partition_writer to compute
  `compact_fingerprint(fp)` and cast `aligned_offset` to `u32` at push
  time (both operations already happen in `RawEntry::new` and `insert`;
  we just move them upstream by one hop).

This lands as **step 0** of the PR, before any mmap work, and is
independently testable.

## Design

Keep the current two-phase architecture (scatter → index). Rewrite the
index phase so that:

1. Each partition's Robin Hood table is built **directly into a file-
   backed mmap** (`part-NN/index.bin`), with `psls` as a small in-RAM
   `Vec<u8>`. No `Vec<Bucket>`, no `Vec<RawEntry>`.
2. Per-partition builds are driven by a rayon pool of size
   `--index-parallelism` (default 2). Peak RSS ≈
   `index_parallelism × working_set_of_one_partition`, and that working
   set is kernel-managed page cache — evictable under pressure.
3. A final **concat phase** streams each `part-NN/index.bin` into
   `index.all`, padding to 2 MiB boundaries between partitions, recording
   `(offset, n_buckets)` per partition. Uses a fixed-size copy buffer;
   O(1) heap. Order of partitions in `index.all` is irrelevant because
   `meta.json` holds the offsets and the reader does not assume
   contiguous/ordered layout.

### Per-partition build (mmap)

```
                    ┌──────────────────────────────────┐
 spill.bin  ───▶    │  build_partition_index_mmap(dir) │
 (streamed)         │                                  │
                    │  1. n = SpillReader::open().count│
                    │  2. n_buckets = ceil(n / 0.95)   │
                    │  3. ftruncate index.bin          │
                    │       → n_buckets * 8 bytes      │
                    │  4. mmap RW (MAP_SHARED)         │
                    │  5. psls = vec![0u8; n_buckets]  │
                    │  6. for rec in SpillReader:      │
                    │       insert(&mut mmap, &mut psls│
                    │              , rec)              │
                    │     ↳ on PslOverflow:            │
                    │         drop mmap                │
                    │         n_buckets = ceil(*1.5)   │
                    │         ftruncate + zero         │
                    │         remap + reset psls       │
                    │         restart scan from spill  │
                    │  7. msync(MS_ASYNC) + drop       │
                    │  8. unlink spill.bin             │
                    │  9. return (n_buckets, retries)  │
                    └──────────────────────────────────┘
```

Notes:

- `insert` is the existing `mcfreeze_format::index::insert`, but
  operating on `&mut [Bucket]` from the mmap slice. The displacement
  logic is unchanged.
- `psls` stays in RAM because it is ~1 B × n_buckets (~230 KB for 222K
  keys). Keeping it on the heap is trivial and keeps the inner loop
  fast. If this ever matters we can move it to a second mmap with zero
  algorithmic change.
- PSL retry grows the table 1.5× as today. With mmap this means
  `munmap` → `ftruncate` → re-zero → `mmap` → restart the spill scan.
  `ftruncate` extension zero-fills new pages, but the *existing* region
  needs to be zeroed because the home position depends on `n_buckets`.
  Simplest: truncate to 0, then truncate to the new size. Kernel
  guarantees zero pages.
- Spill is read twice on an overflow retry — this is identical to the
  current in-memory behavior, which also reconstructs the table from
  scratch. The spill is small and sequential, so the cost is cache-
  friendly.
- `Bucket` is `#[repr(C)] Pod`, 8 bytes, so `bytemuck::cast_slice_mut`
  turns the mmap byte slice into `&mut [Bucket]` safely. No unsafe
  beyond what `memmap2` already requires.

### Concat phase (single thread)

```
 offsets   = Vec::with_capacity(n)
 n_buckets = Vec::with_capacity(n)
 file      = File::create(index.all)
 file_off  = 0

 for i in 0..n:
   path = part-i/index.bin
   len  = metadata(path).len()
   offsets.push(file_off)
   n_buckets.push(len / 8)

   src = File::open(path)
   io::copy(&mut src, &mut file)      // fixed 64 KiB buf inside io::copy
   file_off += len

   if i + 1 < n && file_off % INDEX_ALIGNMENT != 0:
     aligned = round_up(file_off, INDEX_ALIGNMENT)
     file.set_len(aligned)
     file.seek(SeekFrom::Start(aligned))
     file_off = aligned

   fs::remove_file(path)
```

`IndexDone.index_offsets` / `index_n_buckets` are populated from the
`offsets` / `n_buckets` vectors. Aggregate stats (`n_keys`, `retries`,
`fill_rate_*`) come from the per-partition results returned by the build
phase.

### Crash recovery / idempotency

- `index.done` remains the sentinel for phase completion. Present ⇒
  skip the whole index phase.
- On crash mid-build, some partitions have `index.bin` and no
  `spill.bin`, others still have `spill.bin` and no `index.bin`, others
  might have both mid-transition. On restart we rebuild per partition
  using the following rule:
  - If `index.bin` exists and `spill.bin` does not → partition done,
    use `metadata(index.bin).len() / 8` as `n_buckets`.
  - If `spill.bin` exists → (re)build the partition, overwriting any
    partial `index.bin`.
  - Neither file → treat as empty partition (`n_buckets = 0`), matches
    the current empty-partition path.
- On crash mid-concat, `index.all` is partial and `index.done` does not
  exist. The per-partition `index.bin` files may have been deleted for
  the partitions already concatenated. To make concat crash-safe, we
  delay the `remove_file` call until after `index.all` is fully written
  and `index.done` is about to be written. Concretely: concat copies
  all partitions, writes `index.done`, then sweeps
  `part-NN/index.bin`. On restart after a concat crash the sweep is
  idempotent (missing files are ignored).

## Code changes

### `rust/crates/mcfreeze-format`

- Add `memmap2` dependency (if not already present).
- New module / function: `index::build_mmap(file: &File, spill: impl
  Iterator<Item = Result<RawEntry, E>>, n_keys: usize) -> Result<(usize,
  usize)>` returning `(n_buckets, retries)`.
  - Owns the `ftruncate` + `mmap` + PSL-retry loop.
  - Internally calls the existing `insert` on `&mut [Bucket]` obtained
    from `bytemuck::cast_slice_mut(&mut mmap[..])`.
- Keep `insert`, `bucket_count`, `FILL_RATE`, `MAX_PSL` as-is.
- `write_unified_index(&[Vec<Bucket>])`: delete. No production caller
  remains. If any test still needs it, move it behind `#[cfg(test)]`
  inside the test module.

### `rust/crates/mcfreeze-loader`

- `build.rs`:
  - `IndexBuildPhase::run` becomes two sub-steps:
    1. **Per-partition mmap build.** rayon pool sized by
       `self.parallelism`. For each `i in 0..n`:
       - Open `part-i/spill.bin` via `SpillReader`. If absent, skip
         (empty partition; emit zeroed `PartitionIndexDone`).
       - Create/truncate `part-i/index.bin`.
       - Call `index::build_mmap` with the spill iterator.
       - `fs::remove_file(spill.bin)`.
       - Return `PartitionIndexDone { n_keys, n_buckets, fill_rate,
         retries }`.
    2. **Concat.** Sequential loop as described above. Builds `offsets`
       and `n_buckets` vectors, writes `index.all`, writes `index.done`,
       sweeps `part-NN/index.bin`.
  - Idempotency logic extended with the restart rules above.
  - Progress callback `(1, 0)` is fired once per partition as today,
    after its mmap build completes (not during concat — concat is fast
    relative to build).
- `spill.rs`: no change. `SpillReader` already streams.
- `scatter.rs`: no change. Scatter still produces `data.bin` +
  `spill.bin` per partition and writes `scatter.done`.

### `rust/crates/mcfreeze-cli`

- `load.rs`: no functional change. `--index-parallelism` still drives
  the rayon pool size inside `IndexBuildPhase`. Document that peak RSS
  is `≈ index_parallelism × one_partition_working_set`.

## Tests

### `mcfreeze-format`

- `index::build_mmap`:
  - Round-trip: build a small table into a `tempfile::NamedTempFile`,
    then cast the file bytes back to `&[Bucket]` and probe every key.
  - PSL overflow: construct a pathological input that forces ≥1 retry
    (existing test corpus for `index::build` has one), assert `retries
    > 0` and all keys still probe correctly.
  - Empty input: `n_keys == 0` → file length 0, `n_buckets == 0`.

### `mcfreeze-loader::build`

- `mmap_build_produces_index_all`: end-to-end via `scatter_and_build`,
  assert `index.all` exists, every partition's bytes are reachable at
  the offsets recorded in the returned `IndexDone`, and probing via the
  reader returns the inserted values.
- `spill_files_removed_after_build`: unchanged expectation.
- `per_partition_index_files_removed_after_concat`: new — assert no
  `part-NN/index.bin` remains after `run` completes.
- `index_build_is_idempotent`: unchanged expectation — second run is a
  no-op and `index.all` is byte-identical.
- `restart_after_partial_build`: simulate a crash by running the first
  phase on N partitions, deleting `index.done` if present, removing
  `index.bin` from a subset of partitions (keeping their `spill.bin`),
  then re-running. Assert all keys probe correctly and `spill.bin` is
  gone everywhere.
- `restart_after_partial_concat`: pre-populate `part-NN/index.bin` for
  all partitions with known contents, run `IndexBuildPhase::run`, assert
  the build phase is skipped (spill already consumed) and concat
  succeeds.

### Manual / integration

- Re-run `mcf load --output gh-files --partitions 64 --key-column id`
  against the existing `gh-files/` snapshot (scatter already done) and
  confirm the process completes without OOM. Record peak RSS via
  `/usr/bin/time -l` (macOS) or `/usr/bin/time -v` (Linux).

## Rollout

Single PR against a fresh branch off `main` (orthogonal to k8s work):

1. **Step 0 — shrink `SpillRecord`.** Audit fingerprint bit layout,
   bump `SPILL_MAGIC`, shrink struct to 8 B, update
   `SPILL_RECORD_SIZE`, move `compact_fingerprint` + `offset as u32`
   into `partition_writer` push path, update tests. Land as its own
   commit; `cargo test -p mcfreeze-format -p mcfreeze-loader` green.
2. Add `memmap2` to `mcfreeze-format` if absent.
3. Implement `index::build_mmap` + unit tests.
4. Rewrite `IndexBuildPhase::run` to use it + concat phase.
5. Delete `write_unified_index` (or move to `#[cfg(test)]`).
6. Update `build.rs` tests.
7. Run `cargo fmt`, `cargo clippy --all-targets`, `cargo test -p
   mcfreeze-format -p mcfreeze-loader`.
8. Manual re-run of `mcf load` against a fresh snapshot dir (the existing
   `gh-files/spill.bin` uses the old magic and must be re-scattered).

No migration: the on-disk format is unchanged, existing snapshots are
readable as-is.

## Open questions

- Do we want `--index-parallelism` renamed or documented as an RSS knob?
  Default of 2 seems fine; higher values trade RSS for wall time.
- Should the concat phase use `copy_file_range(2)` on Linux for zero-
  copy? Optional optimization, not required for correctness. Skip in
  v1; revisit if concat becomes measurable.
- Should `psls` also be mmap-backed? Not in v1. It is small and the
  inner loop benefits from it being a plain `&mut [u8]` slice with no
  page-fault overhead.
