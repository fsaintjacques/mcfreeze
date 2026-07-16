<!-- SPDX-License-Identifier: Apache-2.0 -->

# V5: Transparent value compression (zstd, zstd + learned dictionary)

## Motivation

V5 values are stored verbatim. Typical datasets (protobuf-encoded
records, JSON fragments, URLs) compress 2–4× — but only with shared
context: a lone 69–256 B value gives LZ77 almost no window to find
matches in, so **plain per-value compression barely touches the small
values that dominate our datasets**. A dictionary learned from the data
restores the cross-record redundancy without giving up per-value random
access — and zstd dictionaries carry pre-trained entropy tables
(Huffman/FSE), which is where small values win most: matches are short,
literals dominate, and entropy coding is what compresses literals.

What compression buys in V5 specifically, because everything scales
with data bytes:

- **records/block ↑** → fences/key ↓ (fences cost
  `32 bits × block_size⁻¹ × avg_stored_record`), shrinking the resident
  index further;
- **page-cache density ↑** — more distinct hot records resident per GB;
- **disk ↓** and, for the diskless builder, **bucket/arrival bandwidth ↓**
  (arrival stays raw; blocks/heap shrink);
- larger logical values stay inline under the same `inline_threshold`.

Cost: one zstd decompress per hit (~1.5 GB/s with a digested
dictionary; ~200–500 ns for typical values — noise against a 50–100 µs
pread, a few percent of a page-cache-hot get), plus build-time
compression CPU during emit.

## Modes

```
--compression none | zstd | zstd-dict      (default: none, measure first)
```

- `none` — bytes stored verbatim; on-disk output byte-identical to
  today's V5.
- `zstd` — each value zstd-compressed independently (level ~3). Only
  wins on values with internal redundancy (≳1 KiB, repetitive
  payloads).
- `zstd-dict` — zstd with a per-snapshot dictionary (default 64 KiB)
  learned at plan time from a prefix of one partition's arrival log
  (ZDICT/COVER training).
  The headline mode for small-value datasets: the dictionary carries
  both the match context AND the entropy tables that per-value
  compression lacks. Small-records-with-dictionary is the workload
  zstd was designed around.

Plus one knob: `min_compress_len` (default 64 B) — values smaller than
this skip the compression attempt entirely and store raw. A guard
against wasting build CPU on values too small to win.

The mode and threshold are **plan-time decisions**: pinned in `v5.plan`
alongside `block_size` and `sketch`, immutable for the snapshot,
recorded in `meta.json`. Mixed-mode partition sets cannot exist for the
same reason mixed-sketch sets cannot. `meta.compression` declares the
snapshot's codec and dictionary once; the per-record bit only selects
between that codec and the raw fallback.

## Unit of compression: the value, not the block

Compressing whole 4 KiB blocks (Parquet-page style) would compress
better than dictionary-per-value — but it breaks V5's load-bearing
invariant that block `i` lives at `i × block_size` with no offset
stored anywhere, and it forces decompressing ~60 records to read one.
Rejected. Per-value compression preserves every V5 invariant — the
scan, fences, checksums, and stub machinery are untouched — and the
learned dictionary exists precisely to recover the cross-record
redundancy that per-block compression would have captured.

## Record flag: bit 30, length = stored bytes

One new flag bit in the record header; one uniform rule; no payload
envelope.

```
[8B verify_fingerprint] [4B length|flags] [payload] [pad to 8B]

flags: bit 31 = out-of-line (unchanged)
       bit 30 = compressed         (new)
length: low 30 bits — ALWAYS the stored byte count at the location
        (inline payload bytes, or the heap extent a stub preads;
        the compressed length when bit 30 is set)
```

- **Compressed payload** (bit 30 set) = a bare zstd frame, magicless
  with the frame content size pinned (both knobs exposed by the `zstd`
  crate) — the frame self-describes its decompressed size, so no
  side-channel `raw_len` is needed anywhere. Frame overhead ~5–8 B,
  paid only by values that compression is already winning on.
- **Raw payload** (bit 30 clear) = the bare value bytes, byte-identical
  to today — zero overhead on raw-stored values.
- **Per-record fallback**: the writer sets bit 30 only when the zstd
  frame is strictly smaller than the raw value; incompressible and
  sub-`min_compress_len` values store raw. Storage never expands.
- The block scan and stub pread are untouched: `length` is what is
  stored, always. Nothing decompresses during a scan — only the one
  record whose vfp matched.
- `MAX_VALUE_LEN` drops to 2³⁰ − 1 (1 GiB); the 16 MiB ingestion cap
  makes this a non-event.
- `inline_threshold` applies to the **stored** size — the inline/heap
  decision is made after compression, which is what keeps
  fat-but-compressible values inline.
- Stubs are structurally unchanged (`[8B heap_offset][4B
  value_checksum]`); bit 30 on a stub means the heap extent is a zstd
  frame.

**Compression is part of format V5's definition, not an extension.**
No V5 snapshot has ever been generated (the format is pre-GA and PR
#22 is unmerged), so this lands in the V5 line before any reader or
dataset ships — there is no "old V5 reader" for a compressed snapshot
to meet, by fiat rather than by versioning. Two requirements make
that airtight rather than aspirational:

- the V5 record decoder masks `length` with 2³⁰ − 1 **unconditionally**
  (bit 30 is always decoded as a flag, never folded into length), from
  the first implementation commit onward;
- bit 30 set while `meta.compression` is absent is a corruption error —
  a stray or future-foreign record fails loudly, never silently.

`format_version` stays 5; a bump is reserved for changes that meet
already-shipped readers.

Reader guards: a frame's content size is attacker-influenceable bytes
and is bounds-checked (≤ `MAX_VALUE_LEN`) before allocation; a decode
failure or content-size mismatch after a clean checksum is an error,
never a miss.

Checksums verify **stored bytes, before decompression**: the block
checksum already covers inline payloads; the stub's `value_checksum`
covers the stored heap extent. Media corruption is caught before zstd
ever sees the bytes. Dictionary integrity is handled at open (below) —
a corrupt dictionary would otherwise decompress cleanly into wrong
bytes.

## Dictionary lifecycle

```
<snapshot>/
  dict.bin            zstd-dict mode only: raw ZDICT output, no framing
```

- **Sample = a prefix of one partition's arrival log.** Scatter is
  untouched: partition routing is by hash, so any single partition is
  already an unbiased sample of the key space, and its `arrival.bin`
  is the sample pool. At `plan()`, read the first ~10 MB of partition
  0's arrival log and parse the frames — that comfortably covers the
  ~100× dictionary size ZDICT wants (~6.4 MB for a 64 KiB dict). No
  reservoir code, no transient `samples.bin`, no appender changes.
- **Train at plan()**: the existing scatter→build barrier is the
  natural home (it already decides `block_size`). Train via ZDICT
  (COVER), write `dict.bin`, record the mode + dictionary checksum in
  `v5.plan`.
- **Durability ordering** (the data-before-marker rule applied to the
  dictionary): `dict.bin` is written and fsynced (`write_data_file`)
  **before** its checksum lands in `v5.plan` (the transient carrier
  that finalize copies into `meta.json`), which is itself published
  atomically before any partition builds — and therefore strictly
  before `build.done` markers and the `meta.json` completeness
  sentinel. A `v5.plan` naming a dictionary checksum implies the
  dictionary is durable; on object-store marker-mode layouts the same
  ordering means `meta.json` can never be observed ahead of `dict.bin`.
  Resume **trusts** the recorded checksum (COVER retraining is not
  assumed byte-stable across runs or libzstd versions); a mismatch
  under this ordering is genuine corruption and fails loudly —
  deleting `v5.plan` + `dict.bin` retrains from scratch.
- **Auto-tune sees stored sizes directly**: compress every sampled
  value with the fresh dictionary (fallback rules and frame overhead
  included) and build a stored-size histogram; the block-size rule
  runs on *its* median. This beats scaling the raw median by an
  average ratio, which mis-tunes whenever compressibility correlates
  with size (it usually does). The scatter-time raw histogram remains
  the input for `none` mode.
- **Known bias, made observable**: the prefix is unbiased across keys
  but not across time — it holds the earliest-arriving source rows, so
  value-shape drift over table order (schema evolution) underfits the
  tail. Deeper sampling is not cheap (arrival frames are
  variable-length with no sync markers; random offsets are not
  parseable), so the default stays prefix-of-one-partition — and the
  stats make the bias visible: the plan-time **sample ratio** and the
  build-time **achieved ratio** are both recorded, and divergence
  between them is exactly the signature of a biased sample.
- **Load at open**: `dict.bin` is read into owned memory and verified
  against `meta.compression.dict_checksum` — a corrupt dictionary
  produces silently wrong values (the one failure mode value checksums
  cannot catch, exactly like the sketch's false negatives), so it
  fails the open.
- **Build**: `build_partition` compresses each value against the
  dictionary during emit (after the sort, before the assembler). The
  dictionary is shared read-only across build threads.

Dependency: the `zstd` crate throughout — it vendors and builds the
libzstd sources during `cargo build` (no system library, no install
step; the only friction is needing a C cross-toolchain when
cross-compiling). The reader holds a digested `DDict` per snapshot for
near-free per-lookup dictionary reference; decompression *contexts*
(`DCtx`) are not thread-safe and are per-call or thread-local-pooled —
the ~200–500 ns estimate assumes a reused context, so the pool is not
optional on the hot path. If a C-free serving binary
is ever required, `ruzstd` (pure-Rust decoder, dictionary-capable,
~2× slower — still noise here) can replace the read side without any
on-disk change; the conformance suite should then add C-encoded →
ruzstd-decoded roundtrips.

## meta.json

```json
"compression": { "codec": "zstd", "dict": true, "min_compress_len": 64,
                "zstd_version": "1.5.6", "dict_checksum": 1234567890 }
```

Absent/`null` = none. The reader rejects unknown codecs at
`SnapshotDesc::load` (same posture as sketch kinds); it does not need
`min_compress_len` (writer-side only, recorded for provenance).
`dict: true` requires `dict.bin`, whose integrity is anchored here:
`dict_checksum` (checksum32 of the file) lives in `meta.json` rather
than embedded in the file, so `dict.bin` stays raw ZDICT output that
standard zstd tooling can use directly (`zstd -D dict.bin`). `zstd_version` records the vendored
libzstd that trained the dictionary and compressed the values —
provenance for debugging ratio drift across toolchain bumps (decoding
is compatible across versions; trained-dictionary *bytes* are not
reproducible across them).

**Stats**: the per-partition `build.done` markers and the aggregate
`index.done` sentinel (embedded into `meta.json` stats) record
`value_bytes_raw` and `value_bytes_stored`, plus `n_compressed` /
`n_raw` record counts — the achieved ratio is a first-class
observable per partition and per snapshot, not something derived from
`du`. Also recorded once: `n_samples`, `dict_len`, and the
sample-measured ratio the auto-tuner used.

## Read path delta

```
... vfp match, checksum verified ...
if record.flags.compressed:
    raw = zstd_decompress(payload, ddict?)?    // size from the frame;
    return raw                                 // error, never a miss
```

Decompression happens only after a vfp match — misses and non-matching
records in scanned blocks never pay it. The sketch, fences, and pread
budget are all unchanged.

## What changes where (seam audit)

- `v5/block.rs`: bit 30 (`push_*` gain a compressed flag; `Record`
  exposes it), `MAX_VALUE_LEN` → 2³⁰ − 1. Scan logic untouched.
- `v5/compress.rs` (new): codec enum, dict train/load/verify, compress
  with raw fallback, bounded decompress.
- `v5/builder.rs`: sampling in the appender, training + ratio scaling
  in `plan()`, compression at emit, `v5.plan` + stats fields.
- `v5/meta.rs`: `compression` section + validation.
- `v5/reader.rs`: dict load at open, decompress on hit.
- CLI: `--compression`, dry-run validation, resume warning (pinned).
- Loader/server: **no changes** — everything rides the existing seam.

## Sizing sanity check (StackOverflow users, 18.7M keys)

Raw: 69 B avg stored record, 59.6 records/block, 1.29 GB blocks,
1.26 MB fences. At an (assumed, to be measured) 2.5× dictionary ratio
on the ~57 B values: ~23 B frame bodies + ~6 B frame overhead ≈ 41 B
stored records → ~100 records/block → ~790 MB blocks, ~0.8 MB fences,
and roughly 1.7× more hot records per GB of page cache. (Frame
overhead eats a visible slice at this value size — the benchmark
decides whether `zstd-dict` earns its keep per dataset, now directly
readable from the stored/raw byte stats.) The benchmark suite (stage 6
of the V5 plan) gains a `--compression` dimension to replace these
assumptions with numbers.

## Go / no-go criteria

Pre-committed before the benchmark runs, so the numbers meet a bar
instead of a mood:

- **Per-dataset enable bar**: `zstd-dict` is worth enabling for a
  dataset iff `value_bytes_stored / value_bytes_raw ≤ 0.75` (≥ 1.33×)
  as reported by build stats — below that, the fence/cache/disk wins
  are marginal against the dictionary lifecycle complexity and the
  hit-path decompress.
- **Default flip bar**: `zstd-dict` becomes the default only if the
  median production dataset clears 1.5× *and* the p99 hit-latency
  delta vs `none` stays within 5% on the page-cache-hot benchmark.
- Plain `zstd` (no dict) ships as a mode but is expected to fail the
  bar on small-value datasets; it exists for the ≳1 KiB tier.

## Open questions

- `min_compress_len` default: 64 B is likely too low — with ~5–8 B
  frame overhead and the strict-smaller rule, values just above 64 B
  rarely win and only burn build CPU; the benchmark should sweep
  {64, 128, 256} and the default should follow the data.
- Dictionary size default (64 KiB) and sample budget (prefix bytes):
  measure ratio vs dict size on real datasets; ZDICT has diminishing
  returns past ~110 KiB for small values.
- Prefix-sample bias: if real datasets show sample-vs-achieved ratio
  divergence, add a header-walk stride sampler (sequential frame walk
  skipping values, collecting every Nth) over one or more partitions.
- Per-partition dictionaries: better locality of context (each
  partition's key space is a uniform sample, so likely no win) vs N×
  dict RAM and N× training. Default global; revisit only with evidence.
- `zstd-dict` without enough samples (tiny snapshots): fall back to
  plain `zstd` with a warning, or fail? Lean fallback + warn, recorded
  in stats.
- Compression level: default 3; benchmark negative levels (build CPU)
  vs 3–7 (ratio) on real datasets.
- Value checksum over raw instead of stored bytes would also catch
  dict/codec bugs end-to-end, at the cost of checksumming after
  decompress on every hit (~1 GB/s xxhash on tiny values — likely
  negligible). Decide with the benchmark.
- lz4 as an alternative codec (~2× faster decompress, pure-Rust
  `lz4_flex`, notably worse ratio on small values — no entropy stage,
  so it uses only the match-window half of a trained dictionary):
  rejected as the default because ratio is the metric that moves
  fences/cache/disk while decompress speed hides under the pread; the
  codec-agnostic flag bit leaves the door open if a measured workload
  ever turns up decompress-bound.

## Implementation order

1. **`v5/compress.rs`** + bit 30 in `v5/block.rs`: compress with raw
   fallback, bounded decompress, unit tests (roundtrip both modes,
   sub-threshold and incompressible raw paths, corrupt-dict rejection,
   content-size mismatch and oversized content-size → error,
   bit 30 without meta.compression → corruption).
2. **Builder, dictionary lifecycle** (2a): plan-time prefix sampling,
   ZDICT training, `dict.bin` write + durability ordering, `v5.plan`
   pinning, resume tests (torn-dict, plan-without-dict, retrain-from-
   scratch). Lands and is verified before anything consumes the dict.
3. **Builder, compression** (2b): emit-time compression with fallback,
   stored-size histogram auto-tune, meta/stats plumbing (raw/stored
   byte counters, sample vs achieved ratio).
4. **Reader**: dict load + verify at open, decompress on hit;
   conformance grows a compression axis (`none`/`zstd`/`zstd-dict` ×
   existing cases) plus corrupt-dict and truncated-dict open failures.
5. **CLI**: `--compression`, validation, messages.
6. **Benchmark**: ratio, records/block, fence bytes, hit latency delta
   vs `none` on gh-files + StackOverflow, judged against the go/no-go
   bars above.
