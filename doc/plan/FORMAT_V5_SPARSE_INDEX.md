<!-- SPDX-License-Identifier: Apache-2.0 -->

# Format V5: Sparse Index (fingerprint-sorted blocks)

## Motivation

The V4 index costs 8 bytes per bucket at 0.95 fill ≈ **8.42 B/key**, and it
must stay page-cache resident to keep lookups at one `pread`. That is the
dominant RAM cost of a serving node:

| Keys | V4 index (resident) |
|------|---------------------|
| 500M | 4.2 GB |
| 1B   | 8.4 GB |
| 10B  | 84 GB |

Target for V5: **≥10× smaller resident footprint** (10B keys ≤ 8 GB) while
keeping the lookup I/O budget at **median 1 `pread`, worst case 2 in
practice** (probabilistic, not invariant — quantified in fences.bin).

## Core idea

Sort each partition's records by fingerprint and index **blocks, not keys**.

The V4 index spends 32 bits/key on an offset and 32 bits/key on a
fingerprint. Both stop being per-key once the data is sorted:

- **Offset → 0 bits.** Records are packed densely into fixed-size blocks;
  block `i` lives at `i × block_size`. Position is computed, not stored.
  The fine position within a block is found by scanning the block after the
  `pread` — anything inside the block just read is free to locate.
- **Fingerprint → 32 bits per block.** Sorted order means one boundary
  fingerprint ("fence") per block routes all ~16 keys inside it. Per-key
  discrimination reuses the verify fingerprint already stored in every
  record header in the data file.
- **Fill slack → 0.** A dense sorted array has no empty buckets.

| | V4 (hash index) | V5 (fences) |
|---|---|---|
| Offset | 32 bits/key | 0 (implicit) |
| Fingerprint | 32 bits/key | 32 bits/block ≈ 2 bits/key |
| Fill slack | ~3.4 bits/key | 0 |
| **Total** | **~67 bits/key** | **~2 bits/key** |

An optional per-partition **sketch** (Bloom / binary-fuse filter over key
fingerprints) restores V4's zero-I/O misses: only sketch false positives pay
a wasted `pread`.

Per-record offsets inside a block are deliberately absent: the storage stack
reads in 4 KiB granularity regardless (page cache pages, NVMe sectors), so a
byte-exact offset cannot reduce physical I/O for sub-block values — it would
only skip a ~16-record in-memory scan, at a cost of ~32 bits/key of RAM.
Location data finer than "which block" belongs to CPU, not RAM.

Intra-partition sub-partitioning is likewise subsumed: sorting by
fingerprint is partitioning taken to its limit, and a sub-partition boundary
is just a coarse fence — the format already has a fence every 4 KiB. Sub-
partitions exist only as a transient radix pass inside the build (see
Construction) and must never appear in `meta.json`. Raising the partition
count N instead buys nothing the sort doesn't already provide, and costs
real per-partition overhead: open fds (4 files per partition, ×2
generations during hot-swap), per-instance sketch overhead (small filters
have worse bits-per-key), and scatter fan-out. N stays sized by serving
concerns (data volume / heap capacity, fd budget), exactly as in V4.

## Directory layout

```
<snapshot>/
  meta.json
  data/
    part-NN/
      blocks.bin          fingerprint-sorted records in fixed-size blocks
      heap.bin            out-of-line values > inline_threshold
      fences.bin          this partition's fence array
      sketch.bin          optional; this partition's filter
      build.done          per-partition build stats; resume evidence
```

`build.done` is permanent: it is the partition's "scatter and build
completed" evidence for `scatter_probe` on a rerun, playing the role
V4's `index.all`-exists check plays. (V4 needs no extra file because
its unified index doubles as the evidence.)

There is no `index.all` successor. Fences and sketches are small enough
to be **owned, not borrowed**: at open, the reader copies each
`fences.bin`/`sketch.bin` into an anonymous 2 MiB-aligned allocation
advised with `MADV_HUGEPAGE`, then closes the fd. V4's single-file mmap
machinery (in-file 2 MiB alignment padding, per-partition offsets, the
concat build step, `MADV_POPULATE_READ` prewarm) existed because 84 GB
had to stay page-cache-backed and evictable; a structure the design
requires to be resident anyway gains nothing from file backing.

This buys portability: anonymous THP works on every Linux kernel, while
file-backed THP for `fences.all` would depend on
`CONFIG_READ_ONLY_THP_FOR_FS` (disabled on Ubuntu kernels, being removed
upstream) or filesystem large-folio support. At 10B keys the fence
arrays (~2.5 GB) fit entirely in ~2000 × 2 MiB L2 TLB entries: 100%
static TLB coverage, vs 5% for the V4 index.

The trade, stated plainly: owned memory forfeits kernel evictability.
Page-cache-backed fences degrade to slow under memory pressure; an
anonymous allocation can only OOM. Since prewarm semantics were always
"these bytes are resident, period," loud failure is the intended
behavior — but it is a behavioral difference from V4.

Hot-swap is unchanged in shape and slightly cleaner: the old
generation's fence memory is plain RAM with no tie to the old mount;
only `blocks.bin`/`heap.bin` fds pin it for `MNT_DETACH`. The load-time
copy is also the natural home for derived search structures (the S+
tree in Open questions) without any on-disk format change.

## meta.json

```json
{
  "format_version": 5,
  "hash_algorithm": "xxhash64",
  "verify_seed": 5888146898842593941,
  "block_size": 4096,
  "inline_threshold": 2048,
  "sketch": { "kind": "binary_fuse8" },
  "partitions": [
    { "n_blocks": 9830400 }
  ],
  "stats": { ... },
  "encoding": { ... }
}
```

- `block_size`: power-of-two multiple of 4 KiB. One block = one `pread`.
  **Auto-tuned by the loader** (see below); `--block-size` overrides.
- `inline_threshold`: values ≤ threshold are stored inline in a block;
  larger values go to `heap.bin` behind a stub. Default `block_size / 2`,
  which bounds per-block padding waste and puts the median record inline
  under the auto-tune rule.

### Block size auto-tuning

The loader sees every record during scatter, so nobody has to guess:

```
block_size = clamp(next_pow2(2 × median_record_size), 4 KiB, 64 KiB)
```

- The `2×` factor makes `inline_threshold = block_size / 2` clear the
  median, so the median lookup is 1 `pread`.
- The 4 KiB floor is the physical I/O minimum (page cache granularity,
  NVMe sector): smaller blocks cannot reduce I/O, and 4 KiB is the
  zero-read-amplification point where one lookup = one page = one I/O.
- The 64 KiB cap bounds read amplification and per-hot-key page-cache
  footprint; past it, a stubbed value (2 preads) is cheaper than the
  bandwidth. Blocks ≥ 16 KiB should also carry the u16 offset footer
  (see Open questions) to keep the in-block scan sub-linear.

Typical current datasets (100–256 B values) resolve to 4 KiB; a
multi-KiB-median dataset resolves to 8–16 KiB without operator input.
Larger blocks quarter the fence array and inline fatter values; smaller
blocks maximize distinct hot keys resident per GB of page cache. The
reader takes `block_size` from `meta.json` and is indifferent to the
choice. `--block-size` remains as an explicit override for benchmarking
and for distributions the median heuristic serves poorly (e.g. strongly
bimodal sizes).
- `sketch`: `null` when the filter is disabled.
- Per partition: `n_blocks`, for validation against
  `len(fences.bin) / 4` — derivable, kept explicit to catch truncation.
  No offsets: each partition's fence array and sketch are whole files.

## blocks.bin

Records sorted by full 64-bit fingerprint, packed into `block_size` blocks.
**No record straddles a block boundary**: when the next record does not fit,
the block is zero-padded and the record starts the next block. Every inline
hit is therefore exactly one `pread`.

The last 4 bytes of every block are a **checksum**: `xxhash64` of bytes
`[0, block_size - 4)` truncated to 32 bits. Usable record capacity is
`block_size - 4`. The reader verifies it after every block `pread`, before
scanning — the scan walks the block by trusting each record's `length`
field, so a corrupted length would otherwise silently derail it. A
mismatch is an error (surfaced as `result=error`, never a miss). Cost:
~0.1% disk, one hash of the block per lookup.

Record (8-byte aligned within the block):

```
[8B verify_fingerprint] [4B length|flags] [payload] [pad to 8B]
```

- `verify_fingerprint`: `xxhash64(key, verify_seed)`, biased 0 → 1 so that
  0 remains the block-padding sentinel (the scanner stops at vfp == 0 or
  end of block).
- `length|flags`: low 31 bits = payload byte length; top bit = out-of-line.
- Inline record: payload = value bytes (covered by the block checksum).
- Stub record (out-of-line): payload = `[8B heap_offset] [4B value_checksum]`,
  and `length` holds the value's true byte length. The value lives at
  `heap_offset` in the partition's `heap.bin`; `value_checksum`
  (`xxhash64` of the value, truncated to 32 bits) is verified after the
  heap `pread`, since heap bytes are not covered by any block checksum.
  The stub is 24 bytes — the checksum occupies what was alignment padding.

The V4 64-byte value alignment is dropped — it only existed so the index
offset could be a `u32` in 64 B units. This removes ~32 B average padding
per record; block-boundary padding (≈ half the average record size per
block, ~3% at 256 B records) takes its place. Net disk usage is slightly
lower than V4, and the 256 GB-per-partition offset cap disappears.

## fences.bin

Per partition: `n_blocks × u32`, where `fence[i]` = high 32 bits of the
first record's fingerprint in block `i`. Sorted by construction. Loaded
into an owned, 2 MiB-aligned, `MADV_HUGEPAGE`-advised allocation at open;
the fd is closed immediately after the copy.

Fences are non-decreasing, not strictly increasing: all records sharing
`h = high32(fp)` form one contiguous run in the sorted partition, and a
run crossing a block boundary makes the next block's fence repeat `h`.
The run may also *start* mid-block, so the block before the first
`fence == h` is always a candidate.

Lookup: let `lo = partition_point(fence, f < h)` (index of the first
fence ≥ `h`):

- if `lo == 0` and `fence[0] > h`: miss, zero I/O;
- otherwise the candidates are every block `i ≥ lo` with `fence[i] == h`
  (ascending), then block `lo − 1` (if `lo > 0`) **last**. Return on the
  first vfp match.

Probe order matters: about `1/B` of present keys (B = records/block,
~6% at B = 16) share their block head's high32, making `fence[lo] == h`
— and for essentially all of them the key is in block `lo`, not
`lo − 1`. Probing `lo − 1` first would waste a `pread` on every such
lookup (~1.06 expected preads/hit instead of ~1.0001). The backward
block holds the key only on a true backward straddle, at the tie rate
quantified below, so it is probed last. When no fence equals `h` (the
overwhelmingly common case), `lo − 1` is the only candidate.

In the common case no fence equals `h` and this degenerates to the
single block `lo − 1`: one binary search, one `pread`. The equal-fence
peek is a forward linear scan from `lo`, not a second binary search; it
examines exactly as many entries as there are blocks the lookup must
read anyway.

Tie rate: partition routing consumes the *low* bits of `fp` and fences
the *high* 32, so only same-partition keys can collide in fence space —
the tie rate scales with keys per partition, not total keys. With `n`
keys per partition and `B` records per block, the number of other
same-partition keys sharing a given key's high32 is Poisson with
`λ = n / 2^32`, giving

```
P(lookup needs a 2nd candidate block) ≈ 1 − e^(−λ/B) ≈ n / (2^32 × B)
```

(verified by simulation). At `B ≈ 16` (256 B records, 4 KiB blocks),
by total keys and partition count N:

| Keys | N = 64 | N = 256 | N = 1024 | N = 4096 |
|------|--------|---------|----------|----------|
| 500M | 0.011% | 0.003%  | 0.0007%  | 0.0002%  |
| 1B   | 0.023% | 0.006%  | 0.0014%  | 0.0004%  |
| 10B  | 0.23%  | 0.057%  | 0.014%   | 0.0036%  |

Three or more candidate blocks require more than `B` keys sharing one
high32 value inside one partition — Poisson tail ~10⁻⁴² per fence value
at 10B keys / 1024 partitions, expected count zero for every realistic
configuration. (The ~3 full-64-bit birthday collisions expected at 10B
keys land in one partition by construction but form 2-record runs,
inside the 2-block candidate set.) "Worst case 2 `pread`s" therefore
holds in practice as a probabilistic statement, not an invariant: the
reader must handle arbitrary run lengths, and the budget table cites
the formula. A wider `fence_width` (40/64 bits via meta) buys nothing
at these scales; 32 bits suffices once the per-partition collision
domain is accounted for.

Ties are a correctness obligation, not a performance one: at 10B keys /
1024 partitions, ~1.4M present keys live in boundary-straddling runs,
and a reader that skips the backward or equal-fence candidates
false-misses every one of them. Such runs will essentially never arise
in a random test corpus of feasible size, so the conformance suite must
construct them deliberately (see Implementation order).

Sizing: fences cost `32 bits × block_size⁻¹ × avg_record_size` per key —
they scale with **data bytes**, not key count:

```
fence_bytes = blocks.bin bytes / block_size × 4
```

## sketch.bin

Optional per-partition filter over the full 64-bit key fingerprints,
queried before the fence search. Loaded and owned like `fences.bin`.
Immutable build-once filters fit the static-snapshot model.

Selection criteria, in the order they actually discriminate:

1. **False-positive rate ε** — every false positive is one wasted
   50–100 µs `pread`; expected miss cost = ε reads.
2. **Bits/key** — resident RAM, 1 bit/key = 1.25 GB at 10B keys.
3. **Probe cost (cachelines)** — see below; a tiebreaker only.

Candidates, measured at 10M keys/partition (10B keys / 1024) on the
static-filter bench (`xorf` 0.12 for fuse; 64 B cache-line-blocked
Bloom):

| Kind | Bits/key | ε (measured) | b/key ÷ log₂(1/ε) | Lines/probe |
|------|----------|--------------|--------------------|-------------|
| blocked Bloom k=3 | 4.0 | 14.8% | 1.45 | 1 |
| blocked Bloom k=4 | 6.0 | 5.8% | 1.46 | 1 |
| blocked Bloom k=5 | 8.0 | 2.3% | 1.47 | 1 |
| binary fuse 8 | 9.0 | 0.39% | 1.13 | 3, independent |
| binary fuse 16 | 18.0 | 0.0014% | 1.12 | 3, independent |
| ribbon/BuRR r=5 | ~5.1 | 3.1% | ~1.01 | ~2, adjacent |
| cuckoo f=8 | 8.4 | ~3% | ~1.7 | 2, independent |

The middle column is the honest comparison: bits spent per bit of
rejection power (information-theoretic optimum 1.0). Fuse filters beat
Bloom by ~30% per bit; ribbon (BuRR) is near-optimal but its Rust
implementations are immature; cuckoo is dominated (worse ratio than
Bloom, plus a build that can fail and needs eviction loops — its
strengths are deletion and incremental insert, which a build-once
snapshot never uses).

**Probe cost is a non-criterion at this design's scale.** The worst
candidate touches 3 cachelines, and fuse's 3 loads are independent, so
they overlap into ~1 DRAM latency (~100 ns; measured 8–19 ns/probe
while cache-warm). The probe defends a 50–100 µs `pread` — three
orders of magnitude — and at 1M probes/s even 3 lines/probe is
~200 MB/s of DRAM traffic against >100 GB/s available. Cachelines
would discriminate only in a CPU-bound batched (MGET) path, and even
there prefetch pipelining hides the difference.

**Default: `binary_fuse8`.** Versus bloom-4 it spends 2.3× the bits to
cut wasted preads 38× (ε 14.8% → 0.39%); versus bloom-8 it spends
+1 bit/key for 6×. At 0.39%, a fully miss-dominated node wastes one
pread per 256 lookups — the sketch is no longer a factor in I/O
provisioning, whereas at bloom-4's 15% it still is. Small-partition
overhead is negligible (9.18 b/key at 500k keys vs 9.00 at 40M), so
the per-partition split costs nothing. `blocked_bloom` (4–6 b/key,
k=3–4) remains in `meta.sketch.kind` for the RAM-capped 10B tier,
where fuse-8's 11.3 GB breaks the ≤8 GB budget and bloom-4's 5 GB
fits.

Implementation notes:

- `xorf::BinaryFuse8` fits the owned-load model exactly: its
  `DmaSerializable` trait emits a fixed-size descriptor + the raw
  fingerprint array (that pair *is* the `sketch.bin` layout), and
  `BinaryFuse8Ref::from_dma(descriptor, fingerprints)` queries
  zero-copy over the 2 MiB-aligned owned allocation.
- Fuse construction fails on duplicate keys. The builder feeds fps in
  sorted order (phase 2 buckets ascend), so dedup is a free
  adjacent-equal skip — required anyway, since the ~3 expected 64-bit
  fp collisions at 10B keys would otherwise abort the build. Build
  cost is ~0.4 s and ~24 B/key transient RAM per 10M-key partition,
  inside the phase-2 `sketch_builder` budget.
- Binary fuse 16 is not worth a kind: 9 more bits/key to go from
  1-in-256 to 1-in-71000 wasted preads buys nothing observable.
- If the 10B tier materializes and bloom-4's 15% ε hurts, the upgrade
  path is a BuRR-style ribbon filter (ε=2⁻ʳ at ~0.1–0.5% space
  overhead — e.g. 3.1% at ~5.0 b/key, 6.25% at ~4.0), which strictly
  dominates Bloom at equal bits; RocksDB's Standard128Ribbon measures
  ~30% space savings over its Bloom at equal FPR, for 3–4× the build
  CPU (~140 ns/key). That is a new `sketch.kind`, not a format change;
  the Rust implementations are pre-production (`ribbon-filter` 0.2,
  `pleat` 0.1, neither implements bumping) — vet one or port RocksDB's
  C++ when the need is real, not before. (ZOR filters, arXiv
  2602.03525, promise near-BuRR space with fuse-like queries but are
  prototype-only as of early 2026.)

Size by miss traffic: hit-dominated workloads can disable the sketch
entirely; miss-heavy workloads want binary-fuse-8.

## Read path

```
lookup(key):
  fp        = xxhash64(key, 0)          // bias 0 → 1
  partition = fp & (N - 1)

  if sketch enabled and !sketch[partition].contains(fp):
    return NOT_FOUND                     // 0 preads

  h  = high32(fp)
  lo = partition_point(fences[partition], f < h)     // RAM only
  if lo == 0 and fences[partition][0] > h:
    return NOT_FOUND                                 // 0 preads
  candidates = [i for i in lo.. while fence[i] == h]
             ++ [lo - 1 if lo > 0]     // backward block last (see fences.bin)
  for block in candidates:
    buf = pread(blocks_fd[partition], block_size, block * block_size)
    verify_block_checksum(buf) or return ERROR
    for record in scan(buf):             // walk headers, stop at vfp == 0
      if record.vfp == xxhash64(key, verify_seed):
        if record.inline:
          return record.payload          // 1 pread total
        else:
          value = pread(heap_fd[partition], record.length,
                        record.heap_offset)           // 2 preads total
          verify_value_checksum(value, record) or return ERROR
          return value
  return NOT_FOUND                       // sketch fp or true miss: 1-2 preads
```

I/O budget:

| Case | preads |
|------|--------|
| Inline hit (the bulk) | 1 |
| Fence-tie hit (rate ≈ n/(2³²·B), see fences.bin) | ≤ 2 in practice |
| Out-of-line hit | 2 |
| Miss, sketch negative | 0 |
| Miss, sketch false positive or sketch disabled | 1 (full candidate set on a fence tie) |

Metrics mapping: `result=miss` stays "0 preads paid" only when the sketch
rejects; a block scanned without a vfp match is the V5 analog of
`collision` (paid I/O, no value). The `mcf_request_duration_seconds`
labels carry over unchanged.

## Construction

Records must end up fingerprint-sorted, so the index phase becomes a
two-pass distribution (radix) sort. Sorting is where the transient
sub-partitioning lives: the top bits of the fingerprint split each
partition into buckets small enough to sort in RAM, and because bucket
order is sort order, concatenating the sorted buckets *is* the sorted
partition — no merge step exists.

### Phase 1 — Scatter (arrival order)

As today, route each record to partition `fp & (N-1)` and append
`(fp, vfp, value)` to a transient per-partition `arrival.bin` in arrival
order. Each writer also maintains a record-size histogram (log2 buckets
suffice for a power-of-two decision). Write `scatter.done` — including
the merged histogram — when all sources drain.

`spill.bin` disappears: the sort key travels with the record.

Between phases, derive `block_size` once, globally, from the merged
histogram via the auto-tune rule (unless `--block-size` is given), and
`inline_threshold = block_size / 2`. It must be decided before the first
block is cut and is immutable for the snapshot.

### Phase 2 — Radix + sort + build

Per partition (rayon pool of `--index-parallelism`, as in the mmap index
build plan):

1. One streaming pass over `arrival.bin`, appending each record to one of
   S transient bucket files by the top `log2(S)` bits of `fp` (buffered
   sequential writes). S is chosen so a bucket ≈ 128–256 MiB; fingerprints
   are uniform, so bucket sizes are Poisson-tight (σ/µ ≈ 0.1% at 10B keys)
   — no skew handling needed.
2. For each bucket in ascending order: load into RAM, `sort_unstable` by
   `fp`, then stream out `blocks.bin` blocks — cutting at
   `block_size - 4`, sealing each block with its checksum, diverting
   values > `inline_threshold` to `heap.bin` behind a checksummed stub,
   and emitting one fence per block. Delete the bucket file.
3. Feed every `fp` to the sketch builder (binary fuse needs the full key
   set; it gets it across the bucket passes).
4. Write the accumulated fences to `fences.bin` and the finalized filter
   to `sketch.bin`; delete `arrival.bin`; write per-partition done marker.

Peak RAM ≈ `index_parallelism × (bucket_size + sketch_builder)` by
construction, independent of partition and key count. Both passes read and
write sequentially. Total I/O volume is two passes over the data — the
same as any external sort — but reorganized into embarrassingly parallel
in-RAM sorts with no merge machinery.

### Phase 3 — Finalize

Write `meta.json` last. There is no concat step: `fences.bin` and
`sketch.bin` are written per partition during phase 2, alongside
`blocks.bin` and `heap.bin`.

Crash recovery: the recovery unit is the partition. On restart, a
partition with a done marker is skipped; one with `arrival.bin` still
present is rebuilt from scratch (stale bucket files and partial outputs
are swept first — the radix pass and block build are deterministic, so
rebuilding is idempotent). `meta.json`'s presence remains the snapshot
completeness sentinel, as in V4.

## Size estimates

At 256 B average record (typical for current datasets), ~16 records/block:

| Keys | Fences | + Bloom 4b/key | + Fuse 8b/key | V4 index |
|------|--------|----------------|----------------|----------|
| 500M | 125 MB | 375 MB | ~690 MB | 4.2 GB |
| 1B   | 250 MB | 750 MB | ~1.4 GB | 8.4 GB |
| 10B  | 2.5 GB | **7.5 GB** | ~13.8 GB | 84 GB |

Fences + 4-bit Bloom meets the ≤8 GB @ 10B target (11×); fences alone are
34× smaller than V4. Larger average records raise fence cost
proportionally (fences track bytes); at 1 KiB averages, quadruple the fence
column or double `block_size`.

## What V5 gives up vs V4

1. **Misses are no longer free by construction.** V4 misses resolve in the
   RAM index with zero syscalls. V5 needs the sketch for that, and a sketch
   false positive still pays one `pread`. Hit-heavy workloads can skip the
   sketch; miss-heavy ones size it accordingly.
2. **Build does a full sorted rewrite of the data.** External merge sort is
   sequential I/O but roughly doubles scatter-phase write volume (runs +
   final blocks) versus V4's append-once scatter.
3. **CPU per lookup**: a ~16-record header walk after the `pread` replaces
   the SIMD bucket probe. Sub-microsecond against a 50–100 µs NVMe read;
   the SIMD probe machinery (`probe_group`, AVX2/NEON paths) is retired.

## Alternatives considered

- **Hash pages (no index at all).** Route `fp mod n_blocks` directly, no
  fences. Rejected: controlling overflow needs ~25% empty headroom per
  block — ~600 GB of slack at 10B keys / 2.5 TiB data — versus 2.5 GB of
  fences for a dense file. Sorted-dense dominates once any resident index
  is allowed.
- **MPHF + Elias-Fano offsets.** PTHash (~3 bits/key) plus EF-compressed
  offsets (~4–5 bits/key) lands in the same budget with guaranteed
  single-`pread` hits, but with far more moving parts and a fragile build.
  Revisit only if the block scan ever shows up in profiles.
- **Per-record offsets in the index.** See Core idea — refuted by the
  4 KiB I/O floor.
- **Records spanning block boundaries** (instead of padding + heap): works
  (tail fetched by a second `pread`, as V4 does for >4 KiB values) but
  makes "inline hit = exactly 1 pread" conditional and lets fence cost grow
  with byte volume when large values dominate. The stub/heap split keeps
  the indexed region key-dense.
- **Sorted-run scatter + K-way merge** (external merge sort): identical
  two-pass I/O volume, but runs partition by *arrival time*, forcing a
  K-way merge with per-run read buffers; radix buckets partition by *key
  space*, so concatenation suffices. A sort-the-spill-then-gather variant
  was also rejected: gathering issues one random read per record against
  the arrival-order file.

## Open questions

- Auto-tune heuristic robustness: the 2× median rule mishandles strongly
  bimodal size distributions (median tiny, heavy tail of mid-size values
  all stubbed). Is p75 a better anchor? Decide from real dataset
  histograms; `--block-size` covers the gap meanwhile.
- Store the full 64-bit `fp` in each record header (+8 B/record) to allow
  early-exit in the block scan and exact fence ties? Likely unnecessary;
  measure first.
- Per-block `u16` offset footer for binary search within large blocks:
  only relevant if `block_size` ≥ 16 KiB; ~1% disk, zero RAM.
- Sketch default: the *kind* question is settled (binary-fuse-8;
  blocked-bloom only for the RAM-capped 10B tier — see sketch.bin).
  Whether `--sketch` is on or off by default depends on fleet-wide miss
  rate; ship off + the flag, decide after production numbers.
- `heap_offset` width: `u64` is simple; `u40` in 64 B units would shave
  3 B/stub if stub density ever matters.
- Radix bucket sizing: number of top-of-fingerprint bits (equivalently,
  target bucket bytes, default 128–256 MiB) as a build flag alongside
  `--index-parallelism`; the product of the two bounds peak build RAM.
- Fence search structure: v1 ships flat `binary_search` (~23 dependent
  cache misses over a ~39 MB per-partition slice at 10B keys, ~1–2 µs —
  material only against page-cache-hot preads). If profiles justify it,
  build a static S+ tree (SIMD B+ tree over u32 keys, per
  curiouscoding.nl/posts/static-search-tree/) at **load time**, while
  copying `fences.bin` into the owned allocation:
  the flat sorted fence array is exactly the leaf layer, so the
  on-disk format never changes, and the derived internal layers cost
  ~1/16 of the fence bytes (~160 MB at 10B keys) for ~10× single-query
  and ~40× batched (MGET) search throughput. Hash-uniform fences are the
  structure's best case.

## Implementation order

**Prerequisite: `FORMAT_INTERFACE.md`** — the multi-format read/write
seam (Snapshot facade, `GetOutcome::Miss { io }`, `FormatBuilder`,
conformance harness) lands first as a V4-only refactor. V5 then arrives
as a `v5/` module + `FormatId::V5` arm + conformance registration, with
no server changes.

1. **mcfreeze-format**: block/record encode + scan, fence build + search,
   unit tests (roundtrip, boundary padding, fence ties, stub/heap, empty
   partition, corrupted block and heap bytes → error not miss). Fence-tie
   coverage must use deliberately constructed keys — an equal-high32 run
   straddling one block boundary, a run spanning ≥ 2 full blocks, and an
   absent key sharing the run's high32 (forces the full candidate scan
   to a clean miss). At realistic λ these configurations never occur in
   a feasible random corpus, which is exactly how the original
   last-≤-plus-next lookup spec would have shipped a false-miss bug.
2. **mcfreeze-loader**: arrival-order scatter, radix-bucket sort + block
   build, per-partition output files, done-marker recovery; delete spill
   machinery.
3. **Reader**: `v5::SnapshotReader` (fence/sketch load into owned
   2 MiB-aligned allocations, block scan, heap fd) behind the
   `Snapshot` facade's `V5` arm.
4. **Sketch**: builder + query behind `meta.sketch`.
5. **CLI + metrics**: block-size auto-tune from the scatter histogram,
   build flags (`--block-size` override, `--sketch`), metric label
   mapping.
6. **Benchmark vs V4** on gh-files + a synthetic 1B-key snapshot: p50/p99
   lookup latency, preads/lookup by outcome, resident RAM, build wall time.
