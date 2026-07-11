<!-- SPDX-License-Identifier: Apache-2.0 -->

# KV Snapshot Format

## Overview

A snapshot is a static, read-only key-value store written to disk. It is split into N
independent partitions. Keys are routed to partitions by hash, enabling parallel
construction and parallel lookup.

Design goals:
- O(1) lookup with at most a handful of sequential cache-line reads
- 64-byte aligned value storage keeps padding overhead small for short values
- Index is memory-mapped; the OS page cache provides hot-key residency with no
  explicit caching logic
- 8-byte buckets pack 8 entries per cache line, enabling SIMD-accelerated probing

---

## Directory Layout

```
<snapshot>/
  meta.json
  index.all
  data/
    part-00/
      data.bin
    part-01/
      data.bin
    ...
    part-<N-1>/
      data.bin
```

All partition directories live under the `data/` subdirectory of the snapshot
root. Partition directory names are zero-padded to the width of `N-1`
(e.g. `part-00` … `part-63` for N=64).

`index.all` is a single binary file containing all partitions' bucket tables,
concatenated with 2 MiB alignment padding between partitions.

---

## Hashing

Three distinct hashes are derived from each key:

| Hash | Function | Purpose |
|---|---|---|
| Full fingerprint | `xxhash64(key, seed=0)` | Partition routing + source for compact fingerprint |
| Compact fingerprint | High 32 bits of the full fingerprint | Stored in index buckets; used for probing |
| Verify fingerprint | `xxhash64(key, verify_seed)` | Stored in `data.bin` header; detects compact collisions |

The compact fingerprint uses the **high** 32 bits of `xxhash64` while partition
routing uses the **low** bits (`fingerprint & (N-1)`). This decouples the two:
if both used low bits, entries within a partition would share low-bit patterns
and collapse onto a subset of home positions.

A fingerprint value of `0` is biased to `1` in both the full and compact forms
to preserve `0` as the empty-bucket sentinel. The probability of a natural zero
is `1/2^64` (full) or `1/2^32` (compact) and is negligible.

The verify seed is a non-zero `u64` stored in `meta.json`. The default is
`xxhash64("mcfreeze-verify", seed=0)` = `0x517cc1b727220a95`. Using a different
seed from the index fingerprint ensures a compact collision does not imply a
verify collision.

---

## meta.json

```json
{
  "format_version":  4,
  "hash_algorithm":  "xxhash64",
  "verify_seed":     5930276444926693013,
  "partitions": [
    { "index_offset": 0,       "index_n_buckets": 1053 },
    { "index_offset": 2097152, "index_n_buckets": 1048 }
  ],
  "stats": {
    "n_keys":      2000000,
    "created_at":  "2026-04-10T00:00:00Z"
  },
  "encoding": null
}
```

| Field | Type | Description |
|---|---|---|
| `format_version` | u32 | Must be `4` |
| `hash_algorithm` | string | Must be `"xxhash64"` |
| `verify_seed` | u64 | Non-zero seed for verify fingerprints in `data.bin` headers |
| `partitions` | array | One entry per partition (see below) |
| `stats` | object | Build statistics; opaque to the reader |
| `encoding` | object? | Encoding metadata (e.g. protobuf descriptor); optional |

Each partition entry:

| Field | Type | Description |
|---|---|---|
| `index_offset` | u64 | Byte offset within `index.all` where this partition's buckets begin |
| `index_n_buckets` | u64 | Number of bucket slots in this partition |

Partition offsets are 2 MiB aligned (except the last partition, which has no
trailing padding). The reader uses these offsets to slice the mmap into
per-partition bucket tables.

`meta.json` is written **last** during construction. Its presence signals that
the snapshot is complete and durable.

---

## index.all

### Layout

All partitions' bucket arrays concatenated into a single file. Each partition's
array starts at the byte offset recorded in `meta.json`. Partitions are padded
to 2 MiB boundaries (for `MADV_HUGEPAGE` on Linux) except the last.

```
[partition 0 buckets] [pad to 2 MiB] [partition 1 buckets] [pad to 2 MiB] ... [partition N-1 buckets]
```

### Buckets

Each bucket is **8 bytes** (`#[repr(C)]`):

| Bytes | Field | Description |
|---|---|---|
| 0–3 | `fingerprint` | Compact fingerprint (high 32 bits of xxhash64); `0` = empty |
| 4–7 | `offset` | Aligned offset in 64-byte units into `data.bin` |

```
n_buckets = ceil(n_keys / 0.95)
```

With 8 buckets per 64-byte cache line, a Robin Hood probe touching 1–2 groups
typically reads only 1–2 cache lines.

The `offset` field stores the value's position in `data.bin` divided by 64.
Maximum addressable size per partition: `(2^32 - 1) × 64` = 256 GiB. The
value `u32::MAX` is reserved as a sentinel and cannot be used as a valid offset.

### Probing

Probing examines 8 consecutive buckets at a time (one cache line = one "group").
For each group, all 8 fingerprints are compared against the target compact
fingerprint in a single operation.

On x86_64 with AVX2: `_mm256_cmpeq_epi32` compares all 8 fingerprints in
parallel using a 256-bit SIMD register. On aarch64: two 128-bit NEON loads
cover the 8 buckets. A scalar fallback handles the remaining platforms and
groups that wrap around the table boundary.

Each group probe returns:
- An array of 8 offsets (valid offset if fingerprint matched, `NO_MATCH` sentinel
  otherwise)
- A `done` flag: `true` if an empty bucket (`fingerprint == 0`) was encountered,
  meaning the key is definitively absent

---

## data.bin

Values are stored sequentially, each starting at a **64-byte aligned** offset.
Every value is preceded by a 12-byte header:

```
 0         8        12                      aligned to 64
 ┌─────────┬────────┬───────────────────────┬───────────┐
 │ verify  │ length │     value bytes       │  padding  │
 │ (u64le) │(u32le) │                       │  (zeros)  │
 └─────────┴────────┴───────────────────────┴───────────┘
```

| Offset | Width | Field | Description |
|---|---|---|---|
| 0 | 8 | `verify_fingerprint` | `xxhash64(key, verify_seed)` |
| 8 | 4 | `value_length` | Byte count of the value payload |
| 12 | variable | value | Raw value bytes |
| 12+len | variable | padding | Zeros to next 64-byte boundary |

On-disk size per value: `ceil((12 + value_length) / 64) × 64` bytes. Maximum
padding waste: 63 bytes. For typical small values (100–200 bytes) this is under
30% overhead.

`data.bin` is **not** memory-mapped. Values are read on demand via `pread(2)`.
A single syscall reads the header plus value regardless of page alignment.

### Verify Fingerprint

On a compact fingerprint match during probing, the reader `pread`s the value's
header and compares its verify fingerprint against `xxhash64(key, verify_seed)`.
A mismatch indicates a compact collision (two different keys sharing the same
32-bit compact fingerprint); probing continues to the next match. A match
confirms the key identity with overwhelming probability (`1 - 1/2^64`).

---

## Construction

### Phase 1 — Scatter: partition and write data.bin

For each key-value pair `(k, v)`:

1. Compute `fp = xxhash64(k, seed=0)`. Bias to 1 if zero.
2. Select partition: `p = fp & (N - 1)`.
3. Compute `verify = xxhash64(k, verify_seed)`.
4. Write to partition `p`'s `data.bin`:
   - 8-byte verify fingerprint (little-endian)
   - 4-byte value length (little-endian)
   - Value bytes
   - Zero padding to 64-byte boundary
5. Record `(compact_fingerprint(fp), aligned_offset)` to partition `p`'s
   `spill.bin` for deferred index construction.

The write offset advances by `ceil((12 + len(v)) / 64) × 64` bytes after each
value. Partitions are independent; all N partition writers run concurrently.

### Phase 2 — Build index.all

For each partition independently:

1. Read all `(fingerprint, offset)` records from `spill.bin`.
2. Allocate `n_buckets = ceil(n_keys / 0.95)` buckets, all zeroed (empty).
3. Insert each record using Robin Hood insertion:

```
home = compact_fingerprint % n_buckets
pos  = home
psl  = 0

loop:
  if bucket[pos] is empty:
    write (fingerprint, offset) → bucket[pos]
    break

  // Robin Hood: if the existing entry is "richer" (lower PSL), displace it
  if existing_psl < psl:
    swap current entry with bucket[pos]
    swap PSL values

  pos = (pos + 1) % n_buckets
  psl++

  if psl == 255:
    // PSL overflow — grow table to 1.5× buckets and retry all entries
```

4. Concatenate all partition tables into `index.all` with 2 MiB alignment
   padding between partitions.

### Phase 3 — Write meta.json

Write `meta.json` last, after `index.all` and all `data.bin` files are complete
and durable. Record each partition's byte offset and bucket count. Delete
`spill.bin` files.

---

## Reading

### In-Memory Representation

The reader maintains:

- A single read-only `mmap` of `index.all`. On Linux, advised with
  `MADV_HUGEPAGE` for TLB efficiency and `MADV_RANDOM` to disable readahead.
- Per-partition metadata: a slice into the mmap (from `index_offset`, spanning
  `index_n_buckets × 8` bytes) and an open file descriptor to `data.bin` for
  `pread` calls.
- The `verify_seed` loaded from `meta.json`.

### Lookup

```
lookup(key) → value | NOT_FOUND:

  fp           = xxhash64(key, seed=0)
  if fp == 0: fp = 1
  partition    = fp & (N - 1)
  compact_fp   = high_32_bits(fp)
  if compact_fp == 0: compact_fp = 1
  pos          = compact_fp % n_buckets[partition]

  loop:
    result = probe_group(buckets[partition], compact_fp, pos)

    for offset in result.offsets where offset != NO_MATCH:
      // pread the 12-byte header (+ value if it fits in 64 bytes)
      header = pread(data_fd[partition], 64, offset * 64)
      verify = u64_le(header[0..8])
      length = u32_le(header[8..12])

      if verify != xxhash64(key, verify_seed):
        continue                          // compact collision, keep probing

      if length <= 52:
        return header[12 .. 12+length]    // value fits in first pread
      else:
        return pread(data_fd[partition], length, offset * 64 + 12)

    if result.done:
      return NOT_FOUND                    // empty bucket reached

    pos = (pos + 8) % n_buckets[partition]
```
