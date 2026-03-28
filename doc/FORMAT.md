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

---

## Directory Layout

```
<snapshot>/
  meta.json
  data/
    part-00/
      index.idx
      data.bin
    part-01/
      index.idx
      data.bin
    ...
    part-<N-1>/
      index.idx
      data.bin
```

All partition directories live under the `data/` subdirectory of the snapshot
root. Partition directory names are zero-padded to the width of `N-1`
(e.g. `part-00` … `part-63` for N=64).

---

## meta.json

```json
{
  "format_version":  2,
  "n_partitions":    64,
  "hash_algorithm":  "xxhash64",
  "offset_bits":     37,
  "size_bits":       27,
  "n_keys":          1000000000,
  "created_at":      "2026-03-27T00:00:00Z",
  "scatter":         { ... },
  "index":           { ... }
}
```

| Field | Description |
|---|---|
| `format_version` | Increment on incompatible format changes |
| `n_partitions` | Number of partitions N; must be a power of two |
| `hash_algorithm` | Key hash function; currently only `xxhash64` |
| `offset_bits` | Bits allocated to the aligned offset in the `loc` field; currently `37` (max 8 TiB per partition) |
| `size_bits` | Bits allocated to the value size in the `loc` field; currently `27` (max 128 MiB) |
| `n_keys` | Total key count across all partitions |
| `created_at` | ISO 8601 UTC timestamp |
| `scatter` | _(optional)_ Embedded contents of `scatter.done`; opaque to the format layer |
| `index` | _(optional)_ Embedded contents of `index.done`; opaque to the format layer |

`offset_bits + size_bits` must equal 64. There are no spare bits.

---

## index.idx

### Header

The header is 64 bytes (one cache line).

| Offset | Width | Field | Description |
|---|---|---|---|
| 0 | 8 | `magic` | `0x4b5646584944580a` (`"KVFXIDX\n"`) |
| 8 | 2 | `format_version` | Must match `meta.json` |
| 10 | 6 | `_pad` | Reserved, must be zero |
| 16 | 8 | `n_buckets` | Total bucket slots including empty slots |
| 24 | 8 | `n_keys` | Keys stored in this partition |
| 32 | 32 | `_reserved` | Reserved for future use, must be zero |

### Buckets

Buckets immediately follow the 64-byte header. Each bucket is **16 bytes**:

| Bytes | Field | Description |
|---|---|---|
| 0–7 | `fingerprint` | `xxhash64(key)`; zero indicates an empty slot |
| 8–15 | `loc` | Packed: aligned offset and value size |

```
n_buckets = ceil(n_keys / 0.95)
```

Total index size: `64 + n_buckets × 16` bytes.

With 4 buckets per 64-byte cache line, a robin hood probe spanning 3–4 slots
typically touches only 1–2 cache lines.

### `loc` Field Encoding

Bit widths are taken from `meta.json`. Fields are packed from the most significant bit
with no spare bits:

```
 63                          size_bits-1       0
 ┌──────────────────────────┬──────────────────┐
 │      aligned_offset      │       size       │
 │    (offset_bits wide)    │  (size_bits wide) │
 └──────────────────────────┴──────────────────┘
```

**Concrete layout** (`offset_bits`=37, `size_bits`=27):

```
bits 63..27  aligned_offset  (37 bits)  ← in 64-byte units, max 8 TiB
bits 26..0   size            (27 bits)  ← value size in bytes, max 128 MiB
```

Decode:

```
aligned_offset = loc >> size_bits          →  loc >> 27
byte_offset    = aligned_offset << 6       (multiply by 64)
size           = loc & ((1 << size_bits) - 1)
```

### Empty Bucket Sentinel

A bucket with `fingerprint == 0` is empty. During construction, if a key's
`xxhash64` produces `0`, the fingerprint is biased to `1`. The probability of a
natural zero fingerprint is `1/2^64` and is negligible.

---

## data.bin

Values are stored sequentially, each starting at a **64-byte aligned** offset.
Each value occupies `ceil(size / 64) × 64` bytes on disk; only the first `size`
bytes are meaningful. Padding bytes have undefined content.

The byte offset of a value is `aligned_offset << 6`, decoded from the `loc` field
of its index bucket.

Values may cross 4 KiB page boundaries. A `pread(fd, buf, size, offset)` is a
single syscall regardless of alignment; the kernel fetches whichever pages are
needed transparently. The worst case is two 4 KiB page reads instead of one, which
is negligible compared to disk seek latency.

Maximum padding waste per value: 63 bytes (for values of size 1 mod 64). For
typical small values (100–200 bytes) this is under 40% overhead, compared to 97%
with 4 KiB alignment.

---

## Construction

### Step 1 — Partition and Write data.bin

For each key-value pair `(k, v)`:

1. Compute `h = xxhash64(k)`.
2. Select partition: `p = h & (N - 1)`.
3. Write `v` to partition `p`'s `data.bin` at the current write offset, rounded up
   to the next 64-byte boundary.
4. Record `(fingerprint=h, aligned_offset=offset>>6, size=len(v))` for index
   construction.

The write offset advances by `ceil(len(v) / 64) × 64` bytes after each value.

Partitions are independent; all N partition writers can run concurrently.

### Step 2 — Build index.idx

For each partition independently:

1. Allocate `n_buckets = ceil(n_keys / 0.95)` buckets, all zeroed (empty).
2. Allocate a parallel `psl[n_buckets]` array of `u8`, all zeroed. This array
   tracks probe sequence lengths during construction only; it is never written
   to disk.
3. Insert each `(fingerprint, aligned_offset, size)` tuple using Robin Hood insertion:

```
home = fingerprint % n_buckets
pos  = home
psl  = 0

loop:
  if bucket[pos] is empty:
    write (fingerprint, aligned_offset, size) → bucket[pos]
    psl[pos] = psl
    break

  if psl[pos] < psl:                 // robin hood: evict the "rich"
    swap current entry with bucket[pos]
    swap psl[pos] with psl

  pos = (pos + 1) % n_buckets
  psl++
```

4. Discard the `psl` array.
5. Write the 64-byte header followed by the bucket array.

### Step 3 — Write meta.json

Write `meta.json` last, after all partition files are complete and durable. Readers
treat the presence of a valid `meta.json` as the signal that the snapshot is complete.

---

## Reading

### In-Memory Representation

For each partition, the reader maintains:

- A read-only `mmap` of `index.idx` starting at byte 64 (past the header).
- `madvise(MADV_RANDOM)` on the mapped region — index access is random; readahead
  wastes I/O and pollutes the page cache.
- An open file descriptor to `data.bin` for `pread` calls.
- `n_buckets` loaded from the header; `loc` field widths are format constants validated against `meta.json`.

`data.bin` is not memory-mapped. Values are read on demand via `pread`.

### Lookup

```
lookup(key) → value | NOT_FOUND:

  h          = xxhash64(key)
  partition  = h & (N - 1)
  pos        = h % n_buckets[partition]

  loop:
    bucket = index[partition][pos]

    if bucket.fingerprint == 0:           // empty slot
      return NOT_FOUND

    if bucket.fingerprint == h:
      offset = (bucket.loc >> 27) << 6   // decode byte_offset
      size   = bucket.loc & 0x7FFFFFF
      return pread(data_fd[partition], size, offset)

    pos = (pos + 1) % n_buckets[partition]
```

### Multi-Key Lookup (MGET)

For a batch of keys, group lookups by partition after the initial hash, then submit
all `pread` calls across all partitions together. This allows the OS and underlying
storage to service reads concurrently rather than sequentially.

The result order must match the request order; callers should track the original
index of each key through the partition grouping step.
