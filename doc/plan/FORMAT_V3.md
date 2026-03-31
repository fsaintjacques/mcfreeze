# Format V3: Compact Index

## Goal

Redesign the on-disk format for efficient serving at scale (1B+ keys).
This is a clean break — no backward compatibility with V2.

## On-disk layout

### Snapshot directory

```
<snapshot>/
  meta.json               format parameters, partition count, key stats
  index.all               unified index: all partitions concatenated, 2MB-aligned
  data/
    part-0/data.bin        value data (64B-aligned values with 12B header each)
    part-1/data.bin
    ...
```

### meta.json

```json
{
  "format_version": 3,
  "n_partitions": 64,
  "n_keys": 1000000000,
  "hash_algorithm": "xxhash64",
  "bucket_size": 8,
  "fill_rate": 0.95,
  "value_alignment": 64,
  "verify_seed": 42,
  "index_offsets": [0, 134217728, ...],
  "index_n_buckets": [16777216, 15728640, ...]
}
```

- `bucket_size`: always 8
- `verify_seed`: seed for the value header verification hash (must be != 0)
- `index_offsets`: byte offset of each partition's buckets in `index.all`
  (each is 2MB-aligned for huge pages)
- `index_n_buckets`: bucket count per partition (sized to each partition's
  actual key count, not uniform)

### Compact bucket (8 bytes)

```rust
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct CompactBucket {
    fingerprint: u32,  // xxhash64(key, 0) truncated to 32 bits
    offset:      u32,  // byte offset in data.bin, in 64B units
}
```

- `fingerprint == 0` marks an empty bucket (hash result 0 is biased to 1)
- `offset × 64` = byte position in `data/<part>/data.bin`
- 32-bit offset at 64B alignment → 256GB per partition
- Robin Hood insertion with `MAX_PSL = 255`, `FILL_RATE = 0.95`
- 8 buckets per 64B cache line → one SIMD compare per cache line

### Value header (12 bytes, prepended to each value in data.bin)

```
[8B verify_fingerprint] [4B byte_length] [value bytes...] [padding to 64B]
```

- `verify_fingerprint`: `xxhash64(key, verify_seed)` — independent of the
  index fingerprint (different seed)
- `byte_length`: exact value size in bytes (no limit)
- Padding to next 64B boundary after `12 + byte_length`

### Read path

1. Compute `partition = xxhash64(key, 0) % n_partitions`
2. Compute `bucket_fp = xxhash64(key, 0) as u32` (truncate, bias 0→1)
3. Probe `index.all` at `partition × stride + bucket_index × 8`
   - Fingerprint mismatch or empty → miss (no disk I/O)
   - Match → read `offset × 64` from `data/<part>/data.bin`
4. `pread` one 4KB page from the data file (kernel page-aligns the I/O)
5. Parse header: `verify_fp (8B) + byte_length (4B)`
6. Check `xxhash64(query_key, verify_seed) == verify_fp` → mismatch = miss
7. If `byte_length ≤ 4084` → value is in the first page, done
8. Else → `pread` remaining pages

Typical 1–4KB values complete in one page read. The index probe is pure
RAM (mmapped with huge pages). A fingerprint miss avoids disk entirely.

### Unified index file (index.all)

All partition indexes packed into a single contiguous file:

```
[partition 0 buckets, padded to stride]
[partition 1 buckets, padded to stride]
...
[partition N-1 buckets, padded to stride]
```

No file header — the reader looks up offsets from `meta.json`:

```
partition_offset = index_offsets[partition_index]
n_buckets        = index_n_buckets[partition_index]
```

Each partition is sized to its own key count: `ceil(n_keys[i] / FILL_RATE)`.
No wasted buckets for partitions that received fewer keys. Each partition
starts at a 2MB boundary for huge page alignment.

The server mmaps the entire file once with `madvise(MADV_HUGEPAGE)`.

### Build process

1. **Scatter phase** (parallel): hash keys, write key-value pairs to
   per-partition data files with 12-byte value headers, 64B-aligned.
   Count keys per partition.

2. **Index phase** (parallel via rayon):
   - Per partition: `n_buckets[i] = ceil(n_keys[i] / FILL_RATE)`
   - Compute offsets: `offset[i] = align_up(offset[i-1] + n_buckets[i-1] × 8, 2MB)`
   - `fallocate(index.all, offset[N-1] + align_up(n_buckets[N-1] × 8, 2MB))`
   - Each rayon thread builds its partition's hash table in memory,
     then `memcpy` into `index.all` at `offset[i]`
   - On PSL overflow (effectively never at 0.95 fill rate, max PSL ~63
     for 156M keys): discard `index.all`, grow the overflowing partition
     to 1.5× buckets, recompute offsets, and rebuild the entire index
     file from scratch (scatter output is still on disk)

3. **Finalize**: write `meta.json`

## Size estimates

### Index

| Keys | Index size | Fits on node (128GB RAM) |
|------|-----------|--------------------------|
| 500M | 4.2 GB | Yes |
| 1B | 8.4 GB | Yes |
| 5B | 42 GB | Yes |
| 10B | 84 GB | Yes |
| 100B | 842 GB | Shard across 4+ nodes |

### Capacity per partition (32-bit offset, 64B alignment)

| Partitions | Per-partition max | Total max |
|------------|-------------------|-----------|
| 64 | 256 GB | 16 TB |
| 128 | 256 GB | 32 TB |
| 256 | 256 GB | 64 TB |

### Huge page TLB coverage (2000 L2 TLB entries × 2MB)

| Index size | Coverage |
|------------|----------|
| 4.2 GB | 95% |
| 8.4 GB | 48% |
| 42 GB | 10% |
| 84 GB | 5% |

Even at 5% static coverage, temporal locality from skewed access patterns
keeps effective TLB hit rate much higher.

## SIMD probe (V3)

With 8-byte buckets, the SIMD probe compares 32-bit fingerprints:

| ISA | Register | Buckets/load | Compare |
|-----|----------|-------------|---------|
| AVX-512 | 512-bit | 8 (one cache line) | `vpcmpeqd` 16 lanes, mask odd |
| AVX2 | 256-bit | 4 (half cache line) | `vpcmpeqd` 8 lanes, mask odd |
| NEON | 128-bit | 2 | `vceqq_u32` |
| Scalar | — | 1 | `==` |

The `offset` field occupies the other 32 bits of each `u64`. The compare
masks odd lanes (fingerprints are at even 32-bit positions within each
bucket's `u64`).

## madvise hints

| File | Hint | Reason |
|------|------|--------|
| `index.all` | `MADV_HUGEPAGE` | Random access, hot working set, TLB pressure |
| `data/*.bin` | `MADV_RANDOM` | Random access, disable read-ahead to avoid wasted network I/O |

## Auto-tuning partition count

Given estimated total data size, compute:

```
min_partitions = ceil(estimated_total_data / 256GB)
partitions     = next_power_of_two(max(min_partitions, 64))
```

For BigQuery source: use session metadata (`estimated_rows`,
`estimated_bytes`). For CSV from file: two-pass (count then write).
For CSV from stdin: require `--partitions` or default to 64.

Explicit `--partitions` always overrides auto-tuning.

## Implementation order

1. **Value header + verify fingerprint** — changes writer and reader.
   The value header is a prerequisite for removing size from the bucket.

2. **Compact 32/32 bucket** — new `CompactBucket` type, updated SIMD
   probes, new build code.

3. **Unified index file + huge pages** — pack step after index build,
   single mmap with `MADV_HUGEPAGE`, `MADV_RANDOM` on data files.

4. **Auto-tuning** — CLI-only change, uses source metadata to pick
   partition count.
