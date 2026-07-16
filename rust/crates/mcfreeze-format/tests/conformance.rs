// SPDX-License-Identifier: Apache-2.0

//! Format conformance suite: the behavioral definition of "is a valid
//! snapshot format" (doc/plan/FORMAT_INTERFACE.md).
//!
//! Every test runs against every `FormatId` through the production write
//! path (`builder_for` → appenders → build → finalize) and the production
//! read path (`Snapshot` facade). A new format is done when this suite
//! passes for it; deleting an old format is safe because nothing else
//! encodes its behavior.

use mcfreeze_format::{
    builder::V5Options, builder_for, index::fingerprint, meta::Layout, meta::DEFAULT_VERIFY_SEED,
    BuilderConfig, FormatId, GetOutcome, Snapshot,
};
use tempfile::TempDir;

use mcfreeze_format::v5::compress::Mode;

fn build_snapshot_with(
    format: FormatId,
    pairs: &[(&[u8], &[u8])],
    n_partitions: u32,
    v5: V5Options,
) -> TempDir {
    let dir = TempDir::new().unwrap();
    let builder = builder_for(
        format,
        BuilderConfig {
            root: dir.path().to_path_buf(),
            n_partitions,
            verify_seed: DEFAULT_VERIFY_SEED,
            data_buf_bytes: 1024 * 1024,
            spill_buf_bytes: 4096,
            v5,
        },
    )
    .unwrap();

    let layout = Layout::new(n_partitions).unwrap();
    let mut apps: Vec<_> = (0..n_partitions as usize)
        .map(|p| builder.appender(p).unwrap())
        .collect();
    for &(k, v) in pairs {
        let fp = fingerprint(k);
        apps[layout.partition_of(fp)].append(k, fp, v).unwrap();
    }
    for a in apps {
        a.finish().unwrap();
    }

    builder.plan().unwrap();
    let done = builder.build(2, None).unwrap();
    let stats = mcfreeze_format::meta::Stats {
        n_keys: done.n_keys,
        created_at: "2026-01-01T00:00:00Z".into(),
        scatter: None,
        index: None,
    };
    builder.finalize(stats, None).unwrap();
    dir
}

fn assert_hit(snap: &Snapshot, key: &[u8], expected: &[u8]) {
    match snap.get(key).unwrap() {
        GetOutcome::Hit(v) => assert_eq!(v, expected, "wrong value for {key:?}"),
        other => panic!("expected Hit for {key:?}, got {other:?}"),
    }
}

/// Run the whole behavioral spec for one format under one V5 option
/// set (the options are inert for other formats). Every case must hold
/// on every axis point — compression in particular is transparent, so
/// no assertion here may depend on the mode.
fn conformance(format: FormatId, v5: &V5Options) {
    let build_snapshot = |pairs: &[(&[u8], &[u8])], n_partitions: u32| {
        build_snapshot_with(format, pairs, n_partitions, v5.clone())
    };

    // --- roundtrip across partitions ---
    let pairs: &[(&[u8], &[u8])] = &[
        (b"hello", b"world"),
        (b"foo", b"bar"),
        (b"alpha", b"beta gamma delta"),
    ];
    let dir = build_snapshot(pairs, 4);
    let snap = Snapshot::open_path(dir.path()).unwrap();
    assert_eq!(snap.desc().format(), format);
    assert_eq!(snap.desc().n_partitions(), 4);
    assert_eq!(snap.desc().stats().unwrap().n_keys, pairs.len() as u64);
    for &(k, v) in pairs {
        assert_hit(&snap, k, v);
    }

    // --- absent key: a miss, with cost honest to the format's contract.
    // Formats promising free misses (expected_miss_io_rate == 0) must
    // report io: false; paid-miss formats may report either (a key can
    // land below a partition's first fence and miss for free).
    match snap.get(b"definitely-absent").unwrap() {
        GetOutcome::Miss { io } => {
            if snap.expected_miss_io_rate() == 0.0 {
                assert!(!io, "free-miss format paid I/O on a miss");
            }
        }
        other => panic!("expected Miss, got {other:?}"),
    }

    // --- empty and large values ---
    let big = vec![0xABu8; 1024 * 1024];
    let dir = build_snapshot(&[(b"empty", b""), (b"big", &big)], 1);
    let snap = Snapshot::open_path(dir.path()).unwrap();
    assert_hit(&snap, b"empty", b"");
    assert_hit(&snap, b"big", &big);

    // --- many keys, single partition ---
    let vals: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000usize)
        .map(|i| {
            (
                format!("key-{i}").into_bytes(),
                format!("v-{i}").into_bytes(),
            )
        })
        .collect();
    let pairs: Vec<(&[u8], &[u8])> = vals.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    let dir = build_snapshot(&pairs, 1);
    let snap = Snapshot::open_path(dir.path()).unwrap();
    for (k, v) in vals.iter().step_by(97) {
        assert_hit(&snap, k, v);
    }

    // --- cost contract: Miss { io: false } touches no data file ---
    // Truncating every data file after open makes any pread fail loudly.
    // Any miss reported as free must therefore reproduce identically on
    // truncated data — that proves zero data I/O, for every format.
    let dir = build_snapshot(&[(b"present", b"yes")], 2);
    let snap = Snapshot::open_path(dir.path()).unwrap();
    let before = snap.get(b"definitely-absent").unwrap();
    let free_miss = match before {
        GetOutcome::Miss { io } => !io,
        other => panic!("expected Miss, got {other:?}"),
    };
    truncate_data_files(dir.path());
    if free_miss {
        match snap.get(b"definitely-absent").unwrap() {
            GetOutcome::Miss { io: false } => {}
            other => panic!("free miss must not touch data files, got {other:?}"),
        }
    } else {
        // A paid miss must fail loudly on truncated data — never
        // silently degrade into a fabricated answer.
        assert!(
            snap.get(b"definitely-absent").is_err(),
            "paid miss on truncated data must error"
        );
    }

    // --- corruption: truncated data is an error, not a miss ---
    match snap.get(b"present") {
        Err(_) => {}
        Ok(o) => panic!("truncated data must error for a present key, got {o:?}"),
    }

    // --- paid-miss cost contract ---
    // Every format in ALL currently answers absent keys without I/O
    // (V4 by construction, V5 via the default-on sketch), so the paid
    // branch above never runs on defaults. Exercise it through the one
    // filter-less configuration: sketchless V5, where nearly every miss
    // scans a block. Paid misses must (a) report io: true and (b) fail
    // loudly on truncated data, never fabricate an answer.
    if format == FormatId::V5 {
        let dir = build_snapshot_with(
            format,
            &[(b"present", b"yes")],
            1,
            V5Options {
                sketch: false,
                ..v5.clone()
            },
        );
        let snap = Snapshot::open_path(dir.path()).unwrap();
        // Find an absent key whose lookup pays I/O (only fingerprints
        // below the partition's first fence miss for free).
        let paid_key = (0..1000)
            .map(|i| format!("absent-{i}"))
            .find(|k| {
                matches!(
                    snap.get(k.as_bytes()).unwrap(),
                    GetOutcome::Miss { io: true }
                )
            })
            .expect("sketchless V5 must produce a paid miss");
        truncate_data_files(dir.path());
        assert!(
            snap.get(paid_key.as_bytes()).is_err(),
            "paid miss on truncated data must error"
        );
    }
}

/// Truncate every regular file under `data/` to zero bytes.
fn truncate_data_files(root: &std::path::Path) {
    fn walk(dir: &std::path::Path) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path);
            } else {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(0)
                    .unwrap();
            }
        }
    }
    walk(&root.join("data"));
}

#[test]
fn all_formats_conform() {
    for &format in FormatId::ALL {
        conformance(format, &V5Options::default());
    }
}

/// The compression axis: every behavioral case must hold identically
/// under each mode — compression is transparent by definition. (The
/// tiny datasets in `conformance` legitimately fall back from
/// `zstd-dict` to plain zstd; the dictionary-exercising cases with a
/// trainable sample pool live below.)
#[test]
fn v5_conforms_under_every_compression_mode() {
    for mode in [Mode::Zstd, Mode::ZstdDict] {
        conformance(
            FormatId::V5,
            &V5Options {
                compression: mode,
                ..Default::default()
            },
        );
    }
}

/// Dictionary-mode specifics that need a sample pool big enough to
/// train: values decode through dict.bin, and a snapshot whose
/// dictionary is corrupt, truncated, or missing refuses to open — a
/// wrong dictionary decompresses cleanly into wrong values, the one
/// corruption value checksums cannot catch.
#[test]
fn v5_zstd_dict_decodes_and_guards_its_dictionary() {
    let vals: Vec<(Vec<u8>, Vec<u8>)> = (0..2000usize)
        .map(|i| {
            (
                format!("key-{i}").into_bytes(),
                format!("record|user-{i}|group-{}|{}", i % 40, "field ".repeat(15)).into_bytes(),
            )
        })
        .collect();
    let pairs: Vec<(&[u8], &[u8])> = vals.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    let dir = build_snapshot_with(
        FormatId::V5,
        &pairs,
        2,
        V5Options {
            compression: Mode::ZstdDict,
            ..Default::default()
        },
    );
    let dict_path = dir.path().join("dict.bin");
    assert!(dict_path.exists(), "2000 samples must train a dictionary");

    let snap = Snapshot::open_path(dir.path()).unwrap();
    for (k, v) in vals.iter().step_by(53) {
        assert_hit(&snap, k, v);
    }
    drop(snap);

    // Truncated: the checksum anchored in meta.json fails the open.
    let dict = std::fs::read(&dict_path).unwrap();
    std::fs::write(&dict_path, &dict[..dict.len() / 2]).unwrap();
    assert!(Snapshot::open_path(dir.path()).is_err());

    // Corrupt (right length, one bit flipped): same.
    let mut bad = dict.clone();
    let mid = bad.len() / 2;
    bad[mid] ^= 0x01;
    std::fs::write(&dict_path, &bad).unwrap();
    assert!(Snapshot::open_path(dir.path()).is_err());

    // Missing: same.
    std::fs::remove_file(&dict_path).unwrap();
    assert!(Snapshot::open_path(dir.path()).is_err());

    // Restored: opens and serves again.
    std::fs::write(&dict_path, &dict).unwrap();
    let snap = Snapshot::open_path(dir.path()).unwrap();
    assert_hit(&snap, &vals[7].0, &vals[7].1);
}
