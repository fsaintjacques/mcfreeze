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
    builder_for, index::fingerprint, meta::Layout, meta::DEFAULT_VERIFY_SEED, BuilderConfig,
    FormatId, GetOutcome, Snapshot,
};
use tempfile::TempDir;

/// Build a snapshot via the production write path.
fn build_snapshot(format: FormatId, pairs: &[(&[u8], &[u8])], n_partitions: u32) -> TempDir {
    let dir = TempDir::new().unwrap();
    let builder = builder_for(
        format,
        BuilderConfig {
            root: dir.path().to_path_buf(),
            n_partitions,
            verify_seed: DEFAULT_VERIFY_SEED,
            data_buf_bytes: 1024 * 1024,
            spill_buf_bytes: 4096,
            v5: Default::default(),
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

/// Run the whole behavioral spec for one format.
fn conformance(format: FormatId) {
    // --- roundtrip across partitions ---
    let pairs: &[(&[u8], &[u8])] = &[
        (b"hello", b"world"),
        (b"foo", b"bar"),
        (b"alpha", b"beta gamma delta"),
    ];
    let dir = build_snapshot(format, pairs, 4);
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
    let dir = build_snapshot(format, &[(b"empty", b""), (b"big", &big)], 1);
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
    let dir = build_snapshot(format, &pairs, 1);
    let snap = Snapshot::open_path(dir.path()).unwrap();
    for (k, v) in vals.iter().step_by(97) {
        assert_hit(&snap, k, v);
    }

    // --- cost contract: Miss { io: false } touches no data file ---
    // Truncating every data file after open makes any pread fail loudly.
    // Any miss reported as free must therefore reproduce identically on
    // truncated data — that proves zero data I/O, for every format.
    let dir = build_snapshot(format, &[(b"present", b"yes")], 2);
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
        conformance(format);
    }
}
