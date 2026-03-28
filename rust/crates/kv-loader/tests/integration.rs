use std::io::Write;

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use rand::{RngCore, SeedableRng, rngs::StdRng};
use tempfile::TempDir;

use kv_format::reader::SnapshotReader;
use kv_loader::{LoaderConfig, SnapshotLoader, source::CsvSource};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn gen_csv(n: usize, seed: u64) -> (TempDir, Vec<(Vec<u8>, Vec<u8>)>) {
    let mut rng  = StdRng::seed_from_u64(seed);
    let dir      = TempDir::new().unwrap();
    let csv_path = dir.path().join("data.csv");
    let mut file = std::fs::File::create(&csv_path).unwrap();

    let mut pairs = Vec::with_capacity(n);
    for i in 0..n {
        let key_len = 8 + (rng.next_u32() % 24) as usize;
        let val_len = 10 + (rng.next_u32() % 190) as usize;

        let mut key = vec![0u8; key_len];
        let mut val = vec![0u8; val_len];
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut val);
        key[..8].copy_from_slice(&(i as u64).to_le_bytes());

        writeln!(file, "{},{}", B64.encode(&key), B64.encode(&val)).unwrap();
        pairs.push((key, val));
    }
    (dir, pairs)
}

fn loader(root: &std::path::Path, n_partitions: u32) -> SnapshotLoader {
    let config = LoaderConfig {
        n_partitions,
        data_buf_bytes:    1024 * 1024,
        spill_buf_bytes:   64 * 1024,
        index_parallelism: 2,
        ..LoaderConfig::default()
    };
    SnapshotLoader::new(root, config).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn roundtrip_small() {
    let n                = 500;
    let (csv_dir, pairs) = gen_csv(n, 42);
    let snap_dir         = TempDir::new().unwrap();

    let stats = loader(snap_dir.path(), 4)
        .load(&mut CsvSource::from_path(csv_dir.path().join("data.csv"), 64).unwrap())
        .await
        .unwrap();

    assert_eq!(stats.n_keys, n as u64);
    assert!(snap_dir.path().join("meta.json").exists());

    let reader = SnapshotReader::open(snap_dir.path()).unwrap();
    for (key, val) in &pairs {
        let got = reader.get(key).unwrap();
        assert_eq!(got.as_deref(), Some(val.as_slice()), "key={key:?}");
    }
    assert_eq!(reader.get(b"definitely-absent").unwrap(), None);
}

#[tokio::test]
async fn roundtrip_large() {
    let n                = 100_000;
    let (csv_dir, pairs) = gen_csv(n, 99);
    let snap_dir         = TempDir::new().unwrap();

    loader(snap_dir.path(), 64)
        .load(&mut CsvSource::from_path(csv_dir.path().join("data.csv"), 1000).unwrap())
        .await
        .unwrap();

    let reader = SnapshotReader::open(snap_dir.path()).unwrap();
    for (key, val) in &pairs {
        assert_eq!(reader.get(key).unwrap().as_deref(), Some(val.as_slice()));
    }
}

#[tokio::test]
async fn empty_source() {
    let snap_dir = TempDir::new().unwrap();
    let stats    = loader(snap_dir.path(), 4)
        .load(&mut CsvSource::new(b"".as_slice(), 100))
        .await
        .unwrap();

    assert_eq!(stats.n_keys, 0);
    assert!(snap_dir.path().join("meta.json").exists());

    let reader = SnapshotReader::open(snap_dir.path()).unwrap();
    assert_eq!(reader.get(b"anything").unwrap(), None);
}

#[tokio::test]
async fn stats_are_accurate() {
    let n                = 1_000;
    let (csv_dir, pairs) = gen_csv(n, 7);
    let snap_dir         = TempDir::new().unwrap();

    let stats = loader(snap_dir.path(), 4)
        .load(&mut CsvSource::from_path(csv_dir.path().join("data.csv"), 100).unwrap())
        .await
        .unwrap();

    assert_eq!(stats.n_keys, n as u64);
    let expected_bytes: u64 = pairs.iter().map(|(_, v)| v.len() as u64).sum();
    assert_eq!(stats.data_bytes, expected_bytes);
}

#[tokio::test]
async fn meta_json_written_last_and_valid() {
    let (csv_dir, _) = gen_csv(10, 1);
    let snap_dir     = TempDir::new().unwrap();

    loader(snap_dir.path(), 4)
        .load(&mut CsvSource::from_path(csv_dir.path().join("data.csv"), 10).unwrap())
        .await
        .unwrap();

    let raw: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(snap_dir.path().join("meta.json")).unwrap()
    ).unwrap();

    assert_eq!(raw["format_version"], 1);
    assert_eq!(raw["n_partitions"],   4);
    assert_eq!(raw["n_keys"],         10);
    assert_eq!(raw["hash_algorithm"], "xxhash64");
}

#[tokio::test]
async fn spill_files_absent_after_load() {
    let (csv_dir, _) = gen_csv(50, 3);
    let snap_dir     = TempDir::new().unwrap();

    loader(snap_dir.path(), 4)
        .load(&mut CsvSource::from_path(csv_dir.path().join("data.csv"), 10).unwrap())
        .await
        .unwrap();

    for entry in std::fs::read_dir(snap_dir.path()).unwrap() {
        let part = entry.unwrap().path();
        if part.is_dir() {
            let spill = part.join("spill.bin");
            assert!(!spill.exists(), "spill.bin left behind: {spill:?}");
        }
    }
}

#[tokio::test]
async fn progress_callback_fires() {
    use std::sync::{Arc, Mutex};

    let (csv_dir, _) = gen_csv(300_000, 5);
    let snap_dir     = TempDir::new().unwrap();

    let calls  = Arc::new(Mutex::new(Vec::<(u64, u64)>::new()));
    let calls2 = calls.clone();

    let config = LoaderConfig {
        n_partitions:      4,
        data_buf_bytes:    1024 * 1024,
        spill_buf_bytes:   64 * 1024,
        index_parallelism: 2,
        progress_interval: 100_000,
        progress_fn: Some(Arc::new(move |n, b| {
            calls2.lock().unwrap().push((n, b));
        })),
    };
    SnapshotLoader::new(snap_dir.path(), config).unwrap()
        .load(&mut CsvSource::from_path(csv_dir.path().join("data.csv"), 1000).unwrap())
        .await
        .unwrap();

    let recorded = calls.lock().unwrap();
    assert!(recorded.len() >= 2, "expected progress callbacks, got {}", recorded.len());
    for w in recorded.windows(2) {
        assert!(w[1].0 > w[0].0);
    }
}
