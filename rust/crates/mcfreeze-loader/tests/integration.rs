use std::io::Write;

use rand::{rngs::StdRng, RngCore, SeedableRng};
use tempfile::TempDir;

use mcfreeze_format::reader::SnapshotReader;
use mcfreeze_loader::{CsvSource, LoaderConfig, RawEncodingSource, SnapshotLoader};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type KeyValuePairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Generate a CSV file with `key` and `value` columns (hex-encoded random bytes).
fn gen_csv(n: usize, seed: u64) -> (TempDir, KeyValuePairs) {
    gen_csv_offset(n, seed, 0)
}

fn gen_csv_offset(n: usize, seed: u64, global_offset: usize) -> (TempDir, KeyValuePairs) {
    let mut rng = StdRng::seed_from_u64(seed);
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("data.csv");
    let mut file = std::fs::File::create(&csv_path).unwrap();

    // Write header
    writeln!(file, "key,value").unwrap();

    let mut pairs = Vec::with_capacity(n);
    for i in 0..n {
        let val_len = 10 + (rng.next_u32() % 190) as usize;
        let mut val = vec![0u8; val_len];
        rng.fill_bytes(&mut val);

        // Key is a unique string; value is hex-encoded random bytes.
        let key = format!("key_{:08}", global_offset + i);
        let val_hex = hex::encode(&val);

        writeln!(file, "{key},{val_hex}").unwrap();

        // Store as the string bytes that Arrow will read (Utf8 columns).
        pairs.push((key.into_bytes(), val_hex.into_bytes()));
    }
    (dir, pairs)
}

fn loader(root: &std::path::Path, n_partitions: u32) -> SnapshotLoader {
    let config = LoaderConfig {
        n_partitions,
        data_buf_bytes: 1024 * 1024,
        spill_buf_bytes: 64 * 1024,
        index_parallelism: 2,
        ..LoaderConfig::default()
    };
    SnapshotLoader::new(root, config).unwrap()
}

fn open_raw_source(
    csv_path: &std::path::Path,
    batch_size: usize,
) -> RawEncodingSource<CsvSource<std::fs::File>> {
    let csv = CsvSource::from_path(csv_path, batch_size).unwrap();
    // key=column 0, value=column 1
    RawEncodingSource::new(csv, 0, 1)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_source() {
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let snap_dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let csv = CsvSource::with_schema(b"key,value\n".as_slice(), schema, 100).unwrap();
    let mut source = RawEncodingSource::new(csv, 0, 1);

    let stats = loader(snap_dir.path(), 4).load(&mut source).await.unwrap();

    assert_eq!(stats.n_keys, 0);
    assert!(snap_dir.path().join("meta.json").exists());

    let reader = SnapshotReader::open(snap_dir.path()).unwrap();
    assert_eq!(reader.get(b"anything").unwrap(), None);
}

#[tokio::test]
async fn roundtrip_small() {
    let n = 500;
    let (csv_dir, pairs) = gen_csv(n, 42);
    let snap_dir = TempDir::new().unwrap();

    let stats = loader(snap_dir.path(), 4)
        .load(&mut open_raw_source(&csv_dir.path().join("data.csv"), 64))
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
    let n = 100_000;
    let (csv_dir, pairs) = gen_csv(n, 99);
    let snap_dir = TempDir::new().unwrap();

    loader(snap_dir.path(), 64)
        .load(&mut open_raw_source(&csv_dir.path().join("data.csv"), 1000))
        .await
        .unwrap();

    let reader = SnapshotReader::open(snap_dir.path()).unwrap();
    for (key, val) in &pairs {
        assert_eq!(reader.get(key).unwrap().as_deref(), Some(val.as_slice()));
    }
}

#[tokio::test]
async fn stats_are_accurate() {
    let n = 1_000;
    let (csv_dir, pairs) = gen_csv(n, 7);
    let snap_dir = TempDir::new().unwrap();

    let stats = loader(snap_dir.path(), 4)
        .load(&mut open_raw_source(&csv_dir.path().join("data.csv"), 100))
        .await
        .unwrap();

    assert_eq!(stats.n_keys, n as u64);
    let expected_bytes: u64 = pairs.iter().map(|(_, v)| v.len() as u64).sum();
    assert_eq!(stats.data_bytes, expected_bytes);
}

#[tokio::test]
async fn meta_json_written_last_and_valid() {
    let (csv_dir, _) = gen_csv(10, 1);
    let snap_dir = TempDir::new().unwrap();

    loader(snap_dir.path(), 4)
        .load(&mut open_raw_source(&csv_dir.path().join("data.csv"), 10))
        .await
        .unwrap();

    let raw: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(snap_dir.path().join("meta.json")).unwrap())
            .unwrap();

    assert_eq!(raw["format_version"], 4);
    assert_eq!(raw["hash_algorithm"], "xxhash64");
    assert_eq!(raw["partitions"].as_array().unwrap().len(), 4);
    assert_eq!(raw["stats"]["n_keys"], 10);

    assert!(
        raw["stats"]["scatter"].is_object(),
        "scatter must be embedded in meta.json"
    );
    assert!(
        raw["stats"]["index"].is_object(),
        "index must be embedded in meta.json"
    );
    assert!(
        !snap_dir.path().join("scatter.done").exists(),
        "scatter.done must be deleted"
    );
    assert!(
        !snap_dir.path().join("index.done").exists(),
        "index.done must be deleted"
    );
}

#[tokio::test]
async fn spill_files_absent_after_load() {
    let (csv_dir, _) = gen_csv(50, 3);
    let snap_dir = TempDir::new().unwrap();

    loader(snap_dir.path(), 4)
        .load(&mut open_raw_source(&csv_dir.path().join("data.csv"), 10))
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
async fn load_parallel_roundtrip() {
    let n_streams = 4usize;
    let n_per_stream = 2_000usize;
    let snap_dir = TempDir::new().unwrap();

    let mut all_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut sources = Vec::new();

    for s in 0..n_streams {
        let global_offset = s * n_per_stream;
        let (csv_dir, pairs) = gen_csv_offset(n_per_stream, s as u64 * 13 + 7, global_offset);
        all_pairs.extend(pairs);
        sources.push(open_raw_source(&csv_dir.path().join("data.csv"), 200));
        std::mem::forget(csv_dir);
    }

    let config = LoaderConfig {
        n_partitions: 16,
        data_buf_bytes: 1024 * 1024,
        spill_buf_bytes: 64 * 1024,
        channel_capacity: 8,
        index_parallelism: 2,
        ..LoaderConfig::default()
    };
    let stats = SnapshotLoader::new(snap_dir.path(), config)
        .unwrap()
        .load_parallel(sources)
        .await
        .unwrap();

    assert_eq!(stats.n_keys, (n_streams * n_per_stream) as u64);

    let reader = SnapshotReader::open(snap_dir.path()).unwrap();
    for (key, val) in &all_pairs {
        assert_eq!(reader.get(key).unwrap().as_deref(), Some(val.as_slice()));
    }
}

#[tokio::test]
async fn progress_callback_fires() {
    use std::sync::{Arc, Mutex};

    let (csv_dir, _) = gen_csv(300_000, 5);
    let snap_dir = TempDir::new().unwrap();

    let calls = Arc::new(Mutex::new(Vec::<(u64, u64)>::new()));
    let calls2 = calls.clone();

    let config = LoaderConfig {
        n_partitions: 4,
        data_buf_bytes: 1024 * 1024,
        spill_buf_bytes: 64 * 1024,
        channel_capacity: 8,
        index_parallelism: 2,
        progress_interval: 100_000,
        progress_fn: Some(Arc::new(move |n, b| {
            calls2.lock().unwrap().push((n, b));
        })),
    };
    SnapshotLoader::new(snap_dir.path(), config)
        .unwrap()
        .load(&mut open_raw_source(&csv_dir.path().join("data.csv"), 1000))
        .await
        .unwrap();

    let recorded = calls.lock().unwrap();
    assert!(
        recorded.len() >= 2,
        "expected progress callbacks, got {}",
        recorded.len()
    );
    for (n, b) in recorded.iter() {
        assert!(*n > 0, "delta keys must be positive");
        assert!(*b > 0, "delta bytes must be positive");
    }
}
