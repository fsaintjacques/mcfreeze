# Deterministic BQ test fixture for e2e builds.

resource "google_bigquery_dataset" "e2e" {
  dataset_id = "mcfreeze_e2e"
  location   = var.region

  # Auto-delete after 7 days of inactivity to limit cost from stale runs.
  default_table_expiration_ms = 7 * 24 * 60 * 60 * 1000
}

resource "google_bigquery_table" "test_kv" {
  dataset_id          = google_bigquery_dataset.e2e.dataset_id
  table_id            = "test_kv"
  deletion_protection = false

  schema = jsonencode([
    { name = "key", type = "STRING", mode = "REQUIRED" },
    { name = "value", type = "BYTES", mode = "REQUIRED" },
  ])
}

# Seed 1000 deterministic rows: key="k-0000" .. "k-0999", value=SHA256(key).
resource "google_bigquery_job" "seed" {
  job_id   = "mcfreeze-e2e-seed-${formatdate("YYYYMMDDhhmmss", timestamp())}"
  location = var.region

  query {
    query              = <<-SQL
      INSERT INTO `${var.project}.${google_bigquery_dataset.e2e.dataset_id}.${google_bigquery_table.test_kv.table_id}` (key, value)
      SELECT
        FORMAT('k-%04d', num) AS key,
        SHA256(FORMAT('k-%04d', num)) AS value
      FROM UNNEST(GENERATE_ARRAY(0, 999)) AS num
    SQL
    use_legacy_sql     = false
    create_disposition = "CREATE_NEVER"
    write_disposition  = "WRITE_TRUNCATE"
  }

  depends_on = [google_bigquery_table.test_kv]

  lifecycle {
    ignore_changes = [job_id]
  }
}
