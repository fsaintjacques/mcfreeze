# GCS bucket for e2e test artifacts (protobuf descriptors, etc.).

resource "google_storage_bucket" "e2e_artifacts" {
  name     = "${var.project}-mcfreeze-e2e"
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 7 # Auto-delete objects after 7 days.
    }
    action {
      type = "Delete"
    }
  }
}

# Upload the compiled stackoverflow descriptor for the GCS descriptor_uri e2e test.
resource "google_storage_bucket_object" "stackoverflow_desc" {
  name   = "descriptors/stackoverflow.desc"
  bucket = google_storage_bucket.e2e_artifacts.name
  source = "${path.module}/proto/stackoverflow.desc"
}

# Grant the builder SA read access to the e2e artifacts bucket so builder
# Jobs can download protobuf descriptors via descriptor_uri.
resource "google_storage_bucket_iam_member" "builder_gcs_viewer" {
  bucket = google_storage_bucket.e2e_artifacts.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.builder.email}"
}
