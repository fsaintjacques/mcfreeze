# Container image repository for e2e builds.

resource "google_artifact_registry_repository" "frostmap" {
  repository_id = "frostmap"
  format        = "DOCKER"
  location      = var.region

  cleanup_policies {
    id     = "keep-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }
}
