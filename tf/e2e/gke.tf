# Ephemeral GKE cluster for e2e tests.

resource "google_container_cluster" "e2e" {
  name     = var.cluster_name
  location = var.zone

  # We manage the node pool separately to control machine type / count.
  initial_node_count       = 1
  remove_default_node_pool = true

  workload_identity_config {
    workload_pool = "${var.project}.svc.id.goog"
  }

  addons_config {
    gce_persistent_disk_csi_driver_config { enabled = true }
  }

  # E2e cluster is ephemeral; allow terraform destroy.
  deletion_protection = false

  # Speed up creation; disable features we don't need.
  logging_config {
    enable_components = []
  }
  monitoring_config {
    enable_components = []
  }
}

resource "google_container_node_pool" "default" {
  cluster             = google_container_cluster.e2e.id
  name                = "e2e-pool"
  node_count          = var.node_count

  node_config {
    machine_type    = var.machine_type
    spot            = true
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}
