# SPDX-License-Identifier: Apache-2.0
# GCP service accounts and Workload Identity bindings for e2e.

# ---------------------------------------------------------------------------
# GKE node SA — minimal permissions for nodes themselves.
# ---------------------------------------------------------------------------

resource "google_service_account" "gke_nodes" {
  account_id   = "${var.cluster_name}-nodes"
  display_name = "GKE nodes for ${var.cluster_name}"
}

resource "google_project_iam_member" "gke_nodes_log_writer" {
  project = var.project
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_project_iam_member" "gke_nodes_metric_writer" {
  project = var.project
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_project_iam_member" "gke_nodes_ar_reader" {
  project = var.project
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

# ---------------------------------------------------------------------------
# Builder SA — used by mcfreeze build Jobs (BQ read + disk create).
# ---------------------------------------------------------------------------

resource "google_service_account" "builder" {
  account_id   = "${var.cluster_name}-builder"
  display_name = "McFreeze builder for ${var.cluster_name}"
}

resource "google_project_iam_member" "builder_bq_reader" {
  project = var.project
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.builder.email}"
}

resource "google_project_iam_member" "builder_bq_job_user" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.builder.email}"
}

resource "google_project_iam_member" "builder_bq_read_session" {
  project = var.project
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.builder.email}"
}

resource "google_project_iam_member" "builder_disk_admin" {
  project = var.project
  role    = "roles/compute.storageAdmin"
  member  = "serviceAccount:${google_service_account.builder.email}"
}

# K8s SA name must match the Helm release name (default: "mcfreeze") via
# the template {{ include "mcfreeze.builder.name" . }} → "<release>-builder".
resource "google_service_account_iam_member" "builder_wi" {
  service_account_id = google_service_account.builder.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project}.svc.id.goog[${var.namespace}/mcfreeze-builder]"
}

# ---------------------------------------------------------------------------
# Node-agent SA — attaches/detaches disks on each node.
# ---------------------------------------------------------------------------

resource "google_service_account" "node_agent" {
  account_id   = "${var.cluster_name}-node-agent"
  display_name = "McFreeze node-agent for ${var.cluster_name}"
}

resource "google_project_iam_member" "node_agent_disk_admin" {
  project = var.project
  role    = "roles/compute.storageAdmin"
  member  = "serviceAccount:${google_service_account.node_agent.email}"
}

# K8s SA name must match the Helm release name (default: "mcfreeze") via
# the template {{ include "mcfreeze.nodeAgent.name" . }} → "<release>-node-agent".
resource "google_service_account_iam_member" "node_agent_wi" {
  service_account_id = google_service_account.node_agent.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project}.svc.id.goog[${var.namespace}/mcfreeze-node-agent]"
}
