output "cluster_name" {
  description = "GKE cluster name (pass to gcloud container clusters get-credentials)."
  value       = google_container_cluster.e2e.name
}

output "cluster_location" {
  description = "GKE cluster location (zone)."
  value       = google_container_cluster.e2e.location
}

output "image_repo" {
  description = "Full Artifact Registry image path (without tag)."
  value       = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.frostmap.repository_id}/frostmap"
}

output "bq_table" {
  description = "Fully qualified BQ table for e2e test data."
  value       = "${var.project}.${google_bigquery_dataset.e2e.dataset_id}.${google_bigquery_table.test_kv.table_id}"
}

output "builder_sa" {
  description = "GCP service account email for the builder (annotate K8s SA for WI)."
  value       = google_service_account.builder.email
}

output "node_agent_sa" {
  description = "GCP service account email for the node-agent (annotate K8s SA for WI)."
  value       = google_service_account.node_agent.email
}
