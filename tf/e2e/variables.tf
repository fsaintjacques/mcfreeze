variable "project" {
  description = "GCP project ID."
  type        = string
  default     = "bright-modem-406701"
}

variable "region" {
  description = "GCP region for Artifact Registry and BigQuery."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for the GKE cluster (single-zone to minimize cost)."
  type        = string
  default     = "us-central1-a"
}

variable "cluster_name" {
  description = "Name of the GKE cluster."
  type        = string
  default     = "frostmap-e2e"
}

variable "node_count" {
  description = "Number of nodes in the default node pool."
  type        = number
  default     = 1
}

variable "machine_type" {
  description = "Machine type for the GKE nodes."
  type        = string
  default     = "c4a-standard-8"
}

variable "namespace" {
  description = "Kubernetes namespace where frostmap is deployed."
  type        = string
  default     = "frostmap-system"
}
