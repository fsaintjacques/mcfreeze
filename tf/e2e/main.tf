terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Uncomment to persist state in GCS:
  # backend "gcs" {
  #   bucket = "mcfreeze-tf-state"
  #   prefix = "e2e"
  # }
}

provider "google" {
  project = var.project
  region  = var.region
}
