terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.12.0"
    }
  }
}

provider "google" {
  project = var.project_id
}

resource "google_storage_bucket" "pv_prospect_data" {
  name          = var.bucket_name
  location      = "europe-west2"
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }
}

resource "google_service_account" "dvc_sa" {
  account_id   = "dvc-sa"
  display_name = "DVC SA"
}

resource "google_storage_bucket_iam_member" "dvc_sa_bucket" {
  bucket = var.bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dvc_sa.email}"
}

output "service_account_email" {
  value       = google_service_account.dvc_sa.email
  description = "Use this service account for application authentication."
}
