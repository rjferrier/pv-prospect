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
  region  = var.region
}

# Enable required APIs for bootstrap
resource "google_project_service" "bootstrap_apis" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "storage.googleapis.com"
  ])
  service            = each.value
  disable_on_destroy = false
}

# The State Bucket
resource "google_storage_bucket" "tfstate" {
  name          = "${var.project_id}-tfstate"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

# The Artifact Registry (Moved from main config)
module "artifact_registry" {
  source        = "../modules/artifact_registry"
  region        = var.region
  repository_id = "data-extraction"

  depends_on = [google_project_service.bootstrap_apis]
}
