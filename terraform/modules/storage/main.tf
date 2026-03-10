# Cloud Storage resources for data management and DVC
#
# Replace the single bucket with four purpose-specific buckets:
# - staged-raw-data: captures daily extractions
# - staged-model-data: structured parquet data produced by the Transformer
# - versioned-raw-data: corpus of raw CSV data tracked by DVC
# - versioned-model-data: corpus of model-ready Parquet data tracked by DVC

resource "google_storage_bucket" "pv_prospect_data" {
  name                        = "pv-prospect-data"
  location                    = var.region
  uniform_bucket_level_access = true
  hierarchical_namespace {
    enabled = true
  }
}

resource "google_storage_bucket" "staged_raw_data" {
  name                        = "${var.bucket_prefix}-staged-raw-data"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }
}

resource "google_storage_bucket" "staged_model_data" {
  name                        = "${var.bucket_prefix}-staged-model-data"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }
}

resource "google_storage_bucket" "versioned_raw_data" {
  name                        = "${var.bucket_prefix}-versioned-raw-data"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }
}

resource "google_storage_bucket" "versioned_model_data" {
  name                        = "${var.bucket_prefix}-versioned-model-data"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }
}

resource "google_storage_bucket" "versioned_resources" {
  name                        = "${var.bucket_prefix}-versioned-resources"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }
}

resource "google_service_account" "dvc_sa" {
  account_id   = "dvc-sa-v3"
  display_name = "DVC SA"
}

# The DVC SA only needs write access to the versioned buckets.
resource "google_storage_bucket_iam_member" "dvc_sa_versioned_raw" {
  bucket = google_storage_bucket.versioned_raw_data.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dvc_sa.email}"
}

resource "google_storage_bucket_iam_member" "dvc_sa_versioned_model" {
  bucket = google_storage_bucket.versioned_model_data.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dvc_sa.email}"
}

resource "google_storage_bucket_iam_member" "dvc_sa_versioned_resources" {
  bucket = google_storage_bucket.versioned_resources.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dvc_sa.email}"
}
