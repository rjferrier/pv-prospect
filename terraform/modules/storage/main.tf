# Cloud Storage resources for data management and DVC
#
# - staging: single bucket with resources/, raw/, cleaned/, prepared/ prefixes
# - versioned-raw: corpus of raw CSV data tracked by DVC
# - versioned-feature: corpus of model-ready Parquet data tracked by DVC

resource "google_storage_bucket" "pv_prospect_data" {
  name                        = "pv-prospect-data"
  location                    = var.region
  uniform_bucket_level_access = true
  hierarchical_namespace {
    enabled = true
  }
}

resource "google_storage_bucket" "staging" {
  name                        = "${var.bucket_prefix}-staging"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }
}

resource "google_storage_bucket" "versioned_raw" {
  name                        = "${var.bucket_prefix}-versioned-raw"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }
}

resource "google_storage_bucket" "versioned_feature" {
  name                        = "${var.bucket_prefix}-versioned-feature"
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
  bucket = google_storage_bucket.versioned_raw.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dvc_sa.email}"
}

resource "google_storage_bucket_iam_member" "dvc_sa_versioned_feature" {
  bucket = google_storage_bucket.versioned_feature.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dvc_sa.email}"
}
