# Cloud Storage resources for DVC/data management

resource "google_storage_bucket" "pv_prospect_data" {
  name                        = var.bucket_name
  location                    = var.region
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


