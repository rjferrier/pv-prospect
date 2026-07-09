# Cloud Storage resources for data management and DVC
#
# - staging: single bucket with data/ and tracking/ top-level prefixes
#       data/      — resources/, cleaned/, prepared-batches/, prepared/, served/
#       tracking/  — manifests/, cursors/, ledger/, logs/
# - raw: dedicated durable archive of raw extracted CSVs (see archive-raw-data).
#       Extraction writes here and transform reads here (via the shared
#       staged_raw_data_storage config key); a lifecycle rule tiers each object
#       Standard -> Coldline at age 8 days, in place. Not DVC-versioned: raw is
#       append-only + non-refetchable, so it needs durability, not git-tag
#       reconstructability.
# - versioned-feature: corpus of model-ready feature data tracked by DVC
# - versioned-model: trained model artifacts tracked by DVC (lineage role)
#     + plain promoted/{pv,weather}/ serving path read by pv-prospect-app

# Staging. The cleaned/ prefix is intra-run working data: the clean step writes
# it and the prepare step reads it back within the same run, so nothing depends
# on it surviving across runs. It is expired here rather than swept by the
# data-versioner — deleting it object-by-object could never finish inside that
# job's timeout (see reports/versioner-hang.md). Seven days mirrors the raw
# bucket's margin: enough for pipeline catch-up and backfill re-runs.
#
# Lifecycle rules delete objects, not HNS folders, so empty folders accumulate
# under data/cleaned/; pv-prospect-etl/scripts/cleanup_empty_folders.py sweeps
# them.
resource "google_storage_bucket" "staging" {
  name                        = "${var.bucket_prefix}-staging"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 7
      matches_prefix = ["data/cleaned/"]
    }
  }
}

# Dedicated raw-data archive. Objects land Standard (hot, transform consumes
# within ~1-2 days) and auto-tier to Coldline after 8 days, in place at the same
# path — a week of Standard margin covers pipeline catch-up before the cold tail
# tiers down. No retention lock: the archival threat model is not a concern, and a
# lock would block any future correction.
resource "google_storage_bucket" "raw" {
  name                        = "${var.bucket_prefix}-raw"
  location                    = var.region
  uniform_bucket_level_access = true

  hierarchical_namespace {
    enabled = true
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = 8
    }
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

resource "google_storage_bucket" "versioned_model" {
  name                        = "${var.bucket_prefix}-versioned-model"
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
resource "google_storage_bucket_iam_member" "dvc_sa_versioned_feature" {
  bucket = google_storage_bucket.versioned_feature.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dvc_sa.email}"
}

resource "google_storage_bucket_iam_member" "dvc_sa_versioned_model" {
  bucket = google_storage_bucket.versioned_model.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dvc_sa.email}"
}
