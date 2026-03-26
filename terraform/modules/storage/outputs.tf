output "service_account_email" {
  value       = google_service_account.dvc_sa.email
  description = "Service account email for DVC authentication"
}

output "staging_bucket_name" {
  value       = google_storage_bucket.staging.name
  description = "Name of the staging bucket (resources/, raw/, cleaned/, prepared/ prefixes)"
}

output "versioned_raw_bucket_name" {
  value       = google_storage_bucket.versioned_raw.name
  description = "Name of the versioned raw data bucket"
}

output "versioned_feature_bucket_name" {
  value       = google_storage_bucket.versioned_feature.name
  description = "Name of the versioned feature data bucket"
}
