output "service_account_email" {
  value       = google_service_account.dvc_sa.email
  description = "Service account email for DVC authentication"
}

output "staging_bucket_name" {
  value       = google_storage_bucket.staging.name
  description = "Name of the staging bucket (raw/, cleaned/, prepared/ prefixes)"
}

output "versioned_raw_data_bucket_name" {
  value       = google_storage_bucket.versioned_raw_data.name
  description = "Name of the versioned raw data bucket"
}

output "versioned_model_data_bucket_name" {
  value       = google_storage_bucket.versioned_model_data.name
  description = "Name of the versioned model data bucket"
}

output "versioned_resources_bucket_name" {
  value       = google_storage_bucket.versioned_resources.name
  description = "Name of the versioned resources bucket"
}
