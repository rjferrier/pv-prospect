output "service_account_email" {
  value       = google_service_account.dvc_sa.email
  description = "Service account email for DVC authentication"
}

output "staged_raw_data_bucket_name" {
  value       = google_storage_bucket.staged_raw_data.name
  description = "Name of the staged raw data bucket"
}

output "staged_model_data_bucket_name" {
  value       = google_storage_bucket.staged_model_data.name
  description = "Name of the staged model data bucket"
}

output "versioned_raw_data_bucket_name" {
  value       = google_storage_bucket.versioned_raw_data.name
  description = "Name of the versioned raw data bucket"
}

output "versioned_model_data_bucket_name" {
  value       = google_storage_bucket.versioned_model_data.name
  description = "Name of the versioned model data bucket"
}
