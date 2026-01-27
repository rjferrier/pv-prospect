output "service_account_email" {
  value       = google_service_account.dvc_sa.email
  description = "Service account email for DVC authentication"
}

output "bucket_name" {
  value       = google_storage_bucket.pv_prospect_data.name
  description = "Name of the created storage bucket"
}

output "bucket_url" {
  value       = google_storage_bucket.pv_prospect_data.url
  description = "URL of the storage bucket"
}

