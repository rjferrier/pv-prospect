output "service_url" {
  value       = google_cloud_run_v2_service.service.uri
  description = "URL of the deployed Cloud Run Service"
}

output "service_name" {
  value       = google_cloud_run_v2_service.service.name
  description = "Cloud Run Service name"
}
