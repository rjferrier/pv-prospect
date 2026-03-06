output "job_name" {
  value       = google_cloud_run_v2_job.data_transformation.name
  description = "Cloud Run Job name"
}

output "job_id" {
  value       = google_cloud_run_v2_job.data_transformation.id
  description = "Cloud Run Job fully-qualified ID"
}
