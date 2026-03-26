output "job_name" {
  value       = google_cloud_run_v2_job.job.name
  description = "Cloud Run Job name"
}

output "job_id" {
  value       = google_cloud_run_v2_job.job.id
  description = "Cloud Run Job fully-qualified ID"
}
