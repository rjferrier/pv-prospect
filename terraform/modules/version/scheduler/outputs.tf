output "scheduler_job_name" {
  value       = google_cloud_scheduler_job.weekly_versioning.name
  description = "Cloud Scheduler job name"
}
