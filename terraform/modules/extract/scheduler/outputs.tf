output "scheduler_job_name" {
  value       = google_cloud_scheduler_job.daily_extraction.name
  description = "Cloud Scheduler job name"
}
