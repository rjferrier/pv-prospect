output "scheduler_job_name" {
  value       = google_cloud_scheduler_job.scheduler.name
  description = "Cloud Scheduler job name"
}
