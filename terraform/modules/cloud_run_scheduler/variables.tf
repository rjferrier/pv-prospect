variable "project_id" {
  type        = string
  description = "GCP project ID hosting the Cloud Run Job"
}

variable "region" {
  type        = string
  description = "GCP region for the scheduler and Cloud Run Job"
}

variable "scheduler_job_name" {
  type        = string
  description = "Name for the Cloud Scheduler job"
}

variable "job_name" {
  type        = string
  description = "Name of the Cloud Run Job to invoke"
}

variable "schedule" {
  type        = string
  description = "Cron schedule"
}

variable "time_zone" {
  type        = string
  default     = "UTC"
  description = "Timezone for the cron schedule"
}

variable "service_account_email" {
  type        = string
  description = "Service account email for Scheduler authentication (needs Cloud Run Invoker on the target job)"
}

variable "env_overrides" {
  type        = map(string)
  default     = {}
  description = "Env-var overrides applied to the Cloud Run Job execution"
}
