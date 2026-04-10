variable "region" {
  type        = string
  description = "GCP region for the scheduler"
}

variable "schedule" {
  type        = string
  default     = "0 3 * * *"
  description = "Cron schedule (default: 03:00 UTC daily)"
}

variable "time_zone" {
  type        = string
  default     = "UTC"
  description = "Timezone for the cron schedule"
}

variable "workflow_id" {
  type        = string
  description = "Fully-qualified ID of the workflow to trigger"
}

variable "service_account_email" {
  type        = string
  description = "Service account email for Scheduler authentication"
}

variable "scheduler_job_name" {
  type        = string
  default     = "pv-prospect-daily-extract"
  description = "Name for the Cloud Scheduler job"
}

variable "argument_json" {
  type        = string
  default     = "{}"
  description = "JSON string containing the arguments to pass to the workflow"
}
