variable "region" {
  type        = string
  description = "GCP region for the scheduler"
}

variable "schedule" {
  type        = string
  default     = "0 6 * * 1"
  description = "Cron schedule (default: Monday 06:00 UTC)"
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
