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
  description = "Name for the Cloud Scheduler job"
}

variable "argument_json" {
  type        = string
  default     = "{}"
  description = "JSON string containing the arguments to pass to the workflow"
}

variable "paused" {
  type        = bool
  default     = null
  description = <<-EOT
    Whether the scheduler job is paused (does not fire). Leave null (the
    default) to leave the job's paused state unmanaged by Terraform — the
    provider's `paused` field is computed, so an out-of-band `gcloud scheduler
    jobs pause/resume` then survives `terraform apply`. Set true/false to make
    the paused state Terraform-managed (an explicit value wins over any manual
    change on the next apply).
  EOT
}
