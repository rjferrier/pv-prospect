variable "region" {
  type        = string
  description = "GCP region for the workflow"
}

variable "service_account_email" {
  type        = string
  description = "Service account email for workflow execution"
}

variable "cloud_run_job_name" {
  type        = string
  description = "Name of the Cloud Run Job to invoke"
}

variable "default_data_sources" {
  type        = list(string)
  description = "Default data sources for scheduled runs"
  default     = ["pv", "weather"]
}

variable "default_pv_system_ids" {
  type        = list(number)
  description = "Default PV system IDs for scheduled runs"
}

variable "default_by_week" {
  type        = bool
  description = "Whether to process by week by default"
  default     = false
}
