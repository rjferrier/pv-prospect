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

variable "default_source_descriptors" {
  type        = list(string)
  description = "Default source descriptors for scheduled runs"
  default     = ["openmeteo/quarterhourly", "openmeteo/hourly"]
}

variable "default_pv_system_ids" {
  type        = list(number)
  description = "Default PV system IDs for scheduled runs"
}
