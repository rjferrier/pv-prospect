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

variable "staging_bucket_name" {
  type        = string
  description = "Name of the GCS staging bucket"
}

variable "default_pv_system_ids" {
  type        = list(number)
  description = "Default PV system IDs for scheduled runs"
}

variable "default_locations" {
  type        = list(string)
  description = "Default lat,lon location strings for location-based weather transformation (e.g. [\"50.49,-3.54\"])"
  default     = []
}
