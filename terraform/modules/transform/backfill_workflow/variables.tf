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
  description = "Name of the data-transformation Cloud Run Job"
}

variable "staging_bucket_name" {
  type        = string
  description = "GCS staging bucket; the workflow reads the backfill manifest from gs://<bucket>/resources/manifests/..."
}

variable "backfill_scope" {
  type        = string
  description = "BACKFILL_SCOPE env-var value; must be 'pv_sites' or 'weather_grid'"

  validation {
    condition     = contains(["pv_sites", "weather_grid"], var.backfill_scope)
    error_message = "backfill_scope must be 'pv_sites' or 'weather_grid'."
  }
}

variable "workflow_name_suffix" {
  type        = string
  description = "Hyphenated scope suffix used in the workflow name (e.g. 'pv-sites', 'weather-grid')"
}

variable "default_pv_system_ids" {
  type        = list(number)
  default     = []
  description = "PV system IDs to transform. Used only when backfill_scope == 'pv_sites'."
}

variable "default_locations" {
  type        = list(string)
  default     = []
  description = "Grid-point locations (lat,lon strings) to transform. Used only when backfill_scope == 'weather_grid'."
}
