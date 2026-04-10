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
  description = "Name of the Cloud Run Job to invoke (shared with other extraction workflows)"
}

variable "staging_bucket_name" {
  type        = string
  description = "Name of the GCS staging bucket; the workflow reads the PV backfill manifest from gs://<bucket>/<manifest_object_path>"
}

variable "manifest_object_path" {
  type        = string
  description = "GCS object path (inside the staging bucket) where plan_pv_site_backfill writes the manifest"
  default     = "resources/todays_pv_backfill_manifest.json"
}

variable "default_pv_system_ids" {
  type        = list(number)
  description = "Default PV system IDs to backfill"
}

variable "pv_data_source" {
  type        = string
  description = "DATA_SOURCE env var for PV extraction jobs"
  default     = "pv"
}

variable "weather_data_source" {
  type        = string
  description = "DATA_SOURCE env var for weather extraction jobs"
  default     = "weather"
}
