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
  description = "Name of the Cloud Run Job to invoke (shared with the main extraction workflow)"
}

variable "staging_bucket_name" {
  type        = string
  description = "Name of the GCS staging bucket; the workflow reads the manifest from gs://<bucket>/tracking/manifests/<run_date>/pv-prospect-extract-weather-grid-backfill.json"
}

variable "checkpoint_object_path" {
  type        = string
  description = "GCS object path (inside the staging bucket) where the workflow persists its per-run resume checkpoint"
  default     = "tracking/checkpoints/weather_grid_backfill.json"
}

variable "sleep_seconds_between_batches" {
  type        = number
  description = "Seconds to sleep between dispatching successive extraction batches. Defaults to 720 (12 minutes), which keeps us safely under OpenMeteo's 5,000/hour limit."
  default     = 720
}

variable "data_source" {
  type        = string
  description = "DATA_SOURCE env var passed to the Cloud Run Job (e.g. 'weather')"
  default     = "weather"
}
