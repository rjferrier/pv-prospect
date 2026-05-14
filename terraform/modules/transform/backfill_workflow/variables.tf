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
  description = "GCS staging bucket; the workflow reads the orchestrator manifest from gs://<bucket>/tracking/manifests/<run_date>/<workflow_name>.json (written directly by plan_transform_backfill)"
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

variable "max_extract_runs" {
  type        = number
  default     = 4
  description = "MAX_EXTRACT_RUNS env-var value: how many unconsumed extraction consolidated ledgers one transform-backfill run may consume. Weather-grid extraction emits 2 consolidated ledgers/day and pv-sites 1, so the default of 4 lets a once-daily transform backfill keep pace with weather-grid and burn down a modest pv-sites backlog."
}
