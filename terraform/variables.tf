variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "bucket_prefix" {
  type        = string
  default     = "pv-prospect"
  description = "Prefix for the GCS data buckets"
}

variable "region" {
  type        = string
  default     = "europe-west2"
  description = "GCP region for all resources"
}

variable "extractor_image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag for the data extractor container"
}

variable "transformer_image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag for the data transformation container"
}

variable "extractor_scheduler_cron" {
  type        = string
  default     = "0 2 * * *"
  description = "Cron schedule for daily data extraction (default: 02:00 UTC)"
}

variable "transformer_scheduler_cron" {
  type        = string
  default     = "30 5 * * *"
  description = "Cron schedule for daily data transformation (default: 05:30 UTC, after all extraction runs)"
}

variable "default_pv_model_data_sources" {
  type        = list(string)
  default     = ["pv", "weather"]
  description = "Data sources for the daily scheduled run"
}

variable "default_pv_system_ids" {
  type        = list(number)
  description = "PV system IDs to process in the daily scheduled run"
}

variable "default_locations" {
  type        = list(string)
  default     = []
  description = "Lat,lon location strings for location-based weather extraction/transformation (e.g. [\"50.49,-3.54\"])"
}

variable "default_weather_model_data_sources" {
  type        = list(string)
  default     = ["weather"]
  description = "Data sources used for location-based extraction; must be weather sources only"
}

variable "default_split_by" {
  type        = string
  default     = ""
  description = "Default SPLIT_BY value passed to extract jobs: 'day', 'week', or '' (full range)"
}

variable "default_by_week" {
  type        = bool
  default     = false
  description = "Deprecated — superseded by default_split_by. Retained to avoid warnings from legacy tfvars."
}

variable "extractor_pv_sites_backfill_scheduler_cron" {
  type        = string
  default     = "40 2 * * *"
  description = "Cron schedule for daily PV-site backfill (default: 02:40 UTC, 40 min after main extraction)"
}

variable "extractor_weather_grid_backfill_scheduler_cron" {
  type        = string
  default     = "20 3 * * *"
  description = "Cron schedule for the daily weather grid backfill (default: 03:20 UTC). The workflow runs in a single execution that paces 9 batches with in-batch sleeps (~3 h 24 min wall time under the default sleep_seconds_between_batches=720), so it must start early enough that the transform-side schedule (transformer_weather_grid_backfill_scheduler_cron) reads its consolidated ledger."
}

variable "transformer_pv_sites_backfill_scheduler_cron" {
  type        = string
  default     = "0 6 * * *"
  description = "Cron schedule for daily PV-sites transform backfill (default: 06:00 UTC, 30 min after daily transform)"
}

variable "transformer_weather_grid_backfill_scheduler_cron" {
  type        = string
  default     = "0 8 * * *"
  description = "Cron schedule for daily weather-grid transform backfill (default: 08:00 UTC). Must start after the weather-grid extract finishes and its ledger is consolidated, otherwise the in-container backfill (run_transform_backfill) sees no unconsumed ledgers and exits with no work. Under the default extract schedule (03:20 + ~3h 24min wall) the extract finishes ~06:44 and consolidate adds a few minutes; 08:00 gives ~1h margin."
}

variable "versioner_image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag for the data versioner container"
}

variable "model_trainer_image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag for the model trainer container"
}

variable "app_image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag for the pv-prospect-app container"
}

variable "allow_unauthenticated" {
  type        = bool
  default     = true
  description = "Allow unauthenticated (public) access to the pv-prospect-app Cloud Run Service. true = public demo (default, protected by per-IP rate limiting); false = IAM auth required (private testing)."
}

variable "alert_notification_email" {
  type        = string
  default     = ""
  description = "Email address for monitoring alert notifications. Leave empty to disable email notifications (alerts still fire, visible in Cloud Monitoring UI)."
}

variable "versioner_scheduler_cron" {
  type        = string
  default     = "0 23 * * 0"
  description = "Cron schedule for weekly versioning (default: Sunday 23:00 UTC)"
}

variable "secret_env_vars" {
  type = list(object({
    name      = string
    secret_id = string
    version   = string
  }))
  default     = []
  description = "Environment variables sourced from Secret Manager for Cloud Run Jobs"
}

variable "github_repo" {
  type        = string
  description = "The GitHub repository containing the Actions that will authenticate with GCP. Format: 'owner/repo' (e.g., 'rjferrier/pv-prospect-instance')."
}

variable "backfills_paused" {
  type        = bool
  default     = false
  description = <<-EOT
    Single switch to pause/resume all four historical backfill schedulers (the
    PV-sites and weather-grid extraction backfills and their two transformation
    counterparts). Set true and `terraform apply` to stop them firing — no
    workflow runs, no Cloud Run Job executions, no API cost; cursors freeze in
    place and resume cleanly when set back to false. The daily extraction/
    transformation and weekly versioning schedulers are unaffected. See
    "Pausing the Backfills" in doc/infrastructure.md.
  EOT
}
