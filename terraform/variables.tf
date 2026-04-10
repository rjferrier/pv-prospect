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

variable "image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag for the data extraction container"
}

variable "transformer_image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag for the data transformation container"
}

variable "scheduler_cron" {
  type        = string
  default     = "0 3 * * *"
  description = "Cron schedule for daily extraction (default: 03:00 UTC)"
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

variable "versioner_image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag for the data versioner container"
}

variable "versioner_scheduler_cron" {
  type        = string
  default     = "0 6 * * 1"
  description = "Cron schedule for weekly versioning (default: Monday 06:00 UTC)"
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
