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

variable "default_source_descriptors" {
  type        = list(string)
  default     = ["pvoutput", "openmeteo/historical"]
  description = "Source descriptors for the daily scheduled run"
}

variable "default_pv_system_ids" {
  type        = list(number)
  description = "PV system IDs to process in the daily scheduled run"
}

variable "default_by_week" {
  type        = bool
  default     = false
  description = "Whether to process by week by default"
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
