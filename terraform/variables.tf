variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "bucket_name" {
  type        = string
  default     = "pv-prospect-data"
  description = "Name of the GCS bucket for data storage"
}

variable "region" {
  type        = string
  default     = "europe-west2"
  description = "GCP region for all resources"
}

variable "zone" {
  type        = string
  default     = "europe-west2-a"
  description = "GCP zone for compute resources"
}

