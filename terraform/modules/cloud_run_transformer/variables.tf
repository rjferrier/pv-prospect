variable "region" {
  type        = string
  description = "GCP region for Cloud Run"
}

variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "image_url" {
  type        = string
  description = "Full Artifact Registry image URL (without tag)"
}

variable "image_tag" {
  type        = string
  default     = "latest"
  description = "Docker image tag"
}

variable "raw_data_bucket" {
  type        = string
  description = "GCS bucket name containing staged raw CSV data"
}

variable "model_data_bucket" {
  type        = string
  description = "GCS bucket name for writing output Parquet data"
}

variable "service_account_email" {
  type        = string
  description = "Service account email for Cloud Run Job execution"
}
