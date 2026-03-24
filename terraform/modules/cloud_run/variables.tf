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

variable "staging_bucket" {
  type        = string
  description = "GCS bucket name for staged data (raw/, cleaned/, prepared/ prefixes)"
}

variable "service_account_email" {
  type        = string
  description = "Service account email for Cloud Run Job execution"
}

variable "log_level" {
  type        = string
  default     = "INFO"
  description = "Python log level for the application"
}

variable "secret_env_vars" {
  type = list(object({
    name      = string
    secret_id = string
    version   = string
  }))
  default     = []
  description = "Environment variables sourced from Secret Manager"
}
