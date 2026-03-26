variable "job_name" {
  type        = string
  description = "Cloud Run Job name (e.g. 'data-extraction', 'data-transformation')"
}

variable "region" {
  type        = string
  description = "GCP region for the Cloud Run Job"
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

variable "cpu" {
  type        = string
  default     = "1"
  description = "vCPU limit for the container"
}

variable "memory" {
  type        = string
  default     = "512Mi"
  description = "Memory limit for the container"
}

variable "timeout" {
  type        = string
  default     = "600s"
  description = "Maximum task duration"
}

variable "env_vars" {
  type        = map(string)
  description = "Default environment variables (overridden per-execution by the Workflow)"
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

variable "service_account_email" {
  type        = string
  description = "Service account email for Cloud Run Job execution"
}
