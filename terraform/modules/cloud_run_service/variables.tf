variable "service_name" {
  type        = string
  description = "Cloud Run Service name"
}

variable "region" {
  type        = string
  description = "GCP region for the Cloud Run Service"
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
  description = "vCPU limit"
}

variable "memory" {
  type        = string
  default     = "512Mi"
  description = "Memory limit"
}

variable "min_instance_count" {
  type        = number
  default     = 0
  description = "Minimum number of instances (0 = scale-to-zero)"
}

variable "max_instance_count" {
  type        = number
  default     = 2
  description = "Maximum number of instances"
}

variable "env_vars" {
  type        = map(string)
  default     = {}
  description = "Environment variables"
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
  description = "Service account email for Cloud Run Service execution"
}

variable "allow_unauthenticated" {
  type        = bool
  default     = false
  description = "Allow unauthenticated (public) access. false = IAM auth required."
}
