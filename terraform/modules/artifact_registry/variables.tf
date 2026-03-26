variable "region" {
  type        = string
  description = "GCP region for the registry"
}

variable "repository_id" {
  type        = string
  description = "Artifact Registry repository ID (e.g. 'data-extraction', 'data-transformation')"
}
