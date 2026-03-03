# Artifact Registry for Docker images

resource "google_artifact_registry_repository" "data_extraction" {
  location      = var.region
  repository_id = "data-extraction"
  format        = "DOCKER"
  description   = "Docker images for PV Prospect data extraction pipeline"
}
