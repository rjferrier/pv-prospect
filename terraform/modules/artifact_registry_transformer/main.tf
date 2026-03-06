# Artifact Registry for Docker images

resource "google_artifact_registry_repository" "data_transformation" {
  location      = var.region
  repository_id = "data-transformation"
  format        = "DOCKER"
  description   = "Docker images for PV Prospect data transformation pipeline"
}
