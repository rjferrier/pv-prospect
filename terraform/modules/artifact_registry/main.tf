# Artifact Registry for Docker images

resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = var.repository_id
  format        = "DOCKER"
  description   = "Docker images for PV Prospect ${var.repository_id} pipeline"
}
