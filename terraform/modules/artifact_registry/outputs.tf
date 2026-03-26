output "repository_id" {
  value       = google_artifact_registry_repository.repo.repository_id
  description = "Artifact Registry repository ID"
}

output "repository_url" {
  value       = "${google_artifact_registry_repository.repo.location}-docker.pkg.dev/${google_artifact_registry_repository.repo.project}/${google_artifact_registry_repository.repo.repository_id}"
  description = "Full Docker registry URL for pushing/pulling images"
}
