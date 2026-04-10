output "workflow_id" {
  value       = google_workflows_workflow.weather_grid_backfill.id
  description = "Full resource ID of the weather grid backfill workflow"
}

output "workflow_name" {
  value       = google_workflows_workflow.weather_grid_backfill.name
  description = "Weather grid backfill workflow name"
}
