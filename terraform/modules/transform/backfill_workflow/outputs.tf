output "workflow_id" {
  value       = google_workflows_workflow.transform_backfill.id
  description = "Fully qualified ID of the transform-backfill workflow"
}

output "workflow_name" {
  value       = google_workflows_workflow.transform_backfill.name
  description = "Name of the transform-backfill workflow"
}
