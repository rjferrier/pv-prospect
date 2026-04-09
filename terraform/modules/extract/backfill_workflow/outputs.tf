output "workflow_id" {
  value       = google_workflows_workflow.backfill.id
  description = "Full resource ID of the backfill workflow"
}

output "workflow_name" {
  value       = google_workflows_workflow.backfill.name
  description = "Backfill workflow name"
}
