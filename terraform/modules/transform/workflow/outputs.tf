output "workflow_name" {
  value       = google_workflows_workflow.data_transformation.name
  description = "Workflow name"
}

output "workflow_id" {
  value       = google_workflows_workflow.data_transformation.id
  description = "Workflow fully-qualified ID"
}
