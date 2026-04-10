output "workflow_name" {
  value       = google_workflows_workflow.data_versioning.name
  description = "Workflow name"
}

output "workflow_id" {
  value       = google_workflows_workflow.data_versioning.id
  description = "Workflow fully-qualified ID"
}
