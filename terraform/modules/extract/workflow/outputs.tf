output "workflow_name" {
  value       = google_workflows_workflow.data_extraction.name
  description = "Workflow name"
}

output "workflow_id" {
  value       = google_workflows_workflow.data_extraction.id
  description = "Workflow fully-qualified ID"
}
