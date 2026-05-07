output "workflow_id" {
  value       = google_workflows_workflow.pv_sites_backfill.id
  description = "Full resource ID of the PV-site backfill workflow"
}

output "workflow_name" {
  value       = google_workflows_workflow.pv_sites_backfill.name
  description = "PV-site backfill workflow name"
}
