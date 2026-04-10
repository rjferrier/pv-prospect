# Cloud Scheduler job that triggers the extraction workflow daily

resource "google_cloud_scheduler_job" "daily_extraction" {
  name        = var.scheduler_job_name
  region      = var.region
  schedule    = var.schedule
  time_zone   = var.time_zone
  description = "Triggers PV Prospect data extraction workflow daily"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/${var.workflow_id}/executions"

    body = base64encode(jsonencode({
      argument = var.argument_json
    }))

    headers = {
      "Content-Type" = "application/json"
    }

    oauth_token {
      service_account_email = var.service_account_email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}
