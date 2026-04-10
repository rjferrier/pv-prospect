# Cloud Scheduler job that triggers the versioning workflow weekly

resource "google_cloud_scheduler_job" "weekly_versioning" {
  name        = "pv-prospect-weekly-version"
  region      = var.region
  schedule    = var.schedule
  time_zone   = var.time_zone
  description = "Triggers PV Prospect data versioning workflow weekly"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/${var.workflow_id}/executions"

    body = base64encode(jsonencode({
      argument = jsonencode({})
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
