# Cloud Scheduler job that invokes a Cloud Run Job directly on a cron
# schedule, with caller-supplied env-var overrides.
#
# This is the no-Workflow path for the transform backfill: the work fits
# in a single Cloud Run Job execution (planning, parallel-threaded
# dispatch, consolidation, marker commit all in-process), so there is no
# orchestration layer to mediate the dispatch.

resource "google_cloud_scheduler_job" "scheduler" {
  name        = var.scheduler_job_name
  region      = var.region
  schedule    = var.schedule
  time_zone   = var.time_zone
  description = "Triggers Cloud Run Job ${var.job_name} on a cron schedule"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${var.job_name}:run"

    body = base64encode(jsonencode({
      overrides = {
        containerOverrides = [
          {
            env = [
              for k, v in var.env_overrides : {
                name  = k
                value = v
              }
            ]
          }
        ]
      }
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
