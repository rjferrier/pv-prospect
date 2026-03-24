# Cloud Run Job for data transformation
#
# A single job definition is used for the four different transformation steps
# — the TRANSFORM_STEP env var selects the mode at execution time.

resource "google_cloud_run_v2_job" "data_transformation" {
  name                = "data-transformation"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1

    template {
      max_retries = 1
      timeout     = "900s"

      containers {
        image = "${var.image_url}:${var.image_tag}"

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }

        # Default env vars — overridden per-execution by the Workflow
        env {
          name  = "TRANSFORM_STEP"
          value = "clean_weather"
        }
        env {
          name  = "STAGING_BUCKET"
          value = var.staging_bucket
        }
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "LOG_LEVEL"
          value = var.log_level
        }

        # Date and PV_SYSTEM_ID are also expected to be overridden
        # by the workflow execution.
      }

      service_account = var.service_account_email
    }
  }
}
