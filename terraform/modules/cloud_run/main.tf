# Cloud Run Job for data extraction
#
# A single job definition is used for both "preprocess" and "extract_and_load"
# — the JOB_TYPE env var selects the mode at execution time.

resource "google_cloud_run_v2_job" "data_extraction" {
  name                = "data-extraction"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1

    template {
      max_retries = 1
      timeout     = "600s"

      containers {
        image = "${var.image_url}:${var.image_tag}"

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        # Default env vars — overridden per-execution by the Workflow
        env {
          name  = "JOB_TYPE"
          value = "extract_and_load"
        }
        env {
          name  = "GCS_BUCKET"
          value = var.gcs_bucket
        }
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }

        # API keys from Secret Manager
        dynamic "env" {
          for_each = var.secret_env_vars
          content {
            name = env.value.name
            value_source {
              secret_key_ref {
                secret  = env.value.secret_id
                version = env.value.version
              }
            }
          }
        }
      }

      service_account = var.service_account_email
    }
  }

  lifecycle {
    ignore_changes = [
      # The Workflow overrides these per-execution; don't revert them on apply.
      template[0].template[0].containers[0].env,
    ]
  }
}
