# Generic Cloud Run Job
#
# The mode-selection env var (e.g. JOB_TYPE, TRANSFORM_STEP) is supplied via
# var.env_vars and overridden per-execution by the calling Workflow.

resource "google_cloud_run_v2_job" "job" {
  name                = var.job_name
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1

    template {
      max_retries = 1
      timeout     = var.timeout

      containers {
        image = "${var.image_url}:${var.image_tag}"

        resources {
          limits = {
            cpu    = var.cpu
            memory = var.memory
          }
        }

        # Default env vars — overridden per-execution by the Workflow
        dynamic "env" {
          for_each = var.env_vars
          content {
            name  = env.key
            value = env.value
          }
        }

        # Secrets from Secret Manager (optional; used by extraction for API keys)
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
}
