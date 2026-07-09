# Generic Cloud Run Service
#
# min_instances=0 (scale-to-zero), max_instances capped, CPU allocated only
# during requests.  Set allow_unauthenticated=true for a public demo; default
# is IAM-authenticated (private testing).

locals {
  image_url = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_id}/${var.image_name}"
}

# Guard, not a value source: reading this data source fails the *plan* in seconds
# when image_tag has not been built and pushed. Without it Cloud Run accepts the
# revision, retries the pull for tens of minutes, and Terraform eventually reports
# a misleading "Error code 5 ... Image not found" -- by which time it has already
# recorded the unpullable image as the desired state.
data "google_artifact_registry_docker_image" "image" {
  project       = var.project_id
  location      = var.region
  repository_id = var.repository_id
  image_name    = "${var.image_name}:${var.image_tag}"
}

resource "google_cloud_run_v2_service" "service" {
  name                = var.service_name
  location            = var.region
  deletion_protection = false
  ingress             = "INGRESS_TRAFFIC_ALL"

  # Service-wide floor on instances, distinct from the per-revision template.scaling
  # below. Cloud Run always reports this block, so leaving it undeclared makes every
  # plan propose removing it; declare the zero we already want.
  scaling {
    min_instance_count = 0
  }

  template {
    service_account = var.service_account_email

    scaling {
      min_instance_count = var.min_instance_count
      max_instance_count = var.max_instance_count
    }

    containers {
      image = "${local.image_url}:${var.image_tag}"

      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }
        cpu_idle = true
      }

      dynamic "env" {
        for_each = var.env_vars
        content {
          name  = env.key
          value = env.value
        }
      }

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
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}

resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  count    = var.allow_unauthenticated ? 1 : 0
  location = google_cloud_run_v2_service.service.location
  name     = google_cloud_run_v2_service.service.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
