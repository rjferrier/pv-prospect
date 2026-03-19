# Main Terraform configuration
# Provider and general settings

terraform {
  backend "gcs" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.12.0"
    }
  }
}

provider "google" {
  project = var.project_id
}

# ---------------------------------------------------------------------------
# Required GCP APIs
# ---------------------------------------------------------------------------

resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "workflows.googleapis.com",
    "cloudscheduler.googleapis.com",
    "secretmanager.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# ---------------------------------------------------------------------------
# Shared service account for pipeline execution
# ---------------------------------------------------------------------------

resource "google_service_account" "pipeline" {
  account_id   = "data-extraction-pipeline"
  display_name = "Data Extraction Pipeline"
  description  = "Used by Cloud Run Jobs, Workflows, and Scheduler"
}

# The pipeline SA needs to read/write GCS objects in the staging buckets
resource "google_storage_bucket_iam_member" "pipeline_staged_raw" {
  bucket = module.storage.staged_raw_data_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_staged_model" {
  bucket = module.storage.staged_model_data_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# The pipeline SA needs to run Cloud Run Jobs
resource "google_project_iam_member" "pipeline_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# The pipeline SA needs to execute workflows
resource "google_project_iam_member" "pipeline_workflows_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# The Workflow needs to be able to trigger Cloud Run Job executions
resource "google_project_iam_member" "pipeline_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# The Cloud Run Job needs to read secrets (API keys) from Secret Manager
resource "google_project_iam_member" "pipeline_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

module "storage" {
  source        = "./modules/storage"
  bucket_prefix = var.bucket_prefix
  region        = var.region
}

module "cloud_run" {
  source = "./modules/cloud_run"
  region = var.region

  project_id            = var.project_id
  image_url             = "${var.region}-docker.pkg.dev/${var.project_id}/data-extraction/data-extraction"
  image_tag             = var.image_tag
  gcs_bucket            = module.storage.staged_raw_data_bucket_name
  service_account_email = google_service_account.pipeline.email
  secret_env_vars       = var.secret_env_vars

  depends_on = [google_project_service.apis]
}

module "artifact_registry_transformer" {
  source = "./modules/artifact_registry_transformer"
  region = var.region
}

module "cloud_run_transformer" {
  source = "./modules/cloud_run_transformer"
  region = var.region

  project_id = var.project_id
  # Temporary placeholder since the real image hasn't been built yet
  image_url             = "${var.region}-docker.pkg.dev/${var.project_id}/data-extraction/data-extraction"
  image_tag             = var.transformer_image_tag
  raw_data_bucket       = module.storage.staged_raw_data_bucket_name
  model_data_bucket     = module.storage.staged_model_data_bucket_name
  service_account_email = google_service_account.pipeline.email

  depends_on = [google_project_service.apis, module.artifact_registry_transformer]
}

module "workflows" {
  source = "./modules/workflows"
  region = var.region

  service_account_email      = google_service_account.pipeline.email
  cloud_run_job_name         = module.cloud_run.job_name
  default_data_sources       = var.default_data_sources
  default_pv_system_ids      = var.default_pv_system_ids
  default_by_week            = var.default_by_week

  depends_on = [google_project_service.apis, module.cloud_run]
}

module "workflows_transform" {
  source = "./modules/workflows_transform"
  region = var.region

  service_account_email = google_service_account.pipeline.email
  cloud_run_job_name    = module.cloud_run_transformer.job_name
  default_pv_system_ids = var.default_pv_system_ids

  depends_on = [google_project_service.apis, module.cloud_run_transformer]
}

module "scheduler" {
  source = "./modules/scheduler"
  region = var.region

  workflow_id           = module.workflows.workflow_id
  service_account_email = google_service_account.pipeline.email
  schedule              = var.scheduler_cron

  depends_on = [google_project_service.apis, module.workflows]
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "service_account_email" {
  value       = module.storage.service_account_email
  description = "DVC service account email"
}

output "pipeline_service_account_email" {
  value       = google_service_account.pipeline.email
  description = "Pipeline service account email"
}

output "staged_raw_data_bucket_name" {
  value       = module.storage.staged_raw_data_bucket_name
  description = "Name of the staged raw data storage bucket"
}

output "staged_model_data_bucket_name" {
  value       = module.storage.staged_model_data_bucket_name
  description = "Name of the staged model data storage bucket"
}

output "artifact_registry_url" {
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/data-extraction"
  description = "Docker registry URL"
}

output "artifact_registry_transformer_url" {
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/data-transformation"
  description = "Docker registry URL for data transformation"
}

output "cloud_run_job_name" {
  value       = module.cloud_run.job_name
  description = "Cloud Run Job name"
}

output "workflow_name" {
  value       = module.workflows.workflow_name
  description = "Workflow name"
}

output "scheduler_job_name" {
  value       = module.scheduler.scheduler_job_name
  description = "Cloud Scheduler job name"
}
