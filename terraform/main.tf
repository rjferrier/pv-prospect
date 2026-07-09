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
    "iamcredentials.googleapis.com",
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

# The pipeline SA needs to read/write GCS objects in the staging bucket
resource "google_storage_bucket_iam_member" "pipeline_staging" {
  bucket = module.storage.staging_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# The pipeline SA reads/writes objects in the dedicated raw archive bucket:
# extraction writes, transform reads, and the one-time migration deletes from the
# staging source (already covered by the staging grant above).
resource "google_storage_bucket_iam_member" "pipeline_raw" {
  bucket = module.storage.raw_bucket_name
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

# The Workflow needs to be able to write logs
resource "google_project_iam_member" "pipeline_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# ---------------------------------------------------------------------------
# Shared infrastructure
# ---------------------------------------------------------------------------

module "storage" {
  source        = "./modules/storage"
  bucket_prefix = var.bucket_prefix
  region        = var.region
}

module "seed_resources" {
  source = "./modules/seed_resources"

  staging_bucket_name = module.storage.staging_bucket_name
  point_samples_dir   = "${path.root}/../uk-geo/point_samples"
}

# ---------------------------------------------------------------------------
# Extraction pipeline
# ---------------------------------------------------------------------------

module "artifact_registry_extract" {
  source        = "./modules/artifact_registry"
  region        = var.region
  repository_id = "data-extraction"
}

module "cloud_run_extract" {
  source                = "./modules/cloud_run_job"
  job_name              = "data-extraction"
  region                = var.region
  image_url             = "${var.region}-docker.pkg.dev/${var.project_id}/data-extraction/data-extraction"
  image_tag             = var.extractor_image_tag
  timeout               = "1800s"
  cpu                   = "1"
  memory                = "512Mi"
  service_account_email = google_service_account.pipeline.email
  env_vars = {
    JOB_TYPE             = "extract_and_load"
    STAGING_BUCKET       = module.storage.staging_bucket_name
    GOOGLE_CLOUD_PROJECT = var.project_id
    LOG_LEVEL            = "INFO"
  }
  secret_env_vars = var.secret_env_vars

  depends_on = [google_project_service.apis, module.artifact_registry_extract]
}

module "extractor_workflow" {
  source = "./modules/extract/workflow"
  region = var.region

  service_account_email              = google_service_account.pipeline.email
  cloud_run_job_name                 = module.cloud_run_extract.job_name
  staging_bucket_name                = module.storage.staging_bucket_name
  default_pv_model_data_sources      = var.default_pv_model_data_sources
  default_pv_system_ids              = var.default_pv_system_ids
  default_locations                  = var.default_locations
  default_weather_model_data_sources = var.default_weather_model_data_sources
  default_split_by                   = var.default_split_by

  depends_on = [google_project_service.apis, module.cloud_run_extract]
}

module "extractor_scheduler" {
  source = "./modules/scheduler"
  region = var.region

  scheduler_job_name    = "pv-prospect-daily-extract"
  workflow_id           = module.extractor_workflow.workflow_id
  service_account_email = google_service_account.pipeline.email
  schedule              = var.extractor_scheduler_cron
  argument_json = jsonencode({
    "pv_system_ids" = "all"
  })

  depends_on = [google_project_service.apis, module.extractor_workflow]
}

module "extractor_pv_sites_backfill_workflow" {
  source = "./modules/extract/pv_sites_backfill_workflow"
  region = var.region

  service_account_email = google_service_account.pipeline.email
  cloud_run_job_name    = module.cloud_run_extract.job_name
  staging_bucket_name   = module.storage.staging_bucket_name
  default_pv_system_ids = var.default_pv_system_ids

  depends_on = [google_project_service.apis, module.cloud_run_extract]
}

module "extractor_pv_sites_backfill_scheduler" {
  source = "./modules/scheduler"
  region = var.region

  scheduler_job_name    = "pv-prospect-daily-extract-pv-sites-backfill"
  workflow_id           = module.extractor_pv_sites_backfill_workflow.workflow_id
  service_account_email = google_service_account.pipeline.email
  schedule              = var.extractor_pv_sites_backfill_scheduler_cron
  paused                = var.backfills_paused

  depends_on = [google_project_service.apis, module.extractor_pv_sites_backfill_workflow]
}

module "extractor_weather_grid_backfill_workflow" {
  source = "./modules/extract/weather_grid_backfill_workflow"
  region = var.region

  service_account_email = google_service_account.pipeline.email
  cloud_run_job_name    = module.cloud_run_extract.job_name
  staging_bucket_name   = module.storage.staging_bucket_name

  depends_on = [google_project_service.apis, module.cloud_run_extract]
}

module "extractor_weather_grid_backfill_scheduler" {
  source = "./modules/scheduler"
  region = var.region

  scheduler_job_name    = "pv-prospect-daily-extract-weather-grid-backfill"
  workflow_id           = module.extractor_weather_grid_backfill_workflow.workflow_id
  service_account_email = google_service_account.pipeline.email
  schedule              = var.extractor_weather_grid_backfill_scheduler_cron
  argument_json         = jsonencode({})
  paused                = var.backfills_paused

  depends_on = [google_project_service.apis, module.extractor_weather_grid_backfill_workflow]
}

# ---------------------------------------------------------------------------
# Transformation pipeline
# ---------------------------------------------------------------------------

module "artifact_registry_transform" {
  source        = "./modules/artifact_registry"
  region        = var.region
  repository_id = "data-transformation"
}

module "cloud_run_transform" {
  source    = "./modules/cloud_run_job"
  job_name  = "data-transformation"
  region    = var.region
  image_url = "${var.region}-docker.pkg.dev/${var.project_id}/data-transformation/data-transformation"
  image_tag = var.transformer_image_tag
  # Tight enough to surface a runaway loop quickly; generous enough that
  # the in-container transform-backfill (`run_transform_backfill`) has
  # room to plan, run all phases with parallel threads, consolidate and
  # commit in a single execution even for the weather-grid scope's
  # ~40 K-unit backlog. Daily-transform per-task executions finish in
  # seconds — Cloud Run bills actual runtime, not the timeout.
  timeout               = "14400s"
  cpu                   = "4"
  memory                = "8Gi"
  service_account_email = google_service_account.pipeline.email
  env_vars = {
    TRANSFORM_STEP       = "clean_weather"
    STAGING_BUCKET       = module.storage.staging_bucket_name
    GOOGLE_CLOUD_PROJECT = var.project_id
    LOG_LEVEL            = "INFO"
  }

  depends_on = [google_project_service.apis, module.artifact_registry_transform]
}

module "transformer_workflow" {
  source = "./modules/transform/workflow"
  region = var.region

  service_account_email = google_service_account.pipeline.email
  cloud_run_job_name    = module.cloud_run_transform.job_name
  staging_bucket_name   = module.storage.staging_bucket_name
  default_pv_system_ids = var.default_pv_system_ids
  default_locations     = var.default_locations

  depends_on = [google_project_service.apis, module.cloud_run_transform]
}

module "transformer_scheduler" {
  source = "./modules/scheduler"
  region = var.region

  scheduler_job_name    = "pv-prospect-daily-transform"
  workflow_id           = module.transformer_workflow.workflow_id
  service_account_email = google_service_account.pipeline.email
  schedule              = var.transformer_scheduler_cron
  argument_json = jsonencode({
    "pv_system_ids" = "all"
  })

  depends_on = [google_project_service.apis, module.transformer_workflow]
}

# Transform backfill is in-container (no Cloud Workflow): the scheduler
# invokes the data-transformation Cloud Run Job directly with
# JOB_TYPE=run_transform_backfill and a BACKFILL_SCOPE, and the
# container handles plan + run + consolidate + commit in a single
# execution. See pv_prospect.data_transformation.processing.entrypoint
# (_run_transform_backfill) for the handler.
module "transformer_pv_sites_backfill_scheduler" {
  source = "./modules/cloud_run_scheduler"

  project_id            = var.project_id
  region                = var.region
  scheduler_job_name    = "pv-prospect-daily-transform-pv-sites-backfill"
  job_name              = module.cloud_run_transform.job_name
  schedule              = var.transformer_pv_sites_backfill_scheduler_cron
  service_account_email = google_service_account.pipeline.email
  paused                = var.backfills_paused
  env_overrides = {
    JOB_TYPE       = "run_transform_backfill"
    BACKFILL_SCOPE = "pv_sites"
  }

  depends_on = [google_project_service.apis, module.cloud_run_transform]
}

module "transformer_weather_grid_backfill_scheduler" {
  source = "./modules/cloud_run_scheduler"

  project_id            = var.project_id
  region                = var.region
  scheduler_job_name    = "pv-prospect-daily-transform-weather-grid-backfill"
  job_name              = module.cloud_run_transform.job_name
  schedule              = var.transformer_weather_grid_backfill_scheduler_cron
  service_account_email = google_service_account.pipeline.email
  paused                = var.backfills_paused
  env_overrides = {
    JOB_TYPE       = "run_transform_backfill"
    BACKFILL_SCOPE = "weather_grid"
  }

  depends_on = [google_project_service.apis, module.cloud_run_transform]
}

# ---------------------------------------------------------------------------
# Versioning pipeline
# ---------------------------------------------------------------------------

module "artifact_registry_version" {
  source        = "./modules/artifact_registry"
  region        = var.region
  repository_id = "data-versioner"
}

# The pipeline SA needs write access to the versioned-feature bucket
resource "google_storage_bucket_iam_member" "pipeline_versioned_feature" {
  bucket = module.storage.versioned_feature_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# The pipeline SA needs full access to the versioned-model bucket for DVC
# push + plain serving-path upload from the model-trainer job.
resource "google_storage_bucket_iam_member" "pipeline_versioned_model" {
  bucket = module.storage.versioned_model_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

module "cloud_run_version" {
  source                = "./modules/cloud_run_job"
  job_name              = "data-versioner"
  region                = var.region
  image_url             = "${var.region}-docker.pkg.dev/${var.project_id}/data-versioner/data-versioner"
  image_tag             = var.versioner_image_tag
  timeout               = "1800s"
  cpu                   = "1"
  memory                = "2Gi"
  service_account_email = google_service_account.pipeline.email
  env_vars = {
    GOOGLE_CLOUD_PROJECT = var.project_id
    LOG_LEVEL            = "INFO"
  }
  secret_env_vars = [{
    name      = "GITHUB_DEPLOY_KEY"
    secret_id = "github-deploy-key"
    version   = "latest"
  }]

  depends_on = [google_project_service.apis, module.artifact_registry_version]
}

module "artifact_registry_model" {
  source        = "./modules/artifact_registry"
  region        = var.region
  repository_id = "pv-prospect-model"
}

module "cloud_run_model_trainer" {
  source                = "./modules/cloud_run_job"
  job_name              = "model-trainer"
  region                = var.region
  image_url             = "${var.region}-docker.pkg.dev/${var.project_id}/pv-prospect-model/model-trainer"
  image_tag             = var.model_trainer_image_tag
  timeout               = "3600s"
  cpu                   = "2"
  memory                = "4Gi"
  service_account_email = google_service_account.pipeline.email
  env_vars = {
    GOOGLE_CLOUD_PROJECT = var.project_id
    LOG_LEVEL            = "INFO"
    RUNTIME_ENV          = "default"
  }
  secret_env_vars = [{
    name      = "GITHUB_DEPLOY_KEY"
    secret_id = "github-deploy-key"
    version   = "latest"
  }]

  depends_on = [google_project_service.apis, module.artifact_registry_model]
}

module "version_workflow" {
  source = "./modules/version/workflow"
  region = var.region

  service_account_email = google_service_account.pipeline.email
  cloud_run_job_name    = module.cloud_run_version.job_name
  trainer_job_name      = module.cloud_run_model_trainer.job_name

  depends_on = [google_project_service.apis, module.cloud_run_version, module.cloud_run_model_trainer]
}

module "version_scheduler" {
  source = "./modules/version/scheduler"
  region = var.region

  workflow_id           = module.version_workflow.workflow_id
  service_account_email = google_service_account.pipeline.email
  schedule              = var.versioner_scheduler_cron

  depends_on = [google_project_service.apis, module.version_workflow]
}

# ---------------------------------------------------------------------------
# Prediction API — pv-prospect-app Cloud Run Service
# ---------------------------------------------------------------------------

# Dedicated SA for the serving app: objectViewer on the model bucket only.
resource "google_service_account" "app" {
  account_id   = "pv-prospect-app"
  display_name = "PV Prospect App"
  description  = "Used by the pv-prospect-app Cloud Run Service to read model artifacts"
}

resource "google_storage_bucket_iam_member" "app_versioned_model_reader" {
  bucket = module.storage.versioned_model_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.app.email}"
}

# The app SA reads the validation window artifact and site resources (pv_sites.csv)
# from the staging bucket.  Bucket-level grant is consistent with the other bindings
# in this config; the app only issues object GETs so read-only is minimal-privilege
# for the read use case.
resource "google_storage_bucket_iam_member" "app_staging_reader" {
  bucket = module.storage.staging_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.app.email}"
}

module "artifact_registry_app" {
  source        = "./modules/artifact_registry"
  region        = var.region
  repository_id = "pv-prospect-app"
}

module "cloud_run_app" {
  source                = "./modules/cloud_run_service"
  service_name          = "pv-prospect-app"
  region                = var.region
  project_id            = var.project_id
  repository_id         = "pv-prospect-app"
  image_name            = "pv-prospect-app"
  image_tag             = var.app_image_tag
  cpu                   = "2"
  memory                = "4Gi"
  min_instance_count    = 0
  max_instance_count    = 2
  allow_unauthenticated = var.allow_unauthenticated
  service_account_email = google_service_account.app.email
  env_vars = {
    GOOGLE_CLOUD_PROJECT  = var.project_id
    LOG_LEVEL             = "INFO"
    RUNTIME_ENV           = "default"
    STORE_DIR             = "gs://${module.storage.versioned_model_bucket_name}"
    VALIDATION_WINDOW_DIR = "gs://${module.storage.staging_bucket_name}/data/served/validation-window"
    RESOURCES_DIR         = "gs://${module.storage.staging_bucket_name}/resources"
    ASSETS_DIR            = "gs://${module.storage.staging_bucket_name}/assets"
  }

  depends_on = [google_project_service.apis, module.artifact_registry_app]
}

# ---------------------------------------------------------------------------
# GitHub Actions — Workload Identity Federation
# ---------------------------------------------------------------------------

# Pool: shared across all GitHub Actions workflows in this GCP project
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions"
  display_name              = "GitHub Actions"
  description               = "WIF pool for GitHub Actions workflows"
  disabled                  = false

  depends_on = [google_project_service.apis]
}

# Provider: bound to the pv-prospect-instance GitHub repository
resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub"
  description                        = "OIDC provider for GitHub Actions"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  # Only tokens from this specific repository can use this provider
  attribute_condition = "attribute.repository == \"${var.github_repo}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# Dedicated service account for the GitHub Actions upload workflow
resource "google_service_account" "github_actions" {
  account_id   = "github-actions-upload"
  display_name = "GitHub Actions Static Upload"
  description  = "Used by the upload-static GHA workflow to copy data/static to GCS"
}

# Allow the WIF provider to impersonate this SA (only for tokens from our repo)
resource "google_service_account_iam_member" "github_actions_wif" {
  service_account_id = google_service_account.github_actions.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}

# The SA only needs to write objects to the staging bucket's resources/ prefix
resource "google_storage_bucket_iam_member" "github_actions_staging" {
  bucket = module.storage.staging_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.github_actions.email}"
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "region" {
  value       = var.region
  description = "GCP region"
}

output "service_account_email" {
  value       = module.storage.service_account_email
  description = "DVC service account email"
}

output "pipeline_service_account_email" {
  value       = google_service_account.pipeline.email
  description = "Pipeline service account email"
}

output "staging_bucket_name" {
  value       = module.storage.staging_bucket_name
  description = "Name of the staging bucket"
}

output "versioned_model_bucket_name" {
  value       = module.storage.versioned_model_bucket_name
  description = "Name of the versioned model artifacts bucket"
}

output "artifact_registry_extractor_url" {
  value       = module.artifact_registry_extract.repository_url
  description = "Docker registry URL for data extraction"
}

output "artifact_registry_transformer_url" {
  value       = module.artifact_registry_transform.repository_url
  description = "Docker registry URL for data transformation"
}

output "extractor_cloud_run_job_name" {
  value       = module.cloud_run_extract.job_name
  description = "Extraction Cloud Run Job name"
}

output "extractor_workflow_name" {
  value       = module.extractor_workflow.workflow_name
  description = "Extraction workflow name"
}

output "transformer_scheduler_job_name" {
  value       = module.transformer_scheduler.scheduler_job_name
  description = "Transform Cloud Scheduler job name"
}

output "extractor_pv_sites_backfill_workflow_name" {
  value       = module.extractor_pv_sites_backfill_workflow.workflow_name
  description = "PV-sites backfill workflow name"
}

output "extractor_pv_sites_backfill_scheduler_job_name" {
  value       = module.extractor_pv_sites_backfill_scheduler.scheduler_job_name
  description = "PV-sites backfill Cloud Scheduler job name"
}

output "extractor_weather_grid_backfill_workflow_name" {
  value       = module.extractor_weather_grid_backfill_workflow.workflow_name
  description = "Weather grid backfill workflow name"
}

output "extractor_weather_grid_backfill_scheduler_job_name" {
  value       = module.extractor_weather_grid_backfill_scheduler.scheduler_job_name
  description = "Weather grid backfill Cloud Scheduler job name"
}

output "transformer_pv_sites_backfill_scheduler_job_name" {
  value       = module.transformer_pv_sites_backfill_scheduler.scheduler_job_name
  description = "PV-sites transform backfill Cloud Scheduler job name"
}

output "transformer_weather_grid_backfill_scheduler_job_name" {
  value       = module.transformer_weather_grid_backfill_scheduler.scheduler_job_name
  description = "Weather-grid transform backfill Cloud Scheduler job name"
}

output "extractor_scheduler_job_name" {
  value       = module.extractor_scheduler.scheduler_job_name
  description = "Cloud Scheduler job name for daily extraction"
}

output "artifact_registry_versioner_url" {
  value       = module.artifact_registry_version.repository_url
  description = "Docker registry URL for data versioner"
}

output "artifact_registry_model_trainer_url" {
  value       = module.artifact_registry_model.repository_url
  description = "Docker registry URL for model trainer"
}

output "model_trainer_cloud_run_job_name" {
  value       = module.cloud_run_model_trainer.job_name
  description = "Model trainer Cloud Run Job name"
}

output "artifact_registry_app_url" {
  value       = module.artifact_registry_app.repository_url
  description = "Docker registry URL for pv-prospect-app"
}

output "app_service_url" {
  value       = module.cloud_run_app.service_url
  description = "URL of the pv-prospect-app Cloud Run Service"
}

output "version_workflow_name" {
  value       = module.version_workflow.workflow_name
  description = "Versioning workflow name"
}

output "version_scheduler_job_name" {
  value       = module.version_scheduler.scheduler_job_name
  description = "Versioning Cloud Scheduler job name"
}

output "wif_provider" {
  value       = google_iam_workload_identity_pool_provider.github.name
  description = "WIF provider resource name — set as GitHub Actions Variable WIF_PROVIDER"
}

output "github_actions_sa_email" {
  value       = google_service_account.github_actions.email
  description = "GitHub Actions service account email — set as GitHub Actions Variable GCS_SA_EMAIL"
}
