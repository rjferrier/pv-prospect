# Cloud Run Module

This module manages the Cloud Run Job responsible for executing the data extraction tasks.

Instead of multiple job definitions, a single parameterized job is used. The Cloud Workflow orchestrator injects environment variables like `JOB_TYPE` to switch between `preprocess` and `extract_and_load` modes dynamically at runtime.

## Resources Created

- **Cloud Run Job**: V2 job specification.

## Variables

| Name | Type | Description |
|------|------|-------------|
| `region` | string | GCP region for Cloud Run |
| `project_id` | string | GCP project ID |
| `image_url` | string | Full Artifact Registry image URL (without tag) |
| `image_tag` | string | Docker image tag (default: "latest") |
| `gcs_bucket` | string | GCS bucket name for data output |
| `service_account_email` | string | Service account email for job execution |
| `secret_env_vars` | list(object) | Environment variables sourced securely from Secret Manager |

## Outputs

| Name | Description |
|------|-------------|
| `job_name` | The name of the Cloud Run Job |
| `job_id` | The fully qualified ID of the Cloud Run Job |
