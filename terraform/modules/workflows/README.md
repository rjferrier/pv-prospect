# Cloud Workflows Module

This module defines the serverless orchestrator for the data extraction pipeline. It invokes the Cloud Run Job using the Cloud Run Connector.

The workflow acts as an orchestration engine, doing the following:
1. Runs the `preprocess` job to provision GCS directories and DVC resources.
2. Performs a parallel fan-out over the specified `pv_system_ids` and `source_descriptors` to run instances of the `extract_and_load` Cloud Run job.

## Resources Created

- **Cloud Workflow**: YAML-based execution definition.

## Variables

| Name | Type | Description |
|------|------|-------------|
| `region` | string | GCP region for the workflow |
| `service_account_email` | string | Service account to execute the workflow as |
| `cloud_run_job_name` | string | Name of the Cloud Run Job to invoke |
| `default_source_descriptors` | list(string) | Default sources for scheduled runs |
| `default_pv_system_ids` | list(number) | Default sites for scheduled runs |

## Outputs

| Name | Description |
|------|-------------|
| `workflow_name` | Workflow name |
| `workflow_id` | Fully-qualified Google Cloud Workflow ID |
