# Cloud Scheduler Module

This module manages the cron schedule that triggers the Cloud Workflow on a daily basis.

It invokes the Workflow API via HTTP POST, authenticating seamlessly using OIDC and the pipeline's service account.

## Resources Created

- **Cloud Scheduler Job**: Daily HTTP trigger.

## Variables

| Name | Type | Description |
|------|------|-------------|
| `region` | string | GCP region for the scheduler |
| `workflow_id` | string | Fully-qualified ID of the workflow to trigger |
| `service_account_email` | string | Service account for Scheduler authentication |
| `schedule` | string | Cron schedule (default: `0 3 * * *`) |
| `time_zone` | string | Timezone for schedule execution (default: `UTC`) |

## Outputs

| Name | Description |
|------|-------------|
| `scheduler_job_name` | Cloud Scheduler job name |
