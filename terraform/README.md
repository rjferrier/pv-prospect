# PV Prospect Terraform Infrastructure

This directory contains Terraform configurations for deploying the serverless PV
Prospect data extraction pipeline on Google Cloud Platform.

## Architecture Overview

The pipeline uses a fully serverless, pay-per-use architecture: Cloud Run
Jobs do the work, and Cloud Workflows fans them out for the daily extract,
transform, and version flows. The transformation backfills are an exception —
they invoke a Cloud Run Job directly (planning, execution, and commit happen
in one container) because the per-unit work is small and there is no benefit
to dispatch-side fan-out.

## Directory Structure

```
terraform/
├── bootstrap/                     # Bootstrapping infrastructure (apply once)
│   ├── main.tf                    #   State bucket + Artifact Registry
│   ├── variables.tf
│   └── terraform.tfvars
├── modules/
│   ├── artifact_registry/         # Docker image repository
│   ├── cloud_run_job/             # Reusable Cloud Run Job (extract/transform/version)
│   ├── cloud_run_scheduler/       # Scheduler trigger that invokes a Cloud Run Job directly (transform backfills)
│   ├── extract/
│   │   ├── workflow                       # Daily extract workflow
│   │   ├── pv_sites_backfill_workflow     # PV-sites extraction backfill workflow
│   │   └── weather_grid_backfill_workflow # Weather-grid extraction backfill workflow
│   ├── scheduler/                 # Scheduler trigger that invokes a Cloud Workflow
│   ├── seed_resources/            # Uploads resource CSVs (pv_sites, weather-grid samples) to GCS
│   ├── storage/                   # GCS bucket + IAM
│   ├── transform/workflow         # Daily transform workflow (transform backfills run as direct Cloud Run Jobs — no workflow)
│   └── version/
│       ├── workflow               # Weekly versioning workflow
│       └── scheduler              # Versioning scheduler trigger
├── main.tf                        # Root module configuration
├── variables.tf                   # Root module variables
├── terraform.tfvars               # Variable values (customize this)
├── backend.hcl                    # Remote backend config (bucket name etc.)
└── README.md                      # This file
```

## Bootstrap vs. Main Configuration

There are two separate Terraform configurations here to solve a **chicken-and-egg
problem**: Terraform needs a GCS bucket to store its remote state, but you can't
create that bucket with the same configuration that uses it.

### Bootstrap (`terraform/bootstrap/`)

Manages resources that must exist before the main configuration can run:

- **GCS State Bucket** (`<project_id>-tfstate`) -- stores the main Terraform state
  remotely with versioning enabled.
- **Artifact Registry** -- the Docker repository for pipeline images. It lives here
  because it's similarly foundational: images must be pushed before the Cloud Run
  Job can be deployed.

Bootstrap uses **local state only** (stored in `bootstrap/terraform.tfstate`). This
is intentional -- it's a small, rarely-changed config and bootstrapping its own
state would be circular.

### Main (`terraform/`)

Manages the rest of the pipeline: Storage, IAM, Cloud Run, Workflows, and
Scheduler. Its state is stored remotely in the GCS bucket provisioned by bootstrap.

The Artifact Registry URL is referenced as a computed string
(`<region>-docker.pkg.dev/<project_id>/data-extraction`) rather than a Terraform
output cross-reference, since the two configurations are independent.

## Remote State & Configuration

Both the Terraform **state** and the **variable values** (`terraform.tfvars`) are
stored in the same GCS bucket, making GCS the single source of truth for project
configuration. Neither file is committed to git.

| File | Where it lives |
|---|---|
| Remote state | `gs://<project_id>-tfstate/terraform/state/` |
| `terraform.tfvars` | `gs://<project_id>-tfstate/terraform/terraform.tfvars` |
| `backend.hcl` | Generated locally by `deploy.sh` (gitignored) |

`backend.hcl` cannot be stored in GCS -- it would be circular, since it contains
the bucket name needed to access GCS. It also can't be committed to git without
leaking project-specific infrastructure details. Instead, `deploy.sh` generates it
at runtime from the supplied `PROJECT_ID`.

To edit the project configuration directly, pull `terraform.tfvars` from GCS, edit
it locally, then push it back:

```bash
# Pull
gcloud storage cp \
    gs://<project_id>-tfstate/terraform/terraform.tfvars terraform.tfvars

# ... make your edits ...

# Push
gcloud storage cp \
    terraform.tfvars gs://<project_id>-tfstate/terraform/terraform.tfvars
```

> **Note:** `.tfvars` files are for _input variables_ passed via `-var-file`.
> Backend config files use `-backend-config` and the `.hcl` extension -- they are
> a different mechanism.

## Prerequisites

1. **Google Cloud Platform**:
   - Active GCP project with Billing enabled
   - Required APIs are automatically enabled by this Terraform configuration

2. **Terraform**:
   - Terraform >= 1.0
   - Google Cloud provider ~> 6.12.0

3. **Authentication**:
   ```bash
   gcloud auth application-default login
   ```

## Quick Start (First Time)

### 1. Apply Bootstrap Infrastructure

You only need your GCP project ID to begin. Run this once to create the state
bucket and Artifact Registry:

```bash
cd terraform/bootstrap
terraform init
terraform apply -var="project_id=<your-project-id>"
```

### 2. Upload `terraform.tfvars` to GCS

After bootstrap, push the variable values to GCS:

```bash
gcloud storage cp terraform.tfvars \
    gs://<your-project-id>-tfstate/terraform/terraform.tfvars
```

### 3. Deploy

```bash
bash deploy.sh <your-project-id>
```

`deploy.sh` generates `backend.hcl`, pulls `terraform.tfvars` from GCS, initialises
the backend, builds and pushes the Docker image, and applies the infrastructure.

## Subsequent Deployments

After the initial setup, only the main configuration needs to be touched for normal
deployments. You can just use the deploy script:

```bash
cd terraform/
bash deploy.sh <your-project-id>
```

Bootstrap only needs to be re-applied if foundational resources change (e.g., adding
a new Artifact Registry repository).

## Cost Estimation

This architecture is entirely serverless (scale-to-zero). You only pay for what you
use.

Approximate monthly costs (varies by usage volume):
- **Cloud Scheduler**: Free tier (up to 3 jobs/month free)
- **Cloud Workflows**: Free tier (first 5,000 steps/month free)
- **Cloud Run Jobs**: ~$1-$5 (depending on extraction volume/duration)
- **Artifact Registry**: ~$0.10/GB stored
- **GCS Storage**: Standard GCS pricing

> **Note**: This is significantly cheaper than a continuously running orchestrator
> like Cloud Composer or keeping GKE/VM nodes running.

## Operational Tuning

### Cloud Run Job timeout

The extraction Cloud Run Job (`module.cloud_run_extract`) has a task timeout of
**1800s (30 minutes)**. This was set to accommodate the sequential PV-site backfill
which makes up to ~1,066 API calls per job execution. The earlier value of 600s
caused executions to be cut off mid-run when API latency spiked.

The transformation and versioner jobs have their own, independently configured
timeouts (`900s` and `1800s` respectively).

### Scheduler spacing

Because individual Cloud Run Job tasks can now run for up to 30 minutes, the
extraction workflows are spaced **40 minutes apart** to prevent concurrent
executions from combining to breach PVOutput's 300 requests/hour rate limit:

| Schedule | Trigger time (UTC) | API used |
|---|---|---|
| Daily extraction | 02:00 | PVOutput |
| PV site extraction backfill | 02:40 | PVOutput |
| Weather grid extraction backfill | 03:20 | OpenMeteo only |
| Daily transformation | 05:30 | -- |
| PV-sites transformation backfill | 06:00 | -- |
| Weather-grid transformation backfill | 08:00 | -- |
| Weekly versioning (Sun) | 23:00 | -- |

The weather grid backfill uses OpenMeteo exclusively, so it does not conflict
with PVOutput. It runs in a single execution per day — the 9 batches it
dispatches are paced internally by `sleep_seconds_between_batches` (default
720 s), so a sliding-60-minute window contains at most ~3 batches × 1,330
calls ≈ 3,990 calls, comfortably under OpenMeteo's 5,000/hour limit. Total
wall time ≈ 3 h 24 min — so the extract finishes ~06:44, consolidate adds a
few minutes, and the weather-grid transform backfill at 08:00 has ~1 h
margin before reading the consolidated ledger.

The transformation schedules don't hit any external API, so they could in
principle run back-to-back. They are placed where they are so the planner
sees a fully-consolidated extraction ledger when it runs: 06:00 for the
PV-sites transform backfill (extraction finishes by 03:10), and 08:00 for
the weather-grid transform backfill (extraction finishes by ~06:44).

The default cron expressions are defined as Terraform variables
(`extractor_scheduler_cron`, `extractor_pv_sites_backfill_scheduler_cron`,
`extractor_weather_grid_backfill_scheduler_cron`,
`transformer_scheduler_cron`, `transformer_pv_sites_backfill_scheduler_cron`,
`transformer_weather_grid_backfill_scheduler_cron`,
`versioner_scheduler_cron`) and can be overridden in `terraform.tfvars`
without touching module code.

### Checkpoint-based resume

The PV-sites backfill workflow maintains a GCS position checkpoint so that a
re-triggered execution resumes from where the previous one stopped rather than
starting over:

| Workflow | GCS checkpoint object | Checkpoint key |
|---|---|---|
| `pv-prospect-extract-pv-site-backfill` | `tracking/checkpoints/pv_sites_backfill.json` | Next PV task index (integer) |

The checkpoint is a JSON object written to GCS after each dispatched task.

**Behaviour:**

- **Normal run** -- no checkpoint exists; the workflow processes every task in
  the manifest and deletes the checkpoint on success.
- **Interrupted run** -- checkpoint records the next task index. Re-triggering
  manually loads the checkpoint, logs `"Resuming from checkpoint -- starting at
  PV task N"`, and skips earlier tasks.
- **Next scheduled run** -- because the checkpoint was deleted on the previous
  successful run (or never created), the scheduled run starts fresh.

If a run is interrupted *before* any task completes (e.g. the `plan` step fails),
no checkpoint exists and a re-run starts from the beginning.

The weather-grid backfill does **not** use a checkpoint: it runs as a single
linear pass per day, and a re-trigger after failure re-runs every batch from
index 0. The cost is roughly 3 hours of OpenMeteo budget on the rare manual
retrigger; in exchange there is no checkpoint-shaped coordination surface for
concurrent same-day executions to race on.

## Security Considerations

1. **Service Accounts**:
   - A dedicated `data-extraction-pipeline` service account runs the jobs, adhering
     to the principle of least privilege.
2. **Secrets**:
   - API keys (like PVOutput) are injected securely via Google Secret Manager,
     avoiding plaintext keys in environment variables.
