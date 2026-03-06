# PV Prospect Terraform Infrastructure

This directory contains Terraform configurations for deploying the serverless PV Prospect data extraction pipeline on Google Cloud Platform.

## Architecture Overview

The pipeline uses fully serverless, pay-per-use architecture with Cloud Workflows orchestrating Cloud Run Jobs:

```
PV Prospect Infrastructure
├── Storage Module
│   ├── GCS Bucket
│   └── IAM Bindings
├── Artifact Registry Module
│   └── Docker Image Repository
├── Cloud Run Module
│   └── Ephemeral Batch Jobs (data extraction)
├── Workflows Module
│   └── Orchestration & Fan-out logic
└── Scheduler Module
    └── Daily trigger (Cron)
```

## Directory Structure

```
terraform/
├── bootstrap/                     # Bootstrapping infrastructure (apply once)
│   ├── main.tf                    #   State bucket + Artifact Registry
│   ├── variables.tf
│   └── terraform.tfvars
├── modules/
│   ├── artifact_registry/         # Docker image repository
│   ├── cloud_run/                 # Cloud Run batch jobs
│   ├── scheduler/                 # Cloud Scheduler cron jobs
│   ├── storage/                   # GCS storage
│   └── workflows/                 # Cloud Workflows orchestration
├── main.tf                        # Root module configuration
├── variables.tf                   # Root module variables
├── terraform.tfvars               # Variable values (customize this)
├── backend.hcl                    # Remote backend config (bucket name etc.)
└── README.md                      # This file
```

## Bootstrap vs. Main Configuration

There are two separate Terraform configurations here to solve a **chicken-and-egg problem**: Terraform needs a GCS bucket to store its remote state, but you can't create that bucket with the same configuration that uses it.

### Bootstrap (`terraform/bootstrap/`)

Manages resources that must exist before the main configuration can run:

- **GCS State Bucket** (`<project_id>-tfstate`) — stores the main Terraform state remotely with versioning enabled.
- **Artifact Registry** — the Docker repository for pipeline images. It lives here because it's similarly foundational: images must be pushed before the Cloud Run Job can be deployed.

Bootstrap uses **local state only** (stored in `bootstrap/terraform.tfstate`). This is intentional — it's a small, rarely-changed config and bootstrapping its own state would be circular.

### Main (`terraform/`)

Manages the rest of the pipeline: Storage, IAM, Cloud Run, Workflows, and Scheduler. Its state is stored remotely in the GCS bucket provisioned by bootstrap.

The Artifact Registry URL is referenced as a computed string (`<region>-docker.pkg.dev/<project_id>/data-extraction`) rather than a Terraform output cross-reference, since the two configurations are independent.

## Remote State & Configuration

Both the Terraform **state** and the **variable values** (`terraform.tfvars`) are stored in the same GCS bucket, making GCS the single source of truth for project configuration. Neither file is committed to git.

| File | Where it lives |
|---|---|
| Remote state | `gs://<project_id>-tfstate/terraform/state/` |
| `terraform.tfvars` | `gs://<project_id>-tfstate/terraform/terraform.tfvars` |
| `backend.hcl` | Generated locally by `deploy.sh` (gitignored) |

`backend.hcl` cannot be stored in GCS — it would be circular, since it contains the bucket name needed to access GCS. It also can't be committed to git without leaking project-specific infrastructure details. Instead, `deploy.sh` generates it at runtime from the supplied `PROJECT_ID`.

To edit the project configuration directly, pull `terraform.tfvars` from GCS, edit it locally, then push it back:

```bash
# Pull
gcloud storage cp gs://<project_id>-tfstate/terraform/terraform.tfvars terraform.tfvars

# ... make your edits ...

# Push
gcloud storage cp terraform.tfvars gs://<project_id>-tfstate/terraform/terraform.tfvars
```

> **Note:** `.tfvars` files are for _input variables_ passed via `-var-file`. Backend config files use `-backend-config` and the `.hcl` extension — they are a different mechanism.

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

You only need your GCP project ID to begin. Run this once to create the state bucket and Artifact Registry:

```bash
cd terraform/bootstrap
terraform init
terraform apply -var="project_id=<your-project-id>"
```

### 2. Upload `terraform.tfvars` to GCS

After bootstrap, push the variable values to GCS:

```bash
gcloud storage cp terraform.tfvars gs://<your-project-id>-tfstate/terraform/terraform.tfvars
```

### 3. Deploy

```bash
bash deploy.sh <your-project-id>
```

`deploy.sh` generates `backend.hcl`, pulls `terraform.tfvars` from GCS, initialises the backend, builds and pushes the Docker image, and applies the infrastructure.

## Subsequent Deployments

After the initial setup, only the main configuration needs to be touched for normal deployments. You can just use the deploy script:

```bash
cd terraform/
bash deploy.sh <your-project-id>
```

Bootstrap only needs to be re-applied if foundational resources change (e.g., adding a new Artifact Registry repository).

### Trigger the Pipeline Manually (Optional)

You can trigger an ad-hoc run (e.g., a backfill) using the `gcloud` CLI:

```bash
gcloud workflows run pv-prospect-extract \
  --location=europe-west2 \
  --data='{"pv_system_ids": [12345], "start_date": "2025-06-24", "end_date": "2025-06-25"}'
```

## Cost Estimation

This architecture is entirely serverless (scale-to-zero). You only pay for what you use.

Approximate monthly costs (varies by usage volume):
- **Cloud Scheduler**: Free tier (up to 3 jobs/month free)
- **Cloud Workflows**: Free tier (first 5,000 steps/month free)
- **Cloud Run Jobs**: ~$1–$5 (depending on extraction volume/duration)
- **Artifact Registry**: ~$0.10/GB stored
- **GCS Storage**: Standard GCS pricing

> **Note**: This is significantly cheaper than a continuously running orchestrator like Cloud Composer or keeping GKE/VM nodes running.

## Security Considerations

1. **Service Accounts**:
   - A dedicated `data-extraction-pipeline` service account runs the jobs, adhering to the principle of least privilege.
2. **Secrets**:
   - API keys (like PVOutput) are injected securely via Google Secret Manager, avoiding plaintext keys in environment variables.
