# PV Prospect Terraform Infrastructure

This directory contains the Terraform configuration that **provisions** the PV
Prospect infrastructure on Google Cloud Platform, and the process for applying it.
For what that infrastructure *is* — the buckets, scheduled workflows, Cloud Run
compute, scheduling rationale, and cost model — see
[`doc/infrastructure.md`](../doc/infrastructure.md).

## Contents

- [Directory Structure](#directory-structure)
- [Bootstrap vs. Main Configuration](#bootstrap-vs-main-configuration)
- [Remote State & Configuration](#remote-state--configuration)
- [Prerequisites](#prerequisites)
- [Quick Start (First Time)](#quick-start-first-time)
- [Subsequent Deployments](#subsequent-deployments)
- [Operational Tuning & Cost](#operational-tuning--cost)
- [Security Considerations](#security-considerations)

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

## Operational Tuning & Cost

Job timeouts, scheduler spacing, pausing the backfills, and the cost model are
properties of the running system rather than the provisioning process, so they are
documented in [`doc/infrastructure.md`](../doc/infrastructure.md). The knobs
themselves — the cron schedules, job timeouts, and the `backfills_paused` switch —
are all exposed as variables in `terraform.tfvars`.

## Security Considerations

1. **Service Accounts**:
   - A dedicated `data-extraction-pipeline` service account runs the jobs, adhering
     to the principle of least privilege.
2. **Secrets**:
   - API keys (like PVOutput) are injected securely via Google Secret Manager,
     avoiding plaintext keys in environment variables.
