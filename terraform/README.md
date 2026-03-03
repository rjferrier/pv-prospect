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
├── main.tf                    # Root module configuration
├── variables.tf               # Root module variables
├── terraform.tfvars           # Variable values (customize this)
├── README.md                  # This file
└── modules/
    ├── artifact_registry/     # Docker image repository
    ├── cloud_run/             # Cloud Run batch jobs
    ├── scheduler/             # Cloud Scheduler cron jobs
    ├── storage/               # GCS storage
    └── workflows/             # Cloud Workflows orchestration
```

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

## Quick Start

### 1. Configure Variables

Create or update `terraform.tfvars`:

```hcl
project_id            = "your-gcp-project-id"
bucket_name           = "your-unique-bucket-name"
default_pv_system_ids = [12345]

# Environment variables containing secrets
secret_env_vars = [
  {
    name      = "PVOUTPUT_API_KEY"
    secret_id = "pvoutput-api-key"
    version   = "latest"
  }
]
```

### 2. Initialize Terraform

```bash
terraform init -upgrade
```

### 3. Deploy the Infrastructure

Because there is a dependency between the Docker registry, the Docker image, and the Cloud Run Job, we recommend deploying in stages or using a deployment script:

```bash
# 1. Provision the Registry and APIs first
terraform apply -target=module.artifact_registry

# 2. Build and push Docker image
IMAGE_URL=$(terraform output -raw artifact_registry_url)/data-extraction
docker build -t $IMAGE_URL:latest --target entrypoint -f ../pv-prospect-data-extraction/Dockerfile ..
docker push $IMAGE_URL:latest

# 3. Apply the rest of the infrastructure
terraform apply
```

### 4. Trigger the Pipeline Manually (Optional)

You can trigger an ad-hoc run (e.g., a backfill) using the `gcloud` CLI:

```bash
gcloud workflows run pv-prospect-extract \
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
