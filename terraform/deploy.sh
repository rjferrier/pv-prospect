#!/usr/bin/env bash

set -e

echo "=========================================================="
echo " PV Prospect - Cloud Data Extraction Pipeline Deployment"
echo "=========================================================="

cd "$(dirname "$0")"

# Resolve project ID: first argument, then env var, then gcloud default
PROJECT_ID="${1:-${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}}"
if [ -z "$PROJECT_ID" ]; then
  echo "ERROR: PROJECT_ID is not set. Pass it as the first argument or set the PROJECT_ID env var."
  exit 1
fi

TFSTATE_BUCKET="${PROJECT_ID}-tfstate"
TFVARS_REMOTE="gs://${TFSTATE_BUCKET}/terraform/terraform.tfvars"

# 0. Generate backend.hcl and pull the authoritative tfvars from GCS
echo ""
echo "[0/3] Configuring backend and pulling terraform.tfvars from GCS..."
cat > backend.hcl <<EOF
bucket = "${TFSTATE_BUCKET}"
prefix = "terraform/state"
EOF

gcloud storage cp "$TFVARS_REMOTE" terraform.tfvars

terraform init -backend-config=backend.hcl -upgrade

# 1. Build and push Docker image
echo ""
echo "[1/2] Building and pushing Docker image..."
REGION=$(terraform output -raw region 2>/dev/null || echo "europe-west2")
IMAGE_URL=$(terraform output -raw artifact_registry_url)/data-extraction

echo "Authenticating Docker to $REGION-docker.pkg.dev..."
gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet

echo "Building Image: $IMAGE_URL:latest"
docker build -t "$IMAGE_URL:latest" --target entrypoint -f ../pv-prospect-data-extraction/Dockerfile ..

echo "Pushing Image to Artifact Registry..."
docker push "$IMAGE_URL:latest"

# 2. Apply the rest of the infrastructure
echo ""
echo "[2/2] Provisioning Cloud Run, Workflows, and Scheduler..."
terraform apply -auto-approve

echo ""
echo "=========================================================="
echo " Deployment Complete!"
echo "=========================================================="
echo ""
echo "To test the pipeline via an ad-hoc run, use:"
echo "gcloud workflows run pv-prospect-extract \\"
echo "  --location=$REGION \\"
echo "  --data='{\"pv_system_ids\": [12345], \"start_date\": \"2025-06-24\", \"end_date\": \"2025-06-25\"}'"
