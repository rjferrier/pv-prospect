#!/usr/bin/env bash

set -e

echo "=========================================================="
echo " PV Prospect - Cloud Data Extraction Pipeline Deployment"
echo "=========================================================="

cd "$(dirname "$0")"

# 1. Provision the Registry and APIs first
echo ""
echo "[1/3] Provisioning Artifact Registry and required APIs..."
terraform apply -target=module.artifact_registry -auto-approve

# 2. Build and push Docker image
echo ""
echo "[2/3] Building and pushing Docker image..."
REGION=$(terraform output -raw region 2>/dev/null || echo "europe-west2")
IMAGE_URL=$(terraform output -raw artifact_registry_url)/data-extraction

echo "Authenticating Docker to $REGION-docker.pkg.dev..."
gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet

echo "Building Image: $IMAGE_URL:latest"
docker build -t "$IMAGE_URL:latest" --target entrypoint -f ../pv-prospect-data-extraction/Dockerfile ..

echo "Pushing Image to Artifact Registry..."
docker push "$IMAGE_URL:latest"

# 3. Apply the rest of the infrastructure
echo ""
echo "[3/3] Provisioning Cloud Run, Workflows, and Scheduler..."
terraform apply -auto-approve

echo ""
echo "=========================================================="
echo " Deployment Complete!"
echo "=========================================================="
echo ""
echo "To test the pipeline via an ad-hoc run, use:"
echo "gcloud workflows run pv-prospect-extract --data='{\"pv_system_ids\": [12345], \"start_date\": \"2025-06-24\", \"end_date\": \"2025-06-25\"}'"
