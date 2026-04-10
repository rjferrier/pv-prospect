#!/usr/bin/env bash

set -e

echo "=========================================================="
echo " PV Prospect - Cloud Data Extraction Pipeline Deployment"
echo "=========================================================="

cd "$(dirname "$0")"

usage() {
  echo "Usage: $0 [-p|--project-id PROJECT_ID] [STAGES]"
  echo ""
  echo "  -p, --project-id  GCP project ID (or set PROJECT_ID env var / gcloud default)"
  echo "  STAGES            Comma-separated stages to run (default: all)"
  echo ""
  echo "  Available stages:"
  echo "    registry              — provision Artifact Registries and required APIs"
  echo "    build-extraction      — build and push extraction Docker image"
  echo "    build-transformation  — build and push transformation Docker image"
  echo "    build-versioner       — build and push data-versioner Docker image"
  echo "    terraform-extraction  — apply extraction infrastructure (Cloud Run, Workflows, Schedulers)"
  echo "    terraform-transform   — apply transformation infrastructure (Cloud Run, Workflow)"
  echo "    terraform             — apply all infrastructure (full apply)"
  echo ""
  echo "  Shorthand aliases:"
  echo "    build   = build-extraction,build-transformation,build-versioner"
  echo "    all     = registry,build,terraform  (the default)"
  echo ""
  echo "Examples:"
  echo "  $0                                                       # run everything"
  echo "  $0 -p my-project build-transformation                   # rebuild transformation image only"
  echo "  $0 -p my-project build-transformation,terraform-transform  # redeploy transformation"
  echo "  $0 --project-id my-project terraform                    # re-apply all infra, no image build"
  exit 1
}

# Parse flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project-id) PROJECT_ID="$2"; shift 2 ;;
    -h|--help)       usage ;;
    -*)              echo "ERROR: Unknown flag: $1"; usage ;;
    *)               break ;;
  esac
done

# Resolve project ID: flag > env var > gcloud default
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
if [ -z "$PROJECT_ID" ]; then
  echo "ERROR: PROJECT_ID is not set. Use -p/--project-id or set the PROJECT_ID env var."
  exit 1
fi

# Expand shorthand aliases, then check each stage
expand_stages() {
  echo "$1" | tr ',' '\n' | while read -r stage; do
    case "$stage" in
      all)      echo "registry"; echo "build-extraction"; echo "build-transformation"; echo "build-versioner"; echo "terraform" ;;
      build)    echo "build-extraction"; echo "build-transformation"; echo "build-versioner" ;;
      *)        echo "$stage" ;;
    esac
  done
}

STAGES_ARG="${1:-all}"
STAGES=$(expand_stages "$STAGES_ARG")

run_stage() {
  echo "$STAGES" | grep -qx "$1"
}

TFSTATE_BUCKET="${PROJECT_ID}-tfstate"
TFVARS_REMOTE="gs://${TFSTATE_BUCKET}/terraform/terraform.tfvars"

# 0. Always configure backend and pull tfvars
echo ""
echo "[0] Configuring backend and pulling terraform.tfvars from GCS..."
cat > backend.hcl <<EOF
bucket = "${TFSTATE_BUCKET}"
prefix = "terraform/state"
EOF

gcloud storage cp "$TFVARS_REMOTE" terraform.tfvars

terraform init -backend-config=backend.hcl -upgrade

# Helper: resolve region and image URLs from terraform outputs
resolve_image_urls() {
  REGION=$(terraform output -raw region 2>/dev/null || echo "europe-west2")
  IMAGE_URL_EXTRACT=$(terraform output -raw artifact_registry_url)/data-extraction
  IMAGE_URL_TRANSFORM=$(terraform output -raw artifact_registry_transformer_url)/data-transformation
  IMAGE_URL_VERSION=$(terraform output -raw artifact_registry_versioner_url)/data-versioner
  gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
}

# 1. Provision Artifact Registries and APIs
if run_stage registry; then
  echo ""
  echo "[registry] Provisioning Artifact Registry and required APIs..."
  terraform apply \
    -target=module.artifact_registry_extract \
    -target=module.artifact_registry_transform \
    -target=module.artifact_registry_version \
    -auto-approve
fi

# 2a. Build and push extraction image
if run_stage build-extraction; then
  echo ""
  echo "[build-extraction] Building and pushing extraction Docker image..."
  resolve_image_urls
  echo "Building: $IMAGE_URL_EXTRACT:latest"
  docker build -t "$IMAGE_URL_EXTRACT:latest" --target entrypoint -f ../pv-prospect-data-extraction/Dockerfile ..
  docker push "$IMAGE_URL_EXTRACT:latest"
fi

# 2b. Build and push transformation image
if run_stage build-transformation; then
  echo ""
  echo "[build-transformation] Building and pushing transformation Docker image..."
  resolve_image_urls
  echo "Building: $IMAGE_URL_TRANSFORM:latest"
  docker build -t "$IMAGE_URL_TRANSFORM:latest" --target entrypoint -f ../pv-prospect-data-transformation/Dockerfile ..
  docker push "$IMAGE_URL_TRANSFORM:latest"
fi

# 2c. Build and push versioner image
if run_stage build-versioner; then
  echo ""
  echo "[build-versioner] Building and pushing data-versioner Docker image..."
  resolve_image_urls
  echo "Building: $IMAGE_URL_VERSION:latest"
  docker build -t "$IMAGE_URL_VERSION:latest" --target entrypoint -f ../pv-prospect-data-versioner/Dockerfile ..
  docker push "$IMAGE_URL_VERSION:latest"
fi

# 3a. Apply extraction infrastructure
if run_stage terraform-extraction; then
  echo ""
  echo "[terraform-extraction] Provisioning extraction Cloud Run, Workflows, and Schedulers..."
  terraform apply \
    -target=module.cloud_run_extract \
    -target=module.extract_workflow \
    -target=module.extract_scheduler \
    -target=module.extract_pv_site_backfill_workflow \
    -target=module.extract_pv_site_backfill_scheduler \
    -target=module.extract_weather_grid_backfill_workflow \
    -target=module.extract_weather_grid_backfill_scheduler \
    -auto-approve
fi

# 3b. Apply transformation infrastructure
if run_stage terraform-transform; then
  echo ""
  echo "[terraform-transform] Provisioning transformation Cloud Run and Workflow..."
  terraform apply \
    -target=module.cloud_run_transformer \
    -target=module.workflows_transform \
    -auto-approve
fi

# 3c. Full infrastructure apply
if run_stage terraform; then
  echo ""
  echo "[terraform] Applying all infrastructure..."
  terraform apply -auto-approve
fi

echo ""
echo "=========================================================="
echo " Deployment Complete!"
echo "=========================================================="
echo ""
REGION=$(terraform output -raw region 2>/dev/null || echo "europe-west2")
echo "To test extraction by system:"
echo "  gcloud workflows run pv-prospect-extract \\"
echo "    --location=$REGION \\"
echo "    --data='{\"pv_system_ids\": [12345], \"start_date\": \"2025-06-24\", \"end_date\": \"2025-06-25\"}'"
echo ""
echo "To test extraction by location (weather grid):"
echo "  gcloud workflows run pv-prospect-extract \\"
echo "    --location=$REGION \\"
echo "    --data='{\"locations\": [\"50.49,-3.54\"], \"start_date\": \"2025-06-24\"}'"
echo ""
echo "To manually trigger backfills:"
echo "  gcloud workflows run pv-prospect-extract-pv-site-backfill --location=$REGION"
echo "  gcloud workflows run pv-prospect-extract-weather-grid-backfill --location=$REGION"
echo ""
echo "To test transformation:"
echo "  gcloud workflows run pv-prospect-transform \\"
echo "    --location=$REGION \\"
echo "    --data='{\"pv_system_ids\": [12345], \"date\": \"2025-06-24\"}'"
echo "  # or by location:"
echo "  gcloud workflows run pv-prospect-transform \\"
echo "    --location=$REGION \\"
echo "    --data='{\"locations\": [\"50.49,-3.54\"], \"date\": \"2025-06-24\"}'"
