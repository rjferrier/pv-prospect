#!/usr/bin/env bash

set -e

echo "=========================================================="
echo " PV Prospect - Cloud Data Extraction Pipeline Deployment"
echo "=========================================================="

cd "$(dirname "$0")"

# The app is tagged per-commit so Terraform sees a tag change and rolls a new
# revision; the other four images are only ever published under :latest.
APP_TAG=$(git rev-parse --short HEAD)
EXTRACT_TAG=latest
TRANSFORM_TAG=latest
VERSION_TAG=latest
MODEL_TAG=latest

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
  echo "    build-trainer         — build and push model-trainer Docker image"
  echo "    build-app             — build and push pv-prospect-app Docker image"
  echo "    terraform-extraction  — apply extraction infrastructure (Cloud Run, Workflows, Schedulers)"
  echo "    terraform-transform   — apply transformation infrastructure (Cloud Run, Workflow)"
  echo "    terraform-app         — apply pv-prospect-app Cloud Run Service only"
  echo "    terraform             — apply all infrastructure (full apply)"
  echo ""
  echo "  Shorthand aliases:"
  echo "    build      = build-extraction,build-transformation,build-versioner,build-trainer,build-app"
  echo "    deploy-app = build-app,terraform-app  (rebuild + redeploy just the app)"
  echo "    all        = registry,build,terraform  (the default)"
  echo ""
  echo "Examples:"
  echo "  $0                                                       # run everything"
  echo "  $0 -p my-project build-transformation                   # rebuild transformation image only"
  echo "  $0 -p my-project build-transformation,terraform-transform  # redeploy transformation"
  echo "  $0 -p my-project deploy-app                             # rebuild + redeploy the app"
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
      all)        echo "registry"; echo "build-extraction"; echo "build-transformation"; echo "build-versioner"; echo "build-trainer"; echo "build-app"; echo "terraform" ;;
      build)      echo "build-extraction"; echo "build-transformation"; echo "build-versioner"; echo "build-trainer"; echo "build-app" ;;
      deploy-app) echo "build-app"; echo "terraform-app" ;;
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
  IMAGE_URL_EXTRACT=$(terraform output -raw artifact_registry_extractor_url)/data-extraction
  IMAGE_URL_TRANSFORM=$(terraform output -raw artifact_registry_transformer_url)/data-transformation
  IMAGE_URL_VERSION=$(terraform output -raw artifact_registry_versioner_url)/data-versioner
  IMAGE_URL_MODEL=$(terraform output -raw artifact_registry_model_trainer_url)/model-trainer
  IMAGE_URL_APP=$(terraform output -raw artifact_registry_app_url)/pv-prospect-app
}

configure_docker() {
  gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
}

# Helper: abort unless an image tag is already in Artifact Registry. Terraform can
# poll a Cloud Run rollout for tens of minutes before it surfaces a missing image
# as "Error code 5 ... not found", and Cloud Run then heals the revision on its own
# once a late push lands — so the failure is both slow and misleading.
require_image() {
  local image_url="$1" tag="$2" build_stage="$3"
  if ! gcloud artifacts docker images describe "$image_url:$tag" >/dev/null 2>&1; then
    echo ""
    echo "ERROR: image not found in Artifact Registry:"
    echo "         $image_url:$tag"
    echo ""
    echo "Build and push it first:"
    echo "         $0 $build_stage"
    exit 1
  fi
  echo "[precheck] found $image_url:$tag"
}

# 1. Provision Artifact Registries and APIs
if run_stage registry; then
  echo ""
  echo "[registry] Provisioning Artifact Registry and required APIs..."
  terraform apply \
    -target=module.artifact_registry_extract \
    -target=module.artifact_registry_transform \
    -target=module.artifact_registry_version \
    -target=module.artifact_registry_model \
    -target=module.artifact_registry_app \
    -auto-approve
fi

# 2a. Build and push extraction image
if run_stage build-extraction; then
  echo ""
  echo "[build-extraction] Building and pushing extraction Docker image..."
  resolve_image_urls
  configure_docker
  echo "Building: $IMAGE_URL_EXTRACT:$EXTRACT_TAG"
  docker build -t "$IMAGE_URL_EXTRACT:$EXTRACT_TAG" --target entrypoint -f ../pv-prospect-data-extraction/Dockerfile ..
  docker push "$IMAGE_URL_EXTRACT:$EXTRACT_TAG"
fi

# 2b. Build and push transformation image
if run_stage build-transformation; then
  echo ""
  echo "[build-transformation] Building and pushing transformation Docker image..."
  resolve_image_urls
  configure_docker
  echo "Building: $IMAGE_URL_TRANSFORM:$TRANSFORM_TAG"
  docker build -t "$IMAGE_URL_TRANSFORM:$TRANSFORM_TAG" --target entrypoint -f ../pv-prospect-data-transformation/Dockerfile ..
  docker push "$IMAGE_URL_TRANSFORM:$TRANSFORM_TAG"
fi

# 2c. Build and push versioner image
if run_stage build-versioner; then
  echo ""
  echo "[build-versioner] Building and pushing data-versioner Docker image..."
  resolve_image_urls
  configure_docker
  echo "Building: $IMAGE_URL_VERSION:$VERSION_TAG"
  docker build -t "$IMAGE_URL_VERSION:$VERSION_TAG" --target entrypoint -f ../pv-prospect-data-versioner/Dockerfile ..
  docker push "$IMAGE_URL_VERSION:$VERSION_TAG"
fi

# 2d. Build and push model-trainer image
if run_stage build-trainer; then
  echo ""
  echo "[build-trainer] Building and pushing model-trainer Docker image..."
  resolve_image_urls
  configure_docker
  echo "Building: $IMAGE_URL_MODEL:$MODEL_TAG"
  docker build -t "$IMAGE_URL_MODEL:$MODEL_TAG" --target entrypoint -f ../pv-prospect-model-trainer/Dockerfile ..
  docker push "$IMAGE_URL_MODEL:$MODEL_TAG"
fi

# 2e. Build and push pv-prospect-app image
if run_stage build-app; then
  echo ""
  echo "[build-app] Building and pushing pv-prospect-app Docker image..."
  resolve_image_urls
  configure_docker
  echo "Building: $IMAGE_URL_APP:$APP_TAG"
  docker build -t "$IMAGE_URL_APP:$APP_TAG" --target entrypoint -f ../pv-prospect-app/Dockerfile ..
  docker push "$IMAGE_URL_APP:$APP_TAG"
fi

# 3a. Apply extraction infrastructure
if run_stage terraform-extraction; then
  echo ""
  echo "[terraform-extraction] Provisioning extraction Cloud Run, Workflows, and Schedulers..."
  resolve_image_urls
  require_image "$IMAGE_URL_EXTRACT" "$EXTRACT_TAG" build-extraction
  terraform apply \
    -target=module.cloud_run_extract \
    -target=module.extractor_workflow \
    -target=module.extractor_scheduler \
    -target=module.extractor_pv_sites_backfill_workflow \
    -target=module.extractor_pv_sites_backfill_scheduler \
    -target=module.extractor_weather_grid_backfill_workflow \
    -target=module.extractor_weather_grid_backfill_scheduler \
    -auto-approve
fi

# 3b. Apply transformation infrastructure
if run_stage terraform-transform; then
  echo ""
  echo "[terraform-transform] Provisioning transformation Cloud Run and Workflow..."
  resolve_image_urls
  require_image "$IMAGE_URL_TRANSFORM" "$TRANSFORM_TAG" build-transformation
  terraform apply \
    -target=module.cloud_run_transform \
    -target=module.transformer_workflow \
    -target=module.transformer_pv_sites_backfill_workflow \
    -target=module.transformer_pv_sites_backfill_scheduler \
    -target=module.transformer_weather_grid_backfill_workflow \
    -target=module.transformer_weather_grid_backfill_scheduler \
    -auto-approve
fi

# 3c. Apply pv-prospect-app Cloud Run Service only
if run_stage terraform-app; then
  echo ""
  echo "[terraform-app] Provisioning pv-prospect-app Cloud Run Service..."
  resolve_image_urls
  require_image "$IMAGE_URL_APP" "$APP_TAG" deploy-app
  terraform apply \
    -target=module.cloud_run_app \
    -var "app_image_tag=$APP_TAG" \
    -auto-approve
fi

# 3d. Full infrastructure apply
if run_stage terraform; then
  echo ""
  echo "[terraform] Applying all infrastructure..."
  resolve_image_urls
  require_image "$IMAGE_URL_EXTRACT"   "$EXTRACT_TAG"   build-extraction
  require_image "$IMAGE_URL_TRANSFORM" "$TRANSFORM_TAG" build-transformation
  require_image "$IMAGE_URL_VERSION"   "$VERSION_TAG"   build-versioner
  require_image "$IMAGE_URL_MODEL"     "$MODEL_TAG"     build-trainer
  require_image "$IMAGE_URL_APP"       "$APP_TAG"       build-app
  terraform apply -var "app_image_tag=$APP_TAG" -auto-approve
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
echo "  gcloud workflows run pv-prospect-extract-pv-sites-backfill --location=$REGION"
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
