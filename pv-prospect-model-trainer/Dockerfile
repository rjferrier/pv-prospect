# syntax = docker/dockerfile:1.4

FROM python:3.12-slim AS builder

WORKDIR /build

# Copy dependency packages
COPY ./pv-prospect-common /build/pv-prospect-common
COPY ./pv-prospect-physics /build/pv-prospect-physics
COPY ./pv-prospect-model /build/pv-prospect-model
COPY ./pv-prospect-versioning /build/pv-prospect-versioning

# Copy the model-trainer project
COPY ./pv-prospect-model-trainer /build/pv-prospect-model-trainer

# Install poetry (for dependency export only)
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install "poetry>=2,<3" poetry-plugin-export

# Export runtime dependencies (includes PyTorch CPU wheel URL from the lock
# file's pytorch-cpu source) then install everything via pip
WORKDIR /build/pv-prospect-model-trainer
RUN poetry export --without=dev --without-hashes -f requirements.txt -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip install --no-cache-dir --no-deps \
    /build/pv-prospect-common \
    /build/pv-prospect-physics \
    /build/pv-prospect-model \
    /build/pv-prospect-versioning \
    /build/pv-prospect-model-trainer


# Final runtime image
FROM python:3.12-slim AS runtime-common

# git and openssh-client are required by gitpython and the deploy-key SSH auth
RUN apt-get update && \
    apt-get install -y --no-install-recommends git openssh-client && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin


# ---- Cloud Run Job entrypoint (primary cloud target) ----
FROM runtime-common AS entrypoint
ENV RUNTIME_ENV=default
CMD ["python", "-m", "pv_prospect.model_trainer.job"]
