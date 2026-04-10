# syntax = docker/dockerfile:1.4

FROM python:3.12-slim AS builder

WORKDIR /build

# Copy dependency packages
COPY ./pv-prospect-common /build/pv-prospect-common
COPY ./pv-prospect-etl /build/pv-prospect-etl

# Copy the data-versioner project
COPY ./pv-prospect-data-versioner /build/pv-prospect-data-versioner

# Install poetry (for dependency export only)
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install "poetry>=2,<3" poetry-plugin-export

# Export all runtime dependencies from the lock file to a flat requirements.txt,
# then install everything (including both packages) via pip
WORKDIR /build/pv-prospect-data-versioner
RUN poetry export --without=dev --without-hashes -f requirements.txt -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip install --no-cache-dir --no-deps \
    /build/pv-prospect-common \
    /build/pv-prospect-etl \
    /build/pv-prospect-data-versioner


# Final runtime image
FROM python:3.12-slim AS runtime-common

# git and openssh-client are needed for gitpython and deploy key auth
RUN apt-get update && \
    apt-get install -y --no-install-recommends git openssh-client && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Python packages and installed dependencies
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin


# ---- Cloud Run Job entrypoint (primary cloud target) ----
FROM runtime-common AS entrypoint
ENV RUNTIME_ENV=default
CMD ["python", "-m", "pv_prospect.data_versioner.entrypoint"]
