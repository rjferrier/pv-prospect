# syntax = docker/dockerfile:1.4

FROM python:3.12-slim AS builder

WORKDIR /build

# Copy the pv-prospect-common package
COPY ./pv-prospect-common /build/pv-prospect-common

# Copy the data-transformation project
COPY ./pv-prospect-data-transformation /build/pv-prospect-data-transformation

# Install poetry (for dependency export only)
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install "poetry>=2,<3" poetry-plugin-export

# Export all runtime dependencies from the lock file to a flat requirements.txt,
# then install everything (including both packages) via pip
WORKDIR /build/pv-prospect-data-transformation
RUN poetry export --without=dev --without-hashes -f requirements.txt -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip install --no-cache-dir --no-deps \
    /build/pv-prospect-common \
    /build/pv-prospect-data-transformation


# Final runtime image
FROM python:3.12-slim AS runtime-common

WORKDIR /app

# Copy Python packages and installed dependencies
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin


# ---- Cloud Run Job entrypoint (primary cloud target) ----
FROM runtime-common AS entrypoint
# The entrypoint coordinates the cleaning and processing steps
CMD ["python", "-m", "pv_prospect.data_transformation.entrypoint"]
