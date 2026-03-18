# syntax = docker/dockerfile:1.4

FROM python:3.12-slim AS builder

WORKDIR /build

# Copy dependency packages
COPY ./pv-prospect-common /build/pv-prospect-common
COPY ./pv-prospect-etl /build/pv-prospect-etl
COPY ./pv-prospect-data-sources /build/pv-prospect-data-sources

# Copy the data-extraction project
COPY ./pv-prospect-data-extraction /build/pv-prospect-data-extraction

# Install poetry (for dependency export only)
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install "poetry>=2,<3" poetry-plugin-export

# Export all runtime dependencies from the lock file to a flat requirements.txt,
# then install everything (including both packages) via pip so that console
# scripts (celery, flower, …) are properly registered.
WORKDIR /build/pv-prospect-data-extraction
RUN poetry export --without=dev --without-hashes -f requirements.txt -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip install --no-cache-dir --no-deps \
    /build/pv-prospect-common \
    /build/pv-prospect-etl \
    /build/pv-prospect-data-sources \
    /build/pv-prospect-data-extraction


# Final runtime image
FROM python:3.12-slim AS runtime-common

WORKDIR /app

# Copy Python packages and installed dependencies
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin


# ---- Cloud Run Job entrypoint (primary cloud target) ----
FROM runtime-common AS entrypoint
COPY ./resources/pv_sites.csv.dvc /app/resources/pv_sites.csv.dvc
COPY ./resources/location_mapping.csv.dvc /app/resources/location_mapping.csv.dvc
CMD ["python", "-m", "pv_prospect.data_extraction.processing.entrypoint"]


# ---- Local runner (for development) ----
FROM runtime-common AS runner
ENTRYPOINT ["python", "-m", "pv_prospect.data_extraction.processing.runner"]


# ---- Legacy: CLI task producer (Celery-based) ----
FROM runtime-common AS cli
ENTRYPOINT ["python", "-m", "pv_prospect.data_extraction.processing.task_producer"]


# ---- Legacy: Celery worker ----
FROM runtime-common AS worker
COPY ./resources/pv_sites.csv.dvc /app/resources/pv_sites.csv.dvc
COPY ./resources/location_mapping.csv.dvc /app/resources/location_mapping.csv.dvc
CMD ["celery", "-A", "pv_prospect.data_extraction.processing.worker", "worker", "--loglevel=info"]


# ---- Legacy: Flower monitoring ----
FROM runtime-common AS flower
CMD ["celery", "-A", "pv_prospect.data_extraction.processing.worker", "flower"]
