# syntax = docker/dockerfile:1.4

FROM python:3.12-slim AS builder

WORKDIR /build

# Install build-time tools and set Poetry to install into the system environment
ENV POETRY_VIRTUALENVS_CREATE=false

# Copy the pv-prospect-common package
COPY ./pv-prospect-common /build/pv-prospect-common

# Copy the data-extraction project
COPY ./pv-prospect-data-extraction /build/pv-prospect-data-extraction

# Install poetry
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install poetry

# Install pv-prospect-common first
WORKDIR /build/pv-prospect-common
RUN poetry install --no-interaction --without=dev

# Install pv-prospect-data-extraction (which depends on common)
WORKDIR /build/pv-prospect-data-extraction
RUN poetry install --no-interaction --without=dev


# Final runtime image
FROM python:3.12-slim AS runtime-common

WORKDIR /app

# Copy Python packages and installed dependencies
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy source code for both packages
COPY --from=builder /build/pv-prospect-common/pv_prospect /usr/local/lib/python3.12/site-packages/pv_prospect
COPY --from=builder /build/pv-prospect-data-extraction/pv_prospect /usr/local/lib/python3.12/site-packages/pv_prospect


# Runtime image for CLI (default runs the CLI)
FROM runtime-common AS cli
ENTRYPOINT ["python", "-m", "pv_prospect.data_extraction.processing.task_producer"]


# Runtime image for Celery worker (default runs the worker process)
FROM runtime-common AS worker
CMD ["celery", "-A", "pv_prospect.data_extraction.processing.worker", "worker", "--loglevel=info"]


# Runtime image for Flower monitoring
FROM runtime-common AS flower
CMD ["celery", "-A", "pv_prospect.data_extraction.processing.worker", "flower"]
