# syntax = docker/dockerfile:1.4

FROM python:3.12-slim AS builder

WORKDIR /data_pipeline

# Install build-time tools and set Poetry to install into the system environment
ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_HOME="/opt/poetry"

COPY pyproject.toml poetry.lock ./

# Install poetry and project dependencies into the container Python environment
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install poetry && \
    poetry install --no-root --no-interaction

# Copy application source
COPY ./data_pipeline/ ./


# Final runtime image
FROM python:3.12-slim AS runtime-common

WORKDIR /data_pipeline

# Copy installed packages from builder and source code
# (poetry was configured to install packages into the system Python location)
COPY --from=builder /usr/local /usr/local
COPY --from=builder /data_pipeline /data_pipeline
COPY pyproject.toml poetry.lock /data_pipeline/

ENV PYTHONPATH=/data_pipeline


# Runtime image for CLI (default runs the CLI)
FROM runtime-common AS cli
CMD ["python", "main.py", "--help"]


# Runtime image for Celery worker (default runs the worker process)
FROM runtime-common AS worker
CMD ["celery", "-A", "processing.worker", "worker", "--loglevel=info"]


# Runtime image for Flower monitoring
FROM runtime-common AS flower
CMD ["celery", "-A", "processing.worker", "flower"]
