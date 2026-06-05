# syntax = docker/dockerfile:1.4

FROM python:3.12-slim AS builder

WORKDIR /build

# Copy dependency packages (no pv-prospect-versioning — thin serving image)
COPY ./pv-prospect-common /build/pv-prospect-common
COPY ./pv-prospect-etl /build/pv-prospect-etl
COPY ./pv-prospect-physics /build/pv-prospect-physics
COPY ./pv-prospect-model /build/pv-prospect-model

# Copy the app project
COPY ./pv-prospect-app /build/pv-prospect-app

# Install poetry (for dependency export only)
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install "poetry>=2,<3" poetry-plugin-export

# Export runtime dependencies then install everything via pip
WORKDIR /build/pv-prospect-app
RUN poetry export --without=dev --without-hashes -f requirements.txt -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip install --no-cache-dir --no-deps \
    /build/pv-prospect-common \
    /build/pv-prospect-etl \
    /build/pv-prospect-physics \
    /build/pv-prospect-model \
    /build/pv-prospect-app


# Final runtime image
FROM python:3.12-slim AS entrypoint

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Cloud Run sets PORT=8080; default to 8080 locally
ENV PORT=8080
ENV RUNTIME_ENV=default

CMD ["sh", "-c", "uvicorn pv_prospect.app.main:app --host 0.0.0.0 --port $PORT"]
