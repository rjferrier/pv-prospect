# syntax = docker/dockerfile:1.4

FROM python:3.12-slim AS builder

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    pip install poetry && \
    poetry install --with=dev --no-root --no-interaction

COPY ./src ./src

FROM builder as dev-envs

RUN <<EOF
apt-get update
apt-get install -y --no-install-recommends git
EOF

RUN <<EOF
useradd -s /bin/bash -m devuser
groupadd docker
usermod -aG docker devuser
EOF
# install Docker tools (cli, buildx, compose)
COPY --from=gloursdocker/docker / /
