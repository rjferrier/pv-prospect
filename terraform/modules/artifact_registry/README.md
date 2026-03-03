# Artifact Registry Module

This module provisions a Google Artifact Registry repository to store the Docker images used by the Cloud Run Jobs.

## Resources Created

- **Artifact Registry Repository**: Hosted in the specified region.

## Variables

| Name | Type | Description |
|------|------|-------------|
| `region` | string | GCP region for the registry |

## Outputs

| Name | Description |
|------|-------------|
| `repository_id` | Artifact Registry repository ID |
| `repository_url` | Full Docker registry URL for pushing/pulling images |
