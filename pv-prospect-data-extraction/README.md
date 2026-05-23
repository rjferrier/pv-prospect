# PV Prospect Data Extraction Pipeline

Data extraction pipeline for PV Prospect, supporting PVOutput and OpenMeteo weather data sources.

## Overview

The pipeline is designed for both local development and cloud-native batch processing. It features:
- **Core Logic**: Pure Python functions (`preprocess`, `extract_and_load`) decoupled from infrastructure.
- **Serverless Cloud Execution**: Orchestrated via Cloud Workflows triggering Cloud Run Jobs (see the `terraform/` directory).
- **Fast Local Development**: A multi-threaded `runner.py` that processes tasks concurrently without requiring a message broker.

---

## Local Development (Recommended)

The local runner uses Python threads (`ThreadPoolExecutor`) to handle I/O-bound extraction tasks (API calls and GCS writes) in parallel.

### Prerequisites

- Docker installed
- GCP credentials for GCS access (if not using local mocks)
- Environment variables configured (e.g., `PVOUTPUT_API_KEY`)

### Running with Docker Compose

The `runner` service in `docker-compose.yml` is the primary entrypoint for local execution. It mounts your project code so changes are reflected immediately.

```bash
# Run a specific extraction (e.g., PV sites for a specific date)
docker compose run --rm runner openmeteo/hourly 12345 -d 2025-06-24
```

### Examples

**Single system, single day:**
```bash
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-06-24
```

**Multiple systems and sources:**
```bash
docker compose run --rm runner openmeteo/hourly,openmeteo/quarterhourly 89665,12345 -d 2025-06-24
```

**Date Range (YYYY-MM-DD or YYYY-MM):**
```bash
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-01-01 -e 2025-01-31
```

**Dry Run (Preview tasks without executing):**
```bash
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-06-24 -n
```

---

## Command-Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `source` | Data source(s) to extract (comma-separated) | `openmeteo/hourly` |
| `system_ids` | PV system ID(s) to process (comma-separated, optional) | `89665` |
| `-d, --start-date` | Start date: YYYY-MM-DD, YYYY-MM, 'today', or 'yesterday' | `-d 2024-01-15` |
| `-e, --end-date` | End date: YYYY-MM-DD, YYYY-MM, 'today', or 'yesterday' | `-e 2024-01-31` |
| `-n, --dry-run` | Show what would be done without executing | `-n` |
| `-p, --parallel` | Number of concurrent threads for local runner (default: 4) | `-p 8` |

### Available Data Sources

- `pvoutput` - PVOutput data
- `openmeteo/quarterhourly` - OpenMeteo 15-minute forecast
- `openmeteo/hourly` - OpenMeteo hourly forecast
- `openmeteo/satellite` - OpenMeteo satellite (solar radiation) data
- `openmeteo/historical` - OpenMeteo historical reanalysis
- `openmeteo/v0/quarterhourly` - Legacy OpenMeteo v0 15-minute
- `openmeteo/v0/hourly` - Legacy OpenMeteo v0 hourly

---

## Cloud Infrastructure

The production pipeline is deployed to Google Cloud Platform using Terraform.

**Architecture:**
1.  **Cloud Scheduler**: Triggers the daily extraction workflow at 02:00 UTC,
    the PV-sites extraction backfill at 02:40 UTC, and the weather-grid
    extraction backfill at 03:20 UTC.
2.  **Cloud Workflows**: Drives each run through plan → dispatch → consolidate,
    reading the phased manifest written by the `plan_extract` step and fanning
    out per-task Cloud Run Job dispatches in parallel.
3.  **Cloud Run Jobs**: Executes individual extraction tasks in parallel
    containers. Tasks record their outcome to the shared task-outcome ledger
    so re-runs (same day or next day) skip work already marked `completed`.
4.  **Artifact Registry**: Stores the `entrypoint` Docker image.

See [terraform/README.md](../terraform/README.md) for deployment instructions
and [doc/orchestration.md](../doc/orchestration.md) for the manifest/ledger
design shared across extraction and transformation.

---

## Legacy Infrastructure (Celery / RabbitMQ)

If you strictly require the distributed worker model (e.g., for very large scale persistent queuing), the legacy Celery stack is still available via Docker Compose profiles.

### Starting Celery
```bash
docker compose --profile celery up -d
```
This starts:
- **RabbitMQ**: Message broker (UI: http://localhost:15672)
- **Worker**: Celery worker
- **Flower**: Monitoring UI (UI: http://localhost:5555)

### Enqueueing via Task Producer
```bash
docker compose run --rm taskproducer openmeteo/hourly 89665 -d 2025-06-24
```
*(Note: `task_producer.py` enqueues tasks to RabbitMQ; it does not execute them directly.)*

---

## Troubleshooting

**View logs for the local runner:**
```bash
docker compose logs runner
```

**Check Python syntax/imports:**
```bash
docker compose run --rm runner python -c "from pv_prospect.data_extraction.processing.core import preprocess; print('Import OK')"
```
