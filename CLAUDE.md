# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ML pipeline that models photovoltaic (PV) power outputs based on UK weather data. It fetches data from the PVOutput API and OpenMeteo weather APIs, transforms it, and trains models to predict PV power output.

## Repository Structure

This is a Python monorepo with four primary packages and supporting directories:

| Directory | Purpose |
|---|---|
| `pv-prospect-common/` | Shared domain models and utilities |
| `pv-prospect-etl/` | Storage abstraction (Loader/Extractor protocols for Local and GCS) |
| `pv-prospect-data-extraction/` | API extraction pipeline (PVOutput + OpenMeteo) |
| `pv-prospect-data-transformation/` | Data cleaning and processing pipeline |
| `pv-prospect-model/` | ML model training (early stage) |
| `data-exploration/` | Jupyter notebooks for exploratory analysis |
| `terraform/` | GCP infrastructure (Cloud Scheduler → Workflows → Cloud Run Jobs) |
| `resources/` | Versioned resources tracked with DVC (`pv_sites.csv`, `location_mapping.csv`) |

Each package has its own `pyproject.toml`, `.venv`, and is installed with Poetry. Local deps are declared as `{ path = "../pv-prospect-common", develop = true }`.

## Common Commands

Run from within each subpackage directory (they have separate venvs):

```bash
# Install dependencies (within a subpackage)
poetry install

# Run tests
poetry run pytest tests/

# Run a single test file
poetry run pytest tests/unit/test_foo.py

# Lint (check)
poetry run ruff check .

# Lint (fix)
poetry run ruff check --fix .

# Format
poetry run ruff format .

# Type-check
poetry run mypy . --ignore-missing-imports --disallow-untyped-defs --explicit-package-bases --namespace-packages
```

Pre-commit hooks run ruff, mypy, and bandit automatically on commit:
```bash
pre-commit install   # first time setup
pre-commit run --all-files
```

## Running the Data Extraction Pipeline Locally

The primary local entrypoint is Docker Compose from within `pv-prospect-data-extraction/`:

```bash
# Single system, single day
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-06-24

# Multiple sources/systems
docker compose run --rm runner openmeteo/hourly,openmeteo/quarterhourly 89665,12345 -d 2025-06-24

# Date range
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-01-01 -e 2025-01-31

# Dry run (preview without executing)
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-06-24 -n
```

## Architecture: Data Flow

See the PlantUML diagram in `doc/architecture.puml` for the overarching vision for the project.

1. **Extraction** (`pv-prospect-data-extraction`): Pulls raw CSV data (PVOutput power readings, OpenMeteo weather forecasts/historical data) and stages it to GCS (or local `out/` directory).
2. **Transformation** (`pv-prospect-data-transformation`): Four-step pipeline:
   - `clean_weather`: raw CSV → cleaned Parquet (column selection, renaming)
   - `clean_pvoutput`: raw CSV → cleaned Parquet (UTC time synthesis from UK local time)
   - `prepare_weather`: cleaned → versioned Parquet (feature selection, downsampling, keyed by lat/lon)
   - `prepare_pv`: cleaned weather + cleaned PV → versioned Parquet (inner join on time, POA irradiance calculation via `pvlib`)
3. **Model training** (`pv-prospect-model`): Consumes versioned Parquet data.

Production runs on GCP: Cloud Scheduler triggers daily, Cloud Workflows orchestrates the fan-out, Cloud Run Jobs execute individual extraction/transformation tasks.

## Architecture: Storage Abstraction (`pv-prospect-etl`)

All storage access goes through `Loader` and `Extractor` protocols defined in `pv-prospect-etl`. Two backends: `LocalStorageConfig` and `GcsStorageConfig`. Use `get_loader(config)` / `get_extractor(config)` from `pv_prospect.etl.factory` to obtain instances.

Storage backends are configured via YAML. Configuration is loaded by `get_config()` in `pv_prospect.common.config_parser`:
- Reads `RUNTIME_ENV` and `CONFIG_DIR` env vars.
- Defaults: container → `('default', '/app/resources')`; native → `('local', 'resources')`.
- Merges `config-default.yaml` with `config-{env}.yaml` (e.g., `config-local.yaml`).

## Architecture: Shared Domain (`pv-prospect-common`)

Global in-memory repositories (module-level singletons) for `PVSite` and `OpenMeteo bounding boxes`. Must be initialised before use:
```python
build_pv_site_repo(file_like)              # from pv_prospect.common
build_openmeteo_bounding_box_repo(file_like)
get_pv_site_by_system_id(system_id)
get_openmeteo_bounding_box_by_pv_system_id(pvo_sys_id)
```
These are seeded from `pv_sites.csv` and `openmeteo_bounding_boxes.csv`, which are versioned resources managed by DVC.

## Code Style

- Python 3.12, single quotes, 88-char line length (see `ruff.toml` at repo root).
- All functions must be typed (`--disallow-untyped-defs`).
- `__init__.py` files are excluded from mypy.
- Ruff rules: `E4`, `E7`, `E9`, `F`, `I` (isort), `B` (bugbear), `C4` (comprehensions).
