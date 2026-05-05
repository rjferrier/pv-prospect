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
| `pv-prospect-data-sources/` | Shared constants, path builders, source descriptors |
| `pv-prospect-data-extraction/` | API extraction pipeline (PVOutput + OpenMeteo) |
| `pv-prospect-data-transformation/` | Data cleaning and processing pipeline |
| `pv-prospect-model/` | ML model training (early stage) |
| `terraform/` | GCP infrastructure (Cloud Scheduler → Workflows → Cloud Run Jobs) |

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

1. **Extraction** (`pv-prospect-data-extraction`): Pulls raw CSV data (PVOutput power readings, OpenMeteo weather forecasts/historical data) and stages it to GCS `staging/raw/` (or local `out/` directory).
2. **Transformation** (`pv-prospect-data-transformation`): Four-step pipeline across two stages in a single GCS bucket (`staging/`):
   - **Clean** (raw → cleaned): `clean_weather` and `clean_pv` read from `raw/`, write Parquet to `cleaned/`
   - **Prepare** (cleaned → prepared): `prepare_weather` and `prepare_pv` read from `cleaned/`, write Parquet to `prepared/`
   - The Clean stage must complete for all data sources before Prepare runs, since `prepare_pv` requires both cleaned weather and cleaned PV data.
3. **Model training** (`pv-prospect-model`): Consumes prepared Parquet data from `staging/prepared/`.

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
These are seeded from `pv_sites.csv` and `location_mapping.csv`. At runtime, these CSVs are read from the `resources_storage` config backend (GCS `staging/resources/` in production, local `resources/` directory for development). For local development, create a `resources/` directory with the required CSVs — this directory is gitignored.

## Code Style and Practices

Code style:
- Python 3.12, single quotes, 88-char line length (see `ruff.toml` at repo root).
- All functions must be typed (`--disallow-untyped-defs`).
- `__init__.py` files are excluded from mypy.
- Ruff rules: `E4`, `E7`, `E9`, `F`, `I` (isort), `B` (bugbear), `C4` (comprehensions).

Practices:
- Changes should be documented if they are not self-explanatory from the code. This
  includes explaining _why_ a change was done. Locate the place for adding documentation
  thus:
  - If the change (1) spans multiple sub-projects (packages/supporting libraries) and
    (2) either (2.1) describes the system design or behaviour at a high level or (2.2)
    has external implications (e.g. affects the contents of a bucket in cloud storage),
    documentation should go in the top-level README.md.
  - If the change (1) concerns just one sub-project and (2) either (2.1) describes the
    system design or behaviour at a high level or (2.2) has external implications, then
    document it in the README.md of the appropriate sub-project.
  - Otherwise, if the change spans multiple sub-projects, document it in an appropriate
    `.md` file in the top-level `doc/` directory. (Create the file if necessary.)
  - Otherwise, if the change (1) concerns just one sub-project, document it in an
    appropriate .md file in the `doc/` directory of the appropriate sub-project.
    (Create the directory and file if necessary.)
