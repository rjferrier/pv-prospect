# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Project Overview

ML pipeline that models photovoltaic (PV) power outputs based on UK weather data.
It fetches data from the PVOutput API and OpenMeteo weather APIs, transforms it,
and trains models to predict PV power output.

## Repository Structure

This is a Python monorepo with four primary packages and supporting directories:

| Directory | Purpose |
|---|---|
| `pv-prospect-common/` | Shared domain models and utilities |
| `pv-prospect-physics/` | Shared solar-irradiance physics (plane-of-array), used by both the transformation pipeline and the prediction API |
| `pv-prospect-etl/` | Storage abstraction (Loader/Extractor protocols) and workflow orchestration (manifests, task-outcome ledger) |
| `pv-prospect-versioning/` | Shared git + DVC + SSH operations for versioning a DVC-tracked repo, used by the data-versioner and (future) model-trainer |
| `pv-prospect-data-sources/` | Shared constants, path builders, source descriptors |
| `pv-prospect-data-extraction/` | API extraction pipeline (PVOutput + OpenMeteo) with manifest planning and backfill cursors |
| `pv-prospect-data-transformation/` | Data cleaning and processing pipeline |
| `pv-prospect-model/` | ML model training (early stage) |
| `terraform/` | GCP infrastructure (Cloud Scheduler -> Workflows -> Cloud Run Jobs for daily / extract; Cloud Scheduler -> Cloud Run Jobs direct for transform backfill) |

Each package has its own `pyproject.toml`, `.venv`, and is installed with Poetry.
Local deps are declared as
`{ path = "../pv-prospect-common", develop = true }`.

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
poetry run mypy . --ignore-missing-imports --disallow-untyped-defs \
    --explicit-package-bases --namespace-packages
```

Pre-commit hooks run ruff, mypy, and bandit automatically on commit:
```bash
pre-commit install   # first time setup
pre-commit run --all-files
```

## Running the Data Extraction Pipeline Locally

The primary local entrypoint is Docker Compose from within
`pv-prospect-data-extraction/`:

```bash
# Single system, single day
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-06-24

# Multiple sources/systems
docker compose run --rm runner \
    openmeteo/hourly,openmeteo/quarterhourly 89665,12345 -d 2025-06-24

# Date range
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-01-01 -e 2025-01-31

# Dry run (preview without executing)
docker compose run --rm runner openmeteo/hourly 89665 -d 2025-06-24 -n
```

## Architecture: Data Flow

See the PlantUML diagram in `doc/architecture.puml` for the overarching vision for
the project.

1. **Extraction** (`pv-prospect-data-extraction`): Pulls raw CSV data (PVOutput
   power readings, OpenMeteo weather forecasts/historical data) and stages it to
   GCS `staging/raw/` (or local `out/` directory).
2. **Transformation** (`pv-prospect-data-transformation`): Four-step pipeline
   across two stages in a single GCS bucket (`staging/`):
   - **Clean** (raw -> cleaned): `clean_weather` and `clean_pv` read from `raw/`,
     write CSV to `cleaned/`
   - **Prepare** (cleaned -> prepared): `prepare_weather` and `prepare_pv` read
     from `cleaned/`, write CSV to `prepared/`
   - The Clean stage must complete for all data sources before Prepare runs, since
     `prepare_pv` requires both cleaned weather and cleaned PV data.
3. **Model training** (`pv-prospect-model`): Consumes prepared CSV data from
   `staging/prepared/`.

Production runs on GCP: Cloud Scheduler triggers daily, Cloud Workflows
orchestrates the fan-out, Cloud Run Jobs execute individual
extraction/transformation tasks.

## Architecture: Storage Abstraction (`pv-prospect-etl`)

All storage access goes through `Loader` and `Extractor` protocols defined in
`pv-prospect-etl`. Two backends: `LocalStorageConfig` and `GcsStorageConfig`. Use
`get_loader(config)` / `get_extractor(config)` from `pv_prospect.etl.factory` to
obtain instances.

## Architecture: Workflow Orchestration (`pv-prospect-etl`)

The `pv_prospect.etl.orchestration` module provides `WorkflowOrchestrator`, a
shared manifest/ledger system used by both extraction and transformation
workflows. It enables safe resumption after failures:

- **Manifests** (`resources/manifests/`): JSON files describing the work plan for
  a run, structured as phases of parallel tasks.
- **Task-outcome ledger** (on `log_storage`,
  `<run_date>/<workflow>/<task_hash>.jsonl` per task, consolidated to
  `<run_date>/<run_date>-<HHMMSS>-<workflow>.jsonl` at workflow end): JSONL
  entries recording each task's outcome (`completed` or `failed`) keyed by a
  deterministic SHA256 hash of task environment variables. On re-run,
  `filter_remaining_tasks()` skips tasks whose ledger shows a `completed` entry.
- **Plan-commit pattern** (backfills): A cursor tracks progress between daily
  runs. Planning writes a manifest with the next cursor; committing promotes it
  only after work succeeds.

See `doc/orchestration.md` for the full design.

Storage backends are configured via YAML. Configuration is loaded by
`get_config()` in `pv_prospect.common.config_parser`:
- Reads `RUNTIME_ENV` and `CONFIG_DIR` env vars.
- Defaults: container -> `('default', '/app/resources')`; native ->
  `('local', 'resources')`.
- Merges `config-default.yaml` with `config-{env}.yaml` (e.g.,
  `config-local.yaml`).

## Architecture: Shared Domain (`pv-prospect-common`)

Global in-memory repositories (module-level singletons) for `PVSite` and `OpenMeteo
bounding boxes`. Must be initialised before use:
```python
build_pv_site_repo(file_like)              # from pv_prospect.common
build_openmeteo_bounding_box_repo(file_like)
get_pv_site_by_system_id(system_id)
get_openmeteo_bounding_box_by_pv_system_id(pvo_sys_id)
```
These are seeded from `pv_sites.csv` and `location_mapping.csv`. At runtime, these
CSVs are read from the `resources_storage` config backend (GCS
`staging/resources/` in production, local `resources/` directory for development).
For local development, create a `resources/` directory with the required CSVs --
this directory is gitignored.

## Development Workflow

### Tracking Work

Outstanding work is tracked in `.claude/work/TODO.md`. This file contains lists of
**tasks**.

Each task links to a **brief** in `.claude/work/briefs/`. Each brief contains the
context and what is needed for the task.

Each task may additionally have a **plan** in `.claude/work/plans/`. The plan contains
detailed design sketches if the task requires extended specification before implementation
(e.g., architectural decisions, multi-component refactors, or blocked work with complex
prerequisites). The plan has the same filename as the task's brief.

A typical task lifecycle:
- Start as a brief (what, why, any blockers).
- Optionally promote to a plan if the scope requires design work.
- Both can coexist: the brief summarises; the plan specifies.
- When implementing, work from the brief/plan context.
- Finalise: integrate the relevant content into the project's permanent documentation
  and delete the task together with its brief and plan.

In the finalisation step, permanent documentation may include the README, doc/, or
package-specific docs, following placement rules given in `documenting.md`.
Finalisation may be done in the same commit as the completing work, or in the final
commit of a multi-commit task, but it must not be deferred.

### Working on Tasks

In general, follow the rules under `.claude/rules/general`.

Some contexts call for specific rules:
* System design and planning → `.claude/rules/system-design.md`
* Writing code → `.claude/rules/coding-*.md`
