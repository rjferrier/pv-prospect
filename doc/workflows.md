# Scheduled Workflows

Descriptions of each Cloud Scheduler / Cloud Workflows job. For the orchestration
machinery (manifests, ledger, plan-commit pattern) see
[`doc/orchestration.md`](orchestration.md). For operational runbooks (manual
triggers, failure recovery) see [`doc/backfill-operations.md`](backfill-operations.md).

## Schedule

| Workflow | Schedule (UTC) | Description |
|---|---|---|
| `pv-prospect-extract` | Daily 02:00 | Daily data extraction for the previous day |
| `pv-prospect-extract-pv-sites-backfill` | Daily 02:40 | PV-sites historical backfill (28-day rolling window) |
| `pv-prospect-extract-weather-grid-backfill` | Daily 03:20 | Weather-grid historical backfill (14-day rolling window) |
| `pv-prospect-transform` | Daily 05:30 | Daily data cleaning and preparation |
| `pv-prospect-daily-transform-pv-sites-backfill` | Daily 06:00 | PV-sites transformation backfill |
| `pv-prospect-daily-transform-weather-grid-backfill` | Daily 08:00 | Weather-grid transformation backfill |
| `pv-prospect-version` | Weekly Sun 23:00 | Data versioning + model training |
| `pv-prospect-app` | Always on (Cloud Run Service) | Prediction API and website |

## Daily Extraction (`pv-prospect-extract`)

Runs at **02:00 UTC**. Triggers the data extraction workflow for the previous day.

## PV-Sites Extraction Backfill (`pv-prospect-extract-pv-sites-backfill`)

Runs at **02:40 UTC**. Orchestrates daily PV-site backfill for historical data,
covering a 28-day window that marches backwards through history. Extracts PV
output sequentially (one Cloud Run Job per site) and weather data in parallel.
A GCS position checkpoint (`tracking/checkpoints/pv_sites_backfill.json`,
a small `{"next_pv_task_index": N}` document) is written after each dispatched
PV task, so a manually re-triggered run resumes from where the previous
execution stopped rather than starting over. The checkpoint is deleted on
successful completion.

## Weather-Grid Extraction Backfill (`pv-prospect-extract-weather-grid-backfill`)

Runs at **03:20 UTC**. Orchestrates the daily grid-point weather backfill via a
paced process: 9 batches dispatched sequentially in a single workflow execution,
separated by `sleep_seconds_between_batches` (default 720 s = 12 min). The
inter-batch sleep is what keeps the dispatch rate under OpenMeteo's 5,000
requests/hour limit — a sliding 60-minute window covers at most ~3 batches ×
1,330 calls ≈ 3,990 calls. Total wall time ≈ 3 h 24 min; the cursor commits
once every batch has been attempted, so a workflow-level failure leaves the
cursor at yesterday's position and tomorrow's run re-plans the same window.

## Data Transformation (`pv-prospect-transform`)

Runs at **05:30 UTC**. Orchestrates the data cleaning and preparation steps to
generate prepared datasets for all data extracted earlier in the day. On success,
runs `maintain_validation_window` (see the
[P — Data Preparation](../README.md#p--data-preparation) section of the system
architecture) and `consolidate_logs` as post-pipeline cleanup steps.

## PV-Sites Transformation Backfill (`pv-prospect-daily-transform-pv-sites-backfill`)

Runs at **06:00 UTC**. Unlike the other pipelines, this one has no Cloud
Workflow — Cloud Scheduler invokes the `data-transformation` Cloud Run Job
directly with `JOB_TYPE=run_transform_backfill`. The job plans, runs (with
per-slice thread-pool parallelism: each slice runs clean → prepare → assemble
entirely in memory and writes one prepared partition file directly, without
intermediate `cleaned/` writes), flushes its run ledger to a single consolidated
file, and commits the marker — all in a single execution. Plans its work from
the PV-sites extraction backfill's committed task-outcome ledger: completed
extraction entries gate which slices are ready to transform. A consumed-through
marker bounds each run to the next `MAX_EXTRACT_RUNS` (default 4) unconsumed
extraction ledgers; the marker advances at the end of the run only after every
slice has been attempted (an exit-2 terminating error skips the commit).
Per-slice failures are logged-and-swallowed, so a transiently-failed slice is
a recorded hole rather than a perpetual retry.

## Weather-Grid Transformation Backfill (`pv-prospect-daily-transform-weather-grid-backfill`)

Runs at **08:00 UTC**. Same pattern as the PV-sites transformation backfill above,
but planning from the weather-grid extraction backfill's ledger (weather-only task
graph). Uses an independent marker so it can advance separately from the PV-sites
transform backfill. The start time sits past the weather-grid extract's ~06:44
finish + consolidation, so the planner always sees a consolidated ledger to read
from.

## Data Versioning + Model Training (`pv-prospect-version`)

Runs weekly at **Sunday 23:00 UTC**. A two-step Cloud Workflow:

1. **Data versioner** (`data-versioner` Cloud Run Job): snapshots the prepared CSV
   corpus, pushes to `gs://pv-prospect-versioned-feature`, commits and tags
   `data-v<date>` in the instance repo.
2. **Model trainer** (`model-trainer` Cloud Run Job): clones the instance repo at
   the new data tag, pulls the corpus, trains both models, runs the promotion gate,
   and if promoted: pushes artifacts to `gs://pv-prospect-versioned-model`, commits
   `model-v<date>`. The model trainer always runs after the versioner step, even if
   the versioner's Cloud Run status reports non-success (the versioner has a known
   hang-on-exit bug — see `.claude/work/briefs/versioner-hang.md` — that reports
   failure despite completing all work; the trainer self-verifies by cloning the
   `data-v<date>` tag).

## Prediction & Validation API + Website (`pv-prospect-app`)

A Cloud Run Service that loads the promoted model artifacts from
`gs://pv-prospect-versioned-model/promoted/` at startup and serves the prediction
(`/predict`), validation (`/validate/sites`, `/validate/{system_id}`), and
health/metadata (`/healthz`, `/version`) endpoints, plus a no-build demo website
at `/`. Scale-to-zero with `max_instances=2`. Public by default (per-IP rate
limiting on `/predict` and `/validate/*` is baked into the image). To restrict to
IAM auth, set `allow_unauthenticated = false` in `terraform.tfvars` and re-apply.
See [`pv-prospect-app/README.md`](../pv-prospect-app/README.md) for deployment
instructions.

## Scheduling Rationale

The daily extraction and PV-sites extraction backfill both use the PVOutput API,
which rate-limits at 300 requests/hour. With individual Cloud Run Job tasks capped
at 30 minutes, a 40-minute gap between each PVOutput-using workflow prevents
concurrent API calls that could breach this limit.

The weather-grid extraction backfill uses OpenMeteo exclusively, but its 9 daily
batches would breach the 5,000 requests/hour limit if dispatched back-to-back —
so batches are separated by `sleep_seconds_between_batches` (default 720 s) within
the single execution, keeping a sliding 60-minute window to at most ~3 batches ×
1,330 calls ≈ 3,990 calls.

The transformation workflows do not call any external API, so they can run
back-to-back; they are scheduled after all extraction runs have safely completed.
The full schedule and spacing are documented in
[`terraform/README.md`](../terraform/README.md#scheduler-spacing).
