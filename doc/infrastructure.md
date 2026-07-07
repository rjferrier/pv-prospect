# Infrastructure

Reference for the deployed GCP infrastructure — the storage buckets, the scheduled
workflows that drive the pipeline, and the Cloud Run compute that executes them.

For the orchestration machinery (manifests, ledger, plan-commit pattern) see
[`doc/orchestration.md`](orchestration.md). For operational runbooks (manual
triggers, failure recovery, pausing) see [`doc/runbooks.md`](runbooks.md). For how
to *provision* this infrastructure with Terraform, see
[`terraform/README.md`](../terraform/README.md).

## Contents

- [Overview](#overview)
- [Buckets](#buckets)
- [Scheduled Workflows](#scheduled-workflows)
- [Operational Tuning](#operational-tuning)
- [Cost](#cost)

## Overview

The pipeline runs on a fully serverless, pay-per-use architecture: **Cloud Run
Jobs** do the work, and **Cloud Workflows** fans them out for the daily extract,
transform, and version flows. The transformation backfills are an exception — they
invoke a Cloud Run Job directly (planning, execution, and commit happen in one
container) because the per-unit work is small and there is no benefit to
dispatch-side fan-out. Nothing runs continuously except the app (a scale-to-zero
Cloud Run Service), so idle cost is near zero.

## Buckets

| Bucket | Purpose | Lifecycle |
|---|---|---|
| `gs://pv-prospect-staging` | Working storage: raw/cleaned/prepared data, orchestration tracking, resource CSVs, and the served validation window. | Rewritten every run; not versioned. |
| `gs://pv-prospect-raw` | Durable archive of extracted raw CSVs (see [Raw Data Archive](#raw-data-archive) below). | Standard → Coldline at 8 days. |
| `gs://pv-prospect-versioned-feature` | DVC remote for the prepared feature corpus, pinned by `data-v<date>` git tags. | Accumulate-only. |
| `gs://pv-prospect-versioned-model` | DVC remote for trained model artifacts, plus the `promoted/` prefix the app serves from. | Accumulate-only. |
| `gs://<project_id>-tfstate` | Terraform remote state and `terraform.tfvars` (see [`terraform/README.md`](../terraform/README.md#remote-state--configuration)). | Object-versioned. |

The internal prefix layout of the staging bucket (`data/`, `tracking/`,
`resources/`) is documented in
[`orchestration.md`](orchestration.md#bucket-layout).

### Raw Data Archive

Raw extracted CSVs live in `gs://pv-prospect-raw`, a bucket dedicated to durable
archival rather than working storage — kept separate from `staging` so its
unbounded growth (weather-grid densification, historical backfills) doesn't
churn alongside the `cleaned/`/`prepared/` data that is rewritten on every run.
Raw is **not** DVC-versioned: it is append-only and non-refetchable from the
source APIs, so what it needs is durability, not git-tag reconstructability. A
GCS lifecycle rule tiers each object from Standard to **Coldline** storage class
in place (same path, no cross-bucket move) once it reaches **8 days old** —
comfortably after the daily transform has consumed it (≤1–2 days), while still
leaving a margin for pipeline catch-up after a short outage. Both extraction
(write) and transform (read) reach this bucket through the same
`staged_raw_data_storage` configuration key, so it is the only raw-data path the
pipeline knows about.

## Scheduled Workflows

The pipeline is driven by Cloud Scheduler cron jobs. For operational runbooks
(manual triggers, failure recovery) see [`doc/runbooks.md`](runbooks.md).

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

### Daily Extraction (`pv-prospect-extract`)

Runs at **02:00 UTC**. Triggers the data extraction workflow for the previous day.

### PV-Sites Extraction Backfill (`pv-prospect-extract-pv-sites-backfill`)

Runs at **02:40 UTC**. Orchestrates daily PV-site backfill for historical data,
covering a 28-day window that marches backwards through history. Extracts PV
output sequentially (one Cloud Run Job per site) and weather data in parallel.
A GCS position checkpoint (`tracking/checkpoints/pv_sites_backfill.json`,
a small `{"next_pv_task_index": N}` document) is written after each dispatched
PV task, so a manually re-triggered run resumes from where the previous
execution stopped rather than starting over. The checkpoint is deleted on
successful completion.

### Weather-Grid Extraction Backfill (`pv-prospect-extract-weather-grid-backfill`)

Runs at **03:20 UTC**. Orchestrates the daily grid-point weather backfill via a
paced process: 9 batches dispatched sequentially in a single workflow execution,
separated by `sleep_seconds_between_batches` (default 720 s = 12 min). The
inter-batch sleep is what keeps the dispatch rate under OpenMeteo's 5,000
requests/hour limit — a sliding 60-minute window covers at most ~3 batches ×
1,330 calls ≈ 3,990 calls. Total wall time ≈ 3 h 24 min; the cursor commits
once every batch has been attempted, so a workflow-level failure leaves the
cursor at yesterday's position and tomorrow's run re-plans the same window.

### Data Transformation (`pv-prospect-transform`)

Runs at **05:30 UTC**. Orchestrates the data cleaning and preparation steps to
generate prepared datasets for all data extracted earlier in the day. On success,
runs `maintain_validation_window` (see the
[C. Data Preparation](../README.md#c-data-preparation) section of the system
architecture) and `consolidate_logs` as post-pipeline cleanup steps.

### PV-Sites Transformation Backfill (`pv-prospect-daily-transform-pv-sites-backfill`)

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

### Weather-Grid Transformation Backfill (`pv-prospect-daily-transform-weather-grid-backfill`)

Runs at **08:00 UTC**. Same pattern as the PV-sites transformation backfill above,
but planning from the weather-grid extraction backfill's ledger (weather-only task
graph). Uses an independent marker so it can advance separately from the PV-sites
transform backfill. The start time sits past the weather-grid extract's ~06:44
finish + consolidation, so the planner always sees a consolidated ledger to read
from.

### Data Versioning + Model Training (`pv-prospect-version`)

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

### Prediction & Validation API + Website (`pv-prospect-app`)

A Cloud Run Service that loads the promoted model artifacts from
`gs://pv-prospect-versioned-model/promoted/` at startup and serves the prediction
(`/predict`), validation (`/validate/sites`, `/validate/{system_id}`), and
health/metadata (`/healthz`, `/version`) endpoints, plus a no-build demo website
at `/`. Scale-to-zero with `max_instances=2`. Public by default (per-IP rate
limiting on `/predict` and `/validate/*` is baked into the image). To restrict to
IAM auth, set `allow_unauthenticated = false` in `terraform.tfvars` and re-apply.
See [`pv-prospect-app/README.md`](../pv-prospect-app/README.md) for deployment
instructions.

## Operational Tuning

The controls that shape *how* the deployed workflows run — schedule spacing, task
timeouts, and pausing the backfills — as opposed to the static inventory above.
Each is a Terraform variable, overridable in `terraform.tfvars` without touching
module code (see [`terraform/README.md`](../terraform/README.md)).

### Scheduling Rationale

Individual Cloud Run Job tasks can run for up to 30 minutes (see [Cloud Run Job
Timeouts](#cloud-run-job-timeouts) below), so the API-using workflows are spaced
far enough apart that their executions can't overlap and combine to breach an
upstream rate limit.

| Schedule | Trigger (UTC) | API used |
|---|---|---|
| Daily extraction | 02:00 | PVOutput |
| PV-sites extraction backfill | 02:40 | PVOutput |
| Weather-grid extraction backfill | 03:20 | OpenMeteo only |
| Daily transformation | 05:30 | — |
| PV-sites transformation backfill | 06:00 | — |
| Weather-grid transformation backfill | 08:00 | — |
| Weekly versioning (Sun) | 23:00 | — |

The daily extraction and PV-sites extraction backfill both use the **PVOutput**
API (300 requests/hour), so a 40-minute gap keeps their ≤30-minute executions from
running concurrently and combining to breach the limit.

The weather-grid extraction backfill uses **OpenMeteo** (5,000 requests/hour)
exclusively. Its 9 daily batches would breach that limit dispatched back-to-back,
so they are paced within the single execution by `sleep_seconds_between_batches`
(default 720 s): a sliding 60-minute window then holds at most ~3 batches × 1,330
calls ≈ 3,990 calls. Total wall time ≈ 3 h 24 min, so the extract finishes ~06:44.

The transformation workflows call no external API and could in principle run
back-to-back; they are placed where they are so the planner always reads a fully
consolidated extraction ledger — 06:00 for the PV-sites transform backfill
(extraction done by ~03:10) and 08:00 for the weather-grid transform backfill
(extraction done ~06:44, leaving ~1 h margin for consolidation).

The cron expressions themselves are the `extractor_scheduler_cron`,
`extractor_pv_sites_backfill_scheduler_cron`,
`extractor_weather_grid_backfill_scheduler_cron`, `transformer_scheduler_cron`,
`transformer_pv_sites_backfill_scheduler_cron`,
`transformer_weather_grid_backfill_scheduler_cron`, and `versioner_scheduler_cron`
variables.

### Cloud Run Job Timeouts

The extraction Cloud Run Job has a task timeout of **1800 s (30 min)**, set to
accommodate the sequential PV-site backfill (up to ~1,066 API calls per execution);
an earlier 600 s value cut executions off mid-run when API latency spiked. The
transformation and versioner jobs have independently configured timeouts (900 s and
1800 s respectively). These 30-minute ceilings are what the [Scheduling
Rationale](#scheduling-rationale) spacing assumes.

### Pausing the Backfills

The daily historical backfills are the dominant variable cost, so once the corpus is
deep enough all four backfill schedulers (PV-sites and weather-grid, extraction and
transformation) can be paused together — cleanly and reversibly, with cursors frozen
in place — via the `backfills_paused` Terraform variable. The daily
extraction/transformation and weekly versioning schedulers are left untouched. For
the procedure (the flag, the ad-hoc `gcloud` path, and the reconciliation caveat) see
the [backfill-operations runbook](runbooks/backfill-operations.md#pausing-and-resuming-the-backfills).

## Cost

The architecture is entirely serverless (scale-to-zero) — you pay only for what you
use. Approximate monthly costs (varies by usage volume):

- **Cloud Scheduler** — free tier (up to 3 jobs/month free)
- **Cloud Workflows** — free tier (first 5,000 steps/month free)
- **Cloud Run Jobs** — ~$1–$5 (depending on extraction volume/duration)
- **Artifact Registry** — ~$0.10/GB stored
- **GCS storage** — standard GCS pricing

This is significantly cheaper than a continuously running orchestrator like Cloud
Composer or keeping GKE/VM nodes running. The dominant variable cost is the daily
backfills, which can be dropped to zero by [pausing them](#pausing-the-backfills)
once the corpus is deep enough.
