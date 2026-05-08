# Workflow Orchestration

This document describes the manifest/ledger system that coordinates work
across the extraction and transformation pipelines. The shared logic lives in
the `pv-prospect-etl` package:

- `pv_prospect.etl.orchestration` — orchestrator manifests and the per-task
  outcome ledger that drives both audit and resumption for the daily phased
  workflows.
- `pv_prospect.etl.backfill` — `BackfillScope`, `BackfillCursor`, and the
  plan-commit primitives that extraction and transformation backfills both
  delegate to. Centralising it here means changing a window length in one place
  updates both pipelines.

The Cloud Workflows in `terraform/modules/` consume the manifests these modules
produce.

## Core Concepts

### Task Hashing

Each Cloud Run Job task is identified by a deterministic SHA256 hash of its
environment variables (excluding `TASK_HASH` itself). This hash is:

- Injected into the task's environment as `TASK_HASH` before dispatch.
- Used as the ledger entry filename for the task.
- Stable across re-runs with the same parameters, enabling idempotent recovery.

### Manifests

A manifest is a JSON file describing the work plan for a single workflow run. It
is structured as an ordered list of **phases**:

```json
{"phases": [[task_env_list, ...], [task_env_list, ...], ...]}
```

Tasks within a phase run in parallel; phases execute sequentially. The manifest is
written by a `plan_*` job type and read by the Cloud Workflow to drive dispatch.

Manifest path: `resources/manifests/{workflow_name}_{run_date}.json`

### Task-Outcome Ledger

Each task records its outcome on the `log_storage` filesystem via
`WorkflowOrchestrator.record_outcome()`. The ledger is the single source of
truth for both audit (what each run actually did) and resumption (which tasks to
skip on re-run).

Each entry has the schema:

```json
{
  "recorded_at": "<ISO 8601 UTC>",
  "run_date": "<YYYY-MM-DD>",
  "workflow": "<workflow_name>",
  "task_hash": "<sha256>",
  "descriptor": { "<workflow-specific keys>": "..." },
  "status": "completed" | "failed",
  "error": "<repr of exception, only for failed>"
}
```

The `descriptor` is opaque to the orchestrator and supplied by the entrypoint;
each workflow uses its own keys (e.g. `data_source`, `pv_system_id`,
`start_date` for extraction; `transform_step`, `pv_system_id`, `start_date` for
transformation). This keeps the ledger queryable by domain identifiers without
relying on parsing back the task hash.

During a run, entries live at `<log_storage>/<run_date>/<workflow>/<task_hash>.jsonl`
— one file per task, partitioned by hash so concurrent tasks never write to the
same file. A task that fails and is retried within the same run accumulates
multiple entries in its file. The end-of-workflow `consolidate_logs` job calls
`consolidate_ledger`, which merges per-task files into a single
`<log_storage>/<run_date>/<run_date>-<HHMMSS>-<workflow>.jsonl` sorted by
`recorded_at` and deletes the originals.

`filter_remaining_tasks()` consults the ledger (per-task files mid-run, the
consolidated file post-consolidation) and skips any task whose hash already
appears with `status='completed'`. Failures alone do not cause skipping —
a re-run will re-attempt failed tasks. When `log_fs` is not configured (e.g.
some local dev setups), filtering is disabled and every task is dispatched.

## Daily Workflows (Extraction and Transformation)

Both the daily extraction (`pv-prospect-extract`) and transformation
(`pv-prospect-transform`) workflows follow the same three-step pattern:

1. **Plan** -- A Cloud Run Job (job type `plan_extract` or `plan_transform`)
   inspects available data and writes a phased manifest to GCS.
2. **Dispatch** -- The Cloud Workflow reads the manifest, iterates phases, and
   dispatches tasks in parallel. Each dispatched task carries a `TASK_HASH` env
   var. On completion (or on failure), the task records a ledger entry. The
   workflow retries on 429/500/503.
3. **Consolidate** -- A final `consolidate_logs` job merges micro-log files and
   per-task ledger entries into date-partitioned consolidated files and cleans
   up empty directories.

If a workflow is interrupted mid-run, re-triggering it repeats steps 1-3 but the
ledger filter in step 2 ensures already-completed tasks are skipped.

## Backfill Workflows (Plan-Commit Pattern)

The extraction and transformation backfills use a different pattern from the
daily workflows because they process a moving window across days:

1. **Plan** -- Reads a live **cursor** (tracking where the window left off),
   computes the next window, and writes a manifest containing both the work items
   and the **next cursor** value.
2. **Dispatch** -- Cloud Workflow dispatches tasks. The extract-side weather
   grid backfill additionally maintains a GCS marker tracking completed
   batches (by index) to allow within-run resumption.
3. **Commit** -- Only after all tasks succeed, the next cursor is promoted to
   live. This ensures the cursor only advances when work actually completes.

### Backfill Scope and Window Configuration

`BackfillScope` (defined in `pv_prospect.etl.backfill`) names the two backfill
axes the project tracks. `default_window_days(scope)` returns the cadence both
extraction and transformation use for that scope:

| Scope | Default window | Why |
|---|---|---|
| `PV_SITES` | 28 days | PVOutput's `getstatus.jsp` is single-date only (one HTTP call per system per day) and rate-limits at 300/hour; 10 sites × 28 days = 280 calls is the largest round-week multiple that fits with retry headroom. |
| `WEATHER_GRID` | 14 days | OpenMeteo's natural single-request window for the historical-forecast endpoint. |

### Cursor Files

Each backfill instance has its own cursor and manifest paths. Extraction and
transformation cursors for the same scope are independent so the two pipelines
can advance at their own pace.

| Workflow | Cursor path | What it tracks |
|---|---|---|
| PV-sites extraction backfill | `resources/manifests/pv_backfill_cursor.json` | Exclusive end date of next 28-day window |
| Weather-grid extraction backfill | `resources/manifests/weather_grid_backfill_cursor.json` | Next end date and sample offset |
| PV-sites transformation backfill | `resources/manifests/pv_sites_transform_backfill_cursor.json` | Exclusive end date of next 28-day window |
| Weather-grid transformation backfill | `resources/manifests/weather_grid_transform_backfill_cursor.json` | Exclusive end date of next 14-day window |

### Weather-Grid Extraction Backfill: Two-Run Split

The weather-grid extraction backfill is split into two scheduled Cloud Scheduler
runs (03:20 and 04:30 UTC) to stay within OpenMeteo's 5,000 requests/hour limit.
Run 1 processes 4 batches and exits cleanly. Run 2 resumes from the GCS
checkpoint, processes the remaining batches, and commits the cursor.

### Transformation Backfills

The transformation backfills wrap the daily-transform plan-execute-phases
pattern in a plan-commit envelope. Each run:

1. Calls `plan_transform_backfill` (with `BACKFILL_SCOPE`) to advance the
   scope-specific cursor by one window.
2. Calls `plan_transform` with the resulting `[start_date, end_date)` to produce
   an orchestrator manifest of `clean → prepare → assemble` phases over every
   day in the window.
3. Executes the phased manifest using the same dispatcher the daily transform
   uses; per-task ledger entries make re-runs idempotent.
4. Calls `commit_transform_backfill` to promote the next-cursor — only reached
   if every task succeeded, so a failed run leaves the cursor unchanged for
   tomorrow's scheduled re-plan.

The two scopes use independent cursors, so a hiccup transforming the weather
grid does not block PV-sites progress (or vice versa).

## Retry and Rate-Limit Handling

- **Python layer**: `@retry_on_429` decorator with exponential backoff (1, 2, 5,
  10 minutes), capped at 20 minutes total to avoid exceeding Cloud Run task
  timeout.
- **Workflow layer**: Cloud Workflows retry predicate on HTTP 429/500/503, max 3
  retries with 60-600s exponential backoff.
- **PVOutput**: `PVOutputRateLimiter` tracks rate-limit headers from API
  responses.
- **Scheduling**: Workflows are staggered to prevent concurrent API access from
  breaching rate limits (see `terraform/README.md` for the schedule).

## Error Handling

- Cloud Run Jobs exit with code 1 on failure (not silently swallowed).
- Workflow `try/except` blocks ensure log consolidation always runs, even on
  failure.
- Structured logging includes `status_code`, `url`, and response body for HTTP
  errors.
