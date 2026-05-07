# Workflow Orchestration

This document describes the manifest/checkpoint system that coordinates work
across the extraction and transformation pipelines. The shared logic lives in
the `pv-prospect-etl` package:

- `pv_prospect.etl.orchestration` — orchestrator manifests + per-task checkpoints
  for the daily phased workflows.
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
- Used as the checkpoint filename on completion.
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

### Checkpoints

After each task completes successfully,
`WorkflowOrchestrator.mark_task_completed()` writes a checkpoint file:

```
resources/checkpoints/{workflow_name}/{run_date}/{task_hash}.json
```

On re-run, `filter_remaining_tasks()` scans the checkpoint directory and removes
already-completed tasks from the manifest, so the workflow resumes from where it
left off.

## Daily Workflows (Extraction and Transformation)

Both the daily extraction (`pv-prospect-extract`) and transformation
(`pv-prospect-transform`) workflows follow the same three-step pattern:

1. **Plan** -- A Cloud Run Job (job type `plan_extract` or `plan_transform`)
   inspects available data and writes a phased manifest to GCS.
2. **Dispatch** -- The Cloud Workflow reads the manifest, iterates phases, and
   dispatches tasks in parallel. Each dispatched task carries a `TASK_HASH` env
   var. On completion, the task writes its checkpoint. The workflow retries on
   429/500/503.
3. **Consolidate** -- A final `consolidate_logs` job merges micro-log files into
   date-partitioned consolidated logs and cleans up empty directories.

If a workflow is interrupted mid-run, re-triggering it repeats steps 1-3 but the
checkpoint filter in step 2 ensures already-completed tasks are skipped.

## Backfill Workflows (Plan-Commit Pattern)

The extraction and transformation backfills use a different pattern from the
daily workflows because they process a moving window across days:

1. **Plan** -- Reads a live **cursor** (tracking where the window left off),
   computes the next window, and writes a manifest containing both the work items
   and the **next cursor** value.
2. **Dispatch** -- Cloud Workflow dispatches tasks. The extract-side weather
   grid backfill additionally maintains a GCS checkpoint tracking completed
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
   uses; per-task checkpoints make re-runs idempotent.
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
