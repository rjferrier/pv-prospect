# Workflow Orchestration

This document describes the manifest/checkpoint system that coordinates work
across the extraction and transformation pipelines. The shared logic lives in
`pv_prospect.etl.orchestration` (`pv-prospect-etl` package); the Cloud Workflows
in `terraform/modules/` consume the manifests it produces.

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

The PV site backfill and weather grid backfill use a different pattern because
they process a moving window across days:

1. **Plan** -- Reads a live **cursor** (tracking where the window left off),
   computes the next window, and writes a manifest containing both the work items
   and the **next cursor** value.
2. **Dispatch** -- Cloud Workflow dispatches tasks. For weather grid backfill, a
   GCS checkpoint tracks completed batches (by index), allowing resumption.
3. **Commit** -- Only after all tasks succeed, the next cursor is promoted to
   live. This ensures the cursor only advances when work actually completes.

### Cursor Files

| Workflow | Cursor path | What it tracks |
|---|---|---|
| PV site backfill | `resources/manifests/pv_backfill_cursor.json` | Exclusive end date of next 28-day window |
| Weather grid backfill | `resources/manifests/weather_grid_backfill_cursor.json` | Next end date and sample offset |

### Weather Grid Backfill: Two-Run Split

The weather grid backfill is split into two scheduled Cloud Scheduler runs (03:20
and 04:30 UTC) to stay within OpenMeteo's 5,000 requests/hour limit. Run 1
processes 4 batches and exits cleanly. Run 2 resumes from the GCS checkpoint,
processes the remaining batches, and commits the cursor.

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
