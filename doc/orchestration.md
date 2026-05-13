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

## Bucket Layout

The staging bucket has three top-level prefixes:

```
gs://pv-prospect-staging/
├── data/
│   ├── raw/               (extracted CSV)
│   ├── cleaned/           (parquet)
│   ├── prepared-batches/  (parquet)
│   └── prepared/          (parquet)
├── tracking/
│   ├── manifests/<run_date>/<workflow>.json           (orchestrator phases manifest)
│   ├── manifests/<run_date>/<workflow>.backfill.json  (backfill date-window plan; only backfill workflows)
│   ├── cursors/<workflow>.json                        (live committed backfill cursors)
│   ├── ledger/<run_date>/<workflow>/<task_hash>.jsonl (per-task ledger entries, mid-run)
│   ├── ledger/<run_date>/<run_date>-<HHMMSS>-<workflow>.jsonl  (consolidated, post-consolidation)
│   ├── logs/<run_date>/<workflow>/<HHMMSSffffff>.txt           (LoggingFileSystem write-audit, mid-run)
│   ├── logs/<run_date>/<run_date>-<HHMMSS>-<workflow>.txt      (consolidated)
│   └── checkpoints/<workflow>.json                    (workflow-level position checkpoint; backfill workflows only)
└── resources/             (CSVs, sample files — see terraform/modules/seed_resources)
```

`data/` is *what the pipeline produced*; `tracking/` is *how it produced
it*; `resources/` is supporting data that has been manually collected and
assembled (e.g. `pv_sites.csv`, `location_mapping.csv`, weather-grid point
sample files).

## Core Concepts

### Run Date vs. Data Window

Every Cloud Workflow invocation has a **run date** — the UTC date the workflow
was triggered. It is computed once in the workflow's `init` step
(`text.substring(time.format(sys.now()), 0, 10)`) and propagated to every Cloud
Run Job as the `RUN_DATE` env var. The run date is what partitions manifests,
ledger entries, and write-audit logs into per-run subtrees.

`RUN_DATE` is **distinct from** `START_DATE`/`END_DATE`, which are arguments
describing the data window being processed. A daily extract triggered on
2026-05-11 with `START_DATE=2026-05-10` has `RUN_DATE=2026-05-11`; its ledger
entries land under `tracking/ledger/2026-05-11/`, and the `start_date`
descriptor inside each entry records the data date.

### Task Hashing

Each Cloud Run Job task is identified by a deterministic SHA256 hash of its
environment variables, excluding `TASK_HASH` and `RUN_DATE` (so the same
data-window task across two trigger dates hashes identically — preserving
cross-day resume). The hash is:

- Computed on demand by `compute_task_hash(env)`, called from both
  `filter_remaining_tasks` (plan time) and the container's per-site loop
  (run time). Planners no longer pre-inject `TASK_HASH` as an env entry;
  the helper `inject_task_hash` remains available for callers that still
  want to attach it, but the orchestrator's identity contract is the
  computed hash.
- Used as the ledger entry filename for the task.
- Stable across re-runs with the same parameters, enabling idempotent recovery.

### Manifests

A manifest is a JSON file describing the work plan for a single workflow run. It
is structured as an ordered list of **phases**:

```json
{"phases": [[task_env_list, ...], [task_env_list, ...], ...]}
```

Tasks within a phase run in parallel; phases execute sequentially. The manifest
is written by a `plan_*` job type and read by the Cloud Workflow to drive
dispatch.

Manifest path: `tracking/manifests/<run_date>/<workflow_name>.json`

A backfill workflow additionally writes a date-window plan (start_date,
end_date, next_cursor) at
`tracking/manifests/<run_date>/<workflow_name>.backfill.json` — distinct from
the orchestrator phases manifest above.

### Task-Outcome Ledger

Each task records its outcome on `ledger_storage` via
`WorkflowOrchestrator.record_outcome()`. The ledger is the single source of
truth for both audit (what each run actually did) and resumption (which tasks
to skip on re-run).

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

The `run_date` field matches the path's date prefix and equals the workflow's
UTC trigger date; `recorded_at` is wall-clock UTC at write time (may fall on a
different calendar day if the workflow crosses midnight).

The `descriptor` is opaque to the orchestrator and supplied by the entrypoint;
each workflow uses its own keys (e.g. `data_source`, `pv_system_id`,
`start_date` for extraction; `transform_step`, `pv_system_id`, `start_date`
for transformation). This keeps the ledger queryable by domain identifiers
without relying on parsing back the task hash.

During a run, entries live at
`<ledger_storage>/<run_date>/<workflow>/<task_hash>.jsonl` — one file per task,
partitioned by hash so concurrent tasks never write to the same file. A task
that fails and is retried within the same run accumulates multiple entries in
its file. The end-of-workflow `consolidate_logs` job calls
`consolidate_ledger`, which merges per-task files into a single
`<ledger_storage>/<run_date>/<run_date>-<HHMMSS>-<workflow>.jsonl` sorted by
`recorded_at` and deletes the originals.

`filter_remaining_tasks()` consults the ledger and skips any task whose hash
appears with `status='completed'`. Task identity is computed by
`compute_task_hash(env)` over each task's env at filter time, so neither the
planner nor the container needs to pre-inject a `TASK_HASH` env entry.
`filter_remaining_tasks` scans:

1. The current run's per-task files under `<run_date>/<workflow>/`.
2. Every consolidated ledger file ever produced for this workflow
   (`<*>/<*>-*-<workflow>.jsonl`), to preserve cross-day resume.

Failures alone do not cause skipping — a re-run will re-attempt failed tasks.
When `ledger_fs` is not configured (e.g. some local dev setups), filtering is
disabled and every task is dispatched.

`completed_task_hashes()` exposes the same set directly. The container's
per-site loop uses it to skip sites the ledger already records as completed
before fetching them again (e.g. during a Cloud Run task-attempt retry after
a mid-batch container crash).

### Planner Divergence: Daily Extract vs. Backfills

The two extraction workflows use `filter_remaining_tasks` differently, by
design:

- **Daily extract** (`pv-prospect-extract`) calls `filter_remaining_tasks`
  at plan time. A task that failed today is re-emitted by tomorrow's plan,
  so today's data eventually gets fetched even if a transient API failure
  bit it on the first attempt.
- **Backfill workflows** (`pv-prospect-extract-pv-sites-backfill` and
  `pv-prospect-extract-weather-grid-backfill`) deliberately do **not** call
  `filter_remaining_tasks`. The backfill cursor advances on schedule each
  day; a transiently-failed site becomes a small per-window hole rather
  than a perpetual retry. This is the "predictable cadence" trade-off the
  backfills opt into.

### Write-Audit Logs

`LoggingFileSystem` decorates the staged data filesystems and records a
one-line breadcrumb at
`tracking/logs/<run_date>/<workflow>/<HHMMSSffffff>.txt` for every write
(content: ISO timestamp + `CREATED <label>/<path>`). `consolidate_logs`
merges these into a single `<run_date>-<HHMMSS>-<workflow>.txt` at end of run.

This is independent from the JSONL outcome ledger above: write-audit logs
record *file writes*, the ledger records *task outcomes*.

## Daily Workflows (Extraction and Transformation)

Both the daily extraction (`pv-prospect-extract`) and transformation
(`pv-prospect-transform`) workflows follow the same three-step pattern:

1. **Plan** — A Cloud Run Job (job type `plan_extract` or `plan_transform`)
   inspects available data and writes a phased manifest to
   `tracking/manifests/<run_date>/<workflow>.json`.
2. **Dispatch** — The Cloud Workflow reads the manifest, iterates phases,
   and dispatches tasks in parallel. Each dispatched task carries
   `RUN_DATE` (and the site-identifying env vars) in its env; the
   container computes `TASK_HASH` locally per site. On completion (or
   on failure), the task records a ledger entry. Per-site transient HTTP
   failures (429, 5xx, timeouts, connection resets) retry inside the
   container via `retry_on_transient_http_error` rather than at the
   workflow layer.
3. **Consolidate** — A final `consolidate_logs` job merges write-audit log
   files and per-task ledger entries into per-run consolidated files and
   cleans up empty directories.

If a workflow is interrupted mid-run, re-triggering it repeats steps 1–3 but
the ledger filter in step 2 ensures already-completed tasks are skipped — even
across run dates, so a manual re-trigger the following day for the same
`START_DATE` finds yesterday's completions and skips them.

## Backfill Workflows (Plan-Commit Pattern)

The extraction and transformation backfills use a different pattern from the
daily workflows because they process a moving window across days:

1. **Plan** — Reads a live **cursor** at
   `tracking/cursors/<workflow>.json` (tracking where the window left off),
   computes the next window, and writes a backfill plan manifest at
   `tracking/manifests/<run_date>/<workflow>.backfill.json` containing the
   date window, the **next cursor** value, and (on the extract side) a
   `phases` list in the same shape the daily-extract orchestrator manifest
   uses — one `extract_and_load` task env per dispatched batch. Task
   identity is computed inside each container via `compute_task_hash`, so
   no `TASK_HASH` is pre-injected. For the weather-grid backfill, each
   batch env carries a `LOCATIONS` JSON array of grid-point lat,lon
   strings; the container's per-site loop records one ledger entry per
   `(site, window)` pair.
2. **Dispatch** — Cloud Workflow iterates `phases` and dispatches each task
   with `env: $${task_env}`. The extract-side backfills additionally maintain
   a workflow-level **position checkpoint** at
   `tracking/checkpoints/<workflow>.json` — a single integer indicating the
   next index into the dispatch list to attempt
   (`{"next_batch_index": N}` for weather-grid, `{"next_pv_task_index": N}`
   for pv-sites). The checkpoint advances unconditionally after every
   dispatched task so a same-day re-trigger picks up where the previous
   run stopped rather than retrying failures. The parallel weather phase
   of pv-sites is *not* checkpointed; per-site ledger filtering inside the
   container handles same-day re-trigger redundancy.
3. **Commit** — Only after every task in the manifest has been *attempted*
   (success or exit-1 swallowed crash), the next cursor is promoted to
   live. Exit-2 (`WorkflowTerminatingError`) re-raises out of the run
   pipeline and skips the commit, leaving the cursor unchanged for
   tomorrow. Partial runs (max-batches-per-run hit) flow through
   `consolidate_logs` without committing.

The transformation backfills follow the same skeleton but split planning
across two jobs: `plan_transform_backfill` writes the date window + cursor,
and a separate `plan_transform` step writes the orchestrator phases manifest
at `tracking/manifests/<run_date>/<workflow>.json` (using the same
plan_transform code path the daily transform uses). The two files coexist;
`commit_transform_backfill` reads the `.backfill.json` for the cursor.

### Backfill Scope and Window Configuration

`BackfillScope` (defined in `pv_prospect.etl.backfill`) names the two backfill
axes the project tracks. `default_window_days(scope)` returns the cadence both
extraction and transformation use for that scope:

| Scope | Default window | Why |
|---|---|---|
| `PV_SITES` | 28 days | PVOutput's `getstatus.jsp` is single-date only (one HTTP call per system per day) and rate-limits at 300/hour; 10 sites × 28 days = 280 calls is the largest round-week multiple that fits with retry headroom. |
| `WEATHER_GRID` | 14 days | OpenMeteo's natural single-request window for the historical-forecast endpoint. |

### Cursor Files

Each backfill instance has its own workflow name. Extraction and transformation
cursors for the same scope are independent so the two pipelines can advance at
their own pace.

| Workflow | Cursor path |
|---|---|
| PV-sites extraction backfill | `tracking/cursors/pv-prospect-extract-pv-sites-backfill.json` |
| Weather-grid extraction backfill | `tracking/cursors/pv-prospect-extract-weather-grid-backfill.json` |
| PV-sites transformation backfill | `tracking/cursors/pv-prospect-transform-pv-sites-backfill.json` |
| Weather-grid transformation backfill | `tracking/cursors/pv-prospect-transform-weather-grid-backfill.json` |

### Weather-Grid Extraction Backfill: Two-Run Split

The weather-grid extraction backfill is split into two scheduled Cloud Scheduler
runs (03:20 and 04:30 UTC) to stay within OpenMeteo's 5,000 requests/hour limit.
Run 1 attempts 4 batches and exits early, leaving `next_batch_index` pointing
at the first un-attempted batch. Run 2 reads the checkpoint, attempts the
remaining batches, and commits the cursor. Failed sites within an attempted
batch are recorded in the ledger but not retried — the cursor still advances.

### Transformation Backfills

The transformation backfills wrap the daily-transform plan-execute-phases
pattern in a plan-commit envelope. Each run:

1. Calls `plan_transform_backfill` (with `BACKFILL_SCOPE`) to advance the
   scope-specific cursor by one window. Writes the date-window plan at
   `tracking/manifests/<run_date>/<workflow>.backfill.json`.
2. Calls `plan_transform` with the resulting `[start_date, end_date)` to
   produce an orchestrator manifest at
   `tracking/manifests/<run_date>/<workflow>.json` containing
   `clean → prepare → assemble` phases over every day in the window.
3. Executes the phased manifest using the same dispatcher the daily transform
   uses; per-task ledger entries make re-runs idempotent.
4. Calls `commit_transform_backfill` to promote the next-cursor — only
   reached if every task succeeded, so a failed run leaves the cursor
   unchanged for tomorrow's scheduled re-plan.

The two scopes use independent cursors, so a hiccup transforming the weather
grid does not block PV-sites progress (or vice versa).

## Retry and Rate-Limit Handling

- **Python layer**: `@retry_on_429` decorator with exponential backoff (1, 2,
  5, 10 minutes), capped at 20 minutes total to avoid exceeding Cloud Run task
  timeout.
- **Workflow layer**: Cloud Workflows retry predicate on HTTP 429/500/503, max
  3 retries with 60–600s exponential backoff.
- **PVOutput**: `PVOutputRateLimiter` tracks rate-limit headers from API
  responses.
- **Scheduling**: Workflows are staggered to prevent concurrent API access
  from breaching rate limits (see `terraform/README.md` for the schedule).

## Error Handling

Cloud Run Jobs exit with one of three codes (see
`pv_prospect.etl.entrypoint`):

| Code | Meaning | Workflow reaction |
|------|---------|-------------------|
| 0 | Success — `main()` returned without exception | Task succeeds; ledger records `completed` |
| 1 | Task-level failure — general exception | Task fails; the surrounding workflow swallows it and continues with other tasks |
| 2 | Workflow-terminating — `WorkflowTerminatingError` raised | Task fails; the surrounding workflow aborts (skipping any cursor-commit step) |

Application code raises `WorkflowTerminatingError` (from `pv_prospect.etl`)
for failure modes where continuing other tasks is harmful or pointless --
for example, sustained rate-limiting from an upstream API or a deployment
misconfiguration (missing storage, unknown JOB_TYPE). Everything else is
just an `Exception`, which becomes exit 1.

The workflow YAML side of the policy lives in each workflow's `await_job`
subworkflow. On failure, it reads the failed task's container exit code
via the Cloud Run Tasks API and re-raises with `data.exit_code` populated.
Each per-task `try` clause then branches on `task_err.data.exit_code`:
swallow on 1, re-raise on 2. For backfill workflows the re-raise abort is
what gates the cursor-commit step (an exit-2 failure leaves the cursor
unchanged for tomorrow's retry).

Other notes:

- Workflow `try/except` blocks ensure log consolidation always runs, even
  on failure.
- Structured logging includes `status_code`, `url`, and response body for
  HTTP errors.
