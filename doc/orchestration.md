# Workflow Orchestration

This document describes the manifest/ledger system that coordinates work
across the extraction and transformation pipelines. The shared logic lives in
the `pv-prospect-etl` package:

- `pv_prospect.etl.orchestration` — orchestrator manifests and the per-task
  outcome ledger that drives both audit and resumption for the daily phased
  workflows.
- `pv_prospect.etl.backfill` — `BackfillScope`, `BackfillCursor`, and the
  plan-commit primitives the **extraction** backfills delegate to. The
  transformation backfills do not use a cursor — they plan from the extraction
  backfill's committed ledger instead (see
  [Transformation Backfills](#transformation-backfills)).
- `pv_prospect.etl.storage.ledger` — `list_consolidated_ledgers` and
  `read_completed_descriptors`, the consolidated-ledger readers the
  transformation backfill planner builds its plan from.

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
│   ├── manifests/<run_date>/<workflow>.json           (orchestrator phases manifest — for the daily transform/extract; extract-backfill workflows embed phases inside this same path alongside their date window + next-cursor)
│   ├── manifests/<run_date>/<workflow>.marker.json    (transformation backfill next-marker sidecar)
│   ├── cursors/<workflow>.json                        (extraction backfill cursors / transformation backfill consumed-through markers — same path, different schema)
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

An **extraction** backfill workflow's planner writes a single document at
this path carrying the date-window plan (start_date, end_date, next_cursor)
*and* the orchestrator phases list — the Cloud Workflow consumes both from
one file. A **transformation** backfill writes the manifest in the
**phased v2 layout** (see below) — its planner derives the plan from the
extraction ledger rather than a cursor — plus a small
`<workflow_name>.marker.json` next-marker sidecar.

#### Phased (v2) manifest layout

Used by `plan_transform_backfill` via
`WorkflowOrchestrator.write_phased_manifest`. The v1 single-document shape
inlines every task's full env-list, which for the weather-grid transform
backfill reaches ~10–20 K tasks per phase — far past the Cloud Workflows
2 MiB per-step HTTP-response limit, so the workflow can't even fetch its
own plan. The v2 layout splits the manifest into an index plus chunked
per-phase part files, and hoists each phase's constant env-vars so the
part files carry only the varying fields. Two independent compactions
do the work: hoisting shrinks per-row bytes; chunking bounds per-file
row count regardless of how big the queue gets.

```
tracking/manifests/<run_date>/<workflow>.json                  (index)
tracking/manifests/<run_date>/<workflow>.phase-0.part-0.json   (clean,   part 0)
tracking/manifests/<run_date>/<workflow>.phase-0.part-1.json   (clean,   part 1)
tracking/manifests/<run_date>/<workflow>.phase-1.part-0.json   (prepare, part 0)
...
tracking/manifests/<run_date>/<workflow>.phase-2.part-0.json   (assemble)
```

Index document:

```json
{
  "version": 2,
  "phases": [
    {
      "files": ["<workflow>.phase-0.part-0.json", "<workflow>.phase-0.part-1.json"],
      "common_env": [{"name": "TRANSFORM_STEP", "value": "clean_weather"}, ...],
      "task_keys": ["DATE", "END_DATE", "LOCATION", "START_DATE", "TASK_HASH"]
    },
    ...
  ]
}
```

Per phase-part document:

```json
{
  "rows": [
    ["2026-04-17", "2026-05-01", "50.18,-5.24", "2026-04-17", "0d28..."],
    ...
  ]
}
```

Each row is positional in `task_keys` order; `null` denotes "this task
doesn't carry that env-var" (preserves the legacy absent-key semantics
when a phase mixes tasks with and without optional fields like
`PV_SYSTEM_ID`). At dispatch the Cloud Workflow walks `phases[i].files`
in order, fetches each part file, and expands every task as
`common_env + zip(task_keys, row)` (skipping `null` cells) before
sending the resulting env-list as `containerOverrides.env`.

Chunk size is `TASKS_PER_PHASE_FILE` rows (500) in
`pv_prospect.etl.orchestration`. Two distinct Workflows runtime
ceilings drive this — the binding one in practice is *memory*, not
the HTTP-response limit:

- **2 MiB per-step HTTP-response limit.** Row encoding sits at ~130 B
  each, so even thousands of rows per part fit on their own.
- **256 MiB per-execution memory limit.** Each iteration of the inner
  `parallel: for:` block fans out one workflow branch per row, and
  every branch holds its own `task_op` / `task_op_done` / `exec`
  Cloud Run API responses (~5–15 KiB each) plus framework overhead.
  Retained per-branch state of thousands of branches blows past the
  256 MiB budget — which is what the weather-grid transform backfill
  hit at ~20 K tasks/phase even after the hoisting fix. 500-row chunks
  keep each parallel block's branch count to a few hundred, so the
  retained state is reclaimed at chunk boundaries.

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

The transformation side mirrors this split: the daily transform
(`pv-prospect-transform`) self-filters, while the transformation backfills do
not — their consumed-through marker (see
[Transformation Backfills](#transformation-backfills)) plays the
cadence-bounding role the extraction backfills' cursor plays.

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

## Extraction Backfill Workflows (Plan-Commit Pattern)

The **extraction** backfills use a different pattern from the daily workflows
because they process a moving window across days (the transformation backfills
use yet another pattern — see [Transformation Backfills](#transformation-backfills)):

1. **Plan** — Reads a live **cursor** at
   `tracking/cursors/<workflow>.json` (tracking where the window left off),
   computes the next window, and writes a backfill plan manifest at
   `tracking/manifests/<run_date>/<workflow>.json` containing the date
   window, the **next cursor** value, and a `phases` list in the same shape
   the daily-extract orchestrator manifest uses — one `extract_and_load`
   task env per dispatched batch. Task
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

### Backfill Scope and Window Configuration

`BackfillScope` (defined in `pv_prospect.etl.backfill`) names the two backfill
axes the project tracks. `default_window_days(scope)` returns the **extraction**
cadence for that scope. The transformation backfills have no window config of
their own — they inherit each task's window from whatever the extraction ledger
recorded.

| Scope | Default window | Why |
|---|---|---|
| `PV_SITES` | 28 days | PVOutput's `getstatus.jsp` is single-date only (one HTTP call per system per day) and rate-limits at 300/hour; 10 sites × 28 days = 280 calls is the largest round-week multiple that fits with retry headroom. |
| `WEATHER_GRID` | 14 days | OpenMeteo's natural single-request window for the historical-forecast endpoint. |

### Cursor and Marker Files

Each backfill instance has its own workflow name. The **extraction** backfills
keep a live cursor; the **transformation** backfills keep a consumed-through
marker at the same path (different schema — see
[Transformation Backfills](#transformation-backfills)). All four are
independent so the pipelines advance at their own pace.

| Workflow | Path | Type |
|---|---|---|
| PV-sites extraction backfill | `tracking/cursors/pv-prospect-extract-pv-sites-backfill.json` | cursor |
| Weather-grid extraction backfill | `tracking/cursors/pv-prospect-extract-weather-grid-backfill.json` | cursor |
| PV-sites transformation backfill | `tracking/cursors/pv-prospect-transform-pv-sites-backfill.json` | consumed-through marker |
| Weather-grid transformation backfill | `tracking/cursors/pv-prospect-transform-weather-grid-backfill.json` | consumed-through marker |

### Weather-Grid Extraction Backfill: Single Long-Running Workflow

The weather-grid extraction backfill runs as a single Cloud Workflows execution
per day (one Cloud Scheduler trigger at 03:20 UTC). It dispatches all 9 batches
sequentially, pausing `sleep_seconds_between_batches` (default 720 s) between
successive dispatches; the in-batch sleep is what keeps the dispatch rate under
OpenMeteo's 5,000 requests/hour limit. Total wall time ≈ 3 h 24 min; the
cursor commits once every batch has been attempted. Failed sites within a batch
are recorded in the ledger but not retried — the cursor still advances. There
is no per-batch checkpoint; a workflow-level failure leaves the cursor at
yesterday's position and tomorrow's run re-plans the same window.

### Transformation Backfills

The transformation backfills do **not** use the cursor/plan-commit pattern. A
transformation is a recomputable derivative of the durable extraction harvest,
so rather than marching a cursor of their own they plan from the extraction
backfill's committed record. Each run:

1. Calls `plan_transform_backfill` (with `BACKFILL_SCOPE`). It loads the
   scope's **consumed-through marker** from `tracking/cursors/<workflow>.json`,
   lists the corresponding extraction backfill's consolidated ledgers, and
   takes the oldest `MAX_EXTRACT_RUNS` (default 4) whose filename sorts above
   the marker. Every `completed` extraction entry becomes a transform unit
   `(data_source, identifier, window)`; `failed` entries are skipped — no raw
   data means no transform task, correctly leaving a hole. The resulting
   `clean → prepare → assemble` phases are written as a **phased (v2)
   manifest** — an index at `tracking/manifests/<run_date>/<workflow>.json`
   plus one `<workflow>.phase-<N>.json` per phase (see [Phased (v2) manifest
   layout](#phased-v2-manifest-layout)) — alongside a
   `<workflow>.marker.json` sidecar recording the `next_marker`.
2. Executes the phased manifest using the same dispatcher the daily transform
   uses; per-task ledger entries record each outcome.
3. Calls `commit_transform_backfill`, which promotes the sidecar's
   `next_marker` to the live marker. This is **unconditional** — it runs even
   after swallowed per-task (exit-1) failures, so the marker advances at a
   predictable cadence and a transiently-failed transform task is a recorded
   hole, not a perpetual retry. An exit-2 `WorkflowTerminatingError` aborts the
   run before commit, leaving the marker unchanged for the next run to
   re-derive.

Because the plan is a pure function of the extraction ledger and the marker,
the backfill planner does not self-filter against its own ledger (unlike the
daily transform). Resetting the marker to the empty string re-derives every
transform task from the oldest extraction ledger forward — the supported way
to re-transform after a feature-spec change.

The two scopes use independent markers, so a hiccup transforming the weather
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
| 2 | Workflow-terminating — `WorkflowTerminatingError` raised | Task fails; the surrounding workflow aborts (skipping any cursor- or marker-commit step) |

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
what gates the commit step (an exit-2 failure leaves the extraction cursor —
or the transformation consumed-through marker — unchanged for the next run).

Other notes:

- Workflow `try/except` blocks ensure log consolidation always runs, even
  on failure.
- Structured logging includes `status_code`, `url`, and response body for
  HTTP errors.
