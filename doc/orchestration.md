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

## Contents

- [Bucket Layout](#bucket-layout)
- [Core Concepts](#core-concepts)
- [Daily Workflows (Extraction and Transformation)](#daily-workflows-extraction-and-transformation)
- [Extraction Backfill Workflows (Plan-Commit Pattern)](#extraction-backfill-workflows-plan-commit-pattern)
- [Retry and Rate-Limit Handling](#retry-and-rate-limit-handling)
- [Error Handling](#error-handling)

## Bucket Layout

The staging bucket has three top-level prefixes:

```
gs://pv-prospect-staging/
├── data/
│   ├── raw/               (extracted CSV)
│   ├── cleaned/           (CSV)
│   ├── prepared-batches/  (headerless micro-batch CSV)
│   └── prepared/          (cumulative CSV)
├── tracking/
│   ├── manifests/<run_date>/<workflow>.json           (orchestrator phases manifest — for the daily transform/extract; extract-backfill workflows embed phases inside this same path alongside their date window + next-cursor. Transform backfills don't write manifests — see Transformation Backfills.)
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
one file. The **transformation** backfills don't write a manifest at all —
they don't run inside a Cloud Workflow at all; planning, execution, and
commit happen in a single Cloud Run Job execution (see
[Transformation Backfills](#transformation-backfills)).

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
`<ledger_storage>/<run_date>/<workflow>/<task_hash>.jsonl` — one file per
Cloud Run *task*, named for the per-task `TASK_HASH` the planner injects.
A task whose handler iterates many sub-items (e.g. the weather-grid
backfill batch container, processing ~1,330 grid points in a single
process) buffers every sub-item outcome in an in-memory `LedgerCollector`
and writes the file as a single end-of-task flush — one file per task,
not one per sub-item. The end-of-workflow `consolidate_logs` job calls
`consolidate_ledger`, which merges these per-task files into a single
`<ledger_storage>/<run_date>/<run_date>-<HHMMSS>-<workflow>.jsonl` sorted
by `recorded_at` and deletes the originals.

A *single-process* workflow that runs the whole day's work as one Cloud
Run task — the transformation backfill — skips the scratch step
entirely: it passes a `LedgerCollector` to the orchestrator and calls
`flush(ledger_fs)`, which writes the consolidated
`<run_date>-<HHMMSS>-<workflow>.jsonl` directly. Either way, the
in-memory collector is what prevents the O(N) GCS round-trip cost that
unbounded per-sub-item fan-out would force. Two flush modes on the same
collector accommodate the two workflow shapes:

| Mode | Caller | Output path |
|---|---|---|
| `flush(ledger_fs)` | one Cloud Run task per day (transform backfill) | consolidated `<date>-<HHMMSS>-<workflow>.jsonl` |
| `flush_per_task(ledger_fs, task_hash)` | many Cloud Run tasks per day (daily extract, extract backfills) | scratch `<date>/<workflow>/<task_hash>.jsonl` (merged by `consolidate_ledger`) |

`filter_remaining_tasks()` consults the ledger and skips any task whose hash
appears with `status='completed'`. Task identity is computed by
`compute_task_hash(env)` over each task's env at filter time; the planner
also pre-injects the result as `TASK_HASH` in each env so the container
can name its per-task scratch file at flush time. `filter_remaining_tasks`
scans:

1. The current run's per-task files under `<run_date>/<workflow>/` — skipped
   when the orchestrator uses a `LedgerCollector`, which writes none until
   the end-of-task flush.
2. Every consolidated ledger file ever produced for this workflow
   (`<*>/<*>-*-<workflow>.jsonl`), to preserve cross-day resume.

Failures alone do not cause skipping — a re-run will re-attempt failed tasks.
When `ledger_fs` is not configured (e.g. some local dev setups), filtering is
disabled and every task is dispatched.

#### Cost of the Ledger Scan

Scan (2) reaches across the whole ledger history, so its cost must not scale
with that history. Two properties keep it from doing so:

- **The workflow-name suffix is a listing pattern, not a post-filter.**
  `list_consolidated_ledgers` passes `*-<workflow>.jsonl` to
  `FileSystem.list_files`, and the GCS backend turns it into a server-side
  `matchGlob`. A listing is billed one Class A `ListObjects` op per 1,000
  objects it *pages over*, so filtering client-side would charge for every
  object in `tracking/ledger/` — per-task scratch files included — to find
  the handful of consolidated ones.
- **Callers holding a high-water mark pass it as `since`.** The transform
  backfill's consumed-through marker bounds the listing to run-date
  directories at or after the marker's date; everything below it would be
  discarded anyway.

Left unbounded, this scan is what an accumulation of un-consolidated scratch
files silently taxes: 173k orphaned per-task files once made every workflow
invocation page over 174 listings' worth of objects to read ~57 ledgers. See
`.claude/work/reports/ledger-scan-cost.md`.

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

The transformation side splits differently: the daily transform
(`pv-prospect-transform`) self-filters via the orchestrator at plan time;
the transformation backfill self-filters at run time (inside the same
in-container handler that does the work — see
[Transformation Backfills](#transformation-backfills)). In both
transform cases the goal is the same: skip units already recorded as
`completed` so re-runs only do the unfinished work.

### Write-Audit Logs

`LoggingFileSystem` decorates the staged data filesystems and records a
one-line breadcrumb for every write (content: ISO timestamp +
`CREATED <label>/<path>`). Where breadcrumbs land mirrors the ledger
arrangement above:

- Each Cloud Run task gives its `LoggingFileSystem` a shared in-memory
  `LogCollector`; breadcrumbs are buffered and the task flushes one file
  at end of run (`flush_per_task` for many-tasks-per-day workflows or
  `flush` for the single-task-per-day transform backfill).
- The end-of-workflow `consolidate_logs` job merges the resulting
  per-task files at `<run_date>/<workflow>/<task_hash>.txt` into a
  single `<run_date>-<HHMMSS>-<workflow>.txt`.

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
   task env per dispatched batch, each carrying a pre-injected `TASK_HASH`
   so the container has a stable identity for its end-of-task scratch
   file. For the weather-grid backfill, each batch env carries a
   `LOCATIONS` JSON array of grid-point lat,lon strings; the container's
   per-site loop records one ledger entry per `(site, window)` pair into
   an in-memory `LedgerCollector` and writes all of them as one
   per-task scratch file at end of batch.
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

### The Archive Floor

`MIN_ARCHIVE_DATE` (2016-01-01, also in `pv_prospect.etl.backfill`) is where both
extraction backfills stop marching. OpenMeteo's historical-forecast archive
begins on a fixed per-model start date; 2016-01-01 is the earliest any of the
models we request reaches. The API validates a request against the **union**
floor across those models and rejects the *whole* request with a `400` when
`start_date` falls below it — so a window straddling the floor loses its valid
days too, and the two scopes both truncate their bottom window to
`[MIN_ARCHIVE_DATE, end)` rather than let it straddle.

The floor is fixed, not a sliding window: no amount of waiting or retrying makes
pre-2016 data appear. A backfill that marches past it fails every fetch, every
day, forever — and because a per-site fetch failure is swallowed into a `failed`
ledger entry rather than raising `WorkflowTerminatingError`, the workflow still
reaches its commit step and advances the cursor anyway. Nothing stops it. The
grid-weather corpus below 2016 is therefore a permanent, unfillable gap, not a
hole waiting to be backfilled.

What the two scopes do on arrival differs, because only the weather grid has a
spatial axis to spend further runs on:

- **PV-sites halts.** Its cursor settles on `MIN_ARCHIVE_DATE` as a fixed point,
  and every subsequent run plans the empty window `[floor, floor)` — no slices,
  nothing dispatched, an unchanged cursor committed. The empty-window fixed point
  is also what stops a cursor left *below* the floor from planning an inverted
  window.
- **Weather-grid rolls to the next density pass** — see below.

Neither reaching the floor nor going quiescent is an error; a halted backfill is
a backfill that has finished its work.

### Weather-Grid Density Passes

The weather-grid march is **two-dimensional**: `(next_end_date, density_pass)`.
Time is the fast inner axis, bounded below by the floor; the density pass is the
slow outer axis.

Each Step-3 run covers 8 windows on a **fixed 14-day grid** tiling
`[MIN_ARCHIVE_DATE, today]`, and each window draws exactly one of the 32
grid-point sample files. One sweep of the grid is a *density pass* (~274 windows
at 8/day ≈ 34 daily runs). On reaching the floor the cursor rolls: the pass
increments and time resets to the top of the grid. Because a window's sample file
is the pure function

```
sample_index = (density_pass + window_index) % num_sample_files
```

consecutive windows within a pass draw consecutive sample files, and the *same*
window draws a *different* sample file on every pass. After `k` passes the
historical grid holds `k` distinct sample files per window — a relative spatial
density of `k / 32`. The march goes quiescent at `TARGET_DENSITY_PASSES` (4, i.e.
12.5%); Step 2's rolling recent-history sample continues regardless.
Densification is monotonic, so the target can be raised later without rework or
re-fetching.

Two properties of this scheme are load-bearing:

- **The window grid is fixed, not re-anchored per pass.** Its phase is inherited
  from the as-built march (every window fetched to date is congruent to
  2016-01-08 modulo 14 days), so passes re-use one tiling rather than laying a
  second, offset one over the same history. Were each pass instead to start its
  windows at its own start date — ~34 runs apart, and 34 is not a multiple of
  14 — successive passes would tile the same history with *overlapping* windows,
  and a grid point drawn twice over overlapping date ranges would duplicate rows
  in the prepared corpus.
- **The historical march never reaches the trailing window.** A pass starts one
  full window below `today`, snapped down onto the grid. Step 3's sample file is
  chosen without reference to Step 2's, so date-disjointness is the only thing
  preventing the two from drawing the same grid point over the same days.

Downstream is unaffected: each `(sample_index, window)` pair writes its own
prepared partition (`weather_{start}_{end}_{ver}-{NN}.csv`), so passes accumulate
files rather than colliding, and the transformation backfill picks them up from
the new `completed` descriptors automatically.

Before this design the cursor was one-dimensional and the sample file was a side
effect of how far back the march had gone (`(anchor − sample_offset) % 32`, with
`sample_offset` growing without bound). It ran off the bottom of the archive
instead of building density. A cursor still in that shape — carrying
`next_sample_offset` and no `density_pass` — is read as pass 0 with a floored
`next_end_date`, so it rolls to pass 1 and restarts at the top of the grid on its
next run. It needs no hand migration.

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
(1 Step-2 + 8 Step-3, and just the Step-2 batch once the historical march has
gone quiescent) sequentially, pausing `sleep_seconds_between_batches` (default
720 s) between successive dispatches; the in-batch sleep is what keeps the
dispatch rate under OpenMeteo's 5,000 requests/hour limit. Total wall time ≈
3 h 24 min; the cursor commits once every batch has been attempted. Failed sites
within a batch are recorded in the ledger but not retried — the cursor still
advances. There is no per-batch checkpoint; a workflow-level failure leaves the
cursor at yesterday's position and tomorrow's run re-plans the same window.

### Transformation Backfills

The transformation backfills do **not** use the cursor/plan-commit pattern
and do **not** run inside a Cloud Workflow. A transformation is a
recomputable derivative of the durable extraction harvest, so rather than
marching a cursor of their own they plan from the extraction backfill's
committed record. And because all the per-unit work is GCS-bound and runs
quickly, the entire run fits comfortably inside a single Cloud Run Job
execution.

Cloud Scheduler invokes the `data-transformation` Cloud Run Job directly
(no Workflow), with `JOB_TYPE=run_transform_backfill` and a
`BACKFILL_SCOPE` (`pv_sites` or `weather_grid`). The handler
(`_run_transform_backfill` in
`pv_prospect.data_transformation.processing.entrypoint`) does the full run:

1. **Plan** — `plan_slices(scope, ledger_fs, cursors_fs, max_extract_runs)`
   loads the scope's **consumed-through marker** from
   `tracking/cursors/<workflow>.json`, lists the corresponding extraction
   backfill's consolidated ledgers *from the marker's run date onwards*,
   takes the oldest `MAX_EXTRACT_RUNS`
   (default 4) whose filename sorts above the marker, and turns every
   `completed` descriptor into a `PVSlice` or `WeatherSlice`. Returns the
   slices plus the `next_marker` that the marker should advance to once the
   work succeeds. `failed` extraction entries are skipped (no raw data, no
   transform task — correctly leaving a hole).
2. **Filter** — a direct `completed_task_hashes()` lookup drops slices
   whose hashes already appear `completed` in any prior consolidated
   ledger. (The backfill writes no per-task ledger files, so a fresh run
   resumes from the consolidated layer alone.) This makes the run
   idempotent: a second invocation re-plans the same slices but only the
   unfinished ones run.
3. **Run** — remaining slices are dispatched through a `ThreadPoolExecutor`
   (`MAX_WORKERS=32` by default), each calling `produce_pv_slice` or
   `produce_weather_slice`. Each function reads the slice's raw inputs,
   runs clean -> prepare -> assemble entirely in memory, and writes one
   prepared partition file directly. No `cleaned/` files are written and
   no per-batch CSV hand-off is needed. Per-slice `Exception` is logged
   and swallowed (outcome recorded as `failed` in the ledger);
   `WorkflowTerminatingError` propagates out without advancing the marker.
4. **Flush** — the `LedgerCollector` and `LogCollector` are each flushed
   once, writing a single consolidated
   `<run_date>-<HHMMSS>-<workflow>.jsonl` / `.txt` — the same files the
   cross-day resume scan reads. Because the run is one process there is
   no per-task fan-out and so no O(N) consolidation step; each flush is a
   single GCS write.
5. **Commit** — `save_marker(cursors_fs, workflow_name,
   ConsumedMarker(next_marker))` advances the marker.

Because the plan is a pure function of the extraction ledger and the
marker, lowering the marker re-derives the transform slices covering the
windows above it. Marker-rewind alone is not sufficient to replay a
window, though — the resume filter pools `completed` hashes across every
prior consolidated ledger; see
[`pv-prospect-data-transformation/doc/runbooks/replay-window.md`](../pv-prospect-data-transformation/doc/runbooks/replay-window.md)
for the full recovery procedure.

The two scopes use independent markers, so a hiccup transforming the
weather grid does not block PV-sites progress (or vice versa).

#### Why no Workflow?

The transform-backfill briefly ran as a Cloud Workflow (with chunked
manifests on GCS and per-chunk Cloud Run dispatch) but that path
collided with multiple Workflows runtime ceilings — the 2 MiB
per-HTTP-response limit on manifest fetches, the 256 MiB per-execution
memory budget when fanning out parallel branches, and the hard 100 K
step-count limit at ~50 steps per unit. Each accommodation (sharded
manifest files, hoisted common env, in-container chunk handlers) added
machinery that wasn't intrinsic to the work — and ultimately a path-
prefix mismatch between the chunk-dispatcher's env vars and the
container's storage backend silently advanced the cursor over un-run
work. Running the whole backfill inside one Cloud Run Job execution
removes all four problems at once: GCS reads use the storage client
(no 2 MiB cap), parallelism is bounded by a thread pool (memory under
control), and there are no workflow steps to count. The trade-off is
that one Cloud Run timeout aborts the whole run; the catch-up backlog
is small enough that this hasn't been a real concern.

## Retry and Rate-Limit Handling

- **Python layer**: `@retry_on_429` decorator with exponential backoff (1, 2,
  5, 10 minutes), capped at 20 minutes total to avoid exceeding Cloud Run task
  timeout.
- **Workflow layer**: Cloud Workflows retry predicate on HTTP 429/500/503, max
  3 retries with 60–600s exponential backoff.
- **PVOutput**: `PVOutputRateLimiter` tracks rate-limit headers from API
  responses.
- **Scheduling**: Workflows are staggered to prevent concurrent API access
  from breaching rate limits (see
  [`infrastructure.md`](infrastructure.md#scheduling-rationale) for the schedule).

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
