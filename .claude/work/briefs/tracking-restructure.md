# Restructure `tracking/` prefix to group files by date

Inspecting the state of a day's run currently requires picking through several
disjoint paths: `tracking/ledger/<date>/`, `tracking/manifests/` (if
separate), `tracking/cursors/`. Grouping everything under a single date
directory makes a run's state legible at a glance and enables atomic
archival/cleanup of old runs.

## Proposed layout

```
tracking/
  <date>/
    manifests/
    ledger/
    ...
```

## Decision point — cursors

Cursors are mutable state that advance *across* runs, not per-run artefacts.
Two options:

### (a) Keep cursors at top level

`tracking/cursors/<workflow>.json`. Easy to find; conceptually separate from
the per-date tree. Only the per-run files move.

### (b) Write cursor snapshot under `tracking/<date>/` on each commit

The "current" cursor is wherever the most recent run wrote it. Adds
discoverability of cursor history but makes "what is the live cursor?"
harder to answer without a glob.

**Recommendation:** Option (a) is simpler and probably right unless cursor
history becomes useful. Cursor rewinding (manual recovery from bugs) is
important enough to keep cursors discoverable at a single top-level location.

## Cursor-rewind operations (context for layout decision)

Occasionally after a transform bug, cursors need to be manually rewound to
re-process a window. The pattern:

1. Identify the workflow's cursor file: `tracking/cursors/<workflow>.json`.
2. Move the failed run's consolidated ledger out of the scan root:
   `gsutil mv tracking/ledger/<date>/<date>-*-<workflow>.jsonl
   tracking/superseded-ledgers/<date>/`. The `WorkflowOrchestrator`
   scans only `tracking/ledger/`, so ledgers in `superseded-ledgers/`
   don't mask re-runs.
3. Rewind `consumed_through` to a value sorting strictly below the first
   unconsumed extract ledger you want replayed (e.g., `"2026-05-22"`).
4. Re-trigger the backfill via Cloud Scheduler.

Keeping cursors at `tracking/cursors/` (option a) makes step 1 quick and
reduces discovery friction in emergency recovery.

## Scope

Coordinated change: path builders in `pv-prospect-data-sources`, any
hardcoded paths in orchestration code, GCS object renames (existing files),
and Terraform / Cloud Workflows step references all need updating together.
