# Plug the 2025-08-08 → 2025-09-05 PV window

The pv-sites corpus is missing one 28-day window per site at the start of
the available history. Raw data exists; the gap is purely a transform
miss caused by the `assemble_pv` task-hash collision (now fixed by
commit `8fccfa9`). The cursor has already advanced past the relevant
extract ledger, so neither the daily nor the weekly schedule will replay
it on its own.

## Background: the assemble_pv collision bug

`build_transform_phases` emits one `assemble_pv` task per `pv_system_id`,
keyed by `assemble_pv_inputs.setdefault(pv_system_id, transform_input)`.
The task hash therefore carries only the *first-seen* input's
`(START_DATE, END_DATE)` per system. When a later run's first input
per system happens to share those dates with an earlier completed
entry, `filter_remaining_tasks` masks the whole assemble phase → phase 2
runs 0 units, the collector is dropped, no partition files are written.

The `assemble_weather` side had the structurally identical bug,
fixed in commit `3dd637f` by keying on `(grid_point_sample_index, start_date, end_date)`
and emitting one task per slice. The PV side wasn't updated in lockstep.

The fix is symmetric: key `assemble_pv_inputs` on `(pv_system_id, start_date, end_date)`,
emit one task per `(system, window)`, and change `assemble_prepared_pv`
to write only its slice.

## Sequence

Needs the new data-transformation image deployed first (carries `3dd637f` + `8fccfa9`):

### Cursor-rewind pattern

To re-process a transform-backfill window after a bug, **both** a cursor
rewind and consolidated-ledger removal are required:

- **Cursor rewind:** `gs://pv-prospect-staging/tracking/cursors/<workflow>.json`,
  shape `{"consumed_through": "<extract-ledger-name>"}`. The `plan_units`
  function re-plans every extraction ledger whose name sorts strictly above it,
  so lowering `consumed_through` re-derives the units.

- **But:** `WorkflowOrchestrator.filter_remaining_tasks` pools `completed` hashes
  from every consolidated transform ledger ever written, and `compute_task_hash`
  excludes `RUN_DATE`. So a task recorded `completed` is skipped on re-run
  regardless of run date — unless the ledger is removed.

- **Collector implication:** the in-memory `PreparedBatchCollector` means
  `assemble` only sees data that `prepare` produced *in the same process*.
  If `prepare` is skipped (its `completed` entry survives), the collector is
  empty and `assemble` silently writes a hole.

**How to apply:**
1. Move the failed run's consolidated ledger out of the scan root:
   `gsutil mv gs://pv-prospect-staging/tracking/ledger/2026-05-25/2026-05-25-060204-pv-prospect-transform-pv-sites-backfill.jsonl`
   to `tracking/superseded-ledgers/2026-05-25/`. Keep ledgers for windows
   you are *not* re-running — their task hashes are disjoint (distinct
   `START_DATE`/`END_DATE`).

2. Back up the cursor: `gsutil cp gs://pv-prospect-staging/tracking/cursors/pv-prospect-transform-pv-sites-backfill.json gs://pv-prospect-staging/tracking/cursors-backup-recovery-2026-05-{date}/`.

3. Rewind the cursor to a value sorting strictly below
   `2026-05-23-030740-pv-prospect-extract-pv-sites-backfill.jsonl`
   (the earliest unconsumed extract ledger covering windows we want
   replayed). Setting `{"consumed_through": "2026-05-22"}` works.

4. Trigger `pv-prospect-daily-transform-pv-sites-backfill` once via
   Cloud Scheduler. With `MAX_EXTRACT_RUNS=4`, one run covers the
   2026-05-23 / 2026-05-24 / 2026-05-25 unconsumed ledgers.

5. Verify `gsutil ls gs://pv-prospect-staging/data/prepared/pv/{site}/`
   shows a new `pv_{site}_2025-08-08_2025-09-05.csv` for all 10 sites.

**Wall-clock:** ~10–15 min, mostly waiting on Cloud Run. Best done before
the next weekly versioning (Sunday 23:00 UTC) so v2 includes the
window; if it slips, the plug lands in v3 instead. Re-runs are idempotent:
`assemble` de-dups on `time` with `keep='last'`.
