# Replaying a transform-backfill window

This runbook covers re-processing one or more transform-backfill windows
that completed under buggy code — the canonical case being an
`assemble_*` task that recorded `completed` but wrote no partition file,
leaving a silent hole in `data/prepared/`. See
[`doc/orchestration.md`](../../../doc/orchestration.md) for the underlying
plan / filter / commit design.

## Why marker-rewind alone is not enough

Lowering `consumed_through` in
`gs://<staging-bucket>/tracking/cursors/pv-prospect-transform-<scope>-backfill.json`
causes `plan_units` to re-derive every transform unit covering windows
above the new marker. But the planned units immediately go through
`WorkflowOrchestrator.filter_remaining_tasks`, which pools `completed`
hashes from **every** consolidated transform ledger ever written
(`tracking/ledger/<date>/<date>-*-<workflow>.jsonl`). `compute_task_hash`
excludes `RUN_DATE`, so a unit the bad run recorded `completed` is
filtered out on re-run regardless of which day's ledger holds the entry.

The failure mode is sharp: each slice runs clean -> prepare -> assemble
in a single call stack, so if the slice is filtered out by the completed
hash, no partition file is written — exactly the silent hole the original
bad run produced.

To actually replay, the offending run's consolidated ledger must leave
the resume-scan root. `tracking/superseded-ledgers/` is outside the scan
prefix, so `gsutil mv` there keeps the audit trail without affecting the
re-run.

Hash-scheme caveat: when the fix changed the task-hash shape (e.g.
commit `8fccfa9` keyed `assemble_pv` on `(system, start, end)`), the
old-run hashes won't match the new-run hashes anyway. Moving the ledger
out is still the right move — it documents intent and removes the
nothing-happens risk if a hash collision happens to be reintroduced.

## Procedure

1. **Identify the windows to replay.** Read the offending consolidated
   transform ledger and note its `(start_date, end_date)` set. Confirm
   the corresponding extraction ledger names — the planner consumes
   extract ledgers in name-sorted order, and `consumed_through` is set
   in that name space.

2. **Back up the cursor.**
   ```
   gsutil cp gs://<staging-bucket>/tracking/cursors/pv-prospect-transform-<scope>-backfill.json \
             gs://<staging-bucket>/tracking/cursors-backup-recovery-<YYYY-MM-DD>/
   ```

3. **Move the offending consolidated ledger(s) out of the scan root.**
   ```
   gsutil mv gs://<staging-bucket>/tracking/ledger/<date>/<date>-<HHMMSS>-pv-prospect-transform-<scope>-backfill.jsonl \
             gs://<staging-bucket>/tracking/superseded-ledgers/<date>/
   ```
   Keep ledgers for windows you are *not* replaying — their task hashes
   are disjoint (distinct `START_DATE` / `END_DATE`) and so they don't
   interfere.

4. **Rewind the cursor** to a value sorting strictly below the earliest
   extract ledger name covering the windows to replay. Since
   `consumed_through` is compared lexicographically, the date prefix
   `YYYY-MM-DD` works as a marker between days:
   ```
   echo -n '{"consumed_through": "<YYYY-MM-DD>"}' | \
       gsutil cp - gs://<staging-bucket>/tracking/cursors/pv-prospect-transform-<scope>-backfill.json
   ```
   (Setting the marker to a bare date means the same-dated ledger
   `<date>-<HHMMSS>-...` is treated as unconsumed, since the shorter
   prefix sorts below it.)

5. **Trigger the scheduler job.**
   ```
   gcloud scheduler jobs run pv-prospect-daily-transform-<scope>-backfill --location=europe-west2
   ```
   Each run consumes up to `MAX_EXTRACT_RUNS` (default 4) extract
   ledgers; re-trigger if the cursor still trails after the run
   completes.

6. **Verify** the new partition files exist:
   ```
   gsutil ls gs://<staging-bucket>/data/prepared/pv/<system_id>/pv_<system_id>_<start>_<end>.csv
   ```
   The next data-versioner run will then roll them into the next
   `data-v<YYYY-MM-DD>` tag.

Replays are idempotent: `assemble` de-duplicates on `time` with
`keep='last'`, so re-running over a window that already has a partition
file produces the same content-named file.

## Timing

Best done before the next scheduled data-versioner run so the plug lands
in the next tag. If it slips, the plug lands in the tag after — the
partition file sits in `data/prepared/` until the versioner consumes and
cleans it.
