# Data-Derived Planning for the Transformation Backfills

Origin: `reports/data-pipeline-retrospective.md` §4 (the critique) and §7
(recommendation 1).

## What

Replace the transformation backfills' ledger-and-marker planning with planning
derived from observed data state: the work to do is the set difference
*raw partitions present − prepared partitions present* (reconciled over
`data/prepared-batches/`). Delete the consumed-through markers; keep writing the
transform task-outcome ledger, but demote it to **audit trail only** — no planner
consults it. Additionally, assess whether the daily transform
(`pv-prospect-transform`) should adopt the same single-process, data-derived shape
(which would also eliminate its manifests — see `manifest-env-footprint`).

## Why

Tracking files under `tracking/` are currently authoritative for what transform
work exists and what is done, and nothing reconciles them against bucket contents.
Three concrete failure modes trace to this:

- **Silent gaps.** The documented incident where a path-prefix mismatch let the
  cursor advance over un-run work (`doc/orchestration.md` §"Why no Workflow?") —
  the commit step trusted the tracking file, never the data.
- **Recovery toil.** Marker rewind alone cannot replay a window; consolidated
  ledgers must be manually moved to `superseded-ledgers/` to stop masking re-runs
  (replay-window runbook; the 2026-05-23 outage recovery; the 2026-06-12 corpus
  re-transform).
- **Convention-change hazard.** Task hashes derive from env vars, not output
  content or code version, so a transform-logic change that alters outputs no-ops
  against old `completed` entries unless ledgers are superseded by hand.

Under data-derived planning this bug class degrades to redundant (idempotent)
recomputation instead of silent gaps, and replaying a window collapses to "delete
the prepared partitions and re-trigger".

## Scope boundary

**Extraction is unaffected.** Deliberate failure holes (the predictable-cadence
trade-off) mean raw-file absence cannot distinguish "never planned" from
"attempted and failed"; the extraction ledger and cursors remain authoritative.
Note the planning semantics still compose correctly: a failed extraction leaves no
raw partition, so no transform slice is planned — the same hole the current
ledger-based planner leaves.

## Folded-in scope

The **transform half of `outage-recovery`**: once this lands, transform-side
outage recovery is "delete the affected prepared partitions" — no ledger moves, no
marker rewind. That brief re-scopes to extract-side only (see Follow-ups).

## What is needed

1. **Design precondition — partition-naming contract.** Slice identity must be
   reconstructible from listings alone: `data/raw/` paths must yield the planned
   slices (replacing the extraction-ledger descriptors `plan_slices` reads today)
   and `data/prepared-batches/` filenames must deterministically encode scope,
   site/location, and window for the set difference. Verify the existing path
   builders in `pv-prospect-data-sources` encode enough; if not, the naming change
   is part of this task.
2. **Replace the planner.** Rework `plan_slices` to a raw-vs-prepared listing diff
   (one GCS list per prefix per run — cheap at current scale). Retain a per-run
   work cap to replace `MAX_EXTRACT_RUNS` so a large backlog can't outrun the
   Cloud Run Job timeout.
3. **Delete marker machinery.** Remove the two consumed-through marker files, the
   `ConsumedMarker` save/load paths, and the transform branches of
   `completed_task_hashes()`-based resume filtering.
4. **Demote the ledger.** Keep the `LedgerCollector` flush as audit output. Update
   `doc/orchestration.md` (Transformation Backfills) and the replay-window runbook
   (which collapses to partition deletion).
5. **Assess the daily transform** for the same treatment; if adopted, transform
   manifests cease to exist (then delete `manifest-env-footprint`).

## Sequencing

- **After `version-raw-data`** — the safety precondition: with "delete prepared
  and re-run" as the routine recovery move, raw must be versioned first (PVOutput
  history is non-refetchable).
- **Before `tracking-restructure` and `end-date-semantics`** — avoid renaming or
  migrating tracking files this task deletes.

## Follow-ups on landing

Re-scope the dependent briefs: `outage-recovery` (extract-side runbook only),
`tracking-restructure` (pure legibility/archival; transform markers gone),
`end-date-semantics` (extraction cursors only); verify daily-transform manifest
sizes and delete or re-scope `manifest-env-footprint`.