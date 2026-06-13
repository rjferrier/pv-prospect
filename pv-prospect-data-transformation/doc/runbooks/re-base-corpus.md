# Re-basing the whole PV corpus onto a new feature convention

Use this when a **feature-spec change touches every partition** — e.g. the
`prepare_pv` POA/target convention change in submodule commit `c59f018` — so the
goal is a `data-v<date>` tag whose corpus is **entirely** on the new basis. For
plugging a single hole left by a buggy run, use
[`replay-window.md`](replay-window.md) instead; for the orchestration primitives
(ledgers, markers, plan/commit) see
[`doc/orchestration.md`](../../../doc/orchestration.md).

## Why a re-run of the pipeline is not enough

The data-versioner is **accumulate-only**: `data_versioner/core.py` reads staging,
`dvc add`s it onto the existing corpus (overwriting by path), then wipes staging.
It never *removes* a partition. So re-running the pipeline + versioner leaves any
window you didn't regenerate — and any vestigial `{site}.csv` master — on the old
basis. An all-new-basis tag therefore needs an explicit **clean rebuild** (`dvc rm`
the tree, drop in the regenerated partitions, `dvc add` → push → commit → tag), not
a versioner run.

Two further facts shape the procedure:

- **The corpus is the union of two producers.** The pv-sites *backfill* job emits
  uniform 28-day windows marching backwards through history; the *daily-transform*
  workflow emits the recent rolling **tail**. A full re-base must regenerate
  **both**, or the seam between them is mixed-basis.
- **The writer is merge-not-skip** (`slice_producer.py` → `merge_prepared_frames`,
  `keep='last'`). Overwriting in place is *not* safe across a basis change: if a raw
  day that contributed to a partition is now missing, the old-basis row survives the
  merge. **Delete the old staging partitions before re-transforming**, so a miss is
  absent-and-detectable rather than silently stale.

## Procedure (skeleton)

Run with **all schedulers paused** (a stray daily / backfill / versioner mid-op =
mixed-basis corpus + wiped staging). Resume them unconditionally at the end.

1. **Reset transform state.** Back up cursors. Supersede the backfill's consolidated
   ledgers (move them out of the `tracking/ledger/` scan root — see
   [`replay-window.md`](replay-window.md) for why marker-rewind alone is filtered
   out). Delete the old staging pv partitions. Rewind the backfill marker
   (`consumed_through: ""`) to re-plan all history.
2. **Re-transform the backfill** in one shot (`MAX_EXTRACT_RUNS` > ledger count).
   Gate on an **exact** partition count (windows × sites); a shortfall is a holey
   corpus.
3. **Re-transform the tail** from the backfill seam to today, **one day at a time,
   sequentially** — same-ISO-week days read-modify-write the same rolling weekly
   partition via `merge_prepared_frames`, so concurrent runs race and lose writes.
4. **Clean-rebuild the tag, on `main`.** In a fresh clone (the versioner clones
   `instance_repo_branch: 'main'`, so a rebuild committed elsewhere is regressed by
   the next weekly version): `dvc rm` the entire pv tree (partition `.dvc` **and**
   any `{site}.csv` masters), drop in the new-basis partitions, `dvc add` →
   `dvc push -r feature` → commit → `git tag data-v<date>` → push. `dvc pull` at a
   tag serves exactly that tag's `.dvc` files, so a miss becomes
   absent-and-detectable.
5. **Retrain + measure**, then resume schedulers. The prior `data-v<date>` tag stays
   pullable as the old-basis rollback point (immutable tag; blobs not gc'd).

## Worked example

The `data-v2026-06-12` re-base (PV model onto the served 24 h-mean POA basis) is
recorded end-to-end — phase table, counts, gotchas, and the corrected outcome — in
[`.claude/work/reports/pv-train-on-served-poa.md`](../../../.claude/work/reports/pv-train-on-served-poa.md)
(§2–3).
