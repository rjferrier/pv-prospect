# Report: Scope the Ledger-Consolidation Listing

> Written 2026-07-09. Records the diagnosis and repair of an unbounded GCS
> `ListObjects` cost in the consolidated-ledger scan, and the deletion of the
> 172,692 orphaned scratch ledger files that were driving it.

## Summary

A billing investigation (2026-07-06) found that Cloud Storage cost on
`pv-prospect-staging` was dominated by **operations**, not data at rest:
`ListObjects` (Class A) ran at a nearly-flat ~9,955/day, worth roughly 8–12× the
bucket's at-rest storage cost. The originating brief attributed this to
`WorkflowOrchestrator.completed_task_hashes()`, which calls
`list_consolidated_ledgers()` — an unscoped recursive walk of the whole
`tracking/ledger/` prefix, filtered down to the one workflow of interest only
*after* every object is fetched — on every workflow invocation.

The brief's diagnosis of the code path was right, but its model of *why the walk
was expensive* was incomplete, and this changed what the fix had to be. A listing
is billed one Class A op per **1,000 objects paged over**, not per object
returned. Enumerating `tracking/ledger/` found 173,126 objects, of which
**172,692 (99.7%) were orphaned per-task scratch files in just three run-date
directories** — `2026-05-{17,18,19}/pv-prospect-transform-weather-grid-backfill/`
— residue of the pre-`LedgerCollector` fan-out era, whose end-of-workflow
`consolidate_ledger` step never completed. Every other run date holds ~6 objects.
The walk cost 174 pages because of those three days, not because the ledger
history is long: at ~6 objects/day the rest of the tree would have stayed inside
a single 1,000-object page for years.

So the work split in two. The **cleanup** (deleting the orphans) is what stops
the measured bleed. The **code change** is the guard that stops it recurring, and
it makes the listing's cost proportional to the number of ledgers that match
rather than to the size of the tree:

- `FileSystem.list_files` gained a `start_offset` lower bound, and the GCS
  backend now pushes the basename *pattern* down to a server-side `matchGlob`
  instead of fetching everything and filtering client-side.
- `list_consolidated_ledgers` passes its `*-<workflow>.jsonl` matcher as that
  pattern, and accepts a `since` bound.
- The transform backfill's `_consume_extract_descriptors` passes its
  consumed-through marker as `since` — losslessly, since it discards everything
  at or below the marker regardless.

Measured against the live bucket, the scan that `completed_task_hashes()`
performs went from **174 pages / 43.6 s** to **1 page / 0.06–0.2 s** once both
parts landed. No resume semantics changed: the scan still pools completed task
hashes from every consolidated ledger ever written for a workflow, which is the
invariant `replay-window.md` depends on. A retention window over that history was
considered and **rejected** — see §5.

## Contents

1\. Introduction\
2\. Diagnosis\
3\. What the orphaned files were\
4\. The code change\
5\. What was deliberately not done\
6\. Execution record\
7\. Verification\
8\. Conclusions and follow-ups

References

## 1. Introduction

The 2026-07-06 billing investigation asked why `pv-prospect-staging` costs what it
does. Measured `total_bytes` across the project's europe-west2 buckets is ~9.9
GiB — at the Standard Storage London SKU ($0.023/GiB-month) about $0.008/day.
Against that, Cloud Monitoring's `storage.googleapis.com/api/request_count`
showed `ListObjects` at ~9,955/day with a slow day-over-day drift (9,952 → 9,959
across 2026-06-27 → 07-05). At the Regional HNS Standard Class A price
($0.0000065/op), operations were running ~8–12× the storage cost, and the drift
implied the number would keep climbing.

The brief that opened this task located the call path and asked for the listing
to be scoped. This report records what the listing actually cost and why, what
changed, and what was left alone.

## 2. Diagnosis

`completed_task_hashes()`
(`pv-prospect-etl/pv_prospect/etl/orchestration.py`) is called by
`filter_remaining_tasks()` at plan time and directly by the extraction
container's per-site loop. It calls `list_consolidated_ledgers()`
(`pv-prospect-etl/pv_prospect/etl/storage/ledger.py`), which did:

```python
ledger_fs.list_files('', '*.jsonl', recursive=True)   # every run date, every workflow
```

then `fnmatch`ed the basenames down to `*-<workflow>.jsonl`.

The billing unit matters here. GCS charges one Class A `ListObjects` op per page,
and a page holds up to 1,000 objects **scanned**. Client-side filtering therefore
pays for the whole subtree in order to return a handful of files. Enumerating the
prefix gave:

| Listing | Pages (= Class A ops) | Objects returned | Wall time |
|---|---|---|---|
| Unscoped walk (the code's behaviour) | 174 | 173,126 | 43.6 s |
| `matchGlob` pushdown | 35 | 57 | 2.5 s |
| `matchGlob` + run-date lower bound | 1 | 30 | 0.2 s |

174 pages × ~57 workflow invocations/day ≈ 9,900 `ListObjects`/day, which
reconciles with the observed ~9,955.

The `-<workflow>.jsonl` suffix does real work and had to survive translation: it
is what stops `pv-prospect-extract` from also matching
`pv-prospect-extract-pv-sites-backfill`. The probe confirmed the distinction
holds server-side (57 vs 55 matches).

## 3. What the orphaned files were

Distribution of the 173,126 objects by run-date directory:

| Run date | Objects |
|---|---|
| 2026-05-17 | 57,567 |
| 2026-05-18 | 57,617 |
| 2026-05-19 | 57,657 |
| 2026-06-12 | 33 |
| every other date (52 of them) | 6–7 each |

The three May directories hold 57,564 files each under
`pv-prospect-transform-weather-grid-backfill/`, named `<task_hash>.jsonl` —
per-task scratch ledgers. Each contains one `completed` entry, e.g.

```json
{"recorded_at": "2026-05-17T15:08:54.696351+00:00", "run_date": "2026-05-17",
 "workflow": "pv-prospect-transform-weather-grid-backfill",
 "task_hash": "000017f3…", "descriptor": {"transform_step": "clean_weather",
 "start_date": "2025-08-08", "location": "52.1400,-1.9600",
 "end_date": "2025-08-22"}, "status": "completed"}
```

These are the fan-out that `LedgerCollector` was introduced to eliminate: one
file per *sub-item* rather than per Cloud Run task. `consolidate_ledger` never
merged them — its O(N) serial GCS round-trips do not fit the time budget at
57k files, which is precisely the failure its own docstring now warns about. No
consolidated `transform-weather-grid-backfill` ledger exists for those three
dates; the earliest is `2026-05-25-140228-…`.

Two facts made them safe to delete:

- **They were already invisible to resume.** `completed_task_hashes()` scans
  per-task files only under the *current* run date's prefix
  (`<run_date>/<workflow>/`). Files stamped `2026-05-17` have not been consulted
  by any workflow since 2026-05-17, and never will be again.
- **`gs://pv-prospect-staging` has a 7-day soft-delete policy**
  (`retentionDurationSeconds: 604800`), so the deletion was recoverable via
  `gcloud storage restore` for a week afterwards.

What was lost is the per-task audit trail for three days of
transform-weather-grid units. Their *outputs* are in the corpus; the nine
consolidated ledgers on those dates (daily extract and both extraction backfills)
were untouched.

## 4. The code change

All in `pv-prospect-etl` except the last, since the fix belongs to the shared
storage/orchestration layer both pipelines call.

1. **`storage/base.py`** — `FileSystem.list_files` gains
   `start_offset: str = ''`: only entries whose path sorts lexicographically at
   or after it are returned. On a date-partitioned tree this scans from a date
   rather than from the beginning.

2. **`storage/backends/gcs.py`** — a new `build_match_glob(full_prefix, pattern)`
   translates a basename pattern into `<full_prefix>**/<pattern>` and returns
   `None` for the match-everything pattern `*`. `list_files` passes it to
   `list_blobs(match_glob=…)` along with `start_offset`.

   `**/` matches any run of leading path segments *including none*, which was
   verified against the live bucket: globbing `<date>/**/*.jsonl` returns the
   six files sitting directly under `<date>/`, exactly as the old client-side
   basename `fnmatch` did. This equivalence is what makes the pushdown
   behaviour-preserving for every existing caller — notably
   `consolidate_ledger`, which lists at depth 0 under `<date>/<workflow>/`.
   (Brace alternation, the obvious alternative, is rejected by the API with a
   400.)

3. **`storage/backends/local.py`**, **`storage/logging_fs.py`** — implement and
   forward `start_offset`. `pathlib.Path.glob('**/<pattern>')` already has the
   same zero-segment semantics, so the two backends agree.

4. **`storage/ledger.py`** — `list_consolidated_ledgers` passes
   `*-<workflow>.jsonl` as the listing pattern rather than post-filtering, and
   takes `since: str = ''`, a consolidated-ledger *name* whose leading
   `YYYY-MM-DD` becomes the `start_offset`. `since` is a **bound, not a filter**:
   ledgers from earlier on the same day are still returned, so a caller wanting a
   strict `> since` still says so.

5. **`pv-prospect-data-transformation/.../transform_backfill.py`** —
   `_consume_extract_descriptors` passes `since=marker.consumed_through`. Lossless:
   the next line already discards every entry at or below the marker.

`completed_task_hashes()` itself is unchanged. It benefits from (2) and (4)
automatically, and its semantics — pool completed hashes from every consolidated
ledger ever written for this workflow — are untouched.

Putting the pushdown in the backend rather than at the ledger's call site means
every other pattern-filtered listing gets it too, without a per-caller change:
`consolidate_ledger` and `consolidate_logs` (`*.jsonl` / `*.txt` under a run-date
prefix), `readiness.verify_readiness` and `validation_window` (`*.csv` /
`pv_*.csv` under `data/prepared`), and `sample_file` (`sample_*.csv`). None of
them has a natural lower bound, so none takes `since` — but none of them pays for
non-matching objects any more either. The one caller with an unscoped `list_files('',
recursive=True)` and no pattern is the data-versioner, which genuinely wants every
object; it is unaffected.

## 5. What was deliberately not done

**A retention window on `completed_task_hashes()`.** The obvious next step is to
stop the scan reaching back to the project's beginning: bound it to, say, the
last 30 run dates. It was rejected on three grounds.

- It buys nothing in dollars. Post-cleanup the listing is one page with or
  without a window, and cost is pages.
- It buys little in reads. Daily-workflow consolidated ledgers are 5–11 KiB. The
  one large case — `extract-weather-grid-backfill` at ~3.9 MiB × 55 = ~210 MiB
  read per container invocation — belongs to a workflow that is currently paused.
- It would silently break a documented invariant.
  `pv-prospect-data-transformation/doc/runbooks/replay-window.md` is built on
  `filter_remaining_tasks` pooling completed hashes from *every* consolidated
  ledger ever written: that is exactly why replaying a window requires moving the
  offending ledger out to `tracking/superseded-ledgers/` rather than merely
  rewinding the cursor. A window would make old ledgers stop filtering, changing
  what a marker rewind does without anyone asking for it.

If the ~210 MiB read is ever worth bounding, it should be a deliberate change
that updates `replay-window.md` in the same commit.

**The 140 sibling scratch files** under
`2026-05-{18,19}/pv-prospect-transform-pv-sites-backfill/` (50 and 90) are the
same class of orphan but were outside the authorised deletion scope. They cost
nothing — they no longer push any listing past its first page — and are left in
place.

**Chasing empty HNS folders.** They are not objects and `list_blobs` does not
page over them, so they carry no operations cost.

## 6. Execution record

Deletion, 2026-07-09. The target list was derived by `grep` from a full recursive
listing and checked before use: all 172,692 paths matched
`…/2026-05-1[789]/pv-prospect-transform-weather-grid-backfill/<64-hex>.jsonl`,
and none matched the consolidated-ledger name shape. One object was removed first
to confirm mechanics, then the remainder via `gcloud storage rm -I` reading the
verified path list from stdin. DELETE operations are not billed.

Objects under `tracking/ledger/` before: 173,126. After: 434.

### Deployment status

The cost reduction **does not wait on a deploy**. With 434 objects left under
`tracking/ledger/`, even the old unscoped walk fits in a single 1,000-object
page, so the running Cloud Run Job images already pay one Class A op per scan
instead of 174.

The code change is inert until the `data-extraction` and `data-transformation`
images are rebuilt and deployed. It changes no behaviour when they are — only
what the listing costs, and what it will keep costing if scratch files ever
accumulate again. There is no ordering constraint between the deploy and the
cleanup, and no need to quiesce anything for either.

## 7. Verification

Unit tests: `pv-prospect-etl` 264 passed, `pv-prospect-data-transformation` 154
passed, `pv-prospect-data-extraction` 57 passed, `pv-prospect-data-versioner` 18
passed. `ruff check`, `ruff format --check` and `mypy` clean on all changed
production files.

New coverage: `build_match_glob`'s translations; the local backend's
`start_offset` and its zero-segment recursive-pattern parity with GCS;
`list_consolidated_ledgers`' `since` as a bound rather than a filter; and, at the
`plan_slices` call site, that a *later* extraction ledger on the marker's own day
is still consumed while earlier run dates are not — the regression a naive
lower bound would introduce.

Against the live bucket, driving the real `GcsFileSystem`:

| Call | Before | After |
|---|---|---|
| `list_consolidated_ledgers(fs, 'pv-prospect-extract')` | 174 pages, 43.6 s | 1 page, 0.06 s |
| …with `since=<marker>` | n/a | 1 page, 0.06 s, 2 entries |
| `consolidate_ledger`'s depth-0 listing | 57,564 files | 57,564 files (unchanged) |
| `list_files(prefix, '*')` | 6 | 6 (no glob applied) |

The `since=<marker>` call correctly returns the marker's own ledger (the caller's
strict `>` then drops it) plus everything after — confirming the bound does not
over-trim.

## 8. Conclusions and follow-ups

The measured `ListObjects` volume was set by an accumulation of un-consolidated
scratch files, not by the ledger history's length. The lesson generalises: any
listing whose page count is driven by objects it does not want is one failed
consolidation away from an open-ended operations bill. Pushing the filter
server-side removes the exposure; the `since` bound removes it a second time for
callers that hold a high-water mark.

Expected effect on billing: `ListObjects` on `pv-prospect-staging` should fall
from ~9,955/day to roughly 60–120/day (one or two pages per invocation).
**This should be confirmed in Cloud Monitoring once a full day has elapsed** —
note the four historical backfill schedulers are currently paused
(`backfills-paused`), so the invocation count itself is below its normal level
and the comparison is not like-for-like until they resume.

Follow-ups left open:

- The orphaning is not fully prevented, only made harmless: nothing yet detects a
  `consolidate_ledger` step that fails to finish and leaves scratch files behind.
  `tracking-restructure.md` is the natural home for an archival/pruning policy.
- `data-derived-transform-planning.md` removes the transform backfill's
  `completed_task_hashes()` resume filter altogether and demotes the transform
  ledger to audit. It should leave `_consume_extract_descriptors`' `since` bound
  intact, since the extract-ledger scan survives that change.

## References

- `.claude/work/briefs/ledger-scan-cost.md` (this task's brief; deleted on
  completion)
- `doc/orchestration.md` §"Cost of the Ledger Scan"
- `pv-prospect-data-transformation/doc/runbooks/replay-window.md` — the
  full-history resume invariant
- `.claude/work/reports/data-pipeline-retrospective.md` — the fan-out elimination
  (`LedgerCollector`) whose residue this task cleaned up