# Scope the ledger-consolidation listing to stop an unbounded GCS operations cost

## Context

Billing investigation (2026-07-06) found the daily Cloud Storage cost is dominated
by **operations**, not by data at rest. Measured `total_bytes` across the
project's europe-west2 buckets is ~9.9 GiB — at the "Standard Storage London"
SKU price ($0.023/GiB-month), that's only ~$0.008/day. But Cloud Monitoring's
`storage.googleapis.com/api/request_count` on `pv-prospect-staging` shows
`ListObjects` (a Class A op) running at a nearly-flat **~9,955/day**, with a
slow day-over-day upward drift (9952 → 9959 over 2026-06-27 → 07-05). At the
"Regional HNS Standard Class A/B Operations" SKU prices ($0.0000065/op Class A,
$0.0000005/op Class B — these buckets have `hierarchical_namespace` enabled),
operations account for roughly **8-12x** the at-rest storage cost.

The source: `WorkflowOrchestrator.completed_task_hashes()`
(`pv-prospect-etl/pv_prospect/etl/orchestration.py:198-237`), called by
`filter_remaining_tasks()` on **every** workflow invocation (daily pipelines and
backfills alike), calls `list_consolidated_ledgers()`
(`pv-prospect-etl/pv_prospect/etl/storage/ledger.py:236-257`), which does:

```python
ledger_fs.list_files('', '*.jsonl', recursive=True)
```

— an **unscoped recursive walk of the entire `tracking/ledger/` prefix** (every
run date, every workflow, since the project began), filtered down to the one
workflow of interest only *after* every object is fetched. A 60-second
time-boxed listing of that prefix alone streamed 173,607+ objects before being
cut off — the real count is higher, and grows daily.

This explains the flat-but-slowly-rising `ListObjects` volume: a handful of
scheduled workflow invocations per day, each paying for a full-tree walk whose
page count scales with the *cumulative* ledger history, not with that day's
work.

## Why raw-data deletion doesn't touch this

`tracking/ledger/` is a separate GCS prefix from `data/raw`
(`pv-prospect-etl/.../resources/config-default.yaml`). Deleting the raw CSV
corpus — considered as a cost-saving move in the billing investigation that
prompted this brief — would not reduce this cost at all. Pausing backfills
would only partially help, since the **daily** extraction/transformation
pipelines call the same code path.

## What is needed

Stop `list_consolidated_ledgers` from re-scanning the full ledger history on
every call. Options, not mutually exclusive:

1. **Scope the listing.** Ledger filenames are `<run_date>/...`, so list by a
   bounded date-range prefix (e.g. only run dates plausibly still relevant for
   cross-day resume) instead of `''` (the whole tree).
2. **Prune/consolidate old ledger files.** Once a consolidated ledger's
   completed tasks are no longer needed for resume (retention policy TBD),
   move or delete it out of the scan root — the existing `superseded-ledgers/`
   mechanics (see `tracking-restructure.md`) already do this for manually
   rewound runs; the same idea could apply on a schedule.
3. Check whether `tracking-restructure.md` / `data-derived-transform-planning.md`
   (which already touch the ledger's recovery duties) should absorb this,
   since they're already changing how resume state is scanned.

## Scope

`pv-prospect-etl` (`orchestration.py`, `storage/ledger.py`) is shared code; a
fix here benefits both extraction and transformation workflows — per
`system-design.md`'s consistency rule, don't patch one workflow's call site
only.