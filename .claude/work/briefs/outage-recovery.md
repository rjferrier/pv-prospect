# Generalise Open-Meteo outage recovery into a reusable runbook/script

> **Re-scope pending (2026-07-06).** The transform half of this pattern (steps 2's
> transform counterparts and step 4's superseded-ledgers move) is folded into
> `data-derived-transform-planning` — once that lands, transform recovery is
> "delete the affected prepared partitions" and this brief reduces to an
> extract-side cursor-rewind runbook. See
> `reports/data-pipeline-retrospective.md` §5.

The 2026-05-23 Open-Meteo outage was recovered with a one-off interactive
script (`.tmp/recovery-20260523/recover.py`). The pattern is the same every
time an API outage causes a span of failed extraction tasks whose windows the
scheduler will not revisit.

## Recovery pattern

1. Confirm the API is back.
2. For each affected scope (pv-sites extract, weather-grid extract, and their
   transform counterparts): identify which consolidated ledgers recorded the
   outage's failed tasks.
3. Rewind extract cursors to before the affected window.
4. Move affected consolidated *transform* ledgers to `superseded-ledgers/` so
   their `completed` task hashes don't mask the re-run.
5. Re-trigger each backfill via Cloud Scheduler.

## Goal

The 2026-05-23 script was hardcoded to that day's specific ledger filenames
and cursor values. A general version would accept a date range, enumerate
affected ledgers automatically, and present a dry-run diff before making any
GCS mutations.
