# Re-trigger Weather-Grid Extraction Backfill

See [`doc/backfill-operations.md`](../../../doc/backfill-operations.md) for the
overview of the extract → transform backfill chain.

The weather-grid backfill has no per-batch checkpoint — it runs as a single linear
pass that paces 9 batches across ~3 h 24 min and commits the cursor only once every
batch has been attempted. A workflow-level failure therefore leaves the cursor at
yesterday's position; the next run re-plans the same window from scratch. The cost
of a re-trigger is roughly 3 hours of OpenMeteo budget, but in exchange there is no
checkpoint-shaped coordination surface for concurrent same-day executions to race on.

```bash
gcloud workflows run pv-prospect-extract-weather-grid-backfill \
  --location=europe-west2
```
