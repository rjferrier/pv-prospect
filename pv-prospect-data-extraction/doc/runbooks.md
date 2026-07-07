# Data Extraction Runbooks

Operational procedures for the extraction pipeline, indexing
[`doc/runbooks/`](runbooks/). For the system-wide backfill overview — which spans
both the extraction and transformation tiers — see the top-level
[runbook index](../../doc/runbooks.md).

| Runbook | Use when |
|---|---|
| [Resume PV-Sites Extraction Backfill](runbooks/resume-pv-sites-backfill.md) | `pv-prospect-extract-pv-sites-backfill` timed out or was cancelled — re-trigger and resume from the GCS checkpoint. |
| [Re-trigger Weather-Grid Extraction Backfill](runbooks/resume-weather-grid-backfill.md) | `pv-prospect-extract-weather-grid-backfill` failed — re-trigger the single-pass workflow. |
