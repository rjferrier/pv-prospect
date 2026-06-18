# Backfill Operations

The backfill pipeline runs in two tiers: **extraction backfills** harvest historical
data into GCS, and **transformation backfills** process it into prepared features.
The two tiers are independent — each advances at its own pace via cursors and
consumed-through markers — but transformation is gated on the extraction ledger:
it only transforms windows whose extraction has a committed `completed` record.

For the underlying orchestration machinery (plan-commit pattern, manifests, ledger)
see [`doc/orchestration.md`](orchestration.md). For the full workflow schedule see
[`doc/workflows.md`](workflows.md).

## Triggering Workflows Manually

To run an ad-hoc extraction for a specific system and date:

```bash
gcloud workflows run pv-prospect-extract \
  --location=europe-west2 \
  --data='{"pv_system_ids": [12345], "date": "2025-06-24"}'
```

To trigger a transformation for a specific system and date:

```bash
gcloud workflows run pv-prospect-transform \
  --location=europe-west2 \
  --data='{"pv_system_ids": [12345], "date": "2025-06-24"}'
```

## Runbooks

| Situation | Runbook |
|---|---|
| `pv-prospect-extract-pv-sites-backfill` timed out or was cancelled | [Resume PV-sites extraction backfill](../pv-prospect-data-extraction/doc/runbooks/resume-pv-sites-backfill.md) |
| `pv-prospect-extract-weather-grid-backfill` failed | [Re-trigger weather-grid extraction backfill](../pv-prospect-data-extraction/doc/runbooks/resume-weather-grid-backfill.md) |
| Transformation backfill timed out or crashed | [Resume transformation backfill](../pv-prospect-data-transformation/doc/runbooks/resume-transform-backfill.md) |
| Need to replay specific transformation windows | [Replay a transformation window](../pv-prospect-data-transformation/doc/runbooks/replay-window.md) |
| Need to re-base the corpus onto a new feature convention | [Re-base the corpus](../pv-prospect-data-transformation/doc/runbooks/re-base-corpus.md) |
| Seeding the validation window for the first time | [Seed the validation window](../pv-prospect-data-transformation/doc/runbooks/seed-validation-window.md) |
