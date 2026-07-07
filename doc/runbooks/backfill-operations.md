# Backfill Operations

The backfill pipeline runs in two tiers: **extraction backfills** harvest historical
data into GCS, and **transformation backfills** process it into prepared features.
The two tiers are independent — each advances at its own pace via cursors and
consumed-through markers — but transformation is gated on the extraction ledger:
it only transforms windows whose extraction has a committed `completed` record.

For the underlying orchestration machinery (plan-commit pattern, manifests, ledger)
see [`doc/orchestration.md`](../orchestration.md). For the full workflow schedule see
[`doc/infrastructure.md`](../infrastructure.md).

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

## Pausing and Resuming the Backfills

To stop the historical backfills indefinitely (e.g. to save cost once the corpus
is deep enough), pause their four Cloud Scheduler jobs:

| Scope | Scheduler job |
|---|---|
| PV-sites extraction backfill | `pv-prospect-daily-extract-pv-sites-backfill` |
| Weather-grid extraction backfill | `pv-prospect-daily-extract-weather-grid-backfill` |
| PV-sites transformation backfill | `pv-prospect-daily-transform-pv-sites-backfill` |
| Weather-grid transformation backfill | `pv-prospect-daily-transform-weather-grid-backfill` |

A paused scheduler simply stops firing: no workflow runs, no Cloud Run Job
executes, so there is no compute or API cost (Cloud Scheduler itself is free up
to three jobs and fractions of a cent beyond). Each backfill's cursor freezes
where it is; the plan-commit pattern means resuming just re-plans from the frozen
cursor, so an indefinite pause loses no state. An execution already in flight when
you pause finishes normally — pause only blocks *future* triggers.

This does **not** touch the daily extraction/transformation or the weekly
versioning schedulers — those keep the ongoing corpus and the trained model fresh.
Pause those separately only if you mean to freeze the whole project.

### Preferred: the `backfills_paused` Terraform switch

Set the flag in `terraform.tfvars` and apply — this pauses (or resumes) all four
atomically and records the intent in version control:

```hcl
backfills_paused = true   # false to resume
```

```bash
cd terraform && terraform apply   # via deploy.sh in the normal workflow
```

### Immediate/ad-hoc: `gcloud`

For an instant stop without an apply cycle:

```bash
for j in \
  pv-prospect-daily-extract-pv-sites-backfill \
  pv-prospect-daily-extract-weather-grid-backfill \
  pv-prospect-daily-transform-pv-sites-backfill \
  pv-prospect-daily-transform-weather-grid-backfill ; do
  gcloud scheduler jobs pause "$j" --location=europe-west2   # `resume` to undo
done
```

Because these four jobs' paused state is now Terraform-managed (via
`backfills_paused`), the **next `terraform apply` reconciles them back** to
whatever the flag says. Use `gcloud` for a quick stop; set the flag for anything
indefinite so a routine apply doesn't silently un-pause them.

## Runbooks

| Situation | Runbook |
|---|---|
| `pv-prospect-extract-pv-sites-backfill` timed out or was cancelled | [Resume PV-sites extraction backfill](../../pv-prospect-data-extraction/doc/runbooks/resume-pv-sites-backfill.md) |
| `pv-prospect-extract-weather-grid-backfill` failed | [Re-trigger weather-grid extraction backfill](../../pv-prospect-data-extraction/doc/runbooks/resume-weather-grid-backfill.md) |
| Transformation backfill timed out or crashed | [Resume transformation backfill](../../pv-prospect-data-transformation/doc/runbooks/resume-transform-backfill.md) |
| Need to replay specific transformation windows | [Replay a transformation window](../../pv-prospect-data-transformation/doc/runbooks/replay-window.md) |
| Need to re-base the corpus onto a new feature convention | [Re-base the corpus](../../pv-prospect-data-transformation/doc/runbooks/re-base-corpus.md) |
| Seeding the validation window for the first time | [Seed the validation window](../../pv-prospect-data-transformation/doc/runbooks/seed-validation-window.md) |
