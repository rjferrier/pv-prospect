# Resume PV-Sites Extraction Backfill

See [`doc/runbooks/backfill-operations.md`](../../../doc/runbooks/backfill-operations.md) for the
overview of the extract → transform backfill chain.

If `pv-prospect-extract-pv-sites-backfill` is interrupted (e.g. the Cloud Workflows
execution times out or is cancelled), simply re-trigger it:

```bash
gcloud workflows run pv-prospect-extract-pv-sites-backfill \
  --location=europe-west2
```

The workflow will read the position checkpoint at
`gs://<staging-bucket>/tracking/checkpoints/pv_sites_backfill.json` and resume
dispatching from `next_pv_task_index`, logging `"Resuming from checkpoint --
starting at PV task N"`. No duplicate extraction occurs. The checkpoint is
deleted automatically once the full run completes, so the next scheduled run
starts from scratch.

If a run is interrupted *before any task completes* (e.g. the `plan` step fails),
no checkpoint exists and a re-trigger simply starts from the beginning.
