# Resume Transformation Backfill

The transform backfills run as Cloud Run Job executions (not workflows), so
re-trigger them directly:

```bash
gcloud run jobs execute data-transformation \
  --region=europe-west2 \
  --update-env-vars=JOB_TYPE=run_transform_backfill,BACKFILL_SCOPE=pv_sites
```

Use `BACKFILL_SCOPE=weather_grid` for the weather-grid backfill.

The consumed-through marker is only advanced at the end of a successful run, so a
run that crashed before commit re-derives the same plan from the extraction ledger
on re-trigger. If the prior attempt reached its end-of-run ledger flush, the
orchestrator's `filter_remaining_tasks` reads that consolidated ledger and the
re-run skips the finished units; an attempt that crashed before the flush re-runs
every unit — the clean/prepare/assemble steps overwrite idempotently, so that is
safe, just repeated work.

To deliberately replay one or more windows (e.g. to plug a hole left by a buggy
run, or to re-transform after a feature-spec change), follow
[`replay-window.md`](replay-window.md): marker rewind alone is filtered out by
the cross-run resume scan, so the offending consolidated ledger has to leave the
scan root as well.
