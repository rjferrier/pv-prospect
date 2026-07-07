# Data Transformation Runbooks

Operational procedures for the cleaning and preparation pipeline, indexing
[`doc/runbooks/`](runbooks/). For the system-wide runbook overview see the
top-level [runbook index](../../doc/runbooks.md).

| Runbook | Use when |
|---|---|
| [Resume Transformation Backfill](runbooks/resume-transform-backfill.md) | A transformation backfill Cloud Run Job timed out or crashed. |
| [Replay a Transform-Backfill Window](runbooks/replay-window.md) | You need to re-process one or more specific transform-backfill windows. |
| [Re-base the Corpus](runbooks/re-base-corpus.md) | A feature-spec change touches every partition and the whole corpus must be rebuilt onto the new convention. |
| [Seed the Validation Window](runbooks/seed-validation-window.md) | Seeding the rolling validation-window artifact for the first time before the `maintain_validation_window` step can run. |
