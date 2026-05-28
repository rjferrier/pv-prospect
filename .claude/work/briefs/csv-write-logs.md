# Decommission hand-rolled CSV write logs under `tracking/`

The `tracking/` prefix contains hand-rolled CSV files that record which data
files have been written to staging. These predate the JSONL task-outcome
ledger system introduced with `WorkflowOrchestrator`. The ledger already
records task outcomes with full provenance (task hash, env vars, status, run
date), so the write-log CSVs carry redundant information and require
maintenance with no payoff.

## Sequence

1. Identify all code paths that write these CSVs (grep for `tracking/` writes
   producing `.csv`).
2. Check whether anything reads/queries them (dashboards, scripts, other
   pipeline code).
3. Remove the write calls; remove any dependent read/check calls.
4. Delete the existing files from `gs://pv-prospect-staging/tracking/`.
