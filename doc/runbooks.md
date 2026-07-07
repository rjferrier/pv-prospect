# Runbooks

**System** runbooks — operational procedures that span more than one package —
live in the top-level [`doc/runbooks/`](runbooks/) and are indexed below.

**Component** runbooks specific to a single package live in that package's own
`doc/runbooks/`, indexed from the package's README (and its own `doc/runbooks.md`).
The [Full Documentation](../README.md#full-documentation) map in the top-level
README lists the package landing pages.

For the infrastructure these procedures operate on, see
[`infrastructure.md`](infrastructure.md); for the orchestration machinery, see
[`orchestration.md`](orchestration.md).

## System Runbooks

| Runbook | Use when |
|---|---|
| [Backfill Operations](runbooks/backfill-operations.md) | Triggering workflows manually, pausing/resuming the backfills, or recovering a failed backfill across the extraction and transformation tiers — the operational hub for the backfill chain. |
