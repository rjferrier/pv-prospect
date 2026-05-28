# Extraction failure carry-over registry

Comparable to a dead-letter queue.

When an extraction task fails (network error, rate limit, etc.), the
task-outcome ledger records it as `failed`. The scheduler does not revisit
those windows on subsequent runs; a cursor rewind is needed to replay them,
which replays successful tasks too. There is no lightweight way to say
"re-attempt only the failed tasks from run X."

## Proposed approach

The design agreed in May 2026 was to surface this as a query on the ledger
itself — failed rows are the carry-over registry — but no tooling exists to
enumerate them. A small CLI / helper that lists `status=failed` rows across
consolidated ledgers for a given workflow and date range would make gap audits
and targeted replays much cheaper. Could also serve as the input to the
outage recovery script.
