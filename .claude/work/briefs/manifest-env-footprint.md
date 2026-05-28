# Reduce per-task env footprint in phased manifests

The weather-grid transform backfill manifest hit Cloud Workflows' 2 MiB
per-step HTTP-response limit (2026-05-16). The primary fix — splitting the
manifest into a phases index plus per-phase files — is in place and gives ~3×
headroom at current backlog size.

## Secondary lever

A secondary lever, not yet applied, is eliminating redundancy within each
phase file. Inspecting a weather-grid clean/prepare phase: 8 env keys per
task, 6 of which are constant across every row in the phase (`TRANSFORM_STEP`,
`WORKFLOW_NAME`, `RUN_DATE`, `START_DATE`, `END_DATE`, `DATE`). Only
`LOCATION` and `TASK_HASH` vary. Hoisting the constants into a per-phase
header and storing only the varying fields per row would cut per-task payload
from ~1 000 bytes to under 100 — roughly a 10× reduction, independent of
encoding format. This buys significant headroom as the backfill catches up and
phase sizes grow.
