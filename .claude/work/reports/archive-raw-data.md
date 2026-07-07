# Report: Archive Raw Extracted Data

> Written 2026-07-07. Records the migration of raw extracted CSV data out of the
> `staging` bucket into a dedicated, cost-tiered archive bucket, and the
> decommissioning of the DVC-versioning scaffolding it replaces.

## Summary

Raw extracted data (PVOutput power readings, Open-Meteo weather CSVs) previously
lived under `gs://pv-prospect-staging/data/raw/`, growing without bound
(weather-grid densification, the `openmeteo-2016-floor` backfill) inside the same
bucket as the churny `cleaned/`/`prepared/` working data. The originating brief
called for extending DVC versioning to raw, following the pattern used for the
prepared feature corpus. Investigation (`plans/archive-raw-data.md`) showed this
was the wrong tool: nothing consumes versioned raw, raw is append-only and
non-refetchable (so historical reconstruction is near-valueless — today's corpus
is a superset of every past state), and DVC re-hashing 700k+ objects on every
weekly versioner run is a heavy misfit. What raw needed was **durability**, not
git-tag reconstructability.

The design that shipped: a dedicated `pv-prospect-raw` bucket, Standard storage
class by default, with a GCS lifecycle rule that tiers each object to Coldline
**in place** at age 8 days — no cross-bucket move machinery, no retrieval fee on
the normal (≤8-day) transform read path. Both extraction (write) and transform
(read) reach it through the single shared `staged_raw_data_storage` config key,
so the redirect required no application code change.

Execution ran in two phases. **Phase A** (2026-07-06, submodule commits `5a2524e`
+ `a45ea2c`, instance-repo commit `5263d45`) landed the Terraform, the config
redirect, and the DVC-scaffolding removal — all inert until deployed. **Phase B**
(2026-07-07) was the live migration: quiesce the six extraction/transform Cloud
Scheduler jobs, build and deploy both Cloud Run Job images carrying the redirected
config, bulk-move the existing raw corpus into the new bucket (straight to
Coldline, since it was all already old and already-transformed), verify an actual
extraction+transform round-trip against the new bucket, resume the schedulers,
then empty and destroy the two dead buckets (`versioned-raw`, `pv-prospect-data`).
All steps completed without incident; production was quiesced for
**08:04–08:32 UTC** on 2026-07-07 (~28 minutes, extraction/transform paths only —
the public app and model-training path were unaffected throughout).

## Contents

1\. Motivation and design (recap)\
2\. Migration record\
3\. Verification\
4\. Decommissioning\
5\. Final state\
6\. Follow-ups

## 1. Motivation and design (recap)

Full rationale and cost analysis is in `plans/archive-raw-data.md` (retained
alongside this report is optional per the task lifecycle, but that plan will be
deleted at the end of this task since its content is now superseded by this
record and by the permanent docs). In brief:

- Raw is append-only and non-refetchable (PVOutput history has quota limits;
  Open-Meteo historical fetches are not free). It needs to *survive*, not be
  *versioned* — GCS lifecycle tiering gives durability at a fraction of the
  operational cost of DVC-over-700k-objects.
- A **single bucket with in-place lifecycle tiering** beats a "move consumed raw
  to a cold bucket" design: GCS lifecycle cannot move objects between buckets, so
  that alternative would need Storage Transfer Service or a custom job to reach
  the *same billing outcome* the lifecycle rule gives for free, while also
  splitting raw across two buckets (complicating any future data-derived
  planner's `raw-present` listing).
- Age-8-days Standard→Coldline: transform consumes raw within ~1–2 days; 8 days
  gives a week of margin for pipeline catch-up/short outages before the object
  goes cold. Coldline (not Nearline/Archive) was chosen for its retrieval-fee/
  minimum-duration profile against the "small, rare re-read" access pattern
  (re-transforms, recovery).
- `versioned-feature`/`versioned-model` buckets were assessed and **excluded**
  from Coldline tiering: both are small, stable, and read frequently (weekly
  full-corpus trainer pulls; per-inference model reads), making Coldline a net
  cost *increase* there.

## 2. Migration record

Run 2026-07-07, project `semiotic-effort-474309-t7`, region `europe-west2`.

1. **Bucket provisioned** (targeted `terraform apply`): `pv-prospect-raw`,
   Standard default class, lifecycle rule `SetStorageClass → COLDLINE` at
   `age = 8`, confirmed live via `gcloud storage buckets describe`. Pipeline SA
   granted `objectAdmin`.
2. **Images rebuilt and deployed.** `docker build` (target `entrypoint`) for both
   `data-extraction` and `data-transformation` from the Phase A working tree
   (which carries the `staged_raw_data_storage` → `pv-prospect-raw` redirect in
   `pv-prospect-etl`'s `config-default.yaml`), pushed as `:latest`, then
   `gcloud run jobs update --image …:latest` on both Cloud Run Jobs. (Note: the
   `cloud_run_job` Terraform module has no digest-pinning/force-update
   mechanism, so a re-pushed `:latest` tag would **not** trigger a redeploy via a
   no-diff `terraform apply` — the explicit `gcloud run jobs update` was
   necessary and is the reproducible step for any future redeploy-without-infra-
   change.)
3. **Pipeline quiesced.** All six Cloud Scheduler jobs (`pv-prospect-daily-extract`,
   `…-extract-pv-sites-backfill`, `…-extract-weather-grid-backfill`,
   `…-daily-transform`, `…-daily-transform-pv-sites-backfill`,
   `…-daily-transform-weather-grid-backfill`) paused; confirmed no executions had
   a non-zero `runningCount` before proceeding.
4. **Bulk move.** `gcloud storage mv gs://pv-prospect-staging/data/raw/timeseries
   gs://pv-prospect-raw/ -s COLDLINE` — moved directly to Coldline since the
   entire existing corpus was already old and already consumed by transform, so
   there was no reason to pay for a Standard→Coldline tier-down later. Took
   ~50 minutes for the full corpus. Before running it at scale, the exact
   `mv`-path-mapping was proven with a throwaway canary object — `gcloud storage
   mv` turned out to have **no `--recursive`/`-r` flag** (it recurses by default
   for a prefix source, but the source/destination trailing-slash combination
   matters: `mv SRC/timeseries DST/` gives clean per-object mapping, whereas
   `mv SRC/timeseries DST/timeseries` doubles the trailing path segment). This is
   worth recording since it is easy to get silently wrong at small scale and
   expensive to discover at 700k objects.
5. Post-move: **0** data files (`.csv`/`.json`) remained under
   `gs://pv-prospect-staging/data/raw/`; the new bucket showed the expected
   `timeseries/{openmeteo,pvoutput}/` top-level structure.

## 3. Verification

Rather than trust the deploy blind, a real extraction+transform round-trip was
run against the new bucket before resuming the schedulers:

- Triggered `pv-prospect-extract` for PV system `89665`, date `2026-07-06` — a
  date that did **not** already exist in the corpus (chosen deliberately, since
  the extractor may skip re-fetching an already-present day, which would have
  given a false-positive "success" without proving a fresh write). Confirmed a
  new object `pvoutput_89665_20260706.csv` appeared in
  `gs://pv-prospect-raw/timeseries/pvoutput/89665/` with an `update_time`
  matching the run, and confirmed nothing was written back to the old
  `gs://pv-prospect-staging/data/raw/` path.
- Triggered `pv-prospect-transform` for the same system/date. Confirmed
  `pvoutput_89665_20260706.csv` appeared in
  `gs://pv-prospect-staging/data/cleaned/timeseries/pvoutput/89665/` with an
  `update_time` shortly after the extraction — since that raw file only ever
  existed in the new bucket, this proves the transform's read path resolved
  correctly.

Both workflow executions reported `state: SUCCEEDED` with no errors.

## 4. Decommissioning

- `gs://pv-prospect-versioned-raw` — was empty (0 bytes); destroyed via the
  targeted Terraform apply alongside its DVC-SA IAM binding. No data loss.
- `gs://pv-prospect-data` — legacy bucket (656 MB: an old DVC cache
  `files/md5/…` plus stale `staging/…` raw, last written 2026-03-06, already
  orphaned from Terraform state and unreferenced by the current `.dvc/config`
  before this task). Emptied (`gcloud storage rm --recursive`) then destroyed.
  **Accepted caveat:** any `data-v` git tag old enough to predate the bucket
  split may no longer `dvc pull` successfully, since its `.dvc` files could
  reference blobs that lived in this now-deleted legacy cache. This was judged
  acceptable — archival reproducibility of *raw* was explicitly deprioritised
  for this task, and the *prepared feature corpus* (what those old tags actually
  pin) remains fully retrievable via the still-live `versioned-feature` bucket.
- Unused DVC-raw scaffolding removed in Phase A: the `['remote "raw"']` block in
  the instance repo's `.dvc/config`, and the `versioned_raw_data_storage` entry
  in the ETL config (no code ever consumed it).

A final `terraform plan` after the destroys showed no unexpected remaining diff
against live state — the only outstanding drift is pre-existing and unrelated
(the `pv-prospect-app` Cloud Run Service's `:latest`-vs-pinned-tag image
annotation, and gcloud-vs-Terraform client-metadata annotations on the two jobs
updated directly in step 2 above — neither touches raw or this task's buckets).

## 5. Final state

- `gs://pv-prospect-raw` — 5,956,830,717 bytes (~5.96 GB), matching the
  pre-migration corpus size (5,956,824,437 bytes) plus the one verification test
  object. Standard default class; lifecycle confirmed tiering to Coldline at
  age 8 days. Both extraction (write) and transform (read) confirmed working
  against it in production.
- Total pipeline quiesce window: 2026-07-07 08:04–08:32 UTC (schedulers paused →
  resumed). No extraction/transform runs were scheduled to fire in that window
  beyond what was manually triggered for verification; nothing was lost.
- `gs://pv-prospect-versioned-raw` and `gs://pv-prospect-data` no longer exist.

## 6. Follow-ups

- This was the safety precondition for **`data-derived-transform-planning`**
  (raw is now durable independently of the churny staging bucket, so "delete
  prepared and re-run" is a safe recovery move for that task's planner). That
  task can now proceed.
- The one-time move happened to also serve as an incidental, low-volume
  real-world exercise of `gcloud storage mv`'s path-semantics footgun (§2.4) —
  worth keeping in mind for any future bulk GCS reorganisation in this project.