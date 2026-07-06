# Archive Raw Extracted Data

See the plan (`plans/archive-raw-data.md`) for the full design, cost analysis, and
resolved decisions.

## What

Give raw extracted CSVs a **durable, cost-tiered archive** instead of leaving them
in the working staging bucket. Raw moves to a dedicated `pv-prospect-raw` bucket
that auto-tiers Standard → Coldline via a GCS lifecycle rule (age 8 days). This is
Terraform + one config value + a one-time data move — **no application code, no
DVC, no versioner change**.

This supersedes the original "version raw data" framing (DVC-snapshot raw alongside
the prepared corpus, the diagram's V1/V2 interactions). That approach was dropped:
nothing consumes versioned raw, raw is append-only + non-refetchable, and DVC over
701k objects / ~6 GB is a heavy misfit. What raw actually needs is durability, not
git-tag reconstructability — and GCS object versioning + lifecycle tiering give that
natively.

## Why

- **Safety precondition for `data-derived-transform-planning`.** That task makes
  "delete prepared and re-run" the routine recovery move; it is only safe if raw —
  non-refetchable (PVOutput history limits) — survives independently of the churny
  staging bucket. Because raw re-tiers *in place* (never leaves the bucket), it
  stays one listable corpus, so the planner and its recovery stay correct.
- **Cost.** Raw is the one corpus that grows without bound (weather-grid
  densification, `openmeteo-2016-floor` backfill). Coldlining its cold tail scales
  the saving with the corpus, while transform still reads the hot (≤8-day) copy from
  Standard — no retrieval fees on the normal path.

## What is needed (summary — see plan for detail)

1. **New bucket** `pv-prospect-raw` (Standard default; lifecycle SetStorageClass →
   Coldline at age 8), Terraform.
2. **Redirect raw I/O** by pointing `staged_raw_data_storage` (ETL config) at the
   new bucket — extraction (write) and transform (read) both follow; no code change.
3. **One-time move** of the existing ~6 GB / 701k objects out of
   `staging/data/raw/`, gated around a quiesced window + config redeploy.
4. **Pipeline SA** objectAdmin on the new bucket (Terraform IAM).
5. **Decommission** the empty `versioned-raw` bucket, the moribund `pv-prospect-data`
   bucket (656 MB — empty then destroy), and the unused DVC-raw scaffolding (`raw`
   remote in `.dvc/config`, `versioned_raw_data_storage` in ETL config, the bucket's
   DVC IAM).
6. **Docs:** rewrite the `architecture.puml` V1/V2 interactions, update the top-level
   README (staging no longer holds raw; new raw archive + Coldline tiering), and
   write a short `reports/` entry (external-storage/corpus change).

Feature/model buckets stay Standard — Coldline is a net loss there (they are small,
stable, and read-frequently). See plan.

## Blockers

None. Buckets are provisionable via Terraform; the redirect is config-only.

## Sequencing

Before `data-derived-transform-planning` (the safety precondition). Coordinate the
one-time move / R1a with `openmeteo-2016-floor` (both touch
`data/raw/timeseries/pvoutput/**`).