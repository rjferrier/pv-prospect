# Validation window producer — transformer-maintained recent-days corpus

First task of the Validation API roadmap (`doc/architecture.puml`). Produces the
durable serving artifact that [validation-inmemory-store.md](validation-inmemory-store.md)
loads, [validation-api.md](validation-api.md) serves, and
[validation-serving-docs.md](validation-serving-docs.md) wires up.

## Why

The Validation API shows, per known PV site, how the deployed PV model tracks
*real* recent output. It needs the last 90 days of each site's prepared rows — the
PV model's inputs (`temperature`, `plane_of_array_irradiance`) plus the real
`power` — held somewhere durable the app can load at startup.

It **cannot** read `staging/prepared` directly: the data-versioner's
`_clean_staging` wipes `cleaned/` + `prepared/` on every weekly run, so that prefix
only ever holds the current week's increment. The recent window must therefore live
in a separate, durable serving artifact the weekly clean never touches.

This is a *derived, regenerable serving cache* of a fixed-size recent window — not a
system of record (that's the DVC feature store) and not a query store (the app loads
it wholesale into memory, the `InMemoryData` node). So it's a single structured
object at a GCS served prefix — settled: not a database, not a pile of loose CSVs.

## What

1. **Daily maintenance (transformer).** Add a daily-transform end-of-run step
   (alongside `run_consolidate_logs`) that:
   - reads the current window artifact from the served prefix,
   - merges in the prepared PV rows currently in `staging/prepared/pv/*` (reuse
     `merge_prepared_frames` on key `['system_id', 'time']`; dedup makes re-runs and
     missed days idempotent — staging holds up to a week, so a skipped run recovers),
   - trims to the last 90 days (cutoff = `max(time)` − 90 days, data-relative so a
     temporarily-offline site is not dropped),
   - rewrites the artifact + a `manifest.json` (`updated_at`, window bounds, per-site
     row counts).
   The pipeline SA already has `objectAdmin` on the staging bucket, so the prefix
   `gs://<prefix>-staging/data/served/validation-window/` needs **no new IAM**.

2. **One-time seed (bootstrap).** The transformer only sees the forward increment;
   the 90-day history lives in the DVC feature store, pulled to the local instance
   repo at launch. A one-off, **DVC-free** seed reads the last 90 days of prepared PV
   per site from the local `data/prepared/pv/*/pv_*.csv` partitions, builds the
   artifact in the producer's exact format, and uploads it once. Run manually at
   launch. DVC stays out of the seed entirely; an optional instance-repo wrapper can
   scope the `dvc pull` to just the recent PV partitions (see the plan). The daily step
   assumes the window exists and only merges forward.

## Artifact contract (consumed by the in-memory store)

One object `data/served/validation-window/window.csv` (CSV for consistency with the
prepared corpus + zero new deps; Parquet a later optimisation):

| Column | Source |
|---|---|
| `system_id` | partition path (`prepared/pv/{system_id}/`) — added on assembly |
| `time` | prepared PV |
| `temperature` | prepared PV |
| `plane_of_array_irradiance` | prepared PV |
| `power` | prepared PV — the actual output to compare against |
| `power_max` | prepared PV — clip/censoring flag for the display |

Static per-site attributes (capacities, install age) are **not** duplicated here —
the app reads them from `pv_sites.csv` (already synced to `staging/resources/`).
`manifest.json` sits beside it for window bounds + a cheap freshness signal.

## Scope

`pv-prospect-data-transformation` (new daily end-of-run step; config
`validation_window_storage`, `validation_window_days=90`), a one-off DVC-free seed
reading the local pulled corpus. No new IAM for the producer (staging `objectAdmin`
already held); the app-SA read grant lives in
[validation-serving-docs.md](validation-serving-docs.md). Reuse
`merge_prepared_frames` and `pv-prospect-etl` storage.
