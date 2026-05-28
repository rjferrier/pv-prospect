# Rename prepare→featurise / partition→feature throughout

The codebase has accumulated synonyms for the same concept: "prepared data",
"partition files", "features", "feature sets". Settle on **features** and
**featurise** consistently across the codebase.

## Context

The `TransformUnit` docstring was updated to articulate the M-data-sources →
N-feature-sets data-flow model (raw/cleaned is per source, features are per model,
M need not equal N). The *rename* itself remains parked — it touches too many
things to do alongside other work. The conceptual work (what the rename means) is
done; the mechanical work (applying the rename) is deferred.

## Scope of the rename

When picked up, the rename touches:

- **Path builders:** `weather_partition_path` / `pv_partition_path` →
  `weather_feature_partition_path` / `pv_feature_partition_path`
  (or `..._feature_path`).

- **Transform steps (wire values):** `run_prepare_weather`, `run_prepare_pv`,
  the `prepare_weather` / `prepare_pv` step names, and the `Transformation`
  enum strings → `featurise_weather` / `featurise_pv`. **Important:** enum
  strings are wire values appearing in `TRANSFORM_STEP` env vars, ledger
  descriptors, and manifests, so this is a coordinated-redeploy change
  (image rebuild + manifest re-planning).

- **Storage prefix:** `prepared/` → `feature/` (touches config YAML in three
  packages: `pv-prospect-data-transformation`, `pv-prospect-data-sources`,
  `pv-prospect-model`; also Terraform bucket layout).

## Status

Parked as of 2026-05-22. Pick up when there is a clean window with no
in-flight backfill runs (to avoid mixing prepared-v1 and feature-v1 nomenclature
in live ledgers and GCS state).
