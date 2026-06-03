# Train the weather model

Implement the weather model trainer in `pv-prospect-model` — sibling workstream
to the PV capacity-factor model. **Implementation complete as of 2026-06-03.**

## Status

Package implementation done. See `plans/weather-model-trainer.md` for the full
validation history, spatial-block evaluation results, and design rationale.

## Locked-in design decisions

**Input contract:** `(latitude, longitude, elevation, day_of_year_sin, day_of_year_cos)`.
Elevation is load-bearing — without it the temperature model (RMSE 1.017°C) is
*worse* than IDW (0.784°C); with it, 0.672°C (spatial-block evaluation 2026-06-02).
`cloud_cover` and other dynamic atmospheric state are excluded as a category error
(see the plan for the rationale).

**Output contract:** `(temperature, direct_normal_irradiance, diffuse_radiation)`.
POA irradiance is **not** an output — it depends on panel orientation and is
computed downstream by the prediction API.

**Training data:** Monthly-aggregated prepared weather CSVs. Daily rows are
collapsed to monthly means to discard unlearnable within-month cloud noise.

**Evaluation in the artifact:** Two sections — temporal hold-out (smoke check) and
block-climatology RMSE vs IDW on the temporal test set (honest holdout signal).
Spatial-fold evaluation (geographic hold-out) stays in data-exploration.
