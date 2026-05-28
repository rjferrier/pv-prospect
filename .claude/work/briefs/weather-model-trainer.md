# Train the weather model

Implement the weather model trainer in `pv-prospect-model` — sibling workstream
to the PV capacity-factor model. Validated in data-exploration notebooks; blocked
on data completeness and DNI fidelity.

## Current status (as of 2026-05-29)

- Cross-grid smoke run complete: 805,896 rows, 20,638 grid points, April 2023 – May 2026.
- Test R²: DHI 0.854, temperature 0.240, DNI 0.067.
- **Caveat:** Test set was two disjoint seasonal islands due to a now-closed
  data gap. Re-run validation on the complete corpus before drawing conclusions
  from these numbers.

## Blockers before production training

1. **DNI fidelity:** Current DNI accuracy (R² 0.067) is likely insufficient as PV
   model input; noisy DNI propagates into POA and then into capacity factor. Consider
   whether `cloud_cover` should be added to the feature set (requires extraction +
   preparation pipeline changes).

## Implementation scope for v1

**Input contract:** `(latitude, longitude, day_of_year)` — day encoded as
sin/cos pair. Elevation is deliberately omitted; the model learns it implicitly
via lat/lon over the UK's small span.

**Output contract:** `(temperature, direct_normal_irradiance, diffuse_radiation)`.
POA irradiance is **not** an output — it depends on panel orientation and is
computed downstream by the prediction API.

**Data flow:** Load prepared grid-point CSVs → attach cyclic day_of_year features
→ temporal hold-out split → fit scalers on train set → train 3-output regression
net → evaluate per-target (RMSE, R²) → persist artifact (state_dict, scalers,
feature spec, eval report).

**Architecture:** Mirror the PV model conventions in `pv-prospect-model` (shared
package skeleton, feature building, splits, evaluation, persistence, CLI).

See `briefs/model-trainer.md` for the sibling PV model task and locked-in
cross-model design decisions (split strategy, training infra, etc.).
