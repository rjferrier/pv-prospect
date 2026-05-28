# Train the PV model

Implement the PV model trainer in `pv-prospect-model` — currently in early-stage
validation via `pv-prospect-instance/data-exploration` notebooks. Blocked on
data completeness and resolved open questions about feature set.

## Current status (as of 2026-05-26)

**Data-exploration notebooks — PV side:**

- Smoke-run on cross-site capacity-factor model complete.
- 2,625 rows pre-filter → 2,387 post-censoring (9.1% dropped by inverter-clamp filter).
- Temporal split at 2026-03-23: 1906 train / 481 test.
- Test set: capacity-factor R² = 0.69, RMSE = 0.054, MAE = 0.040.
- Two sites flagged for investigation before lifting: 82517 (over-paneled,
  model overshoots), 79336 (underperforms). Others range 0.59–0.92 R².

**Data-exploration notebooks — weather side:**

- Cross-grid weather model (3-output regression: temperature, DNI, DHI) run complete.
- 805,896 rows, 20,638 grid points, April 2023 – May 2026.
- Test R²: DHI 0.854, temperature 0.240, DNI 0.067.
- **Caveat:** Test set is two disjoint seasonal islands (winter + May 2026 only)
  due to data gap. Results should not drive architecture/feature decisions until
  gap is plugged.

## Blockers before production training

1. **Weather data gap:** Prepared-weather corpus missing 15 months
   (Feb 2025 – May 2026). Re-run weather validation after gap is plugged.
2. **DNI fidelity:** Current DNI accuracy is likely insufficient as PV model
   input; noisy DNI propagates into POA and then into capacity factor.
   Consider whether cloud_cover should be added to weather feature set
   (requires extraction + preparation pipeline changes).
3. **Site 82517 / 79336 investigation:** Determine if model architecture or
   feature set needs adjustment; re-run smoke test.

## Implementation scope for v1

Once blockers are cleared, lift to `pv-prospect-model`:

**Package structure:** Mirror `pv-prospect-data-transformation` conventions.
Modules for feature building (pv, weather), temporal splits, evaluation
(f-space and clamped-power-space metrics), model persistence, and CLI entrypoint.

**PV data flow:** Load per-site prepared CSVs → filter censored days (power_max)
→ compute capacity_factor = power / panel_capacity → augment with day_of_year,
age_years, age_known → concatenate across sites → temporal hold-out split
→ fit StandardScaler on train → train PyTorch net (4 linear layers + dropout)
on capacity_factor with early stopping → evaluate in both spaces → persist
artifact (state_dict, scaler, feature spec, eval report).

**Weather data flow:** Load prepared grid-point CSVs → attach cyclic
day_of_year features → temporal split → fit scalers → train 3-output
regression net on (temperature, DNI, DHI) → evaluate per-target with
RMSE reporting.

**Implementation order:** Package skeleton, feature building (pure, unit-tested),
splits, evaluation, architecture ports, training loop, persistence, CLI,
end-to-end smoke test. ~1–2 days focused work once blockers cleared.

## Locked-in design decisions

- **Target:** capacity_factor (power / panel_capacity), not raw power.
- **Scope:** One PV model across all 10 sites; weather model as sibling workstream.
- **Censoring:** Drop days where power_max exceeds inverter capacity * (1 - 1% margin).
- **Features (PV):** POA, temperature, day_of_year (continuous); age_years, age_known (site attributes).
- **Features (weather):** latitude, longitude, cyclic day_of_year.
- **Resolution:** Daily (sub-daily prepared data not needed).
- **Split:** Temporal hold-out, not random (avoids autocorrelation leak).
- **Evaluation:** Report metrics in f-space and clamped-power-space.
- **Training infra (v1):** Local Python script; Cloud Run Job deployment is later.

See `plans/model-trainer.md` for the full design sketch.
