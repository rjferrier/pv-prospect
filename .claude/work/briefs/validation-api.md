# Validation API endpoint (`/validate`)

Third task of the Validation API roadmap. Depends on
[validation-inmemory-store.md](validation-inmemory-store.md); shares the service and
finalisation with [validation-serving-docs.md](validation-serving-docs.md).

## Why

For a *known* site the recorded output exists, so the model's predictions can be
checked against reality. Feeding the corpus's **real** weather (already carrying
`temperature` + `plane_of_array_irradiance`) through the deployed PV model and
comparing to **real** `power` isolates PV-model error from weather-model error, and
is insulated from the weather-path vintage bias
([weather-pv-vintage-alignment.md](weather-pv-vintage-alignment.md)) because it never
runs the weather model. This matches the diagram's `ValidationApi → PvModel` with no
`calculate_POA` self-loop.

**Honest framing (document in the response).** The deployed model trained on these
sites; recent-window days are partly in-sample (≤ the last `data-v` tag) and partly
out-of-sample (this week's increment). This validates *tracking on known sites* — the
easy case — **not** generalisation to unseen prospect sites (parked:
[cross-site-generalization-eval.md](cross-site-generalization-eval.md)).

## Decisions (settled)

- **Same service as Prediction** — a new route in the existing `pv-prospect-app`
  Cloud Run Service (`ValidationApi` nested in `PvProspectApp`). No new service.
- **Known sites, recent window** — every site in the loaded window; the period is
  bounded by the 90-day window (no arbitrary historical range). No held-out split.

## HTTP contract (sketch)

```
GET /validate/sites
→ { "sites": [ {"system_id": 89665, "window_start": …, "window_end": …}, … ] }

GET /validate/89665
→ {
  "system_id": 89665,
  "series": [ {"date": "2026-03-04", "predicted_kwh": …, "actual_kwh": …,
               "clipped": false}, … ],
  "error": { "mape": …, "power_space_r2": … },
  "model_version": "model-v…", "window_updated_at": "…"
}
```

Parametrised by `system_id` only — **not** tilt/azimuth: the corpus POA is baked to
the site's real installation, which is the whole point.

## Inference chain (reuses everything)

1. Look up the site's window rows in the in-memory store; 404 if not in the window.
2. Assemble features: `day_of_year` (from `time`), `temperature` + POA (window),
   `age_years`/`age_known` (install date from `pv_sites.csv`).
3. `predict_capacity_factor(pv_artifact, rows)` → capacity factor.
4. `clamped_power_pred(cf, panels_capacity, inverter_capacity)` (capacities from
   `pv_sites.csv`) → predicted power; actual is the window `power`. Aggregate to a
   daily series; compute MAPE + power-space R².
5. **`power_max` / clipping:** a row whose `power_max` reached inverter capacity has a
   biased daily-mean `power`. Flag such rows (`clipped: true`) and exclude them from
   the headline error metric; still show them in the series.

## Refactor (system-design consistency)

`chain.py:predict_yield` fuses weather-model → POA → PV-arm. Extract the **PV-arm
tail** — a function taking `[day_of_year, temperature, plane_of_array_irradiance,
age_years, age_known]` (+ capacities) → clamped power / kWh — so Prediction feeds it
weather-model + POA output and Validation feeds it window rows. One PV inference path
serves both, per `system-design.md`.

## Scope

`pv-prospect-app`: `/validate/sites` + `/validate/{system_id}` routes +
request/response models in `main.py`, PV-arm extraction in `chain.py`, an
error-metric helper. Unit tests for the metric and the PV-arm seam. Reuse
`predict_capacity_factor`, `clamped_power_pred`, the Task-2 store + site repo.
