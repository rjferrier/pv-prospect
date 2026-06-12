# Fix the PV yield overestimate — fix space

> **Companion to `briefs/pv-yield-overestimate.md`.** This plan documents the fix
> options and their design. The diagnosis (Gate A / Gate B) is in
> **`reports/weather-pv-vintage-alignment.md`**. The primary fix execution (retrain
> on 24 h-mean POA basis) is in **`reports/pv-train-on-served-poa.md`**: it reduced
> pred/actual from 2.011 → 1.515 but hit a structural Jensen ceiling.

---

## Serve-side fix (the next lever)

The Jensen ceiling means additional PV training cannot close the remaining ~51 % bias.
The residual is a daily→monthly Jensen gap: Gate A's `measure_yield.py` feeds
monthly-mean weather to the chain, and pushing that through a nonlinear MLP introduces
a convexity gap that is independent of training-set coverage. Two routes:

### Option 1 — Serve on the trained basis (chain-only, no retrain)

At inference, feed the **daytime-mean POA** (the model's trained region, where the
curve is ≈ unbiased) and integrate over daylight hours instead of ×24. Touches
`pv-prospect-app`'s chain only (`reconstruct_daily_mean_poa` / energy integration) —
**no retrain**. Lowest blast radius; cheapest to trial.

Design notes:
- `reconstruct_daily_mean_poa` currently returns a 24 h-mean; it must instead return
  a daytime-mean or supply a daylight-fraction alongside the POA.
- Energy integration in `chain.py` changes from `cf × 24` to `cf × daylight_hours`.
- Daylight hours derivable from solar geometry (`pvlib.solarposition` or equivalent)
  given site lat/lon and date; already available from the POA physics stack.
- Gate A (`measure_yield.py`) must be re-run against the modified chain to confirm
  pred/actual ≈ 1.

### Option 2 — Recalibrate the low-POA CF curve

Zero-force the intercept *and* correct the concave low-POA over-prediction by
augmenting low-POA training samples and/or constraining the MLP curvature. Touches
`pv-prospect-model` training. Note: zero-forcing the intercept alone does **not** fix
the concave over-prediction at 25–150 W/m² — both must be addressed together.

Design notes:
- Low-POA augmentation: synthesise pre-dawn / post-dusk samples (POA = 0, power = 0)
  and twilight samples derived from site physics. Add as a training-time augmentation
  step, not corpus data.
- Curvature constraint: a monotone spline or physics-informed constraint that forces
  CF(POA = 0) = 0 and CF / (POA/1000) ≤ 1 at low POA.
- After training, validate the CF-vs-POA curve before Gate A: the concave kink at
  25–150 W/m² should flatten to ≤ 1.0 × CF_physics.

---

## Secondary rider — Option A: one weather source feeds both corpora

Make the PV corpus's POA derive from the **grid weather corpus** (nearest grid
point), eliminating the separate on-site weather extraction. Right on
train/serve-consistency grounds: at inference the PV model only ever sees
grid-resolution, model-predicted weather (`chain.py:108`). Gate B sized the yield
weight at a small ~8 % positive rider (temperature-led; the serve-side and training-side
POA bases would then match for weather too). A ride-along once the serve-side fix
lands — **not** a standalone fix for the 2× bias.

Cross-component scope:

1. **Transformation** — `produce_pv_slice` / `run_prepare_pv` source weather from the
   grid partitions at the site's nearest grid point, not the on-site file (add a
   nearest-grid-point lookup; grid is 0.2° ≈ ≤ 11 km — far below the vintage effect).
2. **Extraction** — drop the per-slice weather task from the PV-sites backfill
   (`pv_backfill.py` `_weather_task_env`, phase 1 of `build_phases`).
3. **Orchestration** — new dependency: PV prepare for date D needs grid weather for D
   present (today they advance on independent markers).
4. **Re-backfill + retrain** — re-prepare the existing PV corpus from grid weather to
   repair historical drift, then retrain. Prior art:
   `backfill_prepared_corpora_20260524.py` (marker rewind + supersede old ledgers +
   trigger workflows). See also `reports/pv-train-on-served-poa.md` §2 for the
   data-pipeline mechanics.

---

## Complementary — Option C: trainer validation gate

After training, before promotion: compute the weather model's DNI/DHI MAPE vs the
corpus on held-out (location, month) cells; log it; **block promotion above a
threshold** (e.g. 20 %). Makes a silently-degraded weather model fail the gate instead
of shipping. Do this regardless of the Option 1/2/A/B choice.

Design: add a `validate_weather_model` step to the trainer (or the promotion script)
that loads the weather corpus, runs the model on held-out cells, and raises if MAPE >
threshold. The threshold and metric go in the trainer config.

---

## Fallback — Option B: stamp + gate the vintage

Only if exact-site fidelity proves material. Keep on-site weather but capture
OpenMeteo `generation_time` (not currently captured — add to `_metadata_from_response`,
`openmeteo.py:325`), stamp it into the raw metadata, and have `prepare_pv` reject a day
whose on-site-weather vintage differs from the grid-weather vintage by > 1 week.
Partial mitigation; perpetuates the train/serve skew Option A removes. Unlikely to be
needed.

---

## Cleanup + unblock W1 (contingent on pred/actual ≈ 1)

Once a fix reaches smoke-test MAPE < 15 %:
- Correct/remove the ~30 % caveat in `app/poa.py`'s docstring and in
  `pv-prospect-app/README.md`.
- Fix the `/predict` response caveat in `main.py:35` (already updated to +100 %; will
  need correcting to reflect the fixed state).
- Fix the now-false "< 1 % self-consistent" note in `app/poa.py`.
- Flip the `TODO.md` note and unblock the website's W1 public launch.

---

## Acceptance test

`measure_yield.py` is the acceptance test: re-run it after any fix
(same window 2025-06-09 → 2026-06-08, same actuals prefix, swap in the new model/chain)
and confirm pred/actual returns to ≈ 1 across sites.
