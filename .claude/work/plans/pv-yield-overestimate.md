# Fix the PV yield overestimate — fix space

> **Companion to `briefs/pv-yield-overestimate.md`.** This plan documents the fix
> options and their design. The diagnosis (Gate A / Gate B) is in
> **`reports/weather-pv-vintage-alignment.md`**. The primary fix execution (retrain
> on 24 h-mean POA basis) is in **`reports/pv-train-on-served-poa.md`**: it reduced
> pred/actual from 2.011 → 1.515. The "Jensen ceiling" reading is **withdrawn** (report
> §5–6): the corpus target is correct (`corpus/true 1.01`) and the residual is the
> `age_years` train/serve convention — closed by **`briefs/pv-age-feature.md`**.

---

## Residual-closing lever (corrected) — the `age_years` feature

The "Jensen ceiling" framing is **withdrawn** (report §5–6). The corpus target is
correct (`corpus/true 1.01`); the +51 % residual is the **`age_years` train/serve
convention** — trained on each site's real install age, served `age=0` — likely a
memorised per-site intercept rather than real degradation. The fix is its own task,
**`briefs/pv-age-feature.md`** (Next): a constrained degradation prior plus a site/age
decomposition (explicit site effect + shared age slope, with embedding dropout for the
unknown-site case), validated by the LOSO eval. See that brief for the design.

### Rejected serve-side routes (retained as rejected-with-reason)

Two serve-side routes were considered as the residual fix and **rejected**: each chases
only ~5 % and would mask the real cause (the age feature) — report §6 ("don't").

- **Serve on the trained basis** — feed the daytime-mean POA at inference and integrate
  over daylight hours instead of ×24 (chain-only, no retrain). Cheap, but ~5 % and
  off-cause.
- **Recalibrate the low-POA CF curve** — zero-force the intercept and constrain the
  concave low-POA over-prediction. Touches `pv-prospect-model`; ~5 % and off-cause.

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

## Cleanup + unblock W1 (now owned by `pv-age-feature`)

Contingent on **promotion** of the retrained model — deferred until
`briefs/pv-age-feature.md` yields a defensible `age=0` prediction. Production still
serves old-basis artifacts, so the caveats remain *true* until promotion; do not touch
them before then. Once promoted (this list now rides with `pv-age-feature`):
- Correct/remove the ~30 % caveat in `app/poa.py`'s docstring and in
  `pv-prospect-app/README.md`.
- Fix the `/predict` response caveat in `main.py:35` (currently +100 %; correct to the
  fixed state).
- Fix the now-false "< 1 % self-consistent" note in `app/poa.py`.
- Flip the `TODO.md` note and unblock the website's W1 public launch.

---

## Acceptance test

`measure_yield.py` is the acceptance test: re-run it after any fix
(same window 2025-06-09 → 2026-06-08, same actuals prefix, swap in the new model/chain)
and confirm pred/actual returns to ≈ 1 across sites.
