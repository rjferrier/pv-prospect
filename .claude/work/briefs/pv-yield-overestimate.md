# Fix the PV yield overestimate (collection of related fixes)

> **Umbrella task.** The `/predict` chain over-estimates annual generation by ~2×
> (Gate A: mean pred/actual 2.011 across 10 sites). Full diagnosis and attribution is
> in **`reports/weather-pv-vintage-alignment.md`**. The primary fix (retrain on the
> served 24 h-mean POA basis) has been executed — see **`reports/pv-train-on-served-poa.md`**
> — and reduced the overestimate to 1.515, hitting a structural Jensen ceiling.
> The fix space and remaining options are in the companion plan
> **`plans/pv-yield-overestimate.md`**.

## The collection

### Serve-side fix (the next lever)

The Jensen ceiling means more PV training cannot reach pred/actual ≈ 1. The remaining
bias is a daily→monthly Jensen gap baked in by pushing a monthly-mean POA through a
nonlinear MLP. Two serve-side routes — see the plan for detail:

- **Serve on the trained basis** — feed the daytime-mean POA at inference and
  integrate over daylight hours instead of ×24. Chain-only, no retrain; lowest blast
  radius.
- **Recalibrate the low-POA CF curve** — zero-force the intercept *and* correct the
  concave low-POA over-prediction (augment low-POA samples / constrain curvature).
  Touches `pv-prospect-model` training.

### Weather-path rider — Option A: one weather source feeds both corpora

Derive the PV corpus POA from the grid weather corpus (nearest grid point), so both
corpora share a vintage by construction and the PV model trains on the same
grid-resolution weather it is served at inference. Gate B sized its yield weight at a
small ~8 % positive rider (temperature-led; served POA is if anything slightly low).
Justified on train/serve-consistency grounds regardless of yield. Cross-component
scope (transformation / extraction / orchestration / re-backfill + retrain) is in the
plan.

### Complementary — Option C: trainer validation gate

After training, before promotion, compute the weather model's DNI/DHI MAPE vs the
corpus on held-out (location, month) cells and **block promotion above a threshold**
(e.g. 20 %), so a silently-degraded weather model fails the gate instead of shipping.
Do this regardless of Option A/B.

### Fallback — Option B: stamp + gate the vintage

Only if exact-site fidelity proves material: capture OpenMeteo `generation_time`,
stamp it, and have `prepare_pv` reject a day whose on-site-weather vintage differs
from the grid-weather vintage by > 1 week. Perpetuates the train/serve skew Option A
removes; unlikely to be needed.

### Cleanup + unblock W1 (contingent on pred/actual ≈ 1)

Once a fix reaches the smoke-test MAPE < 15 %: correct/remove the legacy caveat still
carried in `app/poa.py` and `pv-prospect-app/README.md`; fix the now-false "< 1 %
self-consistent" note in `app/poa.py`; flip the TODO note and unblock the website's
W1 public launch.

## Acceptance

Re-run Gate A (`scripts/measure_yield.py`, same window/store) after any fix —
predicted/actual annual kWh returns to ≈ 1 across sites.
