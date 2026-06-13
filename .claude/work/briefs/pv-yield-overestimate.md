# Fix the PV yield overestimate (collection of related fixes)

> **Umbrella task.** The `/predict` chain over-estimates annual generation by ~2×
> (Gate A: mean pred/actual 2.011 across 10 sites). Full diagnosis and attribution is
> in **`reports/weather-pv-vintage-alignment.md`**. The primary fix (retrain on the
> served 24 h-mean POA basis) has been executed — see **`reports/pv-train-on-served-poa.md`**
> — and reduced the overestimate to 1.515. The corpus target is correct
> (`corpus/true 1.01`); the **+51 % residual is re-attributed to the `age_years`
> train/serve convention** (report §5–6), **not** a Jensen ceiling. Closing it is the
> new **`briefs/pv-age-feature.md`** task.
> The fix space and remaining options are in the companion plan
> **`plans/pv-yield-overestimate.md`**.

## The collection

### Residual-closing lever (corrected) — the `age_years` feature

The "Jensen ceiling" reading is **withdrawn** (report §5–6). The corpus target is
correct (`corpus/true 1.01`); the +51 % residual is the **`age_years` train/serve
convention** (trained on real install ages, served `age=0`), likely a memorised
per-site intercept rather than real degradation. Closing it is **`briefs/pv-age-feature.md`**
(Next) — a constrained degradation prior + site/age decomposition, validated by LOSO.

The two earlier serve-side routes — serving the daytime-mean POA, and recalibrating the
low-POA CF curve — are **rejected** as residual fixes: each chases only ~5 % and would
mask the real cause. Retained as rejected-with-reason in the plan, not as live options.

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

### Cleanup + unblock W1 (now owned by `pv-age-feature`)

Contingent on **promotion** of the retrained model (deferred until `pv-age-feature`
gives a defensible `age=0` prediction) — production still serves old-basis artifacts, so
the caveats are still *true* until then. Once promoted: correct/remove the legacy caveat
in `app/poa.py` and `pv-prospect-app/README.md`, fix the now-false "< 1 % self-consistent"
note in `app/poa.py`, flip the TODO note, and unblock W1. These now ride with
**`briefs/pv-age-feature.md`**.

## Acceptance

Re-run Gate A (`scripts/measure_yield.py`, same window/store) after any fix —
predicted/actual annual kWh returns to ≈ 1 across sites.
