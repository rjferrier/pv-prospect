# Fix the PV yield overestimate (collection of related fixes)

> **Umbrella task.** The deployed `/predict` chain over-estimates annual generation
> by ~2× (Gate A: mean pred/actual 2.0 across 10 sites). Full diagnosis and the
> gated method are in the companion plan **`plans/pv-yield-overestimate.md`**
> (Context → Hypotheses → Method → Results → Conclusions → Recommendations).
>
> The **primary fix** — retraining the PV model on the served (24 h-mean) POA basis,
> which removes the dominant ~83 % of the gap — is **extracted to its own task**:
> **`briefs/pv-train-on-served-poa.md`** (TODO → *Next*). This brief collects the
> remaining, related fixes — riders and cleanup, not the cure.

## Background

This task began as *"Align OpenMeteo vintage between prepared-weather and
prepared-PV corpora"* — the instinct that a weather/PV corpus vintage mismatch caused
a ~30 % yield **under**-estimate. Measuring it end-to-end (Gate A) overturned both the
sign and the cause: the chain **over**-estimates by ~2×, and Gate B attributes that
**~89 % to the PV model itself** (it over-predicts ~83 % even when fed the true site
POA) and only **~11 % to a small, same-signed, temperature-led weather-path**. So the
cure is PV-model-side (the extracted task); the vintage/grid alignment that named this
task survives here as a ~8 % rider, kept for its own train/serve-consistency merits —
not because it drives the bias (it can only *shrink* the gap, never have opened it).

## The collection

### Primary fix — extracted (the cure)

**Train the PV model on the served (24 h-mean) POA basis** →
`briefs/pv-train-on-served-poa.md` / `plans/pv-train-on-served-poa.md`. Removes the
dominant ~83 %. **Hard prerequisite for the website's W1 public launch.** Everything
below is a rider on top of it.

### Alternatives to the primary fix (only if it proves unsuitable)

Same defect, different route — see the plan's *Recommendations* for detail:

- **Serve on the trained basis** — feed the daytime-mean POA and integrate over
  daylight hours instead of ×24 (chain-only, no retrain; lowest blast radius).
- **Recalibrate the low-POA response** — zero-force the intercept *and* correct the
  concave low-POA over-prediction.

### Weather-path rider — Option A: one weather source feeds both corpora

The original vintage/grid-alignment idea: derive the PV corpus POA from the grid
weather corpus (nearest grid point), so both corpora share a vintage *by construction*
and the PV model trains on the same grid-resolution weather it is served at inference.
Right on train/serve-consistency grounds regardless of yield; Gate B sized its yield
weight at the small ~8 % weather-path (a *positive* rider — the served irradiance is if
anything slightly *low*, and the rider is temperature-led). A ride-along once the
primary fix lands, not a standalone cure. Cross-component scope (transformation /
extraction / orchestration / re-backfill + retrain) is in the plan.

### Complementary — Option C: trainer validation gate

After training, before promotion, compute the weather model's DNI/DHI MAPE vs the
corpus on held-out (location, month) cells and **block promotion above a threshold**
(e.g. 20 %), so a silently-degraded weather model fails the gate instead of shipping.
Do this regardless of Option A/B.

### Fallback — Option B: stamp + gate the vintage

Only if exact-site fidelity proves material: keep on-site weather but capture the
OpenMeteo `generation_time`, stamp it, and have `prepare_pv` reject a day whose
on-site-weather vintage differs from the grid-weather vintage by > 1 week. Perpetuates
the train/serve skew Option A removes; unlikely to be needed.

### Cleanup + unblock W1 (contingent on the primary fix landing)

Once the fix lands and the smoke-test MAPE < 15 %: correct/remove the legacy ~30 %
caveat still carried in `app/poa.py` and `pv-prospect-app/README.md` (the `/predict`
caveat in `main.py` is already corrected to the measured +100 %); fix the now-false
"< 1 % self-consistent" note in `app/poa.py`; flip the TODO note and unblock the
website's W1 public launch.

## Acceptance (shared)

Re-run Gate A (`scripts/measure_yield.py`, same window/store) after any fix —
predicted/actual annual kWh returns to ≈ 1 across sites.
