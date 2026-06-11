# Train the PV model on the served (24 h-mean) POA basis

> Extracted as the **primary fix** from the PV yield-overestimate investigation.
> Full diagnosis and the reconciling Gate A/B numbers:
> **`plans/pv-yield-overestimate.md`**. Sibling riders, alternatives, and cleanup:
> **`briefs/pv-yield-overestimate.md`**. Design detail for this task:
> **`plans/pv-train-on-served-poa.md`**.

## Problem

The deployed `/predict` chain over-estimates annual generation by ~2× (Gate A: mean
pred/actual 2.0 across 10 sites). Gate B attributes ~89 % of that to the PV model
itself: even fed the **true** site POA it over-predicts real generation by ~83 %
(geomean 1.826). The root cause is a **train/serve POA-basis mismatch**:

- the PV model **trains on daytime-mean POA** — PVOutput has no night rows, so
  `prepare_pv`'s inner join keeps only daytime hours; corpus POA rarely falls below
  ~100 W/m²;
- the chain **serves the 24 h-mean POA** (`reconstruct_daily_mean_poa`; ~80–150 W/m²
  for the UK, dragged down by night zeros).

Serving therefore evaluates the model in its sparsely-trained, badly-extrapolated
low-POA region, where its concave CF-vs-POA curve over-predicts capacity factor by
1.4–2.8×.

## What's needed

Retrain the PV model on the **same POA convention it is served** — the 24 h-mean,
including the low-POA / night-zero samples — so the served operating range is
in-distribution:

- Decouple `prepare_pv`'s POA / temperature / target from PV-power (daytime) coverage:
  compute POA and temperature over the **full 24 h** weather day, and use an
  energy-based CF target, matching what `chain.py` serves.
- This also removes the **temperature-feature** train/serve mismatch (the model
  currently trains on daytime-warm temperature but is served ~24 h temperature).
- Re-transform the PV corpus and retrain; the weather model is untouched.
- Fix the now-false "< 1 % self-consistent" note in `app/poa.py`.

This was previously logged as the *24h-convention hygiene* tidy-up (believed
yield-neutral); Gate A/B **promote it to the primary fix** for the ~2× overestimate.

## Acceptance

Re-run Gate A (`scripts/measure_yield.py`, same window/store) — predicted/actual
annual kWh returns to ≈ 1 across sites. This **unblocks the website's W1 public
launch**.

## Alternatives

Two other routes target the same defect; pursue only if training on the served basis
proves unsuitable (detail in the umbrella `briefs/pv-yield-overestimate.md`): **serve**
the daytime-mean POA and integrate over daylight hours instead of ×24 (no retrain), or
**recalibrate** the low-POA response directly.