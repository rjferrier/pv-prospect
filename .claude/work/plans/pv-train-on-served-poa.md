# Plan: Train the PV model on the served (24 h-mean) POA basis

> Companion to `briefs/pv-train-on-served-poa.md`. The diagnosis (why this is the
> primary fix) lives in `plans/pv-yield-overestimate.md` — Gate A measured the ~2×
> overestimate; Gate B attributed ~83 % of it to the PV model at the true POA. This
> plan specifies the fix.

## Goal

Eliminate the dominant ~83 % of the +100 % yield overestimate (Gate B PV-intrinsic
geomean 1.826) by aligning the PV model's **training** POA basis with its **serving**
basis (24 h-mean), so inference no longer evaluates the model in an under-trained,
over-predicting low-POA region.

## Why this route (vs the alternatives)

Three routes reach the same end (see the umbrella plan's *Recommendations*): train on
the served basis (this plan), serve on the trained basis, or recalibrate the low-POA
curve. Training on the served basis is the most **principled** — it fixes the corpus
so the model is in-distribution at inference, and removes the temperature-feature
mismatch in the same change — at the cost of a corpus re-transform + retrain. (The
serve-side route is cheaper to trial and is the fallback if retraining underperforms.)

## Design

### 1. `prepare_pv` convention change — `pv-prospect-data-transformation`

- **Today:** `prepare_pv` inner-joins PV power onto weather hours; PVOutput omits
  night rows, so each daily POA / temperature / target is restricted to **daytime**
  hours → daytime-weighted (89665/06-09: POA 270 vs the 24 h mean 214, +26 %), worst
  on the first day of each 2-day window.
- **Change:** compute POA and temperature over the **full 24 h** weather day
  (decoupled from PV-power coverage), and use an **energy-based** CF target so the
  daily CF sits on the same 24 h basis as the served POA. This also removes the
  edge/interior window inconsistency and the temperature train/serve mismatch.
- Keep clipped/censored rows handled as today; the CF–POA relation stays ~linear
  through the origin on the new basis.

### 2. Re-transform + retrain

- Re-prepare the existing PV corpus under the new convention, then retrain the PV
  model. The weather corpus and weather model are untouched.
- Prior art for a corpus re-backfill (marker rewind + supersede old ledgers + trigger
  workflows): `util/pv_prospect/util/backfill_prepared_corpora_20260524.py`.
- If Option A (umbrella) is also adopted, fold its grid-weather sourcing into the same
  re-transform + retrain to avoid a second corpus rebuild.

### 3. Caveat / doc cleanup (on landing)

- Fix the now-false "< 1 % self-consistent" note in `app/poa.py`.
- Per the umbrella's *Cleanup + unblock W1*: once the smoke-test MAPE < 15 %, correct
  the `/predict` caveat (`main.py`) and the legacy ~30 % text in `app/poa.py` and
  `pv-prospect-app/README.md`, flip the TODO note, and unblock W1.

## Acceptance

Re-run Gate A (`scripts/measure_yield.py`, same window/store as the original
measurement) — predicted/actual annual kWh returns to ≈ 1 across sites (trust the
cross-site aggregate; single-year actuals are weather-noisy).

## Risks / open questions

- Retraining with night-zero / low-POA samples reshapes the loss landscape; verify the
  model still fits the daytime CF–POA slope rather than merely learning "low POA → low
  CF" at the expense of mid-range accuracy.
- The Jensen gap (pushing a monthly-mean POA through a nonlinear MLP at inference) also
  feeds the ~83 %; an in-distribution low-POA fit should shrink but may not fully
  remove it — re-measure, don't assume.
- Testing (per `coding-general.md`): carry `prepare_pv` unit tests for the new 24 h
  convention and an energy-target regression alongside the functional change.