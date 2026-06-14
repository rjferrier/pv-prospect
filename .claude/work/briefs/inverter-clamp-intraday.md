# Fix the inverter clamp: clip instantaneous power, not the daily mean

> **The `/predict` inverter clamp is applied to day-averaged power, so an
> inverter smaller than the panel array barely changes the predicted yield.**
> Verified by code trace + corpus data (this conversation). The clamp must move
> *before* the intraday→daily averaging. Fix design is in the companion plan
> **`plans/inverter-clamp-intraday.md`**.

## What

`predict_yield` (`app/chain.py`) computes delivered power as
`clamped_power_pred(cf, panel, inverter) = min(cf·panel, inverter)`
(`pv-prospect-model/.../evaluation.py:42`) and integrates `×24/1000` to kWh.
But `cf` is the **daily-mean** capacity factor: `reconstruct_daily_mean_poa`
(`app/poa.py`) collapses the hourly POA profile to a single 24 h-mean scalar
*before the model is ever called*, so `cf·panel` is the **24 h-mean DC power**
and the `min(…)` clamps the daily mean. Inverter clipping physically acts on
**instantaneous** power, which peaks well above the daily mean — so the clamp is
inert for any realistic inverter.

## Why (the finding, quantified)

From the prepared corpus (8092 rows, 10 sites):

- 24 h-mean capacity factor (what the model predicts): median **0.10**, p99 0.28.
- Instantaneous peak / 24 h-mean (`power_max/power`): median **5.7×**.
- The daily-mean clamp only bites when `inverter/panel < cf_mean` → inverter
  below **~28–31 % of panel** (DC/AC > 3.2) even on the best day. So an inverter
  at, say, 0.7–0.9 × panel (DC/AC 1.1–1.4 — normal, even aggressive, sizing)
  changes the prediction by **nothing**. Exactly the reported symptom.

Two facts compound to produce it:
1. Training **censors** clipped days (`apply_censoring_filter`,
   `features/pv.py:99`, drops rows where instantaneous `power_max ≥
   inverter·(1−margin)`), so `cf` is the **unclipped** daily-mean DC capacity
   factor — correct, and exactly the quantity a serve-side clip should consume.
2. The serve-side clamp is then applied on the daily mean, where it is inert.

Net effect: clipping losses are absent from `/predict` → **yield is
overestimated for any under-sized inverter.**

## Approach (detail in the plan)

Serve-side, **no retrain**. The chain already builds the hourly clear-sky POA
profile inside `reconstruct_daily_mean_poa`; expose it, reconstruct the hourly DC
power by scaling the model's (unclipped) daily-mean prediction to that shape,
apply the inverter `min(…)` **per hour**, then average to the daily mean and
integrate. Reduces *exactly* to today's number when `inverter ≥ peak DC`
(backward compatible). Full design, API, and caveats in
`plans/inverter-clamp-intraday.md`.

## Scope (cross-component)

- **`/predict` only.** The fix lives in `app/poa.py` + `app/chain.py`.
- **`/validate` and model eval are deliberately excluded.** Both consume *real
  measured* daily-mean weather (`validate_site`, `app/validation.py`;
  `eval_in_clamped_power_space`, `evaluation.py`) with clipped days
  censored/excluded from metrics — there is no synthetic intraday shape to clip
  against, and the daily-mean `clamped_power_pred` is correct (and near-vacuous)
  there. They keep it. See the plan for the reasoning.

## Acceptance

Data-anchored on the **over-paneled** corpus subset — 4/10 sites:
82517 (DC/AC 1.65), 56874 (1.26), 25724 (1.09), 24667 (1.08):

- `/predict` yield **strictly decreases** across inverter = panel → 0.8 → 0.5 ×
  panel (reproduces the symptom); inert above the peak-DC threshold.
- Clipping turns on around **DC/AC ≈ 1.3** (clear-summer peak ≈ 0.77 × panel),
  not the broken clamp's ~3.2.
- **Do not** use absolute Gate A pred/actual ≈ 1 as this fix's acceptance — it is
  confounded by the unpromoted age/vintage work (production still serves
  old-basis artifacts at ~1.5×). Isolate the effect: clipping **on vs off** delta
  on the over-paneled subset. The expected annual drop is **modest (low
  single-digit %)** at realistic UK DC/AC — that is correct physics, not a weak
  fix.

## Blockers

None. Self-contained serve-side change; no retrain, no data re-version.
