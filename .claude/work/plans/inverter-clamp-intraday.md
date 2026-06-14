# Fix the inverter clamp: intraday clipping — design

> **Companion to `briefs/inverter-clamp-intraday.md`** (the finding + symptom).
> This plan specifies the fix. Serve-side, `/predict` only, **no retrain, no data
> re-version**.

---

## 1. Root cause (one line)

The inverter `min(cf·panel, inverter)` is applied to the **24 h-mean** DC power,
but the intraday peak (≈ 2.8–5× the daily mean) was already destroyed by
`reconstruct_daily_mean_poa`'s `.mean()` *before the model ran*. Clipping is a
hard nonlinearity on **instantaneous** power; averaging-then-clipping ≠
clipping-then-averaging.

## 2. The fix — magnitude from the model, shape from clear-sky, clip per hour

The chain *already* builds an hourly clear-sky POA profile inside
`reconstruct_daily_mean_poa` and throws all but the mean away. Keep it.

Decomposition:
- **Magnitude (energy):** the model's daily-mean prediction. Because training
  censors clipped days, `cf` is the **unclipped** daily-mean DC capacity factor —
  exactly the right quantity to redistribute and then clip.
- **Shape (intraday distribution):** the normalised hourly clear-sky POA profile
  `shape_norm = poa_hourly / poa_hourly.mean()` (mean = 1 by construction).

Per day `d`, per hour `h`:
```
dc_w[d, h] = cf[d] · panel · shape_norm[h]      # reconstructed instantaneous DC power
ac_w[d, h] = min(dc_w[d, h], inverter)          # inverter clip, per hour
daily_mean_ac_w[d] = mean_h ac_w[d, h]          # re-average to daily mean
kwh[d]     = daily_mean_ac_w[d] · 24 / 1000
```

**Key property — backward compatible.** When `inverter ≥ max_h dc_w[d,h]`, the
`min` is inert and `mean_h(cf·panel·shape_norm) = cf·panel·1 = cf·panel`, i.e. it
reduces *exactly* to today's number. The fix only ever *removes* energy, and only
above the peak-DC threshold.

Vectorises cleanly: `shape_norm` is `(H,)` (one per month, the representative
day), `cf` is `(D,)` over the month's days (it varies per day via `day_of_year` /
`age_years` even though POA is constant within the month). Outer product →
`(D, H)`, clip, mean over `axis=1`.

## 3. Code changes (`pv-prospect-app` only)

### 3.1 `app/poa.py` — expose the hourly profile
Split the existing function so the shape is reusable; the daily-mean stays
*bit-identical* to today (model-feature POA basis unchanged — do **not** reopen
the served-POA-basis question):
```python
def reconstruct_hourly_poa(...) -> np.ndarray:   # current body, return poa_hourly (H,)
def reconstruct_daily_mean_poa(...) -> float:    # return float(reconstruct_hourly_poa(...).mean())
```
Keep hourly (`periods=24, freq='h'`) resolution. Hourly samples are instantaneous,
so the peak/mean is already captured; finer resolution is a noted refinement
(§6), not a requirement.

### 3.2 New pure helper — the clip (in `app/chain.py`)
```python
def clipped_daily_mean_power(
    cf: np.ndarray,            # (D,) unclipped daily-mean capacity factor
    panel_capacity_w: float,
    inverter_capacity_w: float,
    poa_shape_norm: np.ndarray,  # (H,) hourly POA shape, mean == 1
) -> np.ndarray:               # (D,) clipped daily-mean AC power, W
    dc_w = cf[:, None] * panel_capacity_w * poa_shape_norm[None, :]
    ac_w = np.minimum(dc_w, inverter_capacity_w)
    return ac_w.mean(axis=1)
```
Pure → unit-tested directly (rename without underscore; it carries the fix's
logic).

### 3.3 `predict_yield` — wire it in
Per month: get `poa_hourly = reconstruct_hourly_poa(...)`; feed
`poa_hourly.mean()` to the model as today; obtain `cf` (raw, `(D,)`); then
```python
shape_norm = poa_hourly / poa_hourly.mean()
daily_ac_w = clipped_daily_mean_power(cf, panels_capacity_w, inverter_capacity_w, shape_norm)
monthly_kwh.append(float((daily_ac_w * 24 / 1000).sum()))
```
**`/predict` no longer routes through `clamped_power_pred` — replace it, do not
stack a second clamp.** `run_pv_model` can stay as the `/validate` path; for
`/predict` either call `predict_capacity_factor` directly for `cf` or read
`pv_result.capacity_factor` and ignore `clamped_power_w`. Prefer the former so the
dead daily-mean clamp isn't computed on the predict path.

## 4. Scope — what is deliberately NOT changed (consistency rule)

`clamped_power_pred` (`evaluation.py`) and its callers stay:
- **Model eval** (`eval_in_clamped_power_space`): operates on the **censored**
  test set (clipped days dropped) against measured daily-mean truth. There is no
  intraday shape, and the truth isn't clipped — intraday clipping would be wrong.
- **`/validate`** (`validate_site`, `app/validation.py`): feeds **real measured**
  daily-mean POA, compares to **real measured** power, and **excludes clipped
  days** from metrics. No synthetic shape exists; the daily-mean clamp is correct
  (and near-vacuous) here.

Both are "deliberately excluded, with reason" — the intraday reconstruction is a
property of the *climatological* predict path, which is the only place a clear-sky
shape is synthesised.

## 5. Acceptance (data-anchored; not absolute pred/actual)

Over-paneled corpus subset is the signal — 4/10 sites: **82517 (DC/AC 1.65)**,
56874 (1.26), 25724 (1.09), 24667 (1.08).

1. **Unit (`clipped_daily_mean_power`):** (a) reduces to `cf·panel` when
   `inverter ≥ peak DC` (backward compat); (b) strictly monotone-decreasing as
   inverter shrinks; (c) a hand-worked example matches.
2. **Behavioural (`/predict` smoke):** for a fixed over-paneled site, annual yield
   **strictly decreases** across `inverter = panel → 0.8·panel → 0.5·panel`, and
   is **unchanged** for `inverter ≫ peak DC`. This reproduces the reported symptom
   and is the regression guard.
3. **Shape sanity:** assert `max(shape_norm)` per month is in the empirically
   plausible band — clear-sky tilted-POA peak/mean ≈ **June 2.76× → Sep 3.32× →
   Dec 5.17×** (computed this conversation; cf. corpus all-season median
   `power_max/power` 5.7×). Clear-sky is **lower than real cloudy-spike peaks**, so
   the fix **under-clips** (conservative, safe direction) — state this, don't
   "correct" it.
4. **Isolation, not Gate A absolute:** absolute pred/actual ≈ 1 is **confounded**
   by the unpromoted age/vintage work (production serves old-basis artifacts at
   ~1.5×). Report the **clipping on-vs-off delta** on the over-paneled subset
   instead. Magnitude expectation: **low single-digit % annual** drop at realistic
   UK DC/AC (≈1.3) — correct physics, not a weak fix; 82517 (1.65) shows the
   largest effect.

## 6. Known approximations (note, don't over-engineer)

- **Clear-sky shape** sets the peak/mean and hence the clipping fraction. Real
  partly-cloudy days differ (cloud-edge enhancement can briefly exceed clear-sky;
  overcast lowers the peak). Clear-sky is the standard climatological assumption
  and a conservative central estimate (§5.3).
- **DC ∝ POA** linearity ignores intraday temperature (panels hotter at midday →
  lower peak DC), so it slightly *over*-estimates clipping; second-order.
- **Hourly resolution** under-resolves the instantaneous peak slightly; a finer
  grid (e.g. 15-min) is a pure-accuracy refinement. The daily-mean POA feature is
  resolution-invariant to < 0.5 %, so finer resolution does **not** disturb the
  train/serve POA basis — but it is out of scope unless §5.3 shows material
  under-clipping.

## 7. Documentation to update on completion

- `app/poa.py` module docstring: it now yields the shape *and* the mean.
- `pv-prospect-app/README.md`: document that `/predict` applies inverter clipping
  on reconstructed instantaneous power (climatological clear-sky shape), with the
  clear-sky-conservative caveat.
- `app/validation.py` docstring lines 9–12: the "on clipped days … gap is headroom
  lost to the inverter" claim is now demonstrably near-vacuous (the daily-mean
  clamp almost never bites). Correct it while in the file.
- `/predict` response `caveats` (`main.py:342`): add a one-line inverter-clipping
  assumption note.
