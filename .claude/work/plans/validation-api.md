# Plan — Validation API endpoint (`/validate`)

Task 3 of the Validation API roadmap. Brief:
[validation-api.md](../briefs/validation-api.md). Depends on the Task-2 store + PV-site
repo ([validation-inmemory-store.md](validation-inmemory-store.md)); shares the
service and finalisation with [validation-serving-docs.md](validation-serving-docs.md).

## Goal

Add `GET /validate/sites` and `GET /validate/{system_id}` to `pv-prospect-app`. For a
known site, feed the loaded 90-day window's **real** weather (baked
`temperature` + `plane_of_array_irradiance`) through the deployed PV model and compare
to **real** `power`, per day. This isolates PV-model error from weather-model error and
sidesteps the weather-path vintage bias — the validate path never runs the weather
model or `calculate_POA`. One shared `run_pv_model` call serves both `/predict` and `/validate`.

## Design decisions

Recommendations baked in below; the two genuine forks (#3 age-fill, #4 metrics) are
surfaced for confirmation before implementation. #1–#2, #5 follow from the rules /
existing code and need no sign-off.

1. **Reuse the model package's feature machinery — don't re-derive in the app.**
   `pv-prospect-model` already owns the canonical seams (system-design consistency):
   - `compute_age_years(times, install_dates, fill_with)` → `(age_years, age_known)`.
   - The censoring rule `power_max > inverter_capacity * (1 - margin)`
     (`DEFAULT_CENSORING_MARGIN = 0.01`), today buried inside `apply_censoring_filter`.
   - `CONTINUOUS_FEATURES` / `BINARY_FEATURES` (the exact PV feature list).
   The brief's "clipping flag" is precisely this censoring predicate. → **Reuse all
   three.** This needs a small *enabling refactor* in the model package (decision #2).

2. **Enabling refactor in `pv-prospect-model` — its own commit, ahead of the app work**
   (cross-scope per the refactoring rule; keep it minimal):
   - Extract the predicate: `is_clipped(df, margin=DEFAULT_CENSORING_MARGIN) ->
     pd.Series` in `features/pv.py`; rewrite `apply_censoring_filter` as
     `df[~is_clipped(df, margin)].copy()` (behaviour-preserving). Training *drops*
     censored rows; validation *flags* them — same threshold, two callers. Export
     `is_clipped` from `features/__init__.py`.
   - Re-export `clamped_power_pred` (and `eval_in_f_space`, used for power-space R²)
     from `pv_prospect.model.__init__` — they live in `.evaluation` but are used
     cross-package in production. `chain.py` currently reaches into
     `pv_prospect.model.evaluation` directly (pre-existing import-rule violation);
     fix it to the package root in the same commit.

3. **Age fill for the 2 sites without an install date (`61272`, `79336`) — FORK.**
   `pv_sites.csv` has 8/10 sites with `installation_date`; two are blank.
   `compute_age_years(fill_with=None)` fills missing ages with the **median of the rows
   passed**. Training passed the *whole multi-site corpus* (global median); a naive
   per-site validation call would instead see no known ages for those two sites and
   fill `age_years = 0.0` ("brand-new install"), biasing their predictions. The exact
   training fill isn't recoverable without persisting it in the artifact (out of scope;
   needs a retrain). → **Recommend: compute one window-global fill** (median of known
   ages across *all* loaded sites) and pass it as `fill_with` to every per-site
   assembly. Matches training's *method* over the window population; document that the
   two date-less sites carry an imputed-age caveat and park the exact-match fix
   (persist `age_fill` in `feature_spec`) as a follow-up.

4. **Metrics + response richness — CONFIRMED.** Power-space R² is the robust headline
   (`eval_in_f_space` on power; scale-invariant, so daily kWh and daily-mean power give
   the same R²). MAPE explodes on near-zero-output days. → report both `power_space_r2`
   and `mape`; compute **MAPE only over days with `actual_kwh >= floor`** (documented
   floor, e.g. `0.5` kWh/day); **exclude clipped rows from *both* metrics** (the brief
   only says "headline" — extend to both for honesty); clipped + below-floor rows still
   appear in the `series` (flagged), just not in the aggregate.

   **Per the user: each series point carries both projected and actual quantities, in
   output *and* capacity-factor space** — not just kWh. So `SeriesPoint` exposes
   `predicted_kwh`/`actual_kwh` **and** `predicted_cf`/`actual_cf`, where
   `predicted_cf` is the model's raw capacity-factor output (pre-clamp,
   `predict_capacity_factor`) and `actual_cf = actual_daily_mean_power /
   panels_capacity`. This lets a client see the model's direct output alongside the
   delivered (clamped) power, and is what the website's validation chart consumes.

5. **Module placement & test boundary.** New `pv_prospect/app/validation.py` holds the
   pure feature-assembly + pure metric helpers + a thin orchestrator that takes the
   loaded `ModelArtifact`. Endpoints in `main.py` stay thin (look up store/site →
   call orchestrator → shape response). Per the purity rule, the model forward pass is
   *not* unit-tested (collaborator passed at the orchestration level); it is the
   Task-4 smoke test's job. Unit tests cover the pure pieces only.

   Window rows are daily aggregates (`prepare_pv` `timescale_days=1`): daily-mean
   `power`/`temperature`/`plane_of_array_irradiance`, native-cadence per-day
   `power_max`. So "aggregate to a daily series" = one series point per row;
   `kwh = daily_mean_power_w * 24 / 1000`.

## Changes

### `pv-prospect-model` (enabling refactor — separate, first commit)
- `features/pv.py`: add `is_clipped`; rewrite `apply_censoring_filter` over it.
- `features/__init__.py`: export `is_clipped`.
- `model/__init__.py`: re-export `clamped_power_pred`, `eval_in_f_space`.
- Tests: `tests/unit/features/pv/test_is_clipped.py` (threshold + margin, boolean
  Series); keep `test_apply_censoring_filter.py` green.

### `pv_prospect/app/chain.py` (shared PV model seam)
Extract the per-month tail of `predict_yield` (steps 4–6) into the **single** seam that
serves both `/predict` and `/validate`. It returns *both* quantities so the validate
path can expose the raw capacity factor (the user's requirement) without bypassing the
seam:
```python
@dataclass
class RunPvModelResult:
    capacity_factor: np.ndarray   # raw model output, pre-clamp
    clamped_power_w: np.ndarray   # delivered power, post inverter clamp

def run_pv_model(
    pv_artifact: ModelArtifact,
    feature_df: pd.DataFrame,            # CONTINUOUS_FEATURES + BINARY_FEATURES
    panel_capacity_w: np.ndarray,
    inverter_capacity_w: np.ndarray,
) -> RunPvModelResult:
    cf = predict_capacity_factor(pv_artifact, feature_df)
    return RunPvModelResult(cf, clamped_power_pred(cf, panel_capacity_w, inverter_capacity_w))
```
`predict_yield` builds its monthly `feature_df` and uses `.clamped_power_w` (ignores
`cf`); `validate_site` uses both fields. One function, both callers — this is the
brief's "one PV inference path." The daily-power→kWh factor (`* 24 / 1000`) is a
trivial shared constant, not extracted (purity rule — no branching). Fix the
`clamped_power_pred` import to the package root.

### `pv_prospect/app/validation.py` (new)
Imports: `from pv_prospect.model.features import compute_age_years, is_clipped,
CONTINUOUS_FEATURES, BINARY_FEATURES`; `from pv_prospect.model import eval_in_f_space`;
`from pv_prospect.app.chain import run_pv_model`;
`from pv_prospect.common import ...` (PVSite type only).

- **`window_age_fill(windows: dict[int, pd.DataFrame], install_dates: dict[int,
  date | None]) -> float`** (pure): median of known ages across all sites' rows;
  `0.0` if none known. Computed once per request from the loaded store + repo.
- **`assemble_features(window_df, pv_site, age_fill) -> pd.DataFrame`** (pure):
  `day_of_year = time.dt.dayofyear`; `temperature`/`plane_of_array_irradiance`
  passthrough; `age_years, age_known = compute_age_years(time, install_date_series,
  fill_with=age_fill)`; carry `power`, `power_max`, and `inverter_capacity`
  (from `pv_site.inverter_system.capacity`) for clipping + actuals. Output columns =
  `CONTINUOUS_FEATURES + BINARY_FEATURES` + those aux columns.
- **`compute_metrics(predicted_kwh, actual_kwh, clipped, floor) -> ErrorMetrics`**
  (pure): mask out `clipped`; `power_space_r2 = eval_in_f_space(actual, predicted).r2`
  over the kept rows; `mape` over kept rows with `actual_kwh >= floor`.
- **`validate_site(window_df, pv_site, pv_artifact, age_fill, floor) -> SiteValidation`**
  (orchestrator, not unit-tested): assemble → `result = run_pv_model(pv_artifact,
  feature_df, panel_cap, inverter_cap)` → per-day `predicted_kwh` (from
  `result.clamped_power_w`) / `actual_kwh`, `predicted_cf` (= `result.capacity_factor`) /
  `actual_cf` (= actual power / `panels_capacity`), and `clipped = is_clipped(frame)` →
  `compute_metrics`. Returns dataclasses `SeriesPoint(date, predicted_kwh, actual_kwh,
  predicted_cf, actual_cf, clipped)`, `ErrorMetrics(mape, power_space_r2)`,
  `SiteValidation(series, error)`. Both callers go through the one seam.
  - **`actual_cf` is exactly the training target**: `augment_features` defines
    `capacity_factor = power / panels_capacity`, so predicted-vs-actual cf is the
    cleanest apples-to-apples pair (and what the website chart wants).
  - **`predicted_cf` is pre-clamp, `predicted_kwh` is post-clamp**: on a clipped day
    `predicted_cf × panels_capacity × 24/1000 > predicted_kwh` (the gap is headroom
    lost to the inverter — informative, not a bug). Document in the endpoint/README so
    a client doesn't misread it. No metric impact (clipped rows are already excluded).

### `pv_prospect/app/main.py` (routes + models only)
- Pydantic: `SiteSummary(system_id, window_start, window_end)`,
  `ValidateSitesResponse(sites)`; `SeriesPointModel(date, predicted_kwh, actual_kwh,
  predicted_cf, actual_cf, clipped)`, `ErrorModel(mape, power_space_r2)`,
  `ValidateSiteResponse(system_id, series, error, model_version, window_updated_at,
  caveats)`.
- `GET /validate/sites`: `503` if `_window_cache` unloaded; else from
  `_window_cache.current()` list `system_ids`, per-site `window_start/end` = the
  site frame's `time` min/max date.
- `GET /validate/{system_id}`: `503` if model store **or** window unloaded; `404` if
  `store.for_site(system_id) is None`; look up the `PVSite` from the repo
  (`404`/`500` if absent); `age_fill = window_age_fill(...)`;
  `validate_site(...)`; respond with `_store.pv_version` and
  `store.updated_at` (window manifest). Include the honest-framing caveat (in-sample
  vs out-of-sample) in the response per the brief.

## Tests
- `tests/unit/validation/test_window_age_fill.py` — mixed known/unknown install dates →
  global median; all-unknown → `0.0`.
- `tests/unit/validation/test_assemble_features.py` — known site (real `age_years`,
  `age_known=1`) and date-less site (`age_fill`, `age_known=0`); `day_of_year` from
  `time`; temp/POA passthrough; aux columns present.
- `tests/unit/validation/test_compute_metrics.py` — clipped rows excluded from both
  metrics; below-floor days excluded from MAPE; R² scale-invariance sanity.
- `pv-prospect-model` `tests/unit/features/pv/test_is_clipped.py` (above).
- No endpoint / forward-pass unit test — Task-4 smoke test covers the live chain.

## Verification
`poetry run pytest tests/` in **both** `pv-prospect-model` (refactor) and
`pv-prospect-app`; `ruff check .`; `ruff format .`; `mypy .` (CLAUDE.md flags) in both.

## Out of scope (Task 4)
`/healthz`/`/version` window wiring, Terraform app-SA read grant + env, the
predicted-vs-actual smoke test, `architecture.puml`, doc finalisation, optional
monitoring. Website is separate ([website.md](website.md), already planned).

## Finalisation
Task 4 finalises all the validation briefs + plans together. This task commits the
model-package refactor (commit 1) and the app routes + tests (commit 2) only.
