# pv-prospect-model

ML model training for `pv-prospect`. Consumes versioned prepared CSV data and
produces trained model artifacts.

## Package Structure

```
pv_prospect/model/
├── domain.py          # Domain types for both models
├── features/
│   ├── pv.py          # Load prepared per-site CSVs, apply censoring, augment features
│   └── weather.py     # Load prepared weather partitions, monthly downsample, cyclic encoding
├── splits.py          # Temporal hold-out split and StandardScaler fitting
├── evaluation.py      # PV metrics + weather block-climatology evaluation
├── nets/
│   ├── pv.py          # CapacityFactorNet (4-layer feed-forward)
│   └── weather.py     # WeatherNet (4-layer feed-forward, 3 outputs)
├── training/
│   ├── loop.py        # Shared training loop with early stopping
│   ├── pv.py          # train_pv(): full pipeline from data → ModelArtifact
│   └── weather.py     # train_weather(): full pipeline from data → WeatherModelArtifact
├── persistence.py     # save/load artifact for both models
└── entrypoint.py      # CLI: train-pv, train-weather
```

## PV Model Data Flow

1. **Load.** Per-site prepared CSVs are walked from
   `{data_root}/pv/{system_id}/pv_*.csv`. Site capacities and
   `installation_date` are joined from `pv_sites.csv`.
2. **Censor.** Drop rows where `power_max > inverter_capacity * (1 - margin)`
   (default 1%). These rows have a biased daily-mean `power` because the
   inverter clipped within the day.
3. **Target.** `capacity_factor = power / panels_capacity`. Training on
   capacity factor (rather than raw watts) means one model generalises across
   all 10 sites and degrades gracefully on unseen prospect sites.
4. **Augment.** Add `day_of_year` (continuous weather feature). Compute
   `age_years` per row from each site's `installation_date` — but **not** as an
   MLP input: it is routed into a bounded multiplicative degradation factor at
   step 7 (see [Design Notes](#design-notes)). Sites with missing
   `installation_date` get `age_years` imputed from the global median of known
   sites; the `age_known` flag is still computed but is retained as a diagnostic
   column only, no longer a model feature.
5. **Split.** Temporal hold-out at the 80th percentile of the date range.
   Train on the earlier window; test on the later window. Random splits are not
   used — PV-weather rows are autocorrelated day-to-day, so a random split
   inflates reported R².
6. **Scale.** `StandardScaler` fitted on the train weather features only
   (`day_of_year`, `temperature`, `plane_of_array_irradiance`). The `age_years`
   column is passed through **unscaled** — it enters the degradation factor, not
   the MLP.
7. **Train.** `CapacityFactorNet` predicts
   `CF = head(weather) × (1 − r · age_years)` — a 4-layer feed-forward head on
   the weather features only, multiplied by a **bounded degradation factor** with
   a **fixed** rate `r = 0.007/yr` (a non-trainable buffer; physical band
   `[0.005, 0.010]`; see [Design Notes](#design-notes)). Dropout(0.2), no output
   activation. MSE loss on `capacity_factor`, Adam optimizer. Early stopping on a
   temporal val slice (last 10% of train). 100 epochs max, patience 10.
8. **Evaluate.** Metrics in two spaces:
   - *Capacity-factor space*: R², RMSE, MAE on predicted vs actual `f`.
   - *Clamped-power space*: same metrics on
     `min(f_pred × panel_capacity, inverter_capacity)` vs raw `power`.
     This is the honest end-user delivery metric.
   Both reported overall and per-site on the test split.
9. **Persist.** Artifact directory contains `model.pt` (state dict),
   `feature_spec.json`, `training_config.json`, `eval_report.json`.

## Smoke-Run Results (2026-06-02, local DVC mirror)

- 4,644 rows loaded → 3,984 post-censoring (14.2% dropped at 1% margin)
- Temporal split at 2026-02-21: 3,180 train / 804 test
- Early-stopped at epoch 11
- Test R² = 0.746 (capacity-factor), 0.822 (clamped-power)

## CLI Usage

```bash
# Install
poetry install

# Train PV model with defaults (all sites, standard config)
poetry run python -m pv_prospect.model.entrypoint train-pv \
    --data-root /path/to/prepared \
    --pv-sites-csv /path/to/pv_sites.csv \
    --output-dir /path/to/artifact

# Train PV model on specific sites with a custom cutoff
poetry run python -m pv_prospect.model.entrypoint train-pv \
    --data-root /path/to/prepared \
    --pv-sites-csv /path/to/pv_sites.csv \
    --output-dir /path/to/artifact \
    --system-ids 25724,56874,61272 \
    --cutoff-quantile 0.75 \
    --num-epochs 200

# Train weather model with defaults
poetry run python -m pv_prospect.model.entrypoint train-weather \
    --data-root /path/to/prepared \
    --output-dir /path/to/weather-artifact
```

## Artifact Format

Both models use the same four-file layout:

```
<output_dir>/
├── model.pt              # torch.nn.Module state dict
├── feature_spec.json     # feature/target columns, scaler params
├── training_config.json  # all hyperparameters used
└── eval_report.json      # evaluation metrics
```

Load with:

```python
from pv_prospect.model.persistence import load_artifact, load_weather_artifact

# PV artifact
artifact = load_artifact('/path/to/pv-artifact')
# artifact.model        → CapacityFactorNet (eval mode)
# artifact.scaler       → fitted StandardScaler (continuous features only)
# artifact.feature_spec → FeatureSpec (column order, target, scaler params)
# artifact.eval_report  → EvalReport (R², RMSE, MAE per space and per site;
#                          optional `loso` section, see PV Cross-Site Evaluation)

# Weather artifact
artifact = load_weather_artifact('/path/to/weather-artifact')
# artifact.model          → WeatherNet (eval mode)
# artifact.feature_scaler → fitted StandardScaler (inputs)
# artifact.target_scaler  → fitted StandardScaler (outputs)
# artifact.feature_spec   → WeatherFeatureSpec (columns, both scaler params)
# artifact.eval_report    → WeatherEvalReport (temporal + block-clim metrics)
```

## Design Notes

- **One model across all sites.** The weather-driven response is shared across
  all 10 sites (the physics-grounded `capacity_factor` target non-dimensionalises
  capacity), so a single model transfers to an unseen prospect. **Site identity
  is not a feature** — it cannot be populated for a prospect at inference time.
  The residual *per-site level* (orientation, shading, soiling baseline,
  panel/inverter quality) is therefore deliberately **not modelled**; for an
  unknown prospect it is unknowable, and is instead quantified as an uncertainty
  band downstream (see [PV Cross-Site (LOSO) Evaluation](#pv-cross-site-loso-evaluation)).
- **Age is a bounded multiplicative degradation factor, not a free feature.**
  `CF = head(weather) × (1 − r · age_years)` with a **fixed** `r = 0.007/yr`
  (physical band `[0.005, 0.010]`). Age was removed as an MLP input because, with
  only 10 aged sites, `age_years` is near-collinear with site identity and the
  free fit used it as a per-site intercept — a non-monotonic ~1.4–2.2 %/yr
  in-sample rate that inflated the served `age=0` (new-install) prospect by ×1.33.
  The bounded factor makes the `age=0` prediction defensible by construction
  (monotone, ≈ ×1.06 uplift at ~8 yr). The rate is **fixed, not fitted**: a
  within-site identifiability check found the decline robustly *signed* but
  ~2–5 %/yr (non-physical, irradiance-fragile), so physics supplies the rate, not
  this data. Full rationale: `reports/pv-age-feature.md`.
- **Inverter clamping at inference.** The model predicts `f`; the product
  delivers `min(f × panel_capacity, inverter_capacity)`.
- **Weather features (POA, temperature) are already joined** in the prepared
  CSVs. Cloud cover, raw DNI/DHI, humidity, and wind were swept and rejected
  in data-exploration — POA absorbs the atmospheric signal.
- **Tobit / censored regression not used.** Biased days are simply dropped
  (see step 2). The 1% margin was verified against site 82517 (inverter cap
  sharp at 6,000 W, max observed 6,004 W).

## PV Cross-Site (LOSO) Evaluation

Alongside the within-site temporal hold-out (step 8), the PV model carries an
optional **leave-one-site-out (LOSO)** eval — the genuine prospect scenario: for
each of the 10 sites, train on the other 9 and predict the held-out 10th. It is
computed by `training/loso.py::loso_eval`, recorded in the artifact's
`eval_report.json` under a `loso` section (`LosoReport`: per-site power-space
R²/MAPE + level ratio, pooled power R², and the level band), and surfaced in the
trainer's provenance JSON (`pv_loso_pooled_r2`, `pv_loso_band_1sigma`). Run it
standalone with `entrypoint loso-pv`.

LOSO is a **diagnostic, never a promotion gate** — a fold failure cannot abort or
gate the weekly trainer job. Its two purposes:

- **Calibrate the prospect uncertainty band.** The held-out per-site *level*
  (mean actual CF / mean predicted CF, in CF space so the fixed-`r` factor
  cancels and the ratio isolates level) is the out-of-sample spread a prospect
  estimate carries. Measured at **1σ ≈ ±17 %** (`data-v2026-06-12`), with
  out-of-sample mean level ≈ 1.00 (no transfer bias). This is a **floor** — the
  10 are self-selected, well-maintained PVOutput sites, so an arbitrary roof can
  sit below the worst of them.
- **Cross-site transfer sanity.** Pooled power-space R² 0.839 vs the model's own
  within-site 0.844 — a small cross-site penalty, confirming the shared weather
  response transfers.

Full method and results: `reports/pv-age-feature.md`.

---

## Weather Model

Predicts a UK weather **climatology** (not a forecast) for any lat/lon/elevation
point. Used at inference to estimate the expected weather at an unmeasured
prospect site, which is then fed into the PV model.

**Input contract:** `(latitude, longitude, elevation, day_of_year_sin, day_of_year_cos)`

**Output contract:** `(temperature, direct_normal_irradiance, diffuse_radiation)`

POA irradiance is *not* an output — it depends on panel orientation and is
computed downstream in the prediction API.

### Weather Model Data Flow

1. **Load.** Weather CSVs are walked from `{data_root}/weather/weather_*.csv`
   (one partition per geographic cell). Each row has lat/lon/elevation and daily
   weather values.
2. **Downsample to monthly.** Daily rows are collapsed to monthly means per
   grid cell. Overlapping extraction windows can produce duplicate
   `(grid, day)` rows; these are de-duplicated before averaging.
   Monthly aggregation discards within-month cloud-driven noise, which is
   not learnable from static geographic features.
3. **Cyclic day-of-year encoding.** `day_of_year` → `(day_of_year_sin,
   day_of_year_cos)` using a 365.25-day period. The cos component encodes
   the winter–summer axis (peak near 1 Jan, trough near 1 Jul); the sin
   component encodes the spring–autumn axis.
4. **Split.** Temporal hold-out at the 80th-percentile date. Train on the
   earlier window; test on the later window.
5. **Scale.** Two separate `StandardScaler`s — one for features, one for
   targets — both fitted on train only. The target scaler is required because
   temperature, DNI, and DHI have very different physical scales.
6. **Train.** `WeatherNet`: 4 uniform layers × 64 units, Dropout(0.1), 3
   outputs. MSE loss on standardised targets, Adam, early stopping on a
   temporal val slice.
7. **Evaluate.** Two sections in the artifact:
   - *Temporal test*: per-target R²/RMSE on the held-out test rows (smoke
     check; temporal R² reflects interannual noise, not spatial skill).
   - *Block-climatology vs IDW*: model predictions and IDW predictions both
     pushed through the same block-level climatology aggregation
     (`assign_coarse_blocks` → `block_climatology`), then compared against
     observed block climatologies. This is the honest holdout signal.
8. **Persist.** Same four-file artifact layout as the PV model, with
   `feature_spec.json` holding params for both the feature and target scalers.

### Weather Design Notes

- **Elevation is load-bearing.** Without elevation, temperature RMSE (1.017°C)
  is *worse* than IDW interpolation (0.784°C). Adding elevation brings it to
  0.672°C, beating IDW by 14%. The Scottish Highlands have a structural
  temperature bias that survives monthly averaging; lat/lon alone cannot
  represent it.
- **DNI and DHI are near the noise floor.** Their interannual variance is
  cloud-driven and absent from the static feature set. The model does not beat
  IDW for these targets; it is retained because a single artifact covering all
  three outputs simplifies the downstream prediction API.
- **Features must be static and inference-available.** Cloud cover and other
  dynamic atmospheric state are excluded as a category error: they are
  unknowable at a prospect site / future period, and they target variance that
  a long-run yield estimate averages out.
- **Two scalers, not one.** Feature and target distributions have very
  different scales (~15× difference between temperature and DNI). Both are
  stored as tuple params in `WeatherFeatureSpec` for JSON round-trip without
  pickling sklearn objects.
