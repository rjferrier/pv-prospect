# Model Trainer — Design Sketch

Scope: `pv-prospect-model`, PV model and weather model as sibling workstreams
sharing the same scaffolding shape.

## Locked-in decisions

1. **Target = capacity factor.** Train on `f = power / panel_capacity`, not raw
   `power`. Lets one model learn the shared physics across heterogeneous sites
   instead of memorising per-site magnitudes, and generalises to a brand-new
   prospect site (the README's actual product).
2. **One PV model across all sites.** Per-site differences are absorbed by
   site-attribute features rather than per-site weights, so the model
   degrades gracefully on unseen sites.
3. **Inverter clamp at train time = drop censored days.** Where any
   within-day sub-daily reading reached the inverter cap, the daily-mean
   `power` is censored and the daily-mean `f` computed from it is biased
   downward. Such days are dropped from training. Days where no within-day
   reading reached the cap have an honest daily-mean `f` and are kept.
   We are not pursuing Tobit / censored-regression / imputation variants for
   v1.
4. **Inverter clamp at inference.** Predict `f_pred`, then return
   `power_pred = min(f_pred * panel_capacity, inverter_capacity)`.
5. **Training resolution = daily.** Sub-daily PV–weather correlation is poor
   (weather-API sub-model noise); the daily aggregation that exists today is
   what restores the correlation. Training stays at daily resolution.
6. **Daily-max as a clipping flag.** `prepare_pv` augmented to emit
   `power_max` alongside daily-mean `power`. `power_max` is the maximum of
   the native-cadence cleaned PV power within the day, computed from the
   raw `pv_df` *before* the existing join-and-reduce to weather cadence.
   At training feature-building time, drop rows where
   `power_max > inverter_capacity * (1 - margin)`. Default `margin = 0.01`
   (1%). Verified against `.tmp/samples` for 82517: the inverter cap at
   6000 W is sharp (max observed 6004 W = measurement noise around cap), so
   a 1% margin reliably catches both mild-clip and heavy-clip days while
   leaving genuinely uncensored days alone.
7. **All 10 sites used.** No site is excluded for clipping; the daily-max
   flag drops only the censored days from those sites. This preserves the
   non-summer data and the geographic/scale diversity of the over-paneled
   sites (notably 82517 at 55.67°N and 24667 at 1.7 kW).
8. **Feature set (continuous).** `POA, temperature, day_of_year`. The notebook
   baseline (`pv-prospect-instance/data-exploration/main_models/common.py:42`)
   has already swept and rejected humidity, cloud cover, raw DNI/DHI, etc. —
   POA absorbs the atmospheric/cloud signal.
9. **Feature set (site-attribute).** `age_years` at row time +
   `age_known` indicator. Panel brand dropped (ephemeral, 10 brands across 10
   sites = de facto site one-hot). Shading dropped (subjective, owner-input).
   Panel geometry dropped (already baked into POA).
10. **No site one-hot / no site embedding (yet).** Site identity is *not* a
    feature, because it cannot be populated at inference time for a prospect
    site. If validation residuals show site-shaped structure later we can
    revisit with a small embedding, treating "unknown site" as a default
    embedding vector at predict time.
11. **Temporal hold-out, not random split.** Train on the early window
    (e.g. up to a cutoff date), evaluate on the later window. The notebook's
    `train_test_split(..., random_state=42)` leaks autocorrelated days from
    train into test and inflates R²; we are not copying that into production.
12. **Two evaluation surfaces.** Report metrics in both `f`-space (the
    quantity we actually predict) and clamped-`power`-space (the quantity
    the product delivers). The latter is the honest end-user error.
13. **Validate in `data-exploration` first.** Build the cross-site `f` model
    as a notebook in `pv-prospect-instance/data-exploration/main_models/`
    against the existing scaffolding before lifting into `pv-prospect-model`.

## Open decisions — resolved

- ~~**A. Sub-daily prepared data**~~ — superseded. Training stays daily; the
  daily-max clipping flag (locked-in #6) makes sub-daily prepared data
  unnecessary.
- ~~**B. Hourly vs quarter-hourly**~~ — superseded by the same.
- **C. Training infrastructure: local-run for v1.** Iterate in
  `data-exploration` first, then lift to `pv-prospect-model` as a local
  Python script. Productionising as a Cloud Run Job (with GPU) is a later
  step once the design has stabilised.
- **D. Weather model in parallel: yes (revised 2026-05-25).** While the data
  versioner consolidation blocks end-to-end PV runs, the weather model
  scaffold can land in parallel — it consumes a different prepared corpus
  (`staging/prepared/weather/`) and shares no code with the PV side beyond
  conventions. Multi-output regression: predicts
  `(temperature, direct_normal_irradiance, diffuse_radiation)` from
  `(latitude, longitude, day_of_year_sin, day_of_year_cos)`. DNI + DHI →
  POA happens downstream at inference time, in the prediction API, where
  panel geometry is known (per `doc/architecture.puml`). Elevation is
  deliberately omitted — the diagram's inference contract is
  `call_model(lat, lon, day, year)`; the model learns elevation implicitly
  via lat/lon.

## Adjacent work needed before production training

### `prepare_pv` change: emit `power_max` — **done**

Landed in commits `336e8a0` (prepare side) and `c7b1dd4` (`PV_COLUMNS`
update on the assemble side). Default-in to `DEFAULT_KEEP_COLUMNS`; the
column is computed from native-cadence `pv_df['power']` *before*
`reduce_rows` averages it onto weather cadence (a regression test guards
this — see `test_power_max_uses_native_cadence_not_reduced`). Deployed
and prepared data backfilled. Data versioner consolidation still
outstanding.

### `data-exploration` cross-site notebook — **end-to-end smoke run done**

Two files in `pv-prospect-instance/data-exploration/main_models/`:

- `common_cross_site.py` — pure-function module: loader walks per-site
  partitions under `{data_root}/pv/{system_id}/`, joins site metadata,
  applies the `power_max`-based censoring filter, computes
  `capacity_factor = power / panels_capacity` + `day_of_year` +
  `age_years` + `age_known`, sorts globally by time, splits at the 80th
  percentile, fits `StandardScaler` on train only. Returns a
  `CrossSiteDataset` dataclass.
- `neural-network-cross-site-capacity-factor.ipynb` — `CapacityFactorNet`
  (4 dense layers, dropout, lifted from the existing
  `neural-network.ipynb`), trained with Adam + MSE on `capacity_factor`,
  early stopping on a temporal validation slice (last 10% of train).
  Evaluates in both capacity-factor space and clamped-power space,
  overall and per-site.

**Smoke-run results (2026-05-26, DVC-checked-out local mirror):**

- 2,625 rows pre-filter → 2,387 post-censoring (9.1% dropped).
- Temporal split at 2026-03-23: 1906 train / 481 test.
- Test set: capacity-factor R² = 0.69, RMSE = 0.054, MAE = 0.040.
- Test set, clamped-power: R² = 0.80, RMSE = 283 W, MAE = 194 W.
- Early-stopped at epoch 18 (small model, 3,553 params).

**Per-site test R² (capacity-factor space):**

| Tier | Sites |
|---|---|
| ≥0.85 | 25724 (0.92), 56874 (0.88) |
| 0.65–0.85 | 61272 (0.77), 24667 (0.73), 4708 (0.71), 42248 (0.70), 36019 (0.65) |
| 0.5–0.65 | 89665 (0.59) |
| <0.5 | 79336 (0.39), **82517 (−0.83)** |

82517 is the over-paneled site at 55.67°N where the daily-mean `power`
is suppressed even on uncensored days; the model overshoots
systematically. 79336 also underperforms. Both are flagged for
investigation before lifting to `pv-prospect-model`.

### `data-exploration` cross-grid weather notebook — **run**

Two files in `pv-prospect-instance/data-exploration/main_models/`:

- `common_cross_grid.py` — pure-function module: loader walks
  `{data_root}/weather/weather_*.csv` partitions, attaches no metadata
  (lat/lon/elevation are already in the rows from the assemble step in
  `core.py`), adds cyclic `day_of_year_sin`/`day_of_year_cos` features,
  sorts globally by time, splits at the 80th percentile, fits separate
  `StandardScaler`s for features and targets on train only. Returns a
  `CrossGridDataset` dataclass.
- `neural-network-cross-grid-weather.ipynb` — `WeatherNet` (4 dense
  layers × 64 units, dropout, output dim 3), trained with Adam + MSE
  on standardised targets, early stopping on a temporal validation
  slice (last 10% of train). Evaluates per-target after inverse-scaling
  back to physical units; reports per-grid-point RMSE to surface any
  geographically clustered error structure.

**First real-data run (2026-05-26):** 805,896 rows, 20,638 grid points,
April 2023 – May 2026. Temporal split at 2024-12-01 (80th percentile):
643,864 train / 162,032 test. Early-stopped at epoch 11 (best model =
epoch 1).

**Test set results:**

| Target | R² | RMSE |
|---|---|---|
| `temperature` | 0.240 | 3.5 °C |
| `direct_normal_irradiance` | 0.067 | 78.8 W/m² |
| `diffuse_radiation` | 0.854 | 18.9 W/m² |

**Findings:**
- DHI generalises well — strong seasonal/lat signal that day_of_year +
  lat/lon captures cleanly.
- DNI is poor throughout (expected) — dominated by cloud cover
  variability, which is absent from the feature set.
- Temperature has a large train→test gap (0.686 → 0.240). The test
  window falls in winter; worst grid points are Scottish Highlands
  (RMSE 8–9 °C), best are Cornwall (RMSE < 1 °C). Elevation was
  deliberately excluded from the inference contract; the geographic
  evidence here suggests it may be load-bearing for Scotland.
- Early stopping behaviour: val_loss rose monotonically after epoch 1
  while train_loss kept falling — initially read as temporal
  distribution shift, but see below; the actual root cause is the
  test-set composition.

**Test-set composition discovery (most important caveat):**

The bimodal predicted/true scatter plots prompted a closer look at the
test rows' time coverage:

| Cluster | Period | Rows | Temp mean | DNI mean | DHI mean |
|---|---|---|---|---|---|
| Winter | Dec 2024 – Feb 2025 | 72,488 | 4.9 °C | 35 W/m² | 15 W/m² |
| Late spring | **May 2026 only** | 89,544 | 9.7 °C | 105 W/m² | 110 W/m² |

The prepared-weather corpus runs continuously from Apr 2023 to Feb 7
2025, then has a 15-month gap with no partitions, then resumes with
sliding 14-day windows for May 2026 (six overlapping partition files,
consistent with a recently-restarted daily extract). The "test set"
the 80th-percentile split lands on is therefore two disjoint seasonal
islands — not a continuous holdout.

This invalidates parts of the readout above:

- DHI's test R² = 0.854 is largely the model learning "winter mean
  ≈ 15, May mean ≈ 110" from day_of_year. The within-cluster std is
  only 7.8 (winter) and 17.5 (May), so cluster-mean discrimination
  alone scores well.
- Temperature's train→test gap is partly genuine (winter prediction
  is hard) but is also inflated by the unrepresentative test slice.
- The epoch-1 early-stopping behaviour is consistent: any reasonable
  init quickly nails the cluster-mean discrimination, and further
  training can't help without features that distinguish within-cluster
  variation.

The numbers should not be treated as a real generalisation probe
until the prepared-weather gap is plugged — see TODO
"Plug the 2025-02-07 → 2026-05-05 weather gap".

**Open questions before lifting to `pv-prospect-model`:**
- DNI accuracy is likely insufficient as a PV model input; noisy DNI
  propagates into the POA calculation and then into capacity factor.
  Consider whether cloud_cover (or similar atmospheric state variable)
  should be added to the feature set, which would require also
  including it in the weather extraction and preparation pipeline.
- Elevation's exclusion from the inference contract is suggestive but
  not conclusively load-bearing for Scotland — re-evaluate after a
  continuous test set is available.
- Re-run the notebook once the weather corpus is gap-free; the
  current metrics aren't a basis for an architecture or feature-set
  decision.

## Package layout

Mirroring the conventions of `pv-prospect-data-transformation`:

```
pv-prospect-model/
├── pyproject.toml                 # deps: torch, sklearn, pandas, numpy, common, etl
├── README.md
├── Dockerfile                     # later, for Cloud Run Job
├── pv_prospect/
│   └── model/
│       ├── domain.py              # FeatureSpec, ModelArtifact, EvalReport
│       ├── features/
│       │   ├── pv.py              # build_pv_features(): load + compute f + drop clipped + augment
│       │   └── weather.py         # build_weather_features() — stub for v1
│       ├── splits.py              # temporal_holdout(df, time_col, cutoff)
│       ├── nets/
│       │   ├── pv.py              # SolarPowerNet (ported from notebook)
│       │   └── weather.py         # stub
│       ├── training/
│       │   ├── pv.py              # train_pv(features, config) -> ModelArtifact
│       │   ├── weather.py         # stub
│       │   └── loop.py            # shared train loop, optimizer, dataloader plumbing
│       ├── evaluation.py          # eval_in_f_space + eval_in_clamped_power_space
│       ├── persistence.py         # save_artifact / load_artifact (state_dict + scaler + spec)
│       └── entrypoint.py          # CLI for `train_pv` and `train_weather`
└── tests/
    └── unit/
        ├── features/test_pv.py    # f computation, clipping drop, age imputation
        ├── test_splits.py
        ├── evaluation/test_evaluation.py
        └── persistence/test_persistence.py
```

Units that are pure functions (feature building, splits, evaluation,
persistence round-trip) get unit tests. The training loop is glue and gets
integration coverage via a tiny smoke-train on synthetic data.

## Data flow (PV model)

1. **Load.** For each PV site, read the per-site prepared CSV from the
   versioned feature store (or `staging/prepared/pv/{site}.csv` locally).
   The prepared frame carries daily-mean `power`, daily-mean weather, and
   (post-prepare-change) `power_max`. Look up `panel_capacity` and
   `inverter_capacity` from `pv_prospect.common`'s PV-site repo (already
   initialised from `pv_sites.csv`).
2. **Filter censored days.** Drop rows where
   `power_max > inverter_capacity * (1 - margin)`. Default margin = 0.01.
3. **Compute target.** `f = power / panel_capacity` from daily-mean `power`.
4. **Augment features.** Add `day_of_year` (from `time`), `age_years`
   (`(time - installation_date).days / 365.25`), `age_known` (binary). For
   sites with missing `installation_date`, impute `age_years` with the median
   across known sites at that row's time, and set `age_known = 0`.
5. **Concatenate.** Stack all sites' rows into one frame. Keep `system_id` as
   an internal column for stratification only (not a feature).
6. **Split.** Temporal hold-out at a configurable cutoff date.
   `train` = rows with `time < cutoff`, `test` = rows with `time >= cutoff`.
   Default cutoff: 80th percentile of the date range.
7. **Scale.** Fit `StandardScaler` on continuous features over the train set
   only; transform both splits. `age_known` is binary, not scaled.
8. **Train.** PyTorch loop. Architecture: start with the notebook's
   `SolarPowerNet` (4 linear layers + dropout) but resize the output range
   for `f` (range roughly [0, 0.3]). MSE loss on `f`, Adam optimizer, 100
   epochs with early stopping on a held-out slice of train.
9. **Evaluate.**
   - **`f`-space metrics** (MSE, RMSE, MAE, R² on `f_pred` vs `f_true`) —
     compares what the model actually predicts.
   - **Clamped-power-space metrics** (same metrics on
     `min(f_pred * panel_capacity, inverter_capacity)` vs `power`) — the
     end-user delivery metric. Compute per-site as well as overall, to surface
     any site where the model performs systematically badly.
10. **Persist.** Save `state_dict`, fitted scaler, `FeatureSpec` (ordered
    column list + scaler params + categorical-handling rules), training
    config, and eval report into a single artifact directory. The Data
    Versioner picks it up.

## What this implies for adjacent code

- **`prepare_pv` change (Open Question A1).** Removing or parameterising
  `timescale_days=1` so the prepared CSVs become sub-daily. Affects the
  partitioned-prepared layout (filenames already include date windows, but
  the rows-per-file count goes up ~24x at hourly). Worth checking against
  `project_prepared_data_layout` outstanding work.
- **`pv_prospect.common`.** PV-site repo already exposes capacities; nothing
  new there. May want a convenience accessor for `installation_date` if not
  already there.
- **Data Versioner.** Once the model trainer produces an artifact, the
  versioner needs to know to snapshot it (the architecture has this — V4
  pushes models too — but the trainer is the producer it doesn't have yet).
  Out of scope for this plan; flag as the natural follow-up.

## Out of scope for v1

- Hyperparameter sweep / architecture search.
- Site embedding (revisit if site-shaped residuals appear).
- Tobit / censored-regression loss.
- Weather model implementation (only its module skeleton lands).
- Cloud Run Job deployment of training (local-run for now).
- Online serving — that's the Prediction API in `pv-prospect-app`.

## Implementation order once approved

1. `pyproject.toml` + package skeleton + README.
2. `features/pv.py` (pure, unit-tested) — load, drop, compute, augment.
3. `splits.py` (pure, unit-tested).
4. `evaluation.py` (pure, unit-tested) — the two metric surfaces.
5. `nets/pv.py` — port the notebook architecture.
6. `training/pv.py` + `training/loop.py` — glue, smoke-tested.
7. `persistence.py` — round-trip tested.
8. `entrypoint.py` — CLI wrapping the above.
9. End-to-end smoke run on local prepared data.
10. Settle on cutoff date and training defaults from the smoke run; write
    these into a default config.

This is roughly 1–2 days of focused work on the PV model alone, assuming
Open Questions A and B are resolved first (the prepare-resolution change is
the only thing that blocks a real training run).
