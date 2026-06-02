# Weather Model Trainer — Design Sketch

Companion to `briefs/weather-model-trainer.md`; sibling of `pv-model-trainer.md`.

> **Consolidation TODO:** some weather design context (decision D, the cross-grid
> run findings) still lives in `pv-model-trainer.md` from when both models shared
> one plan. It should be migrated here so this plan is self-contained.

## Purpose & contract

Predicts a UK weather **climatology**, not a forecast:
`(latitude, longitude, day_of_year)` →
`(temperature, direct_normal_irradiance, diffuse_radiation)`. Used at inference to
estimate the *expected* weather at an unmeasured prospect site, feeding the
downstream PV model. DNI + DHI → POA (which needs panel geometry) happens
downstream in the prediction API, **not** here.

Developed in parallel with the PV model (decided 2026-05-25, while PV was blocked
on the data-versioner consolidation): it consumes a separate prepared corpus
(`staging/prepared/weather/`) and shares no code with the PV side beyond
conventions.

## Locked-in principles

- **Features must be static and inference-available** — only geographic attributes
  computable for any lat/lon at inference. Dynamic atmospheric state (e.g.
  `cloud_cover`) is excluded as a category error: it is unknowable at a prospect
  site/future period (as unknown as the DNI it would "explain"); it targets
  variance a long-run yield estimate averages out; and short-term needs would use
  OpenMeteo's forecast directly, not this model.
- **Elevation: in.** Static and inference-available (DEM lookup), and it explains a
  *structural* temperature bias — the Scottish-Highland error survives monthly
  averaging, so it is not within-month noise. (Validated in data-exploration.)
- **Coastal proximity: candidate.** Distance-to-coast plus a directional (westward)
  fetch / sea-fraction — static, inference-available, and physically motivated
  (Atlantic SW weather ⇒ western maritime exposure is cloudier/wetter, eastern lee
  drier/sunnier). Prototype via uk-geo's UK landmass geometry (`_load_uk_geometry`;
  the boundary is the coastline).

## Scaffolding (data-exploration)

Two files in `pv-prospect-instance/data-exploration/main_models/`:

- `common_cross_grid.py` — pure-function module: loader walks
  `{data_root}/weather/weather_*.csv` partitions (lat/lon/elevation are already
  in the rows from the assemble step in `core.py`), adds cyclic
  `day_of_year_sin`/`day_of_year_cos`, sorts by time, temporal-splits at the 80th
  percentile, and fits separate feature/target `StandardScaler`s on train only.
  Toggles added since: `monthly=True` (collapse each cell's days to a monthly
  mean) and `include_elevation=True`. Returns a `CrossGridDataset`.
- `neural-network-cross-grid-weather.ipynb` — `WeatherNet` (4 dense layers × 64,
  dropout, 3 outputs), Adam + MSE on standardised targets, early stopping on a
  temporal validation slice; evaluates per-target in physical units plus per-grid
  RMSE. The sibling `neural-network-cross-grid-weather-monthly.ipynb` exercises
  the monthly downsample and a lat/lon-ablated seasonality baseline;
  `include_elevation=True` has also been run.

## Validation history (data-exploration)

**First run (2026-05-26, gapped corpus):** 805,896 rows, 20,638 grid points,
Apr 2023 – May 2026. Temporal split at 2024-12-01: 643,864 train / 162,032 test.
Early-stopped at epoch 11 (best = epoch 1).

| Target | test R² | RMSE |
|---|---|---|
| temperature | 0.240 | 3.5 °C |
| direct_normal_irradiance | 0.067 | 78.8 W/m² |
| diffuse_radiation | 0.854 | 18.9 W/m² |

**Test-set composition discovery — the lesson that drove the evaluation
reframe.** The prepared-weather corpus ran continuously Apr 2023 → 7 Feb 2025,
then had a ~15-month gap, then resumed with sliding 14-day windows for May 2026.
The 80th-percentile split therefore landed on *two disjoint seasonal islands*
(winter 2024-25 + May 2026), not a continuous hold-out. DHI's 0.854 was largely
the model discriminating winter-mean (≈15 W/m²) from May-mean (≈110 W/m²) via
`day_of_year`; the epoch-1 early-stop was that cluster-mean discrimination
saturating immediately. The numbers were not a real generalisation probe.

**Gap closed** (versioning commit, 2026-05-28) and the notebook re-run on the
continuous 2022-08 → 2026-05 corpus, alongside the monthly-downsample variant and
the seasonality baseline. These established the hard ceiling of
`(lat, lon, day_of_year)`:

- **DNI** ≈ unlearnable — even monthly, the model barely matches a calendar-
  month-mean baseline; its variance is cloud-driven, absent from the features.
- **DHI** scores well but almost entirely as *seasonal climatology* — lat/lon
  adds ≈0 over the calendar-month-mean baseline.
- **Temperature** is the one target with genuine spatial skill (beats the
  seasonality baseline by ~+0.17–0.20 R²), and its worst errors cluster in the
  Scottish Highlands — a structural bias that *survives* monthly averaging, i.e.
  elevation, not noise. That is what put elevation back into the feature set
  (it had originally been omitted on the theory the model would learn it
  implicitly via lat/lon over the UK's small span — the Highland error disproved
  that).

These readings — and the recognition that per-row temporal-hold-out R² is the
wrong lens — motivate the evaluation design below.

**Spatially-blocked evaluation — first run (2026-06-02, fold 0, monthly +
elevation, 5-fold, D ∈ {0, 10, 25, 50} km).**

194,512 monthly rows, 20,638 grid points, 1,540 coarse 0.16° blocks. Test
set: fold 0 = 308 blocks / 40,045 rows.

Block-climatology RMSE and bootstrap 1-σ noise floor:

| Target | floor | IDW D=0 | model D=0 | lift | IDW D=10 | model D=10 | lift | IDW D=25 | model D=25 | lift |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| temperature (°C) | 0.54 | 0.78 | 0.63 | **+20%** | 0.81 | 0.72 | **+11%** | 0.96 | 0.74 | **+24%** |
| DNI (W/m²) | 15.6 | 16.5 | 17.3 | −5% | 16.3 | 18.7 | −15% | 17.4 | 18.9 | −9% |
| DHI (W/m²) | 2.48 | 3.11 | 3.98 | −28% | 3.05 | 4.77 | −56% | 3.34 | 4.23 | −27% |

D=50 km result omitted: buffer collapsed the training set to 85 rows (2 blocks).
The 5-fold scheme with test blocks scattered across the UK creates near-Swiss-
cheese training at large buffers — see Evaluation design note on this.

**Elevation ablation at D=0 (same fold, base vs +elevation):**

| Target | base (lat/lon) | +elevation | delta | IDW |
|---|---:|---:|---:|---:|
| temperature (°C) | 1.017 | 0.672 | **−0.35** | 0.784 |
| DNI (W/m²) | 18.60 | 18.27 | −0.33 | 16.53 |
| DHI (W/m²) | 4.36 | 4.34 | −0.02 | 3.11 |

**Conclusions:**
- **Elevation is load-bearing for temperature**: without elevation, the model
  (1.017°C) is *worse* than IDW (0.784°C); adding elevation brings it to
  0.672°C, beating IDW by 14%. The entire temperature lift seen at D=0 (+20%)
  is elevation-driven — lat/lon alone cannot beat spatial interpolation.
- **DNI and DHI**: IDW wins at every setting. Both sit close to their bootstrap
  noise floors (DNI 15.6 W/m², IDW 16.5; DHI 2.48 W/m², IDW 3.11). The model
  adds noise on top of noise regardless of feature set.
- **D-curve caveat**: only D=0 is a clean extrapolation test. The scattered-fold
  buffer depletes training data globally: 1232 blocks at D=0 → 166 at D=25
  (86% gone). D>0 lift numbers are partly a "less training data" effect, not
  pure distance. Report D=0 as the headline; D>0 shows model degrades less than
  IDW under depletion (directional signal) but is not a clean distance curve.
- **D=50 km**: train collapsed to 85 rows (2 blocks) — result meaningless.
  Contiguous fold regions (each fold a single geographic rectangle) would fix
  the D-curve but are a later improvement.

## The evaluation problem

The current notebook uses a temporal hold-out + per-row R². Three mismatches with
the product:

1. It holds out *time*, not *place* — but the product generalises to new *places*.
2. It scores per row — penalising interannual/daily noise a climatology should not
   try to predict.
3. It scores raw weather variables, not a quantity tied to the product goal.

Two empirical constraints (measured 2026-05-29) shape the fix:

- **Grid is a UK-wide continuous field** — 20,638 cells, 49.97–58.66°N /
  −8.17–1.76°E, 90% with an immediate 0.04° eastern neighbour. So hold out spatial
  **blocks**, not sites. (Leave-one-site-out is a PV-side concept; not applicable.)
- **Reference climatology is temporally sparse** — 92% of (cell, month-of-year)
  combinations have only one year of data. A *per-cell* climatology is a noisy
  single-year sample, so the reference must be aggregated to **block** level and
  given a bootstrap noise floor. Interannual variance is spatially correlated, so
  block aggregation removes per-cell measurement noise but **not** interannual
  sampling error — only more years fix that (DNI worst).

## Evaluation design — climatology / spatially-blocked hold-out

**Question it answers:** at extrapolation distance *D* from any training
observation, does the learned model (with its physical features) predict a
location's expected climatology better than inverse-distance interpolation of
known sites?

1. **Split — spatially-blocked hold-out.**
   - Blocks = uk-geo **coarse tiles** (the `(row, col)` lattice,
     `cell_width × resolution` = 0.16° ≈ 18 km × 10 km, ~16 cells) — **not** the
     merged supercells from `generate_cells`, which remote-merge spatially disjoint
     fragments to balance extraction batches (see `plot_packed_cell`); a hold-out
     block must be contiguous so removing it creates a genuine spatial gap. Mirror
     uk-geo's index math (`lat_idx // w`, `lon_idx // w`) in `common_cross_grid.py`
     — no need to import the 3.12 submodule package. Block size configurable
     (aggregate N×N tiles to pool a less-noisy reference).
   - Assign whole blocks to train/test; k-fold for coverage. A buffer ring excludes
     training cells within distance *D* of each test block ⇒ *D* is the
     extrapolation-distance knob (sweep e.g. 0 / 10 / 25 / 50 km).
   - Not random-cell hold-out: ~4 km neighbours are trivially interpolated (the
     same autocorrelation leak we rejected for the random temporal split).

2. **Target — block-level climatology.** Aggregate observations to
   (block, month-of-year) means (and annual). The model is deterministic in
   `(lat, lon, elev, day)`, so its climatology is just its prediction swept over the
   year, aggregated to the same blocks. Attach bootstrap-over-years error bars to
   the observed reference.

3. **Metric — graded only on the model's own outputs (temperature, DNI, DHI).**
   - Per-variable block-climatology **bias** and RMSE, monthly and annual.
   - **No POA and no yield in this evaluation.** Both require panel tilt/azimuth —
     site-specific parameters that belong to the PV model / prediction API, not the
     weather model. Excluding them preserves the model separation; the "does this
     error matter for energy?" question is answered end-to-end later, with real site
     parameters in hand.
   - Lead with **bias**: systematic regional bias destroys a long-run estimate,
     whereas zero-mean scatter partly averages out over a portfolio and asset life.
   - Optional panel-free nuance: weight errors by solar geometry / horizontal
     insolation (a function of lat/lon/day only, no tilt) so sunny-period errors
     count more. Default off, to keep the separation crisp.

4. **Baseline — lift over IDW.** Primary baseline is inverse-distance / nearest-
   neighbour interpolation of *training observations'* climatology ("look at the
   nearest known sites"). Headline result = **lift-over-IDW as a function of D**: at
   small *D*, IDW wins and the eval is meaningless; the question is the distance at
   which the model + physical features start adding value. That curve is how a
   feature earns (or fails to earn) its keep. Floor baseline: global seasonal mean.

5. **Feature screening (the point of all this).** For each feature set
   (base / +elevation / +coastal), report per-variable block-climatology bias &
   RMSE plus lift-over-IDW-vs-D. A feature counts as helping when it lowers
   held-out climatology error / raises lift-over-IDW — not when it merely raises
   per-row temporal R².

## Plumbing (`common_cross_grid.py`)

- `spatial_block_split(df, block_deg, buffer_km, fold)`
- `block_climatology(df, by=['block', 'month_of_year'])`
- `idw_baseline(train_climatology, test_blocks)`
- evaluation module: layered per-variable bias/RMSE + bootstrap floor.
- Loader, scaling, and training loop reused unchanged.

## MVP vs north-star

- **MVP (now):** blocked hold-out + per-variable block-climatology bias/RMSE + IDW
  baseline + bootstrap floor. Sufficient to screen elevation and coastal features
  immediately — it does not block the feature work.
- **Later:** accumulate more years to firm up the climatology (especially DNI); a
  finer coastline if estuary-scale detail is ever needed. Downstream energy-yield
  validation belongs to the end-to-end PV pipeline, not to this model.

## Honest limits

- ~3.7-year record + spatially-correlated interannual variance ⇒ an irreducible
  reference floor (DNI worst). Bootstrap it; do not oversell precision.
- Per-cell coverage is intermittent (the R2 low-discrepancy extraction sampler
  rotates through grid subsets per run) ⇒ block aggregation is mandatory.
