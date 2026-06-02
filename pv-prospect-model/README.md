# pv-prospect-model

ML model training for `pv-prospect`. Consumes versioned prepared CSV data and
produces trained model artifacts.

## Package Structure

```
pv_prospect/model/
├── domain.py          # FeatureSpec, TrainingConfig, EvalReport, ModelArtifact
├── features/
│   └── pv.py          # Load prepared per-site CSVs, apply censoring, augment features
├── splits.py          # Temporal hold-out split and StandardScaler fitting
├── evaluation.py      # Metrics in capacity-factor space and clamped-power space
├── nets/
│   └── pv.py          # CapacityFactorNet (4-layer feed-forward network)
├── training/
│   ├── loop.py        # Shared training loop with early stopping
│   └── pv.py          # train_pv(): full pipeline from data → ModelArtifact
├── persistence.py     # save_artifact() / load_artifact()
└── entrypoint.py      # CLI: train-pv, train-weather (stub)
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
4. **Augment.** Add `day_of_year` (continuous), `age_years`, `age_known`
   (site-attribute). Sites with missing `installation_date` get `age_known = 0`
   and `age_years` imputed from the global median of known sites.
5. **Split.** Temporal hold-out at the 80th percentile of the date range.
   Train on the earlier window; test on the later window. Random splits are not
   used — PV-weather rows are autocorrelated day-to-day, so a random split
   inflates reported R².
6. **Scale.** `StandardScaler` fitted on train continuous features only;
   binary features (`age_known`) are not scaled.
7. **Train.** `CapacityFactorNet` (4 dense layers, Dropout(0.2), no output
   activation). MSE loss on `capacity_factor`, Adam optimizer. Early stopping
   on a temporal val slice (last 10% of train). 100 epochs max, patience 10.
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

# Train with defaults (all sites, standard config)
poetry run python -m pv_prospect.model.entrypoint train-pv \
    --data-root /path/to/prepared \
    --pv-sites-csv /path/to/pv_sites.csv \
    --output-dir /path/to/artifact

# Train on specific sites with a custom cutoff
poetry run python -m pv_prospect.model.entrypoint train-pv \
    --data-root /path/to/prepared \
    --pv-sites-csv /path/to/pv_sites.csv \
    --output-dir /path/to/artifact \
    --system-ids 25724,56874,61272 \
    --cutoff-quantile 0.75 \
    --num-epochs 200
```

## Artifact Format

```
<output_dir>/
├── model.pt              # torch.nn.Module state dict
├── feature_spec.json     # feature order, target column, scaler mean/scale
├── training_config.json  # all hyperparameters used
└── eval_report.json      # metrics in both evaluation spaces
```

Load with:

```python
from pv_prospect.model.persistence import load_artifact

artifact = load_artifact('/path/to/artifact')
# artifact.model        → CapacityFactorNet (eval mode)
# artifact.scaler       → fitted StandardScaler
# artifact.feature_spec → FeatureSpec (column order, scaler params)
# artifact.eval_report  → EvalReport (R², RMSE, MAE per space and per site)
```

## Design Notes

- **One model across all sites.** Per-site differences are absorbed by
  `age_years` / `age_known` features and the physics-grounded `capacity_factor`
  target. Site identity is not a feature because it cannot be populated for a
  prospect site at inference time.
- **Inverter clamping at inference.** The model predicts `f`; the product
  delivers `min(f × panel_capacity, inverter_capacity)`.
- **Weather features (POA, temperature) are already joined** in the prepared
  CSVs. Cloud cover, raw DNI/DHI, humidity, and wind were swept and rejected
  in data-exploration — POA absorbs the atmospheric signal.
- **Tobit / censored regression not used.** Biased days are simply dropped
  (see step 2). The 1% margin was verified against site 82517 (inverter cap
  sharp at 6,000 W, max observed 6,004 W).
