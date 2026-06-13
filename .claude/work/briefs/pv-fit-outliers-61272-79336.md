# Investigate the 61272 / 79336 PV model-fit outliers

> Split out from the served-POA retrain (`reports/pv-train-on-served-poa.md` §5–6) and
> deliberately excluded from `briefs/pv-age-feature.md`: these two sites are **not**
> explained by the `age_years` convention.

## Why

In the Gate A measurement after the served-POA re-base, two sites stand out:

- **61272** is a genuine model-*fit* outlier — at its true POA it over-predicts real
  generation (Gate B B/A ≈ 1.89), the worst of the 10 sites. It has **no install
  date**, so the `age_years` fix (which collapses the other 8 sites to ≈ 1.0) does not
  explain it.
- **79336** also lacks an install date and is worth checking in the same pass.

Because the dominant residual elsewhere is the age convention (handled in
`pv-age-feature`), these two need a **separate, bounded look** at what the model is
getting wrong for them specifically — site features (orientation / tilt / shading in
`pv_sites.csv`), corpus quality (61272 has known site-outage days), or a genuine
CF-curve mis-fit.

## Scope

Diagnostic only: per-site error decomposition for 61272 / 79336 against the corpus and
the chain (reuse `scripts/measure_yield.py` + the corpus). Out of scope: any global
model change (that rides with `pv-age-feature`). Outcome may be a data fix, a per-site
note, or a feature to fold into the age task.
