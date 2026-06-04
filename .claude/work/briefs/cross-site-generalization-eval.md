# Offline cross-site (LOSO) generalisation eval for the PV model

Parked, eyes-open. **Not** part of the Validation API — a separate model-quality
diagnostic.

## Why

The Prediction API's headline feature is speculative yield for a site we have never
seen. The single cross-site `CapacityFactorNet` is trained on all 10 sites with no
site-identity feature, on the bet that capacity-factor non-dimensionalisation +
`age_years`/`age_known` make it generalise to an unseen prospect. That bet is
currently **unvalidated**: the trainer's `eval_report` uses a temporal hold-out
(within-site), and the Validation API (when built) validates only *known* sites.
Neither estimates accuracy on a genuinely unseen site — so the product's central
claim ships on an unmeasured assumption.

This is the explicitly-anticipated risk in the locked-in PV-trainer decisions (#10):
"if validation residuals show site-shaped structure later we can revisit with a small
embedding." This task builds the surface that would reveal that structure.

## What

Add an offline leave-one-site-out (LOSO) eval to the PV trainer, alongside the
existing temporal hold-out — mirroring the weather model's spatially-blocked eval
(`neural-network-cross-grid-weather-spatial-eval`). For each site: train on the other
nine (reuse `train_pv`'s `system_ids` exclusion), predict the held-out site, score
power-space R²/MAPE. Aggregate + per-site into a new `eval_report.loso` section;
surface via `/version` and/or monitoring. Compare the LOSO aggregate to the
within-site number — a large gap is the trip-wire for adding a site embedding (with an
"unknown site" default vector for prospects at predict time).

## Scope

`pv-prospect-model` (training/eval: LOSO loop + `eval_report` schema),
`pv-prospect-model-trainer` (compute + record; offline, n=10 → 10 trainings per run,
acceptable for a weekly job). Out of scope: serving it through the app, and the
embedding itself (adopted only if the trip-wire fires).
