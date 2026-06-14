# Offline cross-site (LOSO) generalisation eval for the PV model

Parked, eyes-open. **Not** part of the Validation API — a separate model-quality
diagnostic.

> **Sequenced with `briefs/pv-age-feature.md`** (Next). **Reframed 2026-06-14 (that plan's
> §3.8):** the embedding question is resolved (per-site level is **not** modelled for W1 —
> it is exposed as an uncertainty band, `briefs/prospect-uncertainty-band.md`). So LOSO's
> primary purpose is now to **calibrate that band** — its out-of-sample per-site error *is*
> the honest prospect spread (wider than the in-sample ±15 %). It still measures cross-site
> transfer; its embedding trip-wire is **demoted** to the known-site `/validate` (W2) path.
>
> **DONE 2026-06-14 (plan `pv-age-feature.md` §Phase 2 outcome).** LOSO implemented
> (`pv-prospect-model/.../training/loso.py`, `EvalReport.loso`, `loso-pv` CLI, defensive
> trainer wiring). Result: prospect band **1σ ≈ ±17 %** out-of-sample (range −34 %…+28 %),
> mean level ≈ 1.00, pooled power R² 0.839 vs the **bounded prior's own** within-site 0.844
> (small cross-site penalty; the old free-age incumbent 0.871 is the wrong baseline). This brief is **closed**;
> it is **deleted at the `pv-age-feature` Phase 3 finalisation** (its content folds into
> that task's report + the model README's LOSO section).

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
surface via `/version` and/or monitoring. The **per-site out-of-sample error is the
prospect uncertainty band's calibration** (feeds `briefs/prospect-uncertainty-band.md`).
Compare the LOSO aggregate to the within-site number — a large gap no longer routes to a
W1 embedding (resolved as won't-build, plan §3.8); it would only motivate the embedding for
the known-site `/validate` (W2) path, or argue for more / more-representative sites.

## Scope

`pv-prospect-model` (training/eval: LOSO loop + `eval_report` schema),
`pv-prospect-model-trainer` (compute + record; offline, n=10 → 10 trainings per run,
acceptable for a weekly job). Out of scope: serving it through the app, and the
embedding itself (adopted only if the trip-wire fires).
