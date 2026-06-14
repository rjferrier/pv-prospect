# TODO

## Next

The **website** fronts both serving surfaces (`User → PredictionApi` /
`User → ValidationApi`). Served from the existing app — no separate frontend, no
build step. Build order **W0 → W2 → W1**. W0, W2 (validation) and **W1
(prediction) are now all built** — the prediction section renders expected yield
with its uncertainty band (`prospect-uncertainty-band`, done below). W1's **model
gate is resolved** (the PV `age_years` fix, `reports/pv-age-feature.md`). W1
*public launch* now depends only on the **operational/outward-facing** steps: the
**promotion deploy** (by hand — see the age-feature task) and the **auth flip**
(`allow_unauthenticated`). The upstream corpus re-base (`pv-train-on-served-poa`)
is delivered; it halved the overestimate and re-attributed the residual to the age
feature, where the W1 gate sat.

- [ ] [Website: map prediction + known-site validation UI](briefs/website.md) — W0/W2/W1 all **built** (validation + prediction sections, tests green). Remaining: public-launch ops (promotion deploy, auth flip) and finalisation (ungrey `doc/architecture.puml`, README website section).
- [x] Validate & fix the PV `age_years` feature: degradation law vs. site fixed-effect — **W1 public-launch gate RESOLVED**. All phases done; bounded-prior model promoted to production (2026-06-14); API caveats cleaned; report at `reports/pv-age-feature.md`.
- [x] Expose a prospect yield uncertainty band — **DONE**: `/predict` returns an `uncertainty` object (`sigma_frac` 0.17 + `annual_kwh_low/high`, the ±17 % 1σ LOSO floor); the website Prediction (W1) section renders it; `openapi.yaml` regenerated; documented in `pv-prospect-app/README.md`. Brief deleted (scope fully delivered; calibration recorded in `reports/pv-age-feature.md` §6).
- [x] [Offline cross-site (LOSO) generalisation eval for the PV model](briefs/cross-site-generalization-eval.md) — **DONE (pv-age-feature Phase 2)**: LOSO loop + `EvalReport.loso` + `loso-pv` CLI + defensive trainer wiring. Calibrated the band (±17 % 1σ), pooled power R² 0.839 vs the bounded prior's own within-site 0.844 (small cross-site penalty). Brief closed; **deleted at pv-age-feature finalisation** (gated on the promotion deploy — report §8)

## Later

- [ ] [Gate A confirmation: age=0 prospect yield ratio](plans/pv-age-feature.md) — deferred one-shot check (no brief; recipe in plan §3.7–3.8): the bounded-prior model's served-age yield vs true generation (expect ≈ 1.15–1.20 vs the 1.515 incumbent). Needs staging-bucket raw-PVOutput actuals via `measure_yield.py --actuals-gcs-prefix`; optional confirmation, feeds the `pv-age-feature` report
- [ ] [Fix the PV yield overestimate: related riders & cleanup](briefs/pv-yield-overestimate.md) — surviving riders only: weather vintage/grid alignment (~8% rider) and the trainer-validation gate. The residual-closing lever moved to **Next** (`pv-age-feature`); the serve-side POA recalibration/aggregation route is **rejected** (chases ~5%, masks the real cause — report §6)
- [ ] [Investigate the 61272 / 79336 PV model-fit outliers](briefs/pv-fit-outliers-61272-79336.md) — 61272 over-predicts at true POA (Gate B B/A 1.89) and both lack install dates, so the `age_years` fix won't explain them (excluded from `pv-age-feature`); a bounded per-site diagnostic
- [ ] [Version raw extracted data alongside prepared data in the weekly versioning run](briefs/version-raw-data.md)
- [ ] [Restructure `tracking/` prefix to group files by date](briefs/tracking-restructure.md)
- [ ] [Investigate the data-versioner hang-on-exit](briefs/versioner-hang.md)
- [ ] [Clarify end-date semantics in backfill cursors and manifests](briefs/end-date-semantics.md)
- [ ] [Decommission hand-rolled CSV write logs under `tracking/`](briefs/csv-write-logs.md)

## Someday/Maybe

- [ ] [Rename prepare→featurise / partition→feature throughout](briefs/featurise-rename.md)
- [ ] [Generalise Open-Meteo outage recovery into a reusable runbook/script](briefs/outage-recovery.md)
- [ ] [Extraction failure carry-over registry](briefs/failure-carry-over.md)
- [ ] [Reduce per-task env footprint in phased manifests](briefs/manifest-env-footprint.md)
- [ ] [Consolidate operational scripts into util/](briefs/util-scripts-consolidation.md)
