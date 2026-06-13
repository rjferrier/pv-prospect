# TODO

## Next

The **website** fronts both serving surfaces (`User → PredictionApi` /
`User → ValidationApi`). Served from the existing app — no separate frontend, no
build step. Build order **W0 → W2 → W1**. W2 (validation) is unblocked and the
only public surface for now; W1 (prediction) is buildable now but its **public
launch waits on the PV `age_years` feature fix** (decision: fix-first — the
`pv-age-feature` task below). The upstream corpus re-base (`pv-train-on-served-poa`)
is delivered; it halved the overestimate and re-attributed the residual to the age
feature, where the W1 gate now sits.

- [ ] [Website: map prediction + known-site validation UI](briefs/website.md)
- [ ] [Validate & fix the PV `age_years` feature: degradation law vs. site fixed-effect](briefs/pv-age-feature.md) — closes the residual +51% `/predict` overestimate (report §5–6); centres on a constrained degradation prior (Option B); **W1 public-launch gate**
- [ ] [Offline cross-site (LOSO) generalisation eval for the PV model](briefs/cross-site-generalization-eval.md) — the validator for `pv-age-feature`: tests whether the age slope transfers to an unseen site or is a memorised per-site intercept; the Prediction API's headline prospect claim currently ships unvalidated

## Later

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
