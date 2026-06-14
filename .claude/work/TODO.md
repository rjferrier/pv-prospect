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
- [ ] [Validate & fix the PV `age_years` feature: degradation law vs. site fixed-effect](briefs/pv-age-feature.md) — **W1 public-launch gate**. Phase 0–1 done (bounded prior, `r`=0.007, committed). **Resolution (plan §3.8):** promote the bounded prior by hand (it misses the gate by ~2.6 pp on per-site *level*); **no embedding for W1**; expose an uncertainty band instead. Remaining: Phase 2 (LOSO) + Phase 3 (promote, caveats, docs, report)
- [ ] [Expose a prospect yield uncertainty band](briefs/prospect-uncertainty-band.md) — W1 product work from the `pv-age-feature` §3.8 resolution: `/predict` returns expected ± margin (≈ ±15 % 1σ, a floor) since per-site level is unmodellable for a prospect; calibrated by the LOSO eval
- [ ] [Offline cross-site (LOSO) generalisation eval for the PV model](briefs/cross-site-generalization-eval.md) — validator for `pv-age-feature`; **now also calibrates the prospect uncertainty band** (its out-of-sample per-site error is the honest spread). Embedding trip-wire demoted to the W2 `/validate` path (plan §3.8)

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
