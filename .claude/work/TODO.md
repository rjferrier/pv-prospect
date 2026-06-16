# TODO

## Next

- [ ] [Show the capacity-factor map on the website (via the staging bucket)](briefs/capacity-factor-map-website.md) — **code/docs done & verified; PNG uploaded** to `gs://pv-prospect-staging/assets/`. **Remaining = deploy** (rebuild image + `terraform apply` so the route and `ASSETS_DIR` ship; panel stays hidden in prod until then). Finalise (delete brief) once live.
- [ ] [Replace Open-Meteo elevation with a cheaper API](briefs/elevation-api.md) — Open-Meteo is used to query elevation for a lat/lon; a free or cheaper dedicated elevation API would reduce costs

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
