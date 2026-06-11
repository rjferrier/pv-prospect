# TODO

## Next

The **website** fronts both serving surfaces (`User → PredictionApi` /
`User → ValidationApi`). Served from the existing app — no separate frontend, no
build step. Build order **W0 → W2 → W1**. W2 (validation) is unblocked and the
only public surface for now; W1 (prediction) is buildable now but its **public
launch waits on the PV yield-overestimate fix** (decision: fix-first — the
`pv-train-on-served-poa` task below).

- [ ] [Website: map prediction + known-site validation UI](briefs/website.md)
- [ ] [Train the PV model on the served (24 h-mean) POA basis](briefs/pv-train-on-served-poa.md) — removes the dominant ~83% of the measured +100% `/predict` yield overestimate (Gate A/B); **W1 public-launch prerequisite**

## Later

- [ ] [Offline cross-site (LOSO) generalisation eval for the PV model](briefs/cross-site-generalization-eval.md) — honest prospect-site accuracy; the Prediction API's headline claim currently ships unvalidated
- [ ] [Fix the PV yield overestimate: related riders & cleanup](briefs/pv-yield-overestimate.md) — weather vintage/grid alignment (~8% rider), trainer-validation gate, low-POA recalibration alternative, and caveat cleanup; the primary PV-model fix is split out to **Next** (`pv-train-on-served-poa`)
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
