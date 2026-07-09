# TODO

## Next

- [ ] [Scope the ledger-consolidation listing to stop an unbounded GCS operations cost](briefs/ledger-scan-cost.md) — **operational bleed**: `WorkflowOrchestrator.completed_task_hashes()` calls `list_consolidated_ledgers()`, an unscoped recursive walk of the entire `tracking/ledger/` prefix on every workflow run (daily and backfill alike). Measured `ListObjects` at ~9,955/day and climbing accounts for ~8-12x the bucket's at-rest storage cost — a separate GCS prefix from `data/raw`, so deleting raw data doesn't touch it, and pausing backfills alone won't fully fix it since the daily pipelines hit the same code path
- [ ] [Floor the backfill march at 2016 and densify the weather grid](briefs/openmeteo-2016-floor.md) — **operational bleed** (`reports/openmeteo-2016-floor.md`): the weather-grid backfill has failed ~8.5k pre-2016 weather fetches/day since 2026-06-19 (Open-Meteo's fixed 2016-01-01 floor vs floor-less cursors). Add a shared `MIN_ARCHIVE_DATE`: halt PV-sites at the floor (R1a — deadline ~late Sep 2026, before it orphans non-refetchable raw PV) and give the weather grid a 2-D densifying cursor with a `target_density` cap (R1b, chosen). Corpus is safe by construction — this is efficiency + the deferred spatial-densification, not a repair
- [ ] [Replace Open-Meteo elevation with a cheaper API](briefs/elevation-api.md) — Open-Meteo is used to query elevation for a lat/lon; a free or cheaper dedicated elevation API would reduce costs

## Later

- [ ] [Gate A confirmation: age=0 prospect yield ratio](plans/pv-age-feature.md) — deferred one-shot check (no brief; recipe in plan §3.7–3.8): the bounded-prior model's served-age yield vs true generation (expect ≈ 1.15–1.20 vs the 1.515 incumbent). Needs staging-bucket raw-PVOutput actuals via `measure_yield.py --actuals-gcs-prefix`; optional confirmation, feeds the `pv-age-feature` report
- [ ] [Fix the PV yield overestimate: related riders & cleanup](briefs/pv-yield-overestimate.md) — surviving riders only: weather vintage/grid alignment (~8% rider) and the trainer-validation gate. The residual-closing lever moved to **Next** (`pv-age-feature`); the serve-side POA recalibration/aggregation route is **rejected** (chases ~5%, masks the real cause — report §6)
- [ ] [Investigate the 61272 / 79336 PV model-fit outliers](briefs/pv-fit-outliers-61272-79336.md) — 61272 over-predicts at true POA (Gate B B/A 1.89) and both lack install dates, so the `age_years` fix won't explain them (excluded from `pv-age-feature`); a bounded per-site diagnostic
- [ ] [Data-derived planning for the transformation backfills](briefs/data-derived-transform-planning.md) — **planned** (`plans/data-derived-transform-planning.md`): plan from *raw-present − prepared-present* (staged `prepared/` ∪ instance-repo corpus — the plan corrects the brief's `prepared-batches/` surface) instead of extraction ledger + consumed-through marker; markers deleted, transform ledger demoted to audit; absorbs the transform half of `outage-recovery` (`reports/data-pipeline-retrospective.md` rec 1). Safety precondition (raw durable in `pv-prospect-raw`) now satisfied — see `reports/archive-raw-data.md`; sequence before `tracking-restructure` / `end-date-semantics`
- [ ] [Restructure `tracking/` prefix to group files by date](briefs/tracking-restructure.md) — sequence after `data-derived-transform-planning`: that task deletes the transform markers and strips the ledger's recovery duties, shrinking this to pure legibility/archival
- [ ] [Investigate the data-versioner hang-on-exit](briefs/versioner-hang.md)
- [ ] [Clarify end-date semantics in backfill cursors and manifests](briefs/end-date-semantics.md) — sequence after `data-derived-transform-planning`: extraction cursors only remain to migrate
- [ ] [Decommission hand-rolled CSV write logs under `tracking/`](briefs/csv-write-logs.md) — independent; proceed regardless (`reports/data-pipeline-retrospective.md` rec 5)

## Someday/Maybe

- [ ] [Rename prepare→featurise / partition→feature throughout](briefs/featurise-rename.md)
- [ ] [Generalise Open-Meteo outage recovery into a reusable runbook/script](briefs/outage-recovery.md) — re-scope pending `data-derived-transform-planning`: the transform half (superseded-ledgers dance) folds into that task; the remainder is an extract-side cursor-rewind runbook
- [ ] [Extraction failure carry-over registry](briefs/failure-carry-over.md) — extraction-only by design (`reports/data-pipeline-retrospective.md` §5): the transform analogue disappears under data-derived planning
- [ ] [Reduce per-task env footprint in phased manifests](briefs/manifest-env-footprint.md) — likely stale: transform backfills no longer write manifests; verify daily-transform manifest sizes, then delete or re-scope (`reports/data-pipeline-retrospective.md` rec 3)
- [ ] [Consolidate operational scripts into util/](briefs/util-scripts-consolidation.md)