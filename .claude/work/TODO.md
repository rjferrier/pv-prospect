# TODO

## Next

Build the Validation API + In-Memory Data roadmap nodes (`doc/architecture.puml`).
The Validation API shows, per known PV site, how the deployed PV model tracks real
recent output (within-site, last 90 days) — the easy case. It does **not** test
generalisation to unseen prospect sites; that is parked separately (see Later).
Ordered by dependency:

- [ ] [2 — App in-memory validation store: startup load + reload](briefs/validation-inmemory-store.md)
- [ ] [3 — Validation API endpoint (`/validate`)](briefs/validation-api.md)
- [ ] [4 — Validation serving wiring, diagram, finalisation](briefs/validation-serving-docs.md)

The **website** fronts both serving surfaces (`User → PredictionApi` /
`User → ValidationApi`). Served from the existing app — no separate frontend, no
build step. W1 (prediction) is unblocked; W2 (validation) follows the Validation
API tasks above; W1's public launch is gated on the vintage-bias decision.

- [ ] [Website: map prediction + known-site validation UI](briefs/website.md)

## Later

- [ ] [Offline cross-site (LOSO) generalisation eval for the PV model](briefs/cross-site-generalization-eval.md) — honest prospect-site accuracy; the Prediction API's headline claim currently ships unvalidated
- [ ] [Align OpenMeteo vintage between prepared-weather and prepared-PV corpora](briefs/weather-pv-vintage-alignment.md) — root cause of ~30% yield underestimate in Prediction API
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
