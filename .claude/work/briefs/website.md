# Brief: PV Prospect website

## What

A public website fronting the platform's two serving surfaces:

- **Prediction / speculation** — pick a UK point on a map, supply panel
  parameters, see a speculative annual energy-yield estimate (`POST /predict`).
- **Validation** — pick one of the known PV sites (listed dynamically by the
  Validation API), see how the deployed PV model tracks real output over the
  recent ~90-day window (Validation API).

These are the user-facing `User → PredictionApi` / `User → ValidationApi` flows in
`doc/architecture.puml`.

## Why

The serving infrastructure exists but has no human-facing surface. This is the
product's front door — and a portfolio demonstration of the model.

## Approach (settled)

Serve from the existing `pv-prospect-app` FastAPI service: no separate frontend,
no CORS, no JS build step. No-build client (vanilla JS / HTMX + CDN libraries:
Leaflet + OpenStreetMap, uPlot/Chart.js). Demo / portfolio ambition. See the plan
for the full design and phasing (W0 substrate → W1 prediction → W2 validation).

## Blockers / dependencies

- **W2 is now unblocked** — the Validation API shipped (`/validate/sites`,
  `/validate/{system_id}`) with a window producer (one-time seed +
  daily `maintain_validation_window`). The plan recommends **leading with W2**:
  it feeds the PV model the in-distribution corpus POA (and bypasses the weather
  model), so it is insulated from the yield bias and is the strongest credibility
  artifact.
- **W1** is buildable now (`/predict` exists), but its **public launch is gated**
  on shipping the PV yield-overestimate fix (decision: fix-first —
  `pv-train-on-served-poa`).
  W1 may be exercised privately (IAM auth) before then; W2 is the only public
  surface until the fix lands.
- **Public launch (either section) forces an auth flip**: the service is private
  today (`allow_unauthenticated = false`); serving the site from it makes
  `/predict` world-callable. `max_instances=2` caps the cost.

Plan: `plans/website.md`.
