# Brief: PV Prospect website

## What

A public website fronting the platform's two serving surfaces:

- **Prediction / speculation** — pick a UK point on a map, supply panel
  parameters, see a speculative annual energy-yield estimate (`POST /predict`).
- **Validation** — pick one of the 10 known PV sites, see how the deployed PV
  model tracks real output over the recent 90-day window (Validation API).

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

- **W2** depends on the Validation API (TODO `## Next` tasks 1–4:
  `validation-window-producer`, `validation-inmemory-store`, `validation-api`,
  `validation-serving-docs`).
- **W1** is otherwise unblocked (`/predict` exists), but its **public launch is
  gated** on the ~30% vintage-bias decision — show an unmissable caveat or fix it
  first (brief `weather-pv-vintage-alignment`).

Plan: `plans/website.md`.
