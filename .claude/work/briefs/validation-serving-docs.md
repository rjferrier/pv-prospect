# Validation API — serving wiring, diagram, finalisation

Fourth task of the Validation API roadmap. Depends on
[validation-window-producer.md](validation-window-producer.md),
[validation-inmemory-store.md](validation-inmemory-store.md), and
[validation-api.md](validation-api.md).

## Why

The prior tasks add the producer, store, and endpoint to the codebase; this makes
them first-class in the deployed Service, observable, and documented — and promotes
the greyed roadmap nodes in `doc/architecture.puml` to built.

## What

- **Serving wiring.** `/validate` ships in the *existing* `pv-prospect-app` Service
  (no new `cloud_run_service`). `/healthz` goes green only once the model store
  **and** the window are loaded; `/version` reports the window's `updated_at`. Confirm
  the Service memory limit holds the window (90 days × ~10 sites is small, but check).

- **Terraform.** The app SA gains object read on the staging bucket's
  `serving/validation-window/` prefix (the producer needs none — the pipeline SA
  already has `objectAdmin` on staging). Add `validation_window_storage` /
  `validation_window_dir` to the transform + app configs.

- **Diagram (label mapping, settled).** In `architecture.puml`: keep `A1: load
  promoted artifacts at startup`; add a `Validation Window` serving-database node with
  `P4: maintain rolling validation window` (`DataTransformer → ValidationWindow`),
  `A2: load validation window at startup` and `A3: reload on generation change`
  (`ValidationWindow → InMemoryData`); **remove** the stale `InMemoryData ←
  DataVersioner` arrow; ungrey `ValidationApi` + `InMemoryData` and their flows.

- **Monitoring (optional, cheap).** `/validate` yields a served accuracy number.
  Optionally emit rolling MAPE/R² to Cloud Monitoring and alert if it drifts from the
  offline `eval_report` value (catches a serving / data-loading regression). Defer if
  it complicates the demo.

- **Smoke test (acceptance gate).** Validate a known site over the window and assert
  predicted-vs-actual daily kWh within a documented tolerance — the single most
  valuable integration test for the feature.

- **Docs (finalisation, per `documenting.md`).** README flow-key row (**Val — model
  validation against actuals**); document `/validate` in `pv-prospect-app/README.md`
  with a `curl` example; note the window serving prefix + `validation_window_days`
  wherever the model store is documented.

## Scope

`pv-prospect-app` (startup / health / version, optional metrics), Terraform (app-SA
read grant; optional `monitoring.tf` alert; Service memory if needed), top-level
`README.md` + `doc/architecture.puml` + `pv-prospect-app/README.md`. Finalise per the
lifecycle: integrate into permanent docs and delete all four validation briefs (and
the producer plan) together.
