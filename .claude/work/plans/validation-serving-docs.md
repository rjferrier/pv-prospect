# Plan — Validation serving wiring, diagram, finalisation

Task 4 of the Validation API roadmap. Brief:
[validation-serving-docs.md](../briefs/validation-serving-docs.md). Depends on Tasks
1–3 (producer, store, endpoint). This task makes `/validate` first-class in the
deployed Service, observable, documented, and promotes the greyed roadmap nodes — then
finalises and deletes the whole validation brief/plan set.

## Goal

Ship `/validate` in the *existing* `pv-prospect-app` Cloud Run Service (no new
service); make window readiness observable; grant the app SA read on the served prefix;
update the architecture diagram and docs; add the acceptance smoke test; finalise.

## Design decisions

One genuine fork (#1, `/healthz` gating) is surfaced for confirmation because it
contradicts the approved Task-2 design. The rest follow the brief / rules.

1. **`/healthz` stays model-only — do NOT gate it on the window. FORK.** The brief says
   "healthz green only once model **and** window loaded." But Task 2 made the window
   load deliberately *non-fatal* so a window failure can't take down `/predict`. If
   `/healthz` is the Cloud Run readiness/liveness probe, gating it on the window
   reintroduces exactly that failure mode — a window-load hiccup deroutes or
   crash-loops the instance and `/predict` goes down with it. → **Recommend: `/healthz`
   gates on the model store only.** Surface window readiness elsewhere:
   - `/version` gains `window_updated_at: str | None` and `window_loaded: bool`.
   - `/validate/*` already `503`s until the window loads (Task 3), and the cache
     self-heals on the next freshness check.
   This *refines* the brief in light of the approved Task-2 decision; if the user
   genuinely wants the window to be a hard readiness gate in prod, flip it here.

2. **`/version` reports the window.** Add `window_updated_at` (from the loaded
   manifest, `None` if unloaded) and `window_loaded` to `VersionResponse`.

3. **Terraform (app-SA read grant + env).** The app SA gains object read scoped to the
   staging bucket's `data/served/validation-window/` prefix (the producer/pipeline SA
   already has `objectAdmin` — no grant needed there). Wire the Service env:
   `VALIDATION_WINDOW_DIR=gs://<staging-bucket>/data/served/validation-window` and
   `RESOURCES_DIR=gs://<staging-bucket>/staging/resources` (matching Task-2 config
   keys). Add `validation_window_storage` / `validation_window_dir` to the transform
   config alongside the producer. Confirm the Service memory limit holds the window
   (90 days × ~10 sites is tiny — sanity-check, don't over-provision).

4. **Diagram (`doc/architecture.puml`) — label mapping settled in the brief.** Keep
   `A1: load promoted artifacts at startup`. Add a `Validation Window` serving-database
   node with `P4: maintain rolling validation window` (`DataTransformer →
   ValidationWindow`), `A2: load validation window at startup` and `A3: reload on
   change` (`ValidationWindow → InMemoryData`). **Remove** the stale `InMemoryData ←
   DataVersioner` arrow. Ungrey `ValidationApi` + `InMemoryData` and their flows.
   (A3 label: "reload on change" — the store checks manifest `updated_at`, not GCS
   generation, per the Task-2 decision; keep the diagram wording generic.)

5. **Smoke test (acceptance gate).** The single most valuable test for the feature:
   validate a known site over its window and assert predicted-vs-actual daily kWh
   within a documented tolerance. Needs a real promoted artifact + a small window
   fixture. Place under an integration/acceptance path (the brief explicitly calls for
   it — this is the instructed exception to "don't auto-create higher-level tests").
   Decide on a fixture: a checked-in tiny window CSV + a small real/stub artifact, or
   gate on a locally-pulled store. Recommend a tiny committed window fixture for a
   couple of sites and the real promoted artifact loaded from a local store dir, skipped
   if absent (`pytest.mark.skipif`).

6. **Monitoring (optional, cheap — defer by default).** `/validate` yields a served
   accuracy number; optionally emit rolling MAPE/R² to Cloud Monitoring and alert on
   drift from the offline `eval_report`. Defer unless it's trivial; note as a
   follow-up so it isn't silently dropped.

## Changes
- `pv_prospect/app/main.py`: extend `VersionResponse` (+ `window_updated_at`,
  `window_loaded`); `/healthz` unchanged (model-only); `/version` reads the cache.
- `terraform/`: app-SA IAM member on the served prefix; Service env vars; transform
  config keys; (optional) `monitoring.tf` alert.
- `doc/architecture.puml`: node + arrows per #4.
- `pv-prospect-app/resources/config-default.yaml`: already carries the Task-2 dev
  defaults; prod values come from env here.
- Smoke test module + fixture (#5).

## Docs (finalisation, per `documenting.md`)
- Top-level `README.md`: add the flow-key row **Val — model validation against
  actuals**; note the window served prefix + `validation_window_days` wherever the
  model store is documented (cross-package / external-implication → top-level).
- `pv-prospect-app/README.md`: document `/validate/sites` + `/validate/{system_id}`
  with a `curl` example and the honest-framing caveat (known-site tracking, *not*
  prospect-site generalisation → [cross-site-generalization-eval](
  ../briefs/cross-site-generalization-eval.md)).
- `pv-prospect-data-transformation` docs: cross-reference the served prefix the
  producer writes (if not already covered by Task 1's finalisation).

## Verification
`poetry run pytest tests/` (app, incl. smoke if a store is present); `ruff`; `mypy`;
`terraform validate`/`plan` for the IAM + env diff; render `architecture.puml`.

## Finalisation (lifecycle)
Integrate the above into permanent docs, then **delete the validation briefs and their
plans together**: briefs `validation-inmemory-store.md`, `validation-api.md`,
`validation-serving-docs.md` (+ the producer brief/plan if not already removed in
Task-1 finalisation); plans `validation-inmemory-store.md`, `validation-api.md`,
`validation-serving-docs.md`. Remove the three `## Next` TODO rows (2–4). Leave the
**website** brief/plan and its TODO row — W2 unblocks once these land but is its own
task.
