# Plan: PV Prospect website

## Goal

A public-facing website fronting the two model-serving surfaces:

1. **Prediction / speculation** — pick a UK location on a map, supply panel
   parameters, and see a speculative annual energy-yield estimate. Backed by the
   existing `POST /predict` API.
2. **Validation** — pick one of the known PV sites and see how the deployed PV
   model tracks real output over the recent ~90-day window. Backed by the (now
   shipped) Validation API.

This is the user-facing culmination of the platform: the `User → PredictionApi`
and `User → ValidationApi` flows in `doc/architecture.puml`.

## What changed since this plan was first drafted

The serving APIs have advanced past the assumptions baked into the original plan.
The refinements below thread these through:

- **Validation API is shipped, not in-progress.** `GET /validate/sites` and
  `GET /validate/{system_id}` serve, with a window producer (`util/seed-validation-window`
  one-time seed + the daily `maintain_validation_window` step in the transform
  workflow) keeping the artifact fresh. **W2 is unblocked** — the original
  "gated on Validation API tasks 1–4" no longer holds.
- **Concrete response contracts exist** (see the table below). The UI codes to
  these exact fields rather than to a sketch. A committed `openapi.yaml`
  (OpenAPI 3.1.0) is the canonical contract, and FastAPI auto-serves it live at
  `/openapi.json` with Swagger UI at `/docs` and ReDoc at `/redoc` — all
  same-origin, for free.
- **Every response carries its own `caveats[]`.** This informs the vintage gate
  (below): the caveat is *already in the payload*, and the client renders whatever
  caveats each endpoint returns rather than hardcoding them.
- **Error contracts are documented**: 422 (Pydantic field bounds + the finer
  `check_uk_domain` polygon), 502 (elevation lookup), 503 (service/model/window
  not ready), 404 (unknown validation site). The client handles each explicitly.

## Settled decisions

| Decision | Choice | Rationale |
|---|---|---|
| Ambition | Demo / portfolio piece | Cost/simplicity; ship a usable slice fast. |
| Hosting | Serve from the existing `pv-prospect-app` service | One service, one deploy, no CORS, no JS build pipeline. |
| Client | No-build: app-served HTML + vanilla JS/HTMX + CDN libs | Avoids a Node toolchain in a Python monorepo. |
| Map | Leaflet + OpenStreetMap tiles | Free, no API key; OSM tile policy fine at demo traffic. |
| Charts | uPlot or Chart.js (CDN) | Lightweight client-side time-series. Pick one at W1 spec time; **default uPlot** (smaller, faster for the dense validation series). |
| Caveats | Render `response.caveats[]` verbatim, per endpoint | Single source of truth in the API; no client-side caveat copy to drift. |
| Site list | Populate from `GET /validate/sites` at runtime | The window's site set is dynamic — never hardcode a count. |

Rejected (kept as a *deferred* optimization): a separate static frontend on
Firebase Hosting + CORS. The static assets are portable, so the split stays cheap
to do later — see "Future: dedicated hosting".

## API contracts the UI consumes

Authoritative source: `pv-prospect-app/openapi.yaml` (regenerate from
`app.openapi()` if routes change). Summary of what the UI binds to:

| Endpoint | Request | Success body (fields the UI uses) | Error codes |
|---|---|---|---|
| `POST /predict` | `latitude` 49.5–61, `longitude` −9–2, `start_date`, `end_date`, `panels_capacity_w`>0, `azimuth_deg` 0–359, `tilt_deg` 0–90, `inverter_capacity_w?`>0, `install_age_years`≥0 | `expected_annual_kwh`, `monthly_kwh[12]`, `assumptions{elevation_m, *_model_version, inverter_capacity_w, climatology}`, `caveats[]` | 422, 502, 503 |
| `GET /version` | — | `pv_model_version`, `weather_model_version`, `pv_critical_metric_r2`, `window_loaded`, `window_updated_at` | 503 |
| `GET /validate/sites` | — | `sites[]{system_id, window_start, window_end}` | 503 |
| `GET /validate/{system_id}` | path `system_id` | `series[]{date, predicted_kwh, actual_kwh, predicted_cf, actual_cf, clipped}`, `error{mape, power_space_r2}`, `model_version`, `window_updated_at`, `caveats[]` | 422, 404, 503 |
| `GET /healthz` | — | `{status: "ok"}` | 503 |

**Metadata injection is already satisfied by `/version`** — it returns model
versions, the critical R², and window status. No `/version` extension and no
server-side templating (no `jinja2`) are required; the shell fetches `/version`
client-side. Add Jinja2 only if templating later proves needed.

**`/validate/{id}` series semantics** (drives the chart, per `validation.py`):

- `predicted_kwh` vs `actual_kwh` (kWh/day) is the **primary overlay**.
- `predicted_cf` / `actual_cf` are the same quantities normalised by panel
  capacity — an optional secondary view; `predicted_cf` is *pre-clamp*, so on
  clipped days `predicted_cf` can exceed what `predicted_kwh` reflects.
- `clipped` flags inverter-limited days. These are **excluded from `error`
  metrics** — mark them on the chart and say so in a tooltip (lost headroom,
  informative, not error).
- Headline metric: **`power_space_r2`** primary, `mape` secondary. Both are
  nullable (degenerate window) — render "n/a" when null.

## Error handling the client must implement

| Code | Source | UI behaviour |
|---|---|---|
| 422 (array `detail`) | Pydantic field bounds | Inline field validation message. |
| 422 (string `detail`) | `check_uk_domain` polygon | "That point is inside the bounding box but outside the UK modelling domain" — the lat/lon box is coarser than the polygon, so an in-box sea/France-corner click passes Pydantic but fails the domain check. Distinguish from field errors by `detail` shape. |
| 502 | elevation lookup failed | Transient — toast + retry affordance. |
| 503 | model store / validation window not loaded (incl. cold "warming" under lazy load) | Non-fatal "warming up, retry shortly" state, *not* a page error. The shell must still render. |
| 404 | site not in window / registry | Shouldn't occur if the picker is fed by `/validate/sites`; handle defensively. |

The map should clamp panning/clicks to the UK bounds to prevent *most* 422s, but
the in-box-outside-polygon 422 is unavoidable and must degrade gracefully.

## Architecture (serve-from-app)

- **Routing.** HTML pages are additive routes on the existing app; the JSON APIs
  keep their current paths. The browser calls `POST /predict` and the Validation
  endpoints **same-origin** — no CORS.
  - `GET /` — landing + sections.
  - Validation as a tab within `/` (**default**) or a `GET /validation` route —
    decide at W0 spec time; tabs are simpler for a single-deploy demo.
  - `GET /static/*` — assets baked into the container image.
  - `/docs`, `/redoc`, `/openapi.json` — already served by FastAPI; link a
    "Developer / API" footer entry to `/docs` rather than building our own.
- **Static serving.** A `StaticFiles` mount; assets copied into the image by the
  Dockerfile. No GCS sync, no separate deploy. (The existing `upload-static.yml`
  Action syncs `data/static/**` CSVs — unrelated to the website.)
- **Lazy model loading (optional, W0).** Defer the model-store load (GCS download
  + `torch.load`) out of `lifespan` to first prediction, so a cold container
  (`min_instances=0`) serves the HTML shell without paying the model-load cost.
  Connects to the `/healthz` + 503 contract below. Deferring the `torch` *import*
  as well needs import restructuring — a further, optional optimization. **Default:
  include the load deferral in W0; defer the import restructuring.**

## Phasing

Recommended order: **W0 → W2 → W1-behind-gate**. W2 (validation) is honest,
insulated from the yield bias, fully unblocked, and the strongest credibility
artifact — lead with it. W1 (prediction) is buildable in parallel but its
*public launch* is gated on the yield-fix decision, so it lands behind that gate.

### W0 — Skeleton + serving substrate (cross-cutting)
- Static-file serving on the app; page shell + layout + shared CSS.
- Shared JS: same-origin API client (with the error-handling table above), map
  helper, chart helper.
- Shell fetches `/version` for the footer (model versions, window freshness).
- Optional: lazy model loading + `/healthz` redefinition (see decisions).
- Unblocks both sections.

### W2 — Validation section (unblocked)
- Site picker populated from `GET /validate/sites` (dynamic; no hardcoded count).
- Overlay `predicted_kwh` vs `actual_kwh` across the window from
  `GET /validate/{system_id}`; mark `clipped` days; headline `power_space_r2`
  (+ `mape`). Optional CF toggle.
- Render the endpoint's own `caveats[]` (in-sample, age-fill) — these are the
  *honest* caveats, distinct from the vintage caveat, and a credibility feature.
- Consumes the Validation API contract; does not duplicate that work.

### W1 — Prediction section (buildable now; public launch vintage-gated)
- Leaflet map clamped to UK bounds (lat 49.5–61, lon −9–2 — matches the API's
  Pydantic bounds); a click sets latitude/longitude. Handle the finer-polygon
  422 (above).
- Form for the `PredictRequest` fields: `panels_capacity_w`, `azimuth_deg`
  (0–359), `tilt_deg` (0–90), `install_age_years`, optional
  `inverter_capacity_w`, and the period (`start_date`/`end_date`, defaulting to a
  representative year).
- Render `monthly_kwh` as a bar chart + the `expected_annual_kwh` headline.
- **Render `response.caveats[]`** as defence-in-depth — the public headline
  number will be post-vintage-fix (decision: fix-first, below), so caveat
  rendering is not the launch mechanism for the number, but the honest caveats
  are still surfaced.

### Launch tasks (before any public exposure)
- **Auth flip.** The service is currently private (`allow_unauthenticated =
  false` in `terraform/variables.tf`; IAM `run.invoker`). Serving a public site
  *from this service* forces `allow_unauthenticated = true`, which makes the
  torch-backed `/predict` world-callable. `max_instances=2` caps the blast radius
  to degraded-service, not a runaway bill; no auth for a demo. Make this an
  explicit, reviewed step.
- **Yield fix shipped** — `pv-train-on-served-poa` is a hard prerequisite
  for W1's *public* exposure (decision: fix-first). W1 may ship privately before.
- **Cloud Run health-check + 5xx alert** re-checked if `/healthz` is redefined.

### Finalisation
- Ungrey `User → ValidationApi`, `ValidationApi → PvModel/InMemoryData` in
  `doc/architecture.puml`; add a "Website" section + flow-key entry to the README.

## Testing

Per `coding-general.md`, each phase carries tests with its functionality:
HTML-route tests (status + shell renders) and a same-origin API-client smoke test
against the relevant endpoint, including the error paths (a 503 "warming" render,
the dual-source 422 distinction). Specifics deferred to per-phase speccing.

## Decisions to resolve

### Resolved
- **Yield bias — RESOLVED: fix it first, then launch W1 publicly.**
  `POST /predict` knowingly over-estimates annual yield by ~2× (mean pred/actual
  2.0) until the yield fix (`pv-train-on-served-poa`). The product call is to
  **sequence that fix before W1's public launch** and present clean numbers,
  rather than launch behind a prominent caveat. Consequences:
  - W0 and W2 proceed now; **W2 is the only public surface until the fix lands.**
  - W1 may be *built* now (and exercised privately behind IAM auth) but **stays
    out of the public site until `pv-train-on-served-poa` ships.**
  - The yield fix is therefore a **W1 public-launch prerequisite**, not just a
    "Later" item — track it as a hard dependency of W1 exposure.
  - The in-payload `caveats[]` are still rendered (defence in depth), but they are
    no longer the launch mechanism for the headline number.
  - The asymmetry holds: Validation feeds the PV model the in-distribution corpus
    (daytime) POA it trained on — where the model is ≈accurate — and bypasses the
    weather model (real corpus weather → PV model → vs actual), so it is
    **insulated** from *both* the dominant PV-model yield bias and the weather-path,
    and is **not** caveated for it (only its own honest in-sample/age-fill
    caveats). The side-by-side ("model tracks reality here, speculative-and-
    caveated there") remains a credibility asset.

### Spec-time defaults (recorded, not blocking)
- **Chart lib:** default **uPlot**; revisit if Chart.js ergonomics win at W1.
- **One page (tabs) vs `/` + `/validation`:** default **tabs within `/`**.
- **Lazy loading + `/healthz`:** default **include load deferral in W0**, defer
  the torch-import restructuring. Under lazy load, redefine `/healthz` as
  liveness (200 once serving) and let model-readiness be a separate signal —
  `/version` already 503s until the store loads and reports `window_loaded`, so
  the shell's `/version` fetch must treat "warming" as non-fatal. Re-check the
  Cloud Run health-check config and the 5xx monitoring alert before changing it.

## Future: dedicated hosting (deferred)

Split the static shell onto **Firebase Hosting** (free tier, HTTPS, CDN) calling
the JSON APIs cross-origin — only if the site grows into a richer SPA or wants
globally edge-cached assets. (Note: "GCS + Cloud CDN" needs a load balancer — more
infra/cost — so Firebase is the cheaper static target.) Adds CORS + a second
deploy target; the portable assets make this a low-cost later move.

## Sequencing summary

- W0 first (substrate, unblocks both).
- **W2 next** — unblocked, honest, the only public surface until the yield fix.
- W1 in parallel/after; built now, but **public exposure waits on the yield
  fix** (decision: fix-first). Exercise W1 privately (IAM auth) in the meantime.
- Auth flip + yield fix are W1 public-launch tasks, not build-time blockers.
