# Plan: PV Prospect website

## Goal

A public-facing website fronting the two model-serving surfaces:

1. **Prediction / speculation** — pick a UK location on a map, supply panel
   parameters, and see a speculative annual energy-yield estimate. Backed by the
   existing `POST /predict` API.
2. **Validation** — pick one of the 10 known PV sites and see how the deployed PV
   model tracks real output over the recent 90-day window. Backed by the
   in-progress Validation API.

This is the user-facing culmination of the platform: the `User → PredictionApi`
and `User → ValidationApi` flows in `doc/architecture.puml`.

## Settled decisions

| Decision | Choice | Rationale |
|---|---|---|
| Ambition | Demo / portfolio piece | Cost/simplicity; ship a usable slice fast. |
| Hosting | Serve from the existing `pv-prospect-app` service | One service, one deploy, no CORS, no JS build pipeline. |
| Client | No-build: app-served HTML + vanilla JS/HTMX + CDN libs | Avoids a Node toolchain in a Python monorepo. |
| Map | Leaflet + OpenStreetMap tiles | Free, no API key; OSM tile policy fine at demo traffic. |
| Charts | uPlot or Chart.js (CDN) | Lightweight client-side time-series. |

Rejected (kept as a *deferred* optimization): a separate static frontend on
Firebase Hosting + CORS. The static assets are portable, so the split stays cheap
to do later — see "Future: dedicated hosting".

## Architecture (serve-from-app)

- **Routing.** HTML pages are additive routes on the existing app; the JSON APIs
  keep their current paths. The browser calls `POST /predict` and the Validation
  endpoints **same-origin** — no CORS.
  - `GET /` — landing + prediction section.
  - `GET /validation` — validation section (or a tab within `/`).
  - `GET /static/*` — assets baked into the container image.
- **Static serving.** A `StaticFiles` mount; assets copied into the image by the
  Dockerfile. No GCS sync, no separate deploy. (The existing `upload-static.yml`
  Action syncs `data/static/**` CSVs — unrelated to the website.)
- **Metadata injection.** The shell fetches model version + caveats from a small
  endpoint (reuse/extend `/version`) rather than server-side templating — keeps
  `jinja2` out of the dependency set. Add Jinja2 only if templating proves needed.
- **Lazy model loading.** Defer the model-store load (GCS download + `torch.load`)
  out of `lifespan` to first prediction, so a cold container serves the HTML shell
  without paying the model-load cost. (Deferring the `torch` *import* as well needs
  import restructuring — a further, optional optimization.) See "Decisions to
  resolve" for the `/healthz` impact.

## Phasing

### W0 — Skeleton + serving substrate (cross-cutting)
- Static-file serving on the app; page shell + layout + shared CSS.
- Shared JS: same-origin API client, map helper, chart helper.
- Lazy model loading + `/healthz` redefinition.
- Unblocks both sections.

### W1 — Prediction section (unblocked: needs only `POST /predict`)
- Leaflet map clamped to UK bounds (lat 49.5–61, lon −9–2 — matches the API's
  `check_uk_domain`); a click sets latitude/longitude.
- Form for the `PredictRequest` fields: `panels_capacity_w`, `azimuth_deg`
  (0–359), `tilt_deg` (0–90), `install_age_years`, optional
  `inverter_capacity_w`, and the period (`start_date`/`end_date`, defaulting to a
  representative year).
- Render `monthly_kwh` as a bar chart + the `expected_annual_kwh` headline.
- Surface response `caveats` prominently (see the vintage gate below).

### W2 — Validation section (gated on the Validation API tasks 1–4)
- Site picker populated from `GET /validate/sites`.
- Overlay model-predicted vs actual output across the 90-day window from
  `GET /validate/{site}`, plus the headline accuracy metric.
- Consumes the Validation API contract; does not duplicate that work.

### Finalisation
- Ungrey `User → ValidationApi`, `ValidationApi → PvModel/InMemoryData` in
  `doc/architecture.puml`; add a "Website" section + flow-key entry to the README.

## Testing

Per `coding-general.md`, each phase carries tests with its functionality:
HTML-route tests (status + shell renders) and a same-origin API-client smoke test
against the relevant endpoint. Specifics deferred to per-phase speccing.

## Decisions to resolve (when speccing the phases)

- **Vintage bias is a launch gate for W1 *only*.** `POST /predict` knowingly emits
  ~30%-low annual yields until the vintage-alignment fix (TODO "Later"). Before any
  public launch, either (a) show an unmissable caveat, or (b) sequence the vintage
  fix first. Do not present the numbers as truth without one of these. Note the
  asymmetry: the Validation section *bypasses* the weather model (real corpus
  weather → PV model → vs. actual), so it is **insulated** from the vintage bias,
  while Prediction runs the full chain and carries it. So the caveat is W1-only —
  do **not** blanket-caveat the honest Validation section. The side-by-side ("model
  tracks reality here, speculative-and-caveated there") is a credibility asset, not
  just a hazard.
- **`/healthz` under lazy loading.** Currently 200 iff `_store` is loaded. With a
  lazy load, redefine it: `/healthz` = liveness (200 once serving); model-
  readiness becomes a separate signal (e.g. `/version` reports "warming"). Check
  the Cloud Run health-check config and the 5xx monitoring alert when changing it.
- **Abuse / cost.** Public torch-backed `/predict` is a mild vector;
  `max_instances=2` already caps it to degraded-service, not a runaway bill. No
  auth needed for a demo — revisit if exposed widely.

## Future: dedicated hosting (deferred)

Split the static shell onto **Firebase Hosting** (free tier, HTTPS, CDN) calling
the JSON APIs cross-origin — only if the site grows into a richer SPA or wants
globally edge-cached assets. (Note: "GCS + Cloud CDN" needs a load balancer — more
infra/cost — so Firebase is the cheaper static target.) Adds CORS + a second
deploy target; the portable assets make this a low-cost later move.

## Sequencing relative to other work

- W1 can proceed now (only `/predict`).
- W2 follows Validation API briefs A/B/C.
- W1's *public launch* is gated on the vintage decision above.
