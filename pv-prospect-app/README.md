# pv-prospect-app

Prediction API for UK PV energy-yield estimates. Chains the weather model → POA
reconstruction → PV model to produce climatological annual/monthly yield estimates.

## Quick start (local dev)

`/predict` needs only the model artifacts. The validation endpoints
(`/validate/*`) additionally need the PV-site registry and the rolling
validation window — see [Validation inputs](#validation-inputs-local) below.

### Model artifacts (required for `/predict`)

Produce the artifacts first with `pv-prospect-model-trainer` (the default config
points `store_dir` at the bootstrap output):

```bash
python -m pv_prospect.model_trainer bootstrap \
    --data-version 2026-05-31 \
    --output-dir /tmp/pv-prospect-models
```

Then start the API:

```bash
poetry install
poetry run uvicorn pv_prospect.app.main:app --reload
```

`/predict` and `/healthz` work as soon as the models load. The `store_dir` value
can be overridden via the `STORE_DIR` env var (e.g.
`STORE_DIR=gs://pv-prospect-versioned-model` in production).

### Validation inputs (local)

`/validate/*` additionally reads two inputs that are **not** bundled with this
repo:

- **`pv_sites.csv`** — the PV-site registry, read from `resources_dir` (default
  `resources/`, empty in a fresh checkout). It originates in the
  `pv-prospect-instance` repo, mirrored to `gs://pv-prospect-staging/resources/`.
- **the validation window** (`window.csv` + `manifest.json`), read from
  `validation_window_dir` (default `/tmp/pv-prospect-validation-window`, also
  empty). It is a derived cache at
  `gs://pv-prospect-staging/data/served/validation-window/`.

Without them, startup logs two non-fatal warnings, `/predict` keeps serving, and
`/validate/*` returns 503. The simplest way to supply both — mirroring production
— is to point their env vars at the staging bucket (requires read access; run
`gcloud auth application-default login` once):

```bash
RESOURCES_DIR=gs://pv-prospect-staging/resources \
VALIDATION_WINDOW_DIR=gs://pv-prospect-staging/data/served/validation-window \
poetry run uvicorn pv_prospect.app.main:app --reload
```

To work offline, point `RESOURCES_DIR` at your local `pv-prospect-instance`
checkout (`.../pv-prospect-instance/data/static`) and seed the validation window
to a local directory (see
[Seeding the Validation Window](../pv-prospect-data-transformation/doc/runbooks/seed-validation-window.md)),
then point `VALIDATION_WINDOW_DIR` at it.

> Do **not** copy `pv_sites.csv` into `pv-prospect-app/resources/`: that path is
> not gitignored, and the registry belongs in the instance repo, not this one.

## Endpoints

```
POST /predict              → energy-yield estimate for a UK PV site
GET  /healthz              → 200 once models are loaded
GET  /version              → loaded model versions + critical metric + window status
GET  /validate/sites       → list known sites and their window date ranges
GET  /validate/{system_id} → per-day predicted vs actual power for a known site
```

### Example request (local / public demo)

```bash
curl -X POST http://localhost:8000/predict \
  -H 'Content-Type: application/json' \
  -d '{
    "latitude": 52.65, "longitude": 0.78,
    "start_date": "2025-01-01", "end_date": "2025-12-31",
    "panels_capacity_w": 7800,
    "azimuth_deg": 180, "tilt_deg": 36,
    "inverter_capacity_w": 8000
  }'
```

### Example request (authenticated Cloud Run Service)

```bash
TOKEN=$(gcloud auth print-identity-token)
curl -X POST https://<service-url>/predict \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "latitude": 52.65, "longitude": 0.78,
    "start_date": "2025-01-01", "end_date": "2025-12-31",
    "panels_capacity_w": 7800,
    "azimuth_deg": 180, "tilt_deg": 36,
    "inverter_capacity_w": 8000
  }'
```

### Validation example (local)

```bash
curl http://localhost:8000/validate/sites

curl http://localhost:8000/validate/89665
```

> **Caveat:** `/validate` measures in-sample fit — the window sites and dates
> overlap with the training corpus. A low error rate confirms the model has
> learned the known sites; it does **not** test generalisation to new prospect
> sites. Cross-site generalisation is tracked separately in
> `briefs/cross-site-generalization-eval.md`.

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `store_dir` | `/tmp/pv-prospect-models` | Model artifact store. Override with `STORE_DIR` env var (e.g. `gs://pv-prospect-versioned-model` in production). |
| `validation_window_dir` | `/tmp/pv-prospect-validation-window` | Validation window artifact directory. Override with `VALIDATION_WINDOW_DIR` env var. |
| `resources_dir` | `resources` | Directory containing `pv_sites.csv`. Override with `RESOURCES_DIR` env var (e.g. `gs://<staging-bucket>/resources` in production). |
| `assets_dir` | `assets` | Directory containing the generated `capacity-factor-map.png` (loaded at startup, served at `/assets/capacity-factor-map.png`). Empty in a fresh checkout. Override with `ASSETS_DIR` env var (e.g. `gs://<staging-bucket>/assets` in production). |
| `rate_limit_enabled` | `true` | Enable per-IP rate limiting on `/predict` and `/validate/*`. Override with `RATE_LIMIT_ENABLED` env var (`false` to disable). |
| `rate_limit_predict` | `20/minute` | Rate limit for `POST /predict`. Override with `RATE_LIMIT_PREDICT` env var (e.g. `10/minute`). |
| `rate_limit_validate` | `30/minute` | Rate limit for `GET /validate/*`. Override with `RATE_LIMIT_VALIDATE` env var. |
| `rate_limit_trusted_hops` | `1` | Number of infrastructure hops to count from the right of `X-Forwarded-For` to find the client IP. `1` = direct `*.run.app`; raise to `2` behind an external load balancer. Override with `RATE_LIMIT_TRUSTED_HOPS` env var. |

### Rate limiting

`/predict` and `/validate/*` are limited per client IP via `slowapi` (in-memory
per-instance storage; no Redis). `/healthz` and `/version` are exempt.

**Client IP on Cloud Run.** Requests arrive behind Google's front-end, so
`request.client.host` is the Google proxy, not the caller. The limiter reads the
client IP from `X-Forwarded-For` — specifically the entry `trusted_hops` places
from the right, which is the infrastructure-stamped (non-spoofable) end. Default
`trusted_hops=1` is correct for direct `*.run.app` access. **Verify on first
deploy**: if all callers share one rate-limit bucket (global throttle), Google
appended an extra internal hop — raise `RATE_LIMIT_TRUSTED_HOPS` to `2`. If a
spoofed `X-Forwarded-For` resets someone's count, the value is too high. The
derived key is logged at `DEBUG` level to support this check.

The limiter state is per-instance (effective ceiling ~2× at `max_instances=2`).
429 responses carry `{"detail": "..."}` matching the app's error contract, plus
`Retry-After` and `X-RateLimit-*` headers. The website's error client surfaces
a "you're going too fast" message rather than a broken UI.

## Known limitations

The PV model is trained on 10 self-selected, well-maintained PVOutput sites, so
predictions carry per-site level uncertainty (±17 % at 1σ out-of-sample). This
represents an optimistic population; an arbitrary roof will likely fall below the
estimate. The model applies a fixed degradation factor (0.7%/year) to handle
age-related decline; this factor is held constant across sites rather than
site-specific to avoid overfitting. See the `pv-age-feature` report for full
rationale and cross-site generalisation analysis.

This floor is exposed, not just documented: every `/predict` response carries an
`uncertainty` object — `sigma_frac` (0.17) plus the `annual_kwh_low` /
`annual_kwh_high` bracket around `expected_annual_kwh`. The single fractional
margin lets a client widen to 2σ without an API change. The constant lives in
`PROSPECT_BAND_1SIGMA_FRAC` (`app/main.py`), which also sources the matching
caveat text so the two cannot drift.

**Inverter clipping.** `/predict` applies the inverter cap to reconstructed
hourly power rather than the daily mean. The intraday shape comes from a
pvlib clear-sky profile (the same shape used for POA reconstruction), so the
clipping fraction is a climatological estimate. Because real partly-cloudy days
can produce brief irradiance spikes above the clear-sky envelope, this slightly
under-estimates clipping losses — a conservative direction. The `/validate`
route uses real measured daily-mean power and excludes clipped days from
metrics, so this approximation does not affect validation accuracy reporting.

## Deployment

`pv-prospect-app` runs as a Terraform-managed Cloud Run Service; there is no CD
pipeline, so deploys are manual. The easiest path is `deploy.sh deploy-app` from
the `terraform/` directory, which expands to `build-app,terraform-app`:

```bash
cd terraform
bash deploy.sh deploy-app
```

Equivalently, manual steps run from the **submodule root** (one level up from
`pv-prospect-app/`) so the shared local packages are visible to the Dockerfile's
`COPY` instructions:

```bash
APP_TAG=$(git rev-parse --short HEAD)
IMAGE_URL=$(cd terraform && terraform output -raw artifact_registry_app_url)/pv-prospect-app
gcloud auth configure-docker europe-west2-docker.pkg.dev --quiet

docker build -t "$IMAGE_URL:$APP_TAG" --target entrypoint -f pv-prospect-app/Dockerfile .
docker push "$IMAGE_URL:$APP_TAG"

cd terraform && terraform apply -var "app_image_tag=$APP_TAG"
```

The image is tagged with the short commit SHA (not `:latest`) so Terraform always
sees a changed tag and rolls Cloud Run to the new revision. Static website assets
are baked into the image (`pv_prospect/app/static/`) and the promoted models load
at startup, so picking up new static assets or a freshly promoted `model-v<date>`
both require a redeploy.

**Public by default.** The service is public (`allow_unauthenticated = true` in
`terraform/variables.tf`), protected by per-IP rate limiting baked into the image
(see [Rate limiting](#rate-limiting)). To restrict to IAM auth, set
`allow_unauthenticated = false` in `terraform.tfvars` and re-apply.

## Served website

The app also serves a no-build demo UI from `/` (HTML + vanilla JS + CDN Leaflet,
mounted at `/static`). It is a single hash-routed page (`ui.js`) with four views:
a **Home** hero landing, **Prediction**, **Validation**, and **About**. It fronts
the same JSON endpoints same-origin:

- **Prediction** — click a UK map point, enter panel parameters, and see the
  expected annual yield with its uncertainty band. The monthly chart has a unit
  toggle (kWh/month · kWh/day · kW avg / kW peak) and ±17 % whiskers.
- **Validation** — predicted-vs-actual over the rolling window, with the same unit
  toggle (kWh/day · kW avg / kW peak); clipped (inverter-limited) days are flagged.

Charts are hand-drawn inline SVG (`chart.js`, no charting library); maps are real
Leaflet widgets. The Home hero shows an *illustrative* UK PV-potential map whose
blue→teal→amber→sun colour ramp matches the capacity-factor render produced by
`pv-prospect-map`. The **real** rendered map is served separately, from the
staging bucket: it is fetched once at startup from `assets_dir` (see
[Configuration](#configuration)) and served same-origin at
`GET /assets/capacity-factor-map.png` (in memory, no per-request bucket read).
The Home page embeds it in a "resource map" panel below the hero — a panel that
stays hidden until the image loads, so a fresh checkout (no asset) or a
not-yet-published bucket shows no broken image. A regenerated render is picked up
on the next app restart. See `pv-prospect-map`'s README for how to produce and
publish the PNG. The OpenAPI contract the UI binds to is committed in
`openapi.yaml` (regenerate from `app.openapi()` if routes change) and served live
at `/docs`.
