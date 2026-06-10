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
[Seeding the Validation Window](../README.md#seeding-the-validation-window)),
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

## Known limitation

The first trained artifacts (`data-v2026-05-31`) carry a ~30 % systematic
underestimate of annual yield due to a data-vintage mismatch between the
prepared-weather and prepared-PV corpora (different OpenMeteo reanalysis
snapshots). This is documented in every `/predict` response under `caveats` and
tracked for resolution in `briefs/weather-pv-vintage-alignment.md`.
