# pv-prospect-app

Prediction API for UK PV energy-yield estimates. Chains the weather model → POA
reconstruction → PV model to produce climatological annual/monthly yield estimates.

## Quick start (local dev)

Produce artifacts first with `pv-prospect-model-trainer`:

```bash
python -m pv_prospect.model_trainer bootstrap \
    --data-version 2026-05-31 \
    --output-dir /tmp/pv-prospect-models
```

Then start the API (the default config points `store_dir` at `/tmp/pv-prospect-models`):

```bash
poetry install
poetry run uvicorn pv_prospect.app.main:app --reload
```

The `store_dir` config value can be overridden via the `STORE_DIR` environment
variable (e.g. `STORE_DIR=gs://pv-prospect-versioned-model` in production).

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
