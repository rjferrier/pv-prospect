# pv-prospect-app

Prediction API for UK PV energy-yield estimates. Chains the weather model → POA
reconstruction → PV model to produce climatological annual/monthly yield estimates.

## Quick start (local dev)

```bash
# Produce artifacts first (requires pv-prospect-model-trainer):
# RUNTIME_ENV=local CONFIG_DIR=... python -m pv_prospect.model_trainer bootstrap \
#     --data-version 2026-05-31 --output-dir /tmp/pv-prospect-models

poetry install
poetry run uvicorn pv_prospect.app.main:app --reload
```

## Endpoints

```
POST /predict   → energy-yield estimate for a UK PV site
GET  /healthz   → 200 once models are loaded
GET  /version   → loaded model versions + critical metric
```

### Example request

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

## Known limitation

The first trained artifacts (`data-v2026-05-31`) carry a ~30 % systematic
underestimate of annual yield due to a data-vintage mismatch between the
prepared-weather and prepared-PV corpora (different OpenMeteo reanalysis
snapshots). This is documented in every `/predict` response under `caveats` and
tracked for resolution in `briefs/weather-pv-vintage-alignment.md`.
