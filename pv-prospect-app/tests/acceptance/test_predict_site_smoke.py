"""Smoke test: POST /predict end-to-end over a real promoted artifact.

Requires a local model store.  Set STORE_DIR to the store directory, or place
the store at /tmp/pv-prospect-models (the default config value).  The Open-Meteo
elevation lookup is the one external call on the path; it is stubbed so the test
stays offline and deterministic (an I/O boundary, not the logic under test).

This is the prediction-path parallel to test_validate_site_smoke.py: the only
end-to-end exercise of the /predict handler's response assembly — in particular
that the uncertainty band lands in the serialized payload, bracketing the
expected estimate at the calibrated 1σ fraction.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pv_prospect.app import main
from pv_prospect.app.main import PROSPECT_BAND_1SIGMA_FRAC, app

_STORE_DIR = os.environ.get('STORE_DIR', '/tmp/pv-prospect-models')

_REQUEST = {
    'latitude': 52.65,
    'longitude': 0.78,
    'start_date': '2024-01-01',
    'end_date': '2024-12-31',
    'panels_capacity_w': 7800,
    'azimuth_deg': 180,
    'tilt_deg': 36,
    'inverter_capacity_w': 8000,
}


@pytest.mark.skipif(
    not Path(_STORE_DIR).exists(),
    reason=f'Model store not present at {_STORE_DIR}; set STORE_DIR to run',
)
def test_predict_returns_uncertainty_band(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('STORE_DIR', _STORE_DIR)
    monkeypatch.setattr(main, 'get_grid_cell_elevation', lambda lat, lon: 10.0)

    # Context-manager TestClient runs the lifespan, loading the store.
    with TestClient(app) as client:
        resp = client.post('/predict', json=_REQUEST)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    expected = body['expected_annual_kwh']
    band = body['uncertainty']
    assert band['sigma_frac'] == PROSPECT_BAND_1SIGMA_FRAC
    assert band['annual_kwh_low'] < expected < band['annual_kwh_high']
    assert len(body['monthly_kwh']) == 12
