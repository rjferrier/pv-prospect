"""Warming-state contract for the /predict route (no lifespan -> no store).

Using TestClient(app) directly (not as a context manager) skips the lifespan,
so the module-level model store stays unloaded.  The prediction UI codes
against this 503 'warming' path, rendering it as a non-fatal retry state
rather than a page error.
"""

from fastapi.testclient import TestClient
from pv_prospect.app.main import app

_VALID_REQUEST = {
    'latitude': 52.65,
    'longitude': 0.78,
    'start_date': '2024-01-01',
    'end_date': '2024-12-31',
    'panels_capacity_w': 4000,
    'azimuth_deg': 180,
    'tilt_deg': 35,
}


def test_predict_warming_returns_503() -> None:
    client = TestClient(app)
    resp = client.post('/predict', json=_VALID_REQUEST)
    assert resp.status_code == 503
    assert isinstance(resp.json()['detail'], str)


def test_predict_field_bounds_rejected_before_store_check() -> None:
    # Out-of-domain longitude trips Pydantic bounds -> 422 with array detail,
    # the 'field_error' path the client distinguishes from the domain 422.
    client = TestClient(app)
    resp = client.post('/predict', json={**_VALID_REQUEST, 'longitude': 5.0})
    assert resp.status_code == 422
    assert isinstance(resp.json()['detail'], list)
