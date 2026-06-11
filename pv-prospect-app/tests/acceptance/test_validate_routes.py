"""Warming-state contract for the /validate routes (no lifespan -> no window).

Using TestClient(app) directly (not as a context manager) skips the lifespan,
so the module-level store/window stay unloaded.  The validation UI codes
against this 503 'warming' path, rendering it as a non-fatal retry state.
"""

from fastapi.testclient import TestClient
from pv_prospect.app.main import app


def test_validate_sites_warming_returns_503() -> None:
    client = TestClient(app)
    resp = client.get('/validate/sites')
    assert resp.status_code == 503
    assert isinstance(resp.json()['detail'], str)


def test_validate_site_warming_returns_503() -> None:
    client = TestClient(app)
    resp = client.get('/validate/89665')
    assert resp.status_code == 503
    assert isinstance(resp.json()['detail'], str)
