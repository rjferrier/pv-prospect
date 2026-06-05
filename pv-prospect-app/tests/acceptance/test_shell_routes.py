"""Smoke tests for the W0 HTML shell routes (no lifespan — no model store needed)."""

from fastapi.testclient import TestClient
from pv_prospect.app.main import app


def test_root_returns_html() -> None:
    client = TestClient(app)
    resp = client.get('/')
    assert resp.status_code == 200
    assert 'text/html' in resp.headers['content-type']
    body = resp.text
    assert '<title>PV Prospect</title>' in body
    assert 'id="panel-validation"' in body
    assert 'id="panel-prediction"' in body


def test_static_css_served() -> None:
    client = TestClient(app)
    resp = client.get('/static/style.css')
    assert resp.status_code == 200
    assert 'text/css' in resp.headers['content-type']
