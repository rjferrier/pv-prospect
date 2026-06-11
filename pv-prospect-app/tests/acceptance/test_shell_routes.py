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


def test_validation_panel_renders_controls() -> None:
    client = TestClient(app)
    body = client.get('/').text
    # W2 controls the validation controller binds to.
    assert 'id="site-select"' in body
    assert 'id="validation-chart"' in body
    assert 'id="validation-status"' in body
    assert 'id="validation-caveats"' in body
    assert '/static/validation.js' in body


def test_static_css_served() -> None:
    client = TestClient(app)
    resp = client.get('/static/style.css')
    assert resp.status_code == 200
    assert 'text/css' in resp.headers['content-type']


def test_static_validation_js_served() -> None:
    client = TestClient(app)
    resp = client.get('/static/validation.js')
    assert resp.status_code == 200
    assert 'javascript' in resp.headers['content-type']
