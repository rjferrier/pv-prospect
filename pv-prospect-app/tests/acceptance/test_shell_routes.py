"""Smoke tests for the HTML shell routes (no lifespan — no model store needed)."""

from fastapi.testclient import TestClient
from pv_prospect.app.main import app


def test_root_returns_html() -> None:
    client = TestClient(app)
    resp = client.get('/')
    assert resp.status_code == 200
    assert 'text/html' in resp.headers['content-type']
    body = resp.text
    assert '<title>PV Prospect' in body
    # Hash-routed single page: home (hero) plus the three app sections.
    assert 'id="home"' in body
    assert 'id="prediction"' in body
    assert 'id="validation"' in body
    assert 'id="about"' in body


def test_validation_panel_renders_controls() -> None:
    client = TestClient(app)
    body = client.get('/').text
    # W2 controls the validation controller binds to.
    assert 'id="site-select"' in body
    assert 'id="val-seg"' in body
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


def test_prediction_panel_renders_controls() -> None:
    client = TestClient(app)
    body = client.get('/').text
    # W1 controls the prediction controller binds to.
    assert 'id="prediction-map"' in body
    assert 'id="prediction-form"' in body
    assert 'id="pred-capacity"' in body
    assert 'id="pred-azimuth"' in body
    assert 'id="pred-tilt"' in body
    assert 'id="pred-value"' in body
    assert 'id="pred-seg"' in body
    assert 'id="prediction-chart"' in body
    assert '/static/prediction.js' in body


def test_static_prediction_js_served() -> None:
    client = TestClient(app)
    resp = client.get('/static/prediction.js')
    assert resp.status_code == 200
    assert 'javascript' in resp.headers['content-type']


def test_static_ui_js_served() -> None:
    client = TestClient(app)
    resp = client.get('/static/ui.js')
    assert resp.status_code == 200
    assert 'javascript' in resp.headers['content-type']
