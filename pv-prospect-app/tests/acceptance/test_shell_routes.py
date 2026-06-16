"""Smoke tests for the HTML shell routes (no lifespan — no model store needed)."""

from pathlib import Path

from fastapi.testclient import TestClient
from pv_prospect.app import main
from pv_prospect.app.main import app
from pv_prospect.app.store import filesystem_for


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


def test_home_references_resource_map() -> None:
    # The home page embeds the served capacity-factor render; the panel is hidden
    # by ui.js until the image loads (so dev / pre-publish shows no broken icon).
    body = TestClient(app).get('/').text
    assert 'id="resource-map"' in body
    assert '/assets/capacity-factor-map.png' in body


def test_assets_map_404_when_absent() -> None:
    # No lifespan runs (TestClient is not used as a context manager), so the
    # capacity-factor map is never fetched and the route degrades to 404.
    resp = TestClient(app).get('/assets/capacity-factor-map.png')
    assert resp.status_code == 404


def test_assets_map_served_from_memory_when_loaded() -> None:
    png = b'\x89PNG\r\n\x1a\nfake-render-bytes'
    saved = main._capacity_factor_map
    main._capacity_factor_map = png
    try:
        resp = TestClient(app).get('/assets/capacity-factor-map.png')
        assert resp.status_code == 200
        assert resp.headers['content-type'] == 'image/png'
        assert resp.content == png
    finally:
        main._capacity_factor_map = saved


def test_capacity_factor_map_round_trips_through_filesystem(tmp_path: Path) -> None:
    # Locks the startup seam the assets route depends on: the filename constant
    # plus filesystem_for(...).read_bytes(...). Neither pv_sites.csv nor the
    # validation window exercises read_bytes (both use read_text), so this is the
    # only coverage of the binary-read path the home-page map relies on.
    png = b'\x89PNG\r\n\x1a\nround-trip-bytes'
    (tmp_path / main.CAPACITY_FACTOR_MAP_FILENAME).write_bytes(png)
    fs = filesystem_for(str(tmp_path))
    assert fs.read_bytes(main.CAPACITY_FACTOR_MAP_FILENAME) == png
