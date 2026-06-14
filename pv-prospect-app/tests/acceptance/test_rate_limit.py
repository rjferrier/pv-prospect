"""End-to-end rate-limiting contract for the public routes.

These use ``TestClient(app)`` without the context manager, so the lifespan is
skipped and the model store stays unloaded. That is deliberate: the limiter runs
*before* the handler's 503 store check, so an over-limit request returns 429
while under-limit requests return 503 — letting us exercise the limiter (429 vs
503) with no model artifacts. Per-client keying is simulated via the
``X-Forwarded-For`` header (trusted_hops=1 → the right-most entry is the client).
"""

from __future__ import annotations

from fastapi.testclient import TestClient
from pv_prospect.app import rate_limit
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


def _predict(client: TestClient, ip: str) -> int:
    resp = client.post('/predict', json=_VALID_REQUEST, headers={'X-Forwarded-For': ip})
    return resp.status_code


def test_predict_rate_limit_fires_with_detail_and_retry_after() -> None:
    rate_limit.SETTINGS.predict = '2/minute'
    client = TestClient(app)

    assert _predict(client, '1.1.1.1') == 503
    assert _predict(client, '1.1.1.1') == 503
    resp = client.post(
        '/predict', json=_VALID_REQUEST, headers={'X-Forwarded-For': '1.1.1.1'}
    )
    assert resp.status_code == 429
    # The {"detail": ...} contract the website's api.js error client keys on.
    assert isinstance(resp.json()['detail'], str)
    assert resp.headers.get('Retry-After') is not None


def test_rate_limit_is_per_client_ip() -> None:
    rate_limit.SETTINGS.predict = '2/minute'
    client = TestClient(app)

    # Exhaust client A.
    assert _predict(client, '1.1.1.1') == 503
    assert _predict(client, '1.1.1.1') == 503
    assert _predict(client, '1.1.1.1') == 429
    # A distinct client is unaffected (own bucket).
    assert _predict(client, '2.2.2.2') == 503


def test_spoofed_xff_cannot_bypass_limit() -> None:
    # The attacker rotates a forged left-most entry every request, hoping for a
    # fresh bucket each time; the real (right-most) IP is fixed, so the limit
    # still trips on the third request.
    rate_limit.SETTINGS.predict = '2/minute'
    client = TestClient(app)

    statuses = [
        client.post(
            '/predict',
            json=_VALID_REQUEST,
            headers={'X-Forwarded-For': f'9.9.9.{i}, 1.1.1.1'},
        ).status_code
        for i in range(3)
    ]
    assert statuses == [503, 503, 429]


def test_validate_routes_are_rate_limited() -> None:
    rate_limit.SETTINGS.validate = '2/minute'
    client = TestClient(app)

    def hit() -> int:
        return client.get(
            '/validate/sites', headers={'X-Forwarded-For': '1.1.1.1'}
        ).status_code

    assert hit() == 503
    assert hit() == 503
    assert hit() == 429


def test_healthz_and_version_are_exempt() -> None:
    # Probes must never 429: set the limits to 1/minute, then hammer the probes.
    rate_limit.SETTINGS.predict = '1/minute'
    rate_limit.SETTINGS.validate = '1/minute'
    client = TestClient(app)

    for _ in range(5):
        assert (
            client.get('/healthz', headers={'X-Forwarded-For': '1.1.1.1'}).status_code
            != 429
        )
        assert (
            client.get('/version', headers={'X-Forwarded-For': '1.1.1.1'}).status_code
            != 429
        )
