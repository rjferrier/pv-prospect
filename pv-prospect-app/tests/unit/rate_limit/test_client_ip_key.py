"""Unit tests for the rate-limit client-IP key function.

The security property under test: the key is read from the RIGHT of
``X-Forwarded-For`` (the infrastructure-stamped end), so a caller cannot lower or
escape their own bucket by prepending forged left-most entries.
"""

from __future__ import annotations

import pytest
from pv_prospect.app import rate_limit
from pv_prospect.app.rate_limit import client_ip_key
from starlette.requests import Request


def _request(
    xff: str | None = None,
    client: tuple[str, int] | None = ('5.6.7.8', 1234),
) -> Request:
    headers = [(b'x-forwarded-for', xff.encode())] if xff is not None else []
    scope = {'type': 'http', 'headers': headers, 'client': client}
    return Request(scope)


@pytest.fixture(autouse=True)
def _trusted_hops(monkeypatch: pytest.MonkeyPatch) -> None:
    # Default to the direct-run.app setting; individual tests override.
    monkeypatch.setattr(rate_limit.SETTINGS, 'trusted_hops', 1)


def test_single_xff_entry_is_the_key() -> None:
    assert client_ip_key(_request('203.0.113.7')) == '203.0.113.7'


def test_distinct_clients_get_distinct_keys() -> None:
    assert client_ip_key(_request('1.1.1.1')) != client_ip_key(_request('2.2.2.2'))


def test_spoofed_leftmost_entries_are_ignored() -> None:
    # Attacker prepends forged values; infrastructure stamps the real IP last.
    # With trusted_hops=1 we read the right-most entry, so the forgery has no
    # effect — every forged prefix maps to the same real-IP key.
    forged_a = client_ip_key(_request('9.9.9.9, 203.0.113.7'))
    forged_b = client_ip_key(_request('6.6.6.6, 7.7.7.7, 203.0.113.7'))
    assert forged_a == forged_b == '203.0.113.7'


def test_trusted_hops_two_selects_second_from_right() -> None:
    # Behind a load balancer the real client sits one place left of the LB hop.
    rate_limit.SETTINGS.trusted_hops = 2
    assert client_ip_key(_request('203.0.113.7, 10.0.0.1')) == '203.0.113.7'


def test_falls_back_to_peer_when_no_xff() -> None:
    assert client_ip_key(_request(None, client=('5.6.7.8', 1234))) == '5.6.7.8'


def test_falls_back_to_peer_when_xff_shorter_than_hops() -> None:
    # trusted_hops=2 but only one entry present: don't read off the end (which
    # would pick the spoofable left-most) — fall back to the trusted peer.
    rate_limit.SETTINGS.trusted_hops = 2
    assert client_ip_key(_request('9.9.9.9', client=('5.6.7.8', 1234))) == '5.6.7.8'


def test_unknown_when_no_xff_and_no_peer() -> None:
    assert client_ip_key(_request(None, client=None)) == 'unknown'
