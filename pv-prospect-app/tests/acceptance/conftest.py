"""Acceptance-test fixtures.

slowapi's in-memory limiter storage is process-global and would otherwise leak
exhausted buckets (and any per-test limit tweaks) across tests. Reset and restore
around every acceptance test so each starts from a clean, enabled limiter.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pv_prospect.app import rate_limit


@pytest.fixture(autouse=True)
def reset_rate_limiter() -> Iterator[None]:
    saved = (
        rate_limit.SETTINGS.predict,
        rate_limit.SETTINGS.validate,
        rate_limit.SETTINGS.trusted_hops,
        rate_limit.limiter.enabled,
    )
    rate_limit.limiter.reset()
    rate_limit.limiter.enabled = True
    yield
    (
        rate_limit.SETTINGS.predict,
        rate_limit.SETTINGS.validate,
        rate_limit.SETTINGS.trusted_hops,
        rate_limit.limiter.enabled,
    ) = saved
    rate_limit.limiter.reset()
