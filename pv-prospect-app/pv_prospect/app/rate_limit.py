"""Per-IP rate limiting for the public app endpoints.

slowapi (the Starlette/FastAPI limiter) baked into the image. It limits
``POST /predict`` and the ``/validate/*`` routes per client IP; ``/healthz``,
``/version``, ``/`` and ``/static`` stay unlimited (uptime / monitoring probes
must never 429). In-memory per-instance storage is acceptable at
``max_instances=2`` (effective ceiling ~2x the configured limit); no Redis.

**Client IP behind Cloud Run.** slowapi's default ``get_remote_address`` reads
``request.client.host``, which on Cloud Run is the Google front-end — every
request would then share one bucket and the limiter would throttle *globally*.
:func:`client_ip_key` instead reads ``X-Forwarded-For``, counting
``trusted_hops`` infrastructure hops **from the right**. The left-most XFF
entries are client-spoofable (a caller can prepend arbitrary values); only the
right-hand entries, stamped by infrastructure we control, are trustworthy.

``trusted_hops`` defaults to ``1`` (take the right-most entry) — correct for a
service called directly on its ``*.run.app`` URL, where the Google front-end
stamps the genuine client IP last. This default fails *closed*: if it is wrong
because Google appends an extra internal hop, callers collapse into one bucket
(over-throttling, annoying) rather than each escaping the limit (exploitable).

**VERIFY ON FIRST DEPLOY.** If rate limiting throttles globally (all callers
share one bucket), Google is appending an extra hop — raise ``trusted_hops`` to
``2``. If a spoofed ``X-Forwarded-For`` bypasses the limit, ``trusted_hops`` is
too high. Putting the service behind an external HTTPS load balancer also shifts
the client IP one place left of the LB hop (``trusted_hops=2``). The derived key
is logged at DEBUG to support this check.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# Code defaults for the no-lifespan path (tests). They mirror the canonical
# runtime values in resources/config-default.yaml; keep the two in step. The
# lifespan copies the resolved AppConfig values into SETTINGS at startup.
_DEFAULT_PREDICT_LIMIT = '20/minute'
_DEFAULT_VALIDATE_LIMIT = '30/minute'
_DEFAULT_TRUSTED_HOPS = 1


@dataclass
class RateLimitSettings:
    """Per-request rate-limit knobs, read live by the limit callables / key_func.

    Mutable so the lifespan can populate it from AppConfig after the limiter
    (and its decorators) are already constructed at import time.
    """

    predict: str = _DEFAULT_PREDICT_LIMIT
    validate: str = _DEFAULT_VALIDATE_LIMIT
    trusted_hops: int = _DEFAULT_TRUSTED_HOPS


SETTINGS = RateLimitSettings()


def client_ip_key(request: Request) -> str:
    """Rate-limit key: the client IP, read safely from ``X-Forwarded-For``.

    Counts :attr:`RateLimitSettings.trusted_hops` entries from the right of
    ``X-Forwarded-For`` (the infrastructure-stamped, non-spoofable end). Falls
    back to the direct peer address when the header is absent or shorter than the
    configured hop count.
    """
    forwarded = request.headers.get('X-Forwarded-For')
    if forwarded:
        parts = [p.strip() for p in forwarded.split(',') if p.strip()]
        hops = SETTINGS.trusted_hops
        if len(parts) >= hops >= 1:
            key = parts[-hops]
            logger.debug('rate-limit key from X-Forwarded-For: %s', key)
            return key
    peer = request.client.host if request.client else 'unknown'
    logger.debug('rate-limit key from peer address: %s', peer)
    return peer


def predict_limit() -> str:
    """Limit string for ``/predict`` (read live so config can be tuned at runtime)."""
    return SETTINGS.predict


def validate_limit() -> str:
    """Limit string for the ``/validate/*`` routes (read live)."""
    return SETTINGS.validate


def rate_limit_exceeded_handler(request: Request, exc: Exception) -> Response:
    """429 handler that matches the app's ``{"detail": ...}`` error contract.

    slowapi's stock handler returns ``{"error": ...}``; the website's api.js error
    client (and every other endpoint here) keys on ``detail``, so we override to
    keep the contract — letting the UI render a "you're going too fast" message
    instead of falling through to its generic ``unknown`` branch. The
    ``Retry-After`` / ``X-RateLimit-*`` headers are injected as usual.
    """
    assert isinstance(exc, RateLimitExceeded)
    response = JSONResponse(
        {'detail': f'Rate limit exceeded: {exc.detail}'}, status_code=429
    )
    return request.app.state.limiter._inject_headers(
        response, request.state.view_rate_limit
    )


# Constructed at import time so the route decorators can reference it. Header
# injection on (so 429s carry Retry-After / X-RateLimit-*); in-memory storage is
# the default. The limit values resolve per-request via the callables above.
limiter = Limiter(key_func=client_ip_key, headers_enabled=True)
