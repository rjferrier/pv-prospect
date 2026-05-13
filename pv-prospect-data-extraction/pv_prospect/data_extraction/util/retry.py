import time
from functools import wraps
from typing import Callable, ParamSpec, TypeVar

from requests import (  # type: ignore[import-untyped]
    ConnectionError as RequestsConnectionError,
)
from requests import HTTPError  # type: ignore[import-untyped]
from requests.exceptions import Timeout  # type: ignore[import-untyped]

P = ParamSpec('P')
R = TypeVar('R')


def _is_transient(exc: BaseException) -> bool:
    """Return True for HTTP failures we should back off and retry.

    Covers rate-limiting (429), server-side faults (5xx), and the
    network-layer transients that `requests` surfaces as `Timeout` or
    `ConnectionError`. Application-layer failures (4xx other than 429,
    parse errors, etc.) are not retried — they're not transient and
    should fail fast.
    """
    if isinstance(exc, HTTPError):
        if exc.response is None:
            return False
        status = exc.response.status_code
        return status == 429 or 500 <= status < 600
    if isinstance(exc, (Timeout, RequestsConnectionError)):
        return True
    return False


def retry_on_transient_http_error(func: Callable[P, R]) -> Callable[P, R]:
    """Decorator that retries on transient HTTP failures.

    Backoff strategy:
    - Initial backoffs: [1, 2, 5, 10] minutes (subsequent retries reuse 10m)
    - Capped at 20 minutes total retry duration to avoid Cloud Run Job timeouts.

    Triggers on:
    - HTTP 429 (rate limited)
    - HTTP 5xx (server error)
    - `requests.exceptions.Timeout` (read / connect timeouts)
    - `requests.exceptions.ConnectionError` (e.g. dropped connections, SSL
      handshake failures surfaced as `ConnectionError`)

    All other exceptions propagate without retry.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        # Backoff times in minutes
        backoff_minutes = [1, 2, 5, 10]
        max_duration_seconds = 1200  # 20 minutes
        attempt = 0
        start_time = time.time()

        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if not _is_transient(e):
                    raise

                # Determine backoff time
                if attempt < len(backoff_minutes):
                    backoff_min = backoff_minutes[attempt]
                else:
                    backoff_min = 10

                backoff_seconds = backoff_min * 60
                elapsed = time.time() - start_time

                if elapsed + backoff_seconds > max_duration_seconds:
                    print(
                        f'⚠️  Transient HTTP error ({type(e).__name__}) — max retry duration (20m) exceeded. Aborting.'
                    )
                    raise

                attempt += 1

                print(
                    f'⚠️  Transient HTTP error ({type(e).__name__}) - attempt {attempt}'
                )
                print(
                    f'   Backing off for {backoff_min} minute{"s" if backoff_min > 1 else ""} ({backoff_seconds} seconds)...'
                )
                time.sleep(backoff_seconds)
                print(f'   Retrying after {backoff_min} minute backoff...')

    return wrapper
