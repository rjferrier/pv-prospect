import time
from functools import wraps
from typing import Callable, ParamSpec, TypeVar

from requests import HTTPError  # type: ignore[import-untyped]

P = ParamSpec('P')
R = TypeVar('R')


def retry_on_429(func: Callable[P, R]) -> Callable[P, R]:
    """
    Decorator that retries a method if a 429 HTTPError is encountered.

    Backoff strategy:
    - Initial backoffs: [1, 2, 5, 10] minutes
    - Capped at 20 minutes total retry duration to avoid Cloud Run Job timeouts.

    Args:
        func: The function to decorate

    Returns:
        The decorated function
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
            except HTTPError as e:
                # Check if it's a 429 error
                if e.response is not None and e.response.status_code == 429:
                    # Determine backoff time
                    if attempt < len(backoff_minutes):
                        backoff_min = backoff_minutes[attempt]
                    else:
                        backoff_min = 10

                    backoff_seconds = backoff_min * 60
                    elapsed = time.time() - start_time

                    if elapsed + backoff_seconds > max_duration_seconds:
                        print(
                            '⚠️  429 Too Many Requests error - max retry duration (20m) exceeded. Aborting.'
                        )
                        raise

                    attempt += 1

                    print(f'⚠️  429 Too Many Requests error - attempt {attempt}')
                    print(
                        f'   Backing off for {backoff_min} minute{"s" if backoff_min > 1 else ""} ({backoff_seconds} seconds)...'
                    )
                    time.sleep(backoff_seconds)
                    print(f'   Retrying after {backoff_min} minute backoff...')
                else:
                    # Re-raise if it's not a 429 error
                    raise
            except Exception:
                # Re-raise any other exceptions
                raise

    return wrapper
