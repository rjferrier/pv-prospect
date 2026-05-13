"""Tests for retry_on_transient_http_error."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pv_prospect.data_extraction.util import retry_on_transient_http_error
from requests import (  # type: ignore[import-untyped]
    ConnectionError as RequestsConnectionError,
)
from requests import HTTPError  # type: ignore[import-untyped]
from requests.exceptions import (  # type: ignore[import-untyped]
    ConnectTimeout,
    ReadTimeout,
    Timeout,
)


def _http_error(status_code: int) -> HTTPError:
    response = MagicMock()
    response.status_code = status_code
    return HTTPError(response=response)


def test_returns_immediately_on_success() -> None:
    func = MagicMock(return_value='ok')
    decorated = retry_on_transient_http_error(func)

    assert decorated() == 'ok'
    assert func.call_count == 1


def test_propagates_non_http_exception_without_retry() -> None:
    func = MagicMock(side_effect=ValueError('boom'))
    decorated = retry_on_transient_http_error(func)

    with pytest.raises(ValueError, match='boom'):
        decorated()
    assert func.call_count == 1


def test_propagates_non_retriable_http_error_without_retry() -> None:
    func = MagicMock(side_effect=_http_error(400))
    decorated = retry_on_transient_http_error(func)

    with pytest.raises(HTTPError):
        decorated()
    assert func.call_count == 1


def test_propagates_http_error_with_no_response_without_retry() -> None:
    func = MagicMock(side_effect=HTTPError(response=None))
    decorated = retry_on_transient_http_error(func)

    with pytest.raises(HTTPError):
        decorated()
    assert func.call_count == 1


@pytest.mark.parametrize('status_code', [429, 500, 502, 503, 504, 599])
def test_retries_on_429_or_5xx(status_code: int) -> None:
    func = MagicMock(side_effect=[_http_error(status_code), 'ok'])
    decorated = retry_on_transient_http_error(func)

    with patch('time.sleep'):
        result = decorated()

    assert result == 'ok'
    assert func.call_count == 2


@pytest.mark.parametrize(
    'exc',
    [
        Timeout('handshake'),
        ReadTimeout('read'),
        ConnectTimeout('connect'),
        RequestsConnectionError('reset'),
    ],
)
def test_retries_on_network_transients(exc: BaseException) -> None:
    func = MagicMock(side_effect=[exc, 'ok'])
    decorated = retry_on_transient_http_error(func)

    with patch('time.sleep'):
        result = decorated()

    assert result == 'ok'
    assert func.call_count == 2


def test_backoff_sequence_starts_at_one_minute() -> None:
    func = MagicMock(side_effect=[_http_error(429), _http_error(429), 'ok'])
    decorated = retry_on_transient_http_error(func)

    with (
        patch('pv_prospect.data_extraction.util.retry.time.sleep') as sleep_mock,
        patch('pv_prospect.data_extraction.util.retry.time.time', return_value=0),
    ):
        decorated()

    waits = [call.args[0] for call in sleep_mock.call_args_list]
    assert waits == [60, 120]


def test_gives_up_when_cap_exceeded() -> None:
    func = MagicMock(side_effect=_http_error(429))
    decorated = retry_on_transient_http_error(func)

    # Simulate time advancing past 20 min on the second check.
    times = iter([0.0, 0.0, 1500.0, 1500.0, 1500.0, 1500.0])
    with (
        patch(
            'pv_prospect.data_extraction.util.retry.time.time',
            side_effect=lambda: next(times),
        ),
        patch('pv_prospect.data_extraction.util.retry.time.sleep'),
    ):
        with pytest.raises(HTTPError):
            decorated()


def test_propagates_args_and_kwargs(monkeypatch: Any) -> None:
    received: dict[str, object] = {}

    def f(*args: object, **kwargs: object) -> str:
        received['args'] = args
        received['kwargs'] = kwargs
        return 'ok'

    decorated = retry_on_transient_http_error(f)
    assert decorated(1, 2, key='value') == 'ok'
    assert received == {'args': (1, 2), 'kwargs': {'key': 'value'}}
