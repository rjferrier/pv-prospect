"""Module-style tests for util.retry.retry_on_429."""

import pytest
from unittest.mock import patch, Mock
from requests import HTTPError, Response

from util.retry import retry_on_429


def test_successful_call_no_retry():
    mock_func = Mock(return_value="success")
    decorated = retry_on_429(mock_func)

    result = decorated("arg1", kwarg1="value1")

    assert result == "success"
    assert mock_func.call_count == 1
    mock_func.assert_called_once_with("arg1", kwarg1="value1")


@patch('time.sleep')
def test_retry_on_429_error_once(mock_sleep):
    mock_func = Mock()

    response_429 = Mock(spec=Response)
    response_429.status_code = 429
    error_429 = HTTPError(response=response_429)

    mock_func.side_effect = [error_429, "success"]
    decorated = retry_on_429(mock_func)

    result = decorated()

    assert result == "success"
    assert mock_func.call_count == 2
    mock_sleep.assert_called_once_with(60)


@patch('time.sleep')
def test_retry_on_429_multiple_times(mock_sleep):
    mock_func = Mock()

    response_429 = Mock(spec=Response)
    response_429.status_code = 429
    error_429 = HTTPError(response=response_429)

    mock_func.side_effect = [error_429, error_429, error_429, "success"]
    decorated = retry_on_429(mock_func)

    result = decorated()

    assert result == "success"
    assert mock_func.call_count == 4

    expected_sleeps = [60, 120, 300]
    assert mock_sleep.call_count == 3
    actual_sleeps = [call[0][0] for call in mock_sleep.call_args_list]
    assert actual_sleeps == expected_sleeps


@patch('time.sleep')
def test_retry_backoff_caps_at_60_minutes(mock_sleep):
    mock_func = Mock()

    response_429 = Mock(spec=Response)
    response_429.status_code = 429
    error_429 = HTTPError(response=response_429)

    mock_func.side_effect = [error_429] * 10 + ["success"]
    decorated = retry_on_429(mock_func)

    result = decorated()

    assert result == "success"
    assert mock_func.call_count == 11

    expected_sleeps = [60, 120, 300, 600, 1200, 1800, 3600, 3600, 3600, 3600]
    actual_sleeps = [call[0][0] for call in mock_sleep.call_args_list]
    assert actual_sleeps == expected_sleeps


def test_non_429_http_error_not_retried():
    mock_func = Mock()

    response_404 = Mock(spec=Response)
    response_404.status_code = 404
    error_404 = HTTPError(response=response_404)

    mock_func.side_effect = error_404
    decorated = retry_on_429(mock_func)

    with pytest.raises(HTTPError):
        decorated()

    assert mock_func.call_count == 1


def test_non_http_error_not_retried():
    mock_func = Mock()
    mock_func.side_effect = ValueError("Some error")

    decorated = retry_on_429(mock_func)

    with pytest.raises(ValueError, match="Some error"):
        decorated()

    assert mock_func.call_count == 1


def test_http_error_without_response_not_retried():
    mock_func = Mock()
    error = HTTPError()
    error.response = None

    mock_func.side_effect = error
    decorated = retry_on_429(mock_func)

    with pytest.raises(HTTPError):
        decorated()

    assert mock_func.call_count == 1

