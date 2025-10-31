"""Module-style unit tests for PVOutput extractor logic."""

import pytest
from datetime import datetime
from unittest.mock import patch

from extractors.pvoutput import (
    PVOutputRateLimiter,
    _to_clean_entries,
    _delete_if_nan,
    _remove_nans,
)


def test_update_from_headers_all_fields():
    limiter = PVOutputRateLimiter()
    headers = {
        'X-Rate-Limit-Limit': '300',
        'X-Rate-Limit-Remaining': '250',
        'X-Rate-Limit-Reset': '1609459200',  # 2021-01-01 00:00:00 UTC
    }

    limiter.update_from_headers(headers)

    assert limiter.rate_limit == 300
    assert limiter.remaining_requests == 250
    assert limiter.reset_time == datetime.fromtimestamp(1609459200)


def test_update_from_headers_partial():
    limiter = PVOutputRateLimiter()
    headers = {
        'X-Rate-Limit-Remaining': '100',
    }

    limiter.update_from_headers(headers)

    assert limiter.rate_limit is None
    assert limiter.remaining_requests == 100
    assert limiter.reset_time is None


def test_update_from_headers_empty():
    limiter = PVOutputRateLimiter()
    headers = {}

    limiter.update_from_headers(headers)

    assert limiter.rate_limit is None
    assert limiter.remaining_requests is None
    assert limiter.reset_time is None


@patch('time.sleep')
@patch('extractors.pvoutput.datetime')
def test_wait_if_needed_should_wait(mock_datetime, mock_sleep):
    limiter = PVOutputRateLimiter()
    limiter.remaining_requests = 0
    limiter.rate_limit = 300

    current = datetime(2024, 1, 1, 12, 0, 0)
    reset = datetime(2024, 1, 1, 12, 5, 0)

    mock_datetime.now.return_value = current
    limiter.reset_time = reset

    limiter.wait_if_needed()

    mock_sleep.assert_called_once_with(300.0)
    assert limiter.remaining_requests is None
    assert limiter.reset_time is None


@patch('time.sleep')
def test_wait_if_needed_no_wait_when_requests_remaining(mock_sleep):
    limiter = PVOutputRateLimiter()
    limiter.remaining_requests = 50
    limiter.rate_limit = 300

    limiter.wait_if_needed()

    mock_sleep.assert_not_called()


@patch('time.sleep')
def test_wait_if_needed_no_wait_when_remaining_none(mock_sleep):
    limiter = PVOutputRateLimiter()
    limiter.remaining_requests = None

    limiter.wait_if_needed()

    mock_sleep.assert_not_called()


@patch('time.sleep')
@patch('extractors.pvoutput.datetime')
def test_wait_if_needed_no_wait_when_reset_time_passed(mock_datetime, mock_sleep):
    limiter = PVOutputRateLimiter()
    limiter.remaining_requests = 0

    current = datetime(2024, 1, 1, 12, 10, 0)
    reset = datetime(2024, 1, 1, 12, 5, 0)

    mock_datetime.now.return_value = current
    limiter.reset_time = reset

    limiter.wait_if_needed()

    mock_sleep.assert_not_called()


def test_delete_if_nan_with_nan():
    assert _delete_if_nan('NaN') == ''


def test_delete_if_nan_with_normal_value():
    assert _delete_if_nan('123.45') == '123.45'
    assert _delete_if_nan('0') == '0'
    assert _delete_if_nan('') == ''


def test_to_clean_entries_single_line():
    text = '20240115,12:00,1000,0.5,250,200,0.8,500,100,25.5,240'
    result = _to_clean_entries(text)

    assert len(result) == 1
    assert result[0] == ['20240115', '12:00', '1000', '0.5', '250', '200', '0.8', '500', '100', '25.5', '240']


def test_to_clean_entries_multiple_lines():
    text = '20240115,12:00,1000,0.5,250;20240115,12:15,1050,0.5,260'
    result = _to_clean_entries(text)

    assert len(result) == 2
    assert result[0] == ['20240115', '12:00', '1000', '0.5', '250']
    assert result[1] == ['20240115', '12:15', '1050', '0.5', '260']


def test_to_clean_entries_with_nan_values():
    text = '20240115,12:00,NaN,0.5,250;20240115,12:15,1050,NaN,260'
    result = _to_clean_entries(text)

    assert len(result) == 2
    assert result[0] == ['20240115', '12:00', '', '0.5', '250']
    assert result[1] == ['20240115', '12:15', '1050', '', '260']


def test_remove_nans_with_ind_values():
    rows = [
        ['20240115', '12:00', '1000'],
        ['20240115', '12:15', '-1.#IND'],
        ['20240115', '12:30', '1100'],
    ]
    result = _remove_nans(rows)

    assert result == [
        ['20240115', '12:00', '1000'],
        ['20240115', '12:15', ''],
        ['20240115', '12:30', '1100'],
    ]


def test_remove_nans_preserves_normal_values():
    rows = [
        ['20240115', '12:00', '-5.5'],
        ['20240115', '12:15', '100'],
    ]
    result = _remove_nans(rows)

    assert result == rows

