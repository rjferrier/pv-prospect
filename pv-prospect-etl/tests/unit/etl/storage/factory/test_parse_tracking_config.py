"""Tests for parse_tracking_config."""

from pv_prospect.etl.storage.backends.local import LocalStorageConfig
from pv_prospect.etl.storage.factory import parse_tracking_config


def test_returns_none_for_none_input() -> None:
    assert parse_tracking_config(None) is None


def test_returns_none_for_empty_dict() -> None:
    assert parse_tracking_config({}) is None


def test_parses_tracking_config() -> None:
    data = {'backend': 'local', 'prefix': '/tmp/tracking'}

    result = parse_tracking_config(data)

    assert isinstance(result, LocalStorageConfig)
    assert result.prefix == '/tmp/tracking'
