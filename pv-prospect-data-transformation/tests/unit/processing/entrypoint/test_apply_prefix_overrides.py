"""Tests for apply_prefix_overrides."""

import pytest
from pv_prospect.data_sources import DataSource, DataSourcesConfig, WeatherGridConfig
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.processing.entrypoint import (
    apply_prefix_overrides,
)
from pv_prospect.etl.storage.backends import GcsStorageConfig


def _gcs(prefix: str, bucket: str = 'pv-prospect-staging') -> GcsStorageConfig:
    return GcsStorageConfig(bucket_name=bucket, prefix=prefix)


def _config(
    *,
    prepared: str = 'data/prepared',
    cursors: str | None = 'tracking/cursors',
    ledger: str | None = 'tracking/ledger',
) -> DataTransformationConfig:
    return DataTransformationConfig(
        resources_storage=_gcs('resources'),
        staged_raw_data_storage=_gcs('data/raw'),
        staged_cleaned_data_storage=_gcs('data/cleaned'),
        staged_prepared_batches_data_storage=_gcs('data/prepared-batches'),
        staged_prepared_data_storage=_gcs(prepared),
        data_sources=DataSourcesConfig(
            pv=DataSource.PVOUTPUT, weather=DataSource.OPENMETEO_HISTORICAL
        ),
        weather_grid=WeatherGridConfig(version=0),
        cursors_storage=_gcs(cursors) if cursors is not None else None,
        ledger_storage=_gcs(ledger) if ledger is not None else None,
    )


def test_no_overrides_returns_config_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('PREPARED_PREFIX_OVERRIDE', raising=False)
    monkeypatch.delenv('CURSORS_PREFIX_OVERRIDE', raising=False)
    monkeypatch.delenv('LEDGER_PREFIX_OVERRIDE', raising=False)
    config = _config()

    result = apply_prefix_overrides(config)

    assert result is config


def test_prepared_override_redirects_prepared_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('PREPARED_PREFIX_OVERRIDE', 'data/prepared-shadow')
    monkeypatch.delenv('CURSORS_PREFIX_OVERRIDE', raising=False)
    monkeypatch.delenv('LEDGER_PREFIX_OVERRIDE', raising=False)
    config = _config()

    result = apply_prefix_overrides(config)

    assert result.staged_prepared_data_storage.prefix == 'data/prepared-shadow'


def test_prepared_override_leaves_bucket_name_intact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('PREPARED_PREFIX_OVERRIDE', 'data/prepared-shadow')
    monkeypatch.delenv('CURSORS_PREFIX_OVERRIDE', raising=False)
    monkeypatch.delenv('LEDGER_PREFIX_OVERRIDE', raising=False)
    config = _config()

    result = apply_prefix_overrides(config)

    assert isinstance(result.staged_prepared_data_storage, GcsStorageConfig)
    assert result.staged_prepared_data_storage.bucket_name == 'pv-prospect-staging'


def test_cursors_override_redirects_cursors_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('PREPARED_PREFIX_OVERRIDE', raising=False)
    monkeypatch.setenv('CURSORS_PREFIX_OVERRIDE', 'tracking/cursors-shadow')
    monkeypatch.delenv('LEDGER_PREFIX_OVERRIDE', raising=False)
    config = _config()

    result = apply_prefix_overrides(config)

    assert result.cursors_storage is not None
    assert result.cursors_storage.prefix == 'tracking/cursors-shadow'


def test_ledger_override_redirects_ledger_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('PREPARED_PREFIX_OVERRIDE', raising=False)
    monkeypatch.delenv('CURSORS_PREFIX_OVERRIDE', raising=False)
    monkeypatch.setenv('LEDGER_PREFIX_OVERRIDE', 'tracking/ledger-shadow')
    config = _config()

    result = apply_prefix_overrides(config)

    assert result.ledger_storage is not None
    assert result.ledger_storage.prefix == 'tracking/ledger-shadow'


def test_overrides_do_not_touch_other_storages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('PREPARED_PREFIX_OVERRIDE', 'data/prepared-shadow')
    monkeypatch.setenv('CURSORS_PREFIX_OVERRIDE', 'tracking/cursors-shadow')
    monkeypatch.setenv('LEDGER_PREFIX_OVERRIDE', 'tracking/ledger-shadow')
    config = _config()

    result = apply_prefix_overrides(config)

    assert result.staged_raw_data_storage.prefix == 'data/raw'
    assert result.staged_cleaned_data_storage.prefix == 'data/cleaned'
    assert result.resources_storage.prefix == 'resources'


def test_cursors_override_noop_when_storage_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Some local configs leave cursors_storage as None; the override
    silently no-ops there rather than crashing."""
    monkeypatch.delenv('PREPARED_PREFIX_OVERRIDE', raising=False)
    monkeypatch.setenv('CURSORS_PREFIX_OVERRIDE', 'tracking/cursors-shadow')
    monkeypatch.delenv('LEDGER_PREFIX_OVERRIDE', raising=False)
    config = _config(cursors=None)

    result = apply_prefix_overrides(config)

    assert result.cursors_storage is None


def test_ledger_override_noop_when_storage_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('PREPARED_PREFIX_OVERRIDE', raising=False)
    monkeypatch.delenv('CURSORS_PREFIX_OVERRIDE', raising=False)
    monkeypatch.setenv('LEDGER_PREFIX_OVERRIDE', 'tracking/ledger-shadow')
    config = _config(ledger=None)

    result = apply_prefix_overrides(config)

    assert result.ledger_storage is None


def test_empty_string_override_is_treated_as_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cloud Run env-overrides sometimes blank out an env var rather than
    unsetting it; an empty value should behave as "no override"."""
    monkeypatch.setenv('PREPARED_PREFIX_OVERRIDE', '')
    monkeypatch.delenv('CURSORS_PREFIX_OVERRIDE', raising=False)
    monkeypatch.delenv('LEDGER_PREFIX_OVERRIDE', raising=False)
    config = _config()

    result = apply_prefix_overrides(config)

    assert result is config
