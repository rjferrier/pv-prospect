"""Tests for run_prepare_weather."""

from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from pv_prospect.data_sources import SourceDescriptor
from pv_prospect.data_sources.ts_descriptors import OpenMeteoTimeSeriesDescriptor
from pv_prospect.data_transformation.core import run_prepare_weather

from tests.unit.helpers.fake_file_system import FakeFileSystem


def _make_location() -> OpenMeteoTimeSeriesDescriptor:
    return OpenMeteoTimeSeriesDescriptor(
        location_id='504900_-35400',
        latitude=Decimal('50.4900'),
        longitude=Decimal('-3.5400'),
    )


def _write_cleaned_csv(
    fs: FakeFileSystem,
    location: OpenMeteoTimeSeriesDescriptor,
    date_str: str,
) -> None:
    """Write a cleaned weather CSV with 24h of data to the fake fs."""
    times = pd.date_range('2026-01-15 00:00:00', periods=24, freq='h')
    rng = np.random.default_rng(42)
    df = pd.DataFrame(
        {
            'time': times,
            'temperature': 5.0 + rng.normal(0, 1, 24),
            'direct_normal_irradiance': np.clip(rng.normal(200, 50, 24), 0, None),
            'diffuse_radiation': np.clip(rng.normal(60, 10, 24), 0, None),
        }
    )
    descriptor = SourceDescriptor.OPENMETEO_HISTORICAL
    source_str = str(descriptor)
    ts_str = str(location)
    filename = f'{source_str.replace("/", "-")}_{ts_str}_{date_str}.csv'
    path = f'timeseries/{source_str}/{ts_str}/{filename}'
    fs._binary_files[path] = df.to_csv(index=False).encode('utf-8')


@pytest.fixture
def location() -> OpenMeteoTimeSeriesDescriptor:
    return _make_location()


@pytest.fixture
def cleaned_fs(location: OpenMeteoTimeSeriesDescriptor) -> FakeFileSystem:
    fs = FakeFileSystem()
    _write_cleaned_csv(fs, location, '20260115')
    return fs


@pytest.fixture
def batches_fs() -> FakeFileSystem:
    return FakeFileSystem()


def test_writes_batch_at_expected_path(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    location: OpenMeteoTimeSeriesDescriptor,
) -> None:
    run_prepare_weather(
        cleaned_fs,
        batches_fs,
        SourceDescriptor.OPENMETEO_HISTORICAL,
        location,
        '20260115',
    )

    expected_path = 'weather/504900_-35400_20260115.csv'
    assert batches_fs.exists(expected_path)


def test_batch_has_no_header(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    location: OpenMeteoTimeSeriesDescriptor,
) -> None:
    run_prepare_weather(
        cleaned_fs,
        batches_fs,
        SourceDescriptor.OPENMETEO_HISTORICAL,
        location,
        '20260115',
    )

    content = batches_fs.read_text('weather/504900_-35400_20260115.csv')
    lines = content.strip().split('\n')
    first_field = lines[0].split(',')[0]
    # First field should be a number (latitude), not a column name
    float(first_field)  # would raise if it were a header


def test_batch_includes_latitude_and_longitude(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    location: OpenMeteoTimeSeriesDescriptor,
) -> None:
    run_prepare_weather(
        cleaned_fs,
        batches_fs,
        SourceDescriptor.OPENMETEO_HISTORICAL,
        location,
        '20260115',
    )

    content = batches_fs.read_text('weather/504900_-35400_20260115.csv')
    fields = content.strip().split('\n')[0].split(',')
    assert float(fields[0]) == pytest.approx(50.49)
    assert float(fields[1]) == pytest.approx(-3.54)


def test_batch_has_correct_number_of_fields(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    location: OpenMeteoTimeSeriesDescriptor,
) -> None:
    run_prepare_weather(
        cleaned_fs,
        batches_fs,
        SourceDescriptor.OPENMETEO_HISTORICAL,
        location,
        '20260115',
    )

    content = batches_fs.read_text('weather/504900_-35400_20260115.csv')
    lines = content.strip().split('\n')
    # latitude, longitude, time, temperature, direct_normal_irradiance, diffuse_radiation
    assert len(lines[0].split(',')) == 6
