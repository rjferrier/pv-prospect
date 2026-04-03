"""Tests for run_prepare_weather."""

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest
from pv_prospect.common.domain import DateRange, GridPoint
from pv_prospect.data_sources import (
    DataSource,
    build_time_series_csv_file_path,
    csv_path_to_metadata_path,
)
from pv_prospect.data_transformation.core import run_prepare_weather
from pv_prospect.etl import TIMESERIES_FOLDER

from tests.unit.helpers.fake_file_system import FakeFileSystem

_DATE_RANGE = DateRange(date(2026, 1, 15), date(2026, 1, 16))

_METADATA = {
    'latitude': 50.5,
    'longitude': -3.5,
    'elevation': 120.0,
}


def _make_grid_point() -> GridPoint:
    return GridPoint.from_id('504900_-35400')


def _write_cleaned_csv(
    fs: FakeFileSystem,
    grid_point: GridPoint,
) -> None:
    """Write a cleaned weather CSV and metadata JSON to the fake fs."""
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
    path = build_time_series_csv_file_path(
        TIMESERIES_FOLDER, DataSource.OPENMETEO_HISTORICAL, grid_point, _DATE_RANGE
    )
    fs._binary_files[path] = df.to_csv(index=False).encode('utf-8')
    fs._files[csv_path_to_metadata_path(path)] = json.dumps(_METADATA)


@pytest.fixture
def grid_point() -> GridPoint:
    return _make_grid_point()


@pytest.fixture
def cleaned_fs(grid_point: GridPoint) -> FakeFileSystem:
    fs = FakeFileSystem()
    _write_cleaned_csv(fs, grid_point)
    return fs


@pytest.fixture
def batches_fs() -> FakeFileSystem:
    return FakeFileSystem()


def test_writes_batch_at_expected_path(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    grid_point: GridPoint,
) -> None:
    run_prepare_weather(
        cleaned_fs,
        batches_fs,
        DataSource.OPENMETEO_HISTORICAL,
        grid_point,
        _DATE_RANGE,
    )

    expected_path = 'weather/504900_-35400_20260115.csv'
    assert batches_fs.exists(expected_path)


def test_batch_has_no_header(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    grid_point: GridPoint,
) -> None:
    run_prepare_weather(
        cleaned_fs,
        batches_fs,
        DataSource.OPENMETEO_HISTORICAL,
        grid_point,
        _DATE_RANGE,
    )

    content = batches_fs.read_text('weather/504900_-35400_20260115.csv')
    lines = content.strip().split('\n')
    first_field = lines[0].split(',')[0]
    # First field should be a number (latitude), not a column name
    float(first_field)  # would raise if it were a header


def test_batch_includes_metadata_values(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    grid_point: GridPoint,
) -> None:
    run_prepare_weather(
        cleaned_fs,
        batches_fs,
        DataSource.OPENMETEO_HISTORICAL,
        grid_point,
        _DATE_RANGE,
    )

    content = batches_fs.read_text('weather/504900_-35400_20260115.csv')
    fields = content.strip().split('\n')[0].split(',')
    assert float(fields[0]) == pytest.approx(_METADATA['latitude'])
    assert float(fields[1]) == pytest.approx(_METADATA['longitude'])
    assert float(fields[2]) == pytest.approx(_METADATA['elevation'])


def test_batch_has_correct_number_of_fields(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    grid_point: GridPoint,
) -> None:
    run_prepare_weather(
        cleaned_fs,
        batches_fs,
        DataSource.OPENMETEO_HISTORICAL,
        grid_point,
        _DATE_RANGE,
    )

    content = batches_fs.read_text('weather/504900_-35400_20260115.csv')
    lines = content.strip().split('\n')
    # latitude, longitude, elevation, time, temperature, direct_normal_irradiance, diffuse_radiation
    assert len(lines[0].split(',')) == 7
