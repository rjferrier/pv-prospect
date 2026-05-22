"""Tests for run_prepare_weather."""

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest
from pv_prospect.common.domain import ArbitrarySite, DateRange
from pv_prospect.data_sources import (
    DataSource,
    build_time_series_csv_file_path,
    csv_path_to_metadata_path,
)
from pv_prospect.data_transformation.processing import (
    WEATHER_COLUMNS,
    PreparedBatchCollector,
    run_prepare_weather,
)
from pv_prospect.etl import TIMESERIES_FOLDER

from tests.unit.helpers.fake_file_system import FakeFileSystem

_DATE_RANGE = DateRange(date(2026, 1, 15), date(2026, 1, 16))
_GRID_POINT_SAMPLE_INDEX = 7
_WINDOW_KEY = (_GRID_POINT_SAMPLE_INDEX, '2026-01-15', '2026-01-16')

_METADATA = {
    'latitude': 50.5,
    'longitude': -3.5,
    'elevation': 120.0,
}


def _make_grid_point() -> ArbitrarySite:
    return ArbitrarySite.from_id('504900_-35400')


def _write_cleaned_csv(fs: FakeFileSystem, grid_point: ArbitrarySite) -> None:
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
def grid_point() -> ArbitrarySite:
    return _make_grid_point()


@pytest.fixture
def cleaned_fs(grid_point: ArbitrarySite) -> FakeFileSystem:
    fs = FakeFileSystem()
    _write_cleaned_csv(fs, grid_point)
    return fs


def _run(
    cleaned_fs: FakeFileSystem, grid_point: ArbitrarySite
) -> PreparedBatchCollector:
    collector = PreparedBatchCollector()
    run_prepare_weather(
        cleaned_fs,
        DataSource.OPENMETEO_HISTORICAL,
        grid_point,
        _DATE_RANGE,
        _GRID_POINT_SAMPLE_INDEX,
        collector,
    )
    return collector


def test_buffers_a_concatenated_frame_keyed_by_sample_and_window(
    cleaned_fs: FakeFileSystem, grid_point: ArbitrarySite
) -> None:
    collector = _run(cleaned_fs, grid_point)

    groups = collector.weather_groups()
    assert list(groups) == [_WINDOW_KEY]
    assert len(groups[_WINDOW_KEY]) == 1


def test_buffered_frame_has_weather_columns(
    cleaned_fs: FakeFileSystem, grid_point: ArbitrarySite
) -> None:
    collector = _run(cleaned_fs, grid_point)

    frame = collector.weather_groups()[_WINDOW_KEY][0]
    assert list(frame.columns) == WEATHER_COLUMNS


def test_buffered_frame_carries_metadata_lat_lon_elevation(
    cleaned_fs: FakeFileSystem, grid_point: ArbitrarySite
) -> None:
    collector = _run(cleaned_fs, grid_point)

    frame = collector.weather_groups()[_WINDOW_KEY][0]
    assert frame['latitude'].iloc[0] == pytest.approx(_METADATA['latitude'])
    assert frame['longitude'].iloc[0] == pytest.approx(_METADATA['longitude'])
    assert frame['elevation'].iloc[0] == pytest.approx(_METADATA['elevation'])


def test_daily_row_is_labelled_with_input_date(
    cleaned_fs: FakeFileSystem, grid_point: ArbitrarySite
) -> None:
    """The downsampled row for 2026-01-15's hourly weather should be
    labelled 2026-01-15 — not 2026-01-16."""
    collector = _run(cleaned_fs, grid_point)

    frame = collector.weather_groups()[_WINDOW_KEY][0]
    assert list(frame['time']) == [pd.Timestamp('2026-01-15 00:00:00')]
