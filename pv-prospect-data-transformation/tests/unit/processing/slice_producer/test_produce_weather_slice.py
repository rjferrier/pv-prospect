"""Tests for produce_weather_slice."""

import io
import json
from datetime import date

import pandas as pd
import pytest
from pv_prospect.common.domain import ArbitrarySite, DateRange
from pv_prospect.data_sources import (
    DataSource,
    build_time_series_csv_file_path,
    csv_path_to_metadata_path,
)
from pv_prospect.data_transformation.processing import (
    SliceOutcome,
    produce_weather_slice,
    weather_partition_path,
)
from pv_prospect.etl import TIMESERIES_FOLDER, WeatherSlice

from tests.unit.helpers.fake_file_system import FakeFileSystem

_WEATHER_SOURCE = DataSource.OPENMETEO_HISTORICAL
_GRID_VERSION = 0
_SLICE = WeatherSlice(
    grid_point_sample_index=7,
    start_date=date(2026, 1, 1),
    end_date=date(2026, 1, 8),
)


def _raw_csv(date_range: DateRange) -> bytes:
    """Hourly OpenMeteo raw weather covering *date_range*."""
    periods = len(date_range) * 24
    df = pd.DataFrame(
        {
            'time': pd.date_range(date_range.start, periods=periods, freq='h'),
            'temperature_best_match': [10.0] * periods,
            'direct_normal_irradiance_best_match': [100.0] * periods,
            'diffuse_radiation_best_match': [50.0] * periods,
        }
    )
    return df.to_csv(index=False).encode('utf-8')


def _location_string(grid_point: ArbitrarySite) -> str:
    loc = grid_point.location
    return f'{loc.latitude},{loc.longitude}'


def _write_raw(
    fs: FakeFileSystem,
    grid_point: ArbitrarySite,
    date_range: DateRange,
    metadata: dict[str, float],
) -> str:
    path = build_time_series_csv_file_path(
        TIMESERIES_FOLDER, _WEATHER_SOURCE, grid_point, date_range
    )
    fs.write_bytes(path, _raw_csv(date_range))
    fs.write_text(csv_path_to_metadata_path(path), json.dumps(metadata))
    return path


@pytest.fixture
def grid_points() -> list[ArbitrarySite]:
    return [
        ArbitrarySite.from_id('504900_-35400'),
        ArbitrarySite.from_id('505000_-35500'),
    ]


def _metadata(grid_point: ArbitrarySite) -> dict[str, float]:
    loc = grid_point.location
    return {
        'latitude': float(loc.latitude),
        'longitude': float(loc.longitude),
        'elevation': 120.0,
    }


def test_all_inputs_present_yields_completed_outcome(
    grid_points: list[ArbitrarySite],
) -> None:
    raw_fs = FakeFileSystem()
    date_range = DateRange(_SLICE.start_date, _SLICE.end_date)
    for grid_point in grid_points:
        _write_raw(raw_fs, grid_point, date_range, _metadata(grid_point))
    prepared_fs = FakeFileSystem()

    outcome = produce_weather_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _WEATHER_SOURCE,
        [_location_string(gp) for gp in grid_points],
        _GRID_VERSION,
    )

    assert outcome == SliceOutcome(status='completed', missing_inputs_count=0)


def test_completed_outcome_writes_one_partition_file(
    grid_points: list[ArbitrarySite],
) -> None:
    raw_fs = FakeFileSystem()
    date_range = DateRange(_SLICE.start_date, _SLICE.end_date)
    for grid_point in grid_points:
        _write_raw(raw_fs, grid_point, date_range, _metadata(grid_point))
    prepared_fs = FakeFileSystem()

    produce_weather_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _WEATHER_SOURCE,
        [_location_string(gp) for gp in grid_points],
        _GRID_VERSION,
    )

    expected_path = weather_partition_path(
        _SLICE.start_date.isoformat(),
        _SLICE.end_date.isoformat(),
        _SLICE.grid_point_sample_index,
        _GRID_VERSION,
    )
    assert prepared_fs.exists(expected_path)


def test_partition_file_contains_one_row_group_per_grid_point(
    grid_points: list[ArbitrarySite],
) -> None:
    raw_fs = FakeFileSystem()
    date_range = DateRange(_SLICE.start_date, _SLICE.end_date)
    for grid_point in grid_points:
        _write_raw(raw_fs, grid_point, date_range, _metadata(grid_point))
    prepared_fs = FakeFileSystem()

    produce_weather_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _WEATHER_SOURCE,
        [_location_string(gp) for gp in grid_points],
        _GRID_VERSION,
    )

    path = weather_partition_path(
        _SLICE.start_date.isoformat(),
        _SLICE.end_date.isoformat(),
        _SLICE.grid_point_sample_index,
        _GRID_VERSION,
    )
    df = pd.read_csv(io.StringIO(prepared_fs.read_text(path)))
    assert {
        (lat, lon) for lat, lon in zip(df['latitude'], df['longitude'], strict=False)
    } == {
        (float(gp.location.latitude), float(gp.location.longitude))
        for gp in grid_points
    }


def test_one_missing_raw_input_yields_partial_outcome(
    grid_points: list[ArbitrarySite],
) -> None:
    raw_fs = FakeFileSystem()
    date_range = DateRange(_SLICE.start_date, _SLICE.end_date)
    # Stage only the first grid point's raw file.
    _write_raw(raw_fs, grid_points[0], date_range, _metadata(grid_points[0]))
    prepared_fs = FakeFileSystem()

    outcome = produce_weather_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _WEATHER_SOURCE,
        [_location_string(gp) for gp in grid_points],
        _GRID_VERSION,
    )

    assert outcome == SliceOutcome(status='partial', missing_inputs_count=1)


def test_partial_outcome_still_writes_a_partition_file(
    grid_points: list[ArbitrarySite],
) -> None:
    """A partial outcome means some grid points were missing — but the
    output is still written, carrying the rows for the grid points that
    were present. A re-run with the missing inputs filled in upgrades
    this to completed via the merge-with-existing path."""
    raw_fs = FakeFileSystem()
    date_range = DateRange(_SLICE.start_date, _SLICE.end_date)
    _write_raw(raw_fs, grid_points[0], date_range, _metadata(grid_points[0]))
    prepared_fs = FakeFileSystem()

    produce_weather_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _WEATHER_SOURCE,
        [_location_string(gp) for gp in grid_points],
        _GRID_VERSION,
    )

    path = weather_partition_path(
        _SLICE.start_date.isoformat(),
        _SLICE.end_date.isoformat(),
        _SLICE.grid_point_sample_index,
        _GRID_VERSION,
    )
    assert prepared_fs.exists(path)


def test_all_inputs_missing_yields_partial_and_writes_nothing(
    grid_points: list[ArbitrarySite],
) -> None:
    raw_fs = FakeFileSystem()
    prepared_fs = FakeFileSystem()

    outcome = produce_weather_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _WEATHER_SOURCE,
        [_location_string(gp) for gp in grid_points],
        _GRID_VERSION,
    )

    assert outcome == SliceOutcome(
        status='partial', missing_inputs_count=len(grid_points)
    )
    path = weather_partition_path(
        _SLICE.start_date.isoformat(),
        _SLICE.end_date.isoformat(),
        _SLICE.grid_point_sample_index,
        _GRID_VERSION,
    )
    assert not prepared_fs.exists(path)


def test_rerun_with_completed_inputs_upgrades_partial_to_completed(
    grid_points: list[ArbitrarySite],
) -> None:
    """The slice task body merges into an existing partition file. A
    second run with the previously-missing inputs added should produce
    a partition file containing every grid point's rows."""
    date_range = DateRange(_SLICE.start_date, _SLICE.end_date)
    prepared_fs = FakeFileSystem()
    locations = [_location_string(gp) for gp in grid_points]

    # Run 1: only the first grid point's raw file present.
    raw_fs_1 = FakeFileSystem()
    _write_raw(raw_fs_1, grid_points[0], date_range, _metadata(grid_points[0]))
    produce_weather_slice(
        _SLICE,
        raw_fs_1,
        prepared_fs,
        _WEATHER_SOURCE,
        locations,
        _GRID_VERSION,
    )

    # Run 2: both grid points' raw files present.
    raw_fs_2 = FakeFileSystem()
    for grid_point in grid_points:
        _write_raw(raw_fs_2, grid_point, date_range, _metadata(grid_point))
    outcome = produce_weather_slice(
        _SLICE,
        raw_fs_2,
        prepared_fs,
        _WEATHER_SOURCE,
        locations,
        _GRID_VERSION,
    )

    assert outcome.status == 'completed'
    path = weather_partition_path(
        _SLICE.start_date.isoformat(),
        _SLICE.end_date.isoformat(),
        _SLICE.grid_point_sample_index,
        _GRID_VERSION,
    )
    df = pd.read_csv(io.StringIO(prepared_fs.read_text(path)))
    assert {
        (lat, lon) for lat, lon in zip(df['latitude'], df['longitude'], strict=False)
    } == {
        (float(gp.location.latitude), float(gp.location.longitude))
        for gp in grid_points
    }
