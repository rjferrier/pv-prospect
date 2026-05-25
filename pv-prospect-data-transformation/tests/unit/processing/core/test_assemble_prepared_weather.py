"""Tests for assemble_prepared_weather."""

import io

import pandas as pd
from pv_prospect.data_transformation.processing import (
    WEATHER_COLUMNS,
    PreparedBatchCollector,
    assemble_prepared_weather,
    weather_partition_path,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem

_START = '2026-01-15'
_END = '2026-01-22'
_SAMPLE = 7
_GRID_VERSION = 0
_PARTITION_PATH = weather_partition_path(_START, _END, _SAMPLE, _GRID_VERSION)


def _batch_csv(rows: list[list[object]]) -> str:
    header = (
        'latitude,longitude,elevation,time,temperature,'
        'direct_normal_irradiance,diffuse_radiation'
    )
    data = '\n'.join(','.join(str(v) for v in row) for row in rows)
    return header + '\n' + data + '\n'


def _batch_frame(rows: list[list[object]]) -> pd.DataFrame:
    """A prepared frame as the in-memory collector receives it from
    prepare_weather: the 'time' column is datetime64, not str."""
    frame = pd.read_csv(io.StringIO(_batch_csv(rows)))
    frame['time'] = pd.to_datetime(frame['time'])
    return frame


def test_partition_filename_encodes_window_grid_version_and_sample_index() -> None:
    assert (
        weather_partition_path('2026-05-01', '2026-05-15', 7, 0)
        == 'weather/weather_2026-05-01_2026-05-15_0-07.csv'
    )


def test_writes_one_partition_file_for_the_sample_window() -> None:
    collector = PreparedBatchCollector()
    collector.add_weather(
        _SAMPLE,
        _START,
        _END,
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]]),
    )
    collector.add_weather(
        _SAMPLE,
        _START,
        _END,
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-16', 9.0, 210.0, 65.0]]),
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(
        prepared_fs, collector, _SAMPLE, _START, _END, _GRID_VERSION
    )

    result = pd.read_csv(io.StringIO(prepared_fs.read_text(_PARTITION_PATH)))
    assert len(result) == 2
    assert list(result.columns) == WEATHER_COLUMNS


def test_only_writes_the_requested_sample_slice() -> None:
    """A second sample's frames sit untouched in the collector — assemble
    writes only the one requested ``(sample, window)``."""
    collector = PreparedBatchCollector()
    collector.add_weather(
        7,
        _START,
        _END,
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]]),
    )
    collector.add_weather(
        8,
        _START,
        _END,
        _batch_frame([[51.00, -4.00, 90.0, '2026-01-15', 7.0, 180.0, 55.0]]),
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(prepared_fs, collector, 7, _START, _END, _GRID_VERSION)

    assert prepared_fs.exists(weather_partition_path(_START, _END, 7, _GRID_VERSION))
    assert not prepared_fs.exists(
        weather_partition_path(_START, _END, 8, _GRID_VERSION)
    )


def test_merges_into_an_existing_partition_file() -> None:
    """A re-transform of the same (sample, window) merges into the
    partition file already present, rather than orphaning it."""
    existing = pd.DataFrame(
        {
            'latitude': [50.49],
            'longitude': [-3.54],
            'elevation': [120.0],
            'time': ['2026-01-15'],
            'temperature': [7.0],
            'direct_normal_irradiance': [180.0],
            'diffuse_radiation': [55.0],
        }
    )
    prepared_fs = FakeFileSystem(
        binary_files={_PARTITION_PATH: existing.to_csv(index=False).encode('utf-8')}
    )
    collector = PreparedBatchCollector()
    collector.add_weather(
        _SAMPLE,
        _START,
        _END,
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-16', 9.0, 210.0, 65.0]]),
    )

    assemble_prepared_weather(
        prepared_fs, collector, _SAMPLE, _START, _END, _GRID_VERSION
    )

    result = pd.read_csv(io.StringIO(prepared_fs.read_text(_PARTITION_PATH)))
    assert sorted(result['time'].tolist()) == ['2026-01-15', '2026-01-16']


def test_deduplicates_on_lat_lon_time_with_fresh_rows_winning() -> None:
    existing = pd.DataFrame(
        {
            'latitude': [50.49],
            'longitude': [-3.54],
            'elevation': [120.0],
            'time': ['2026-01-15'],
            'temperature': [7.0],
            'direct_normal_irradiance': [180.0],
            'diffuse_radiation': [55.0],
        }
    )
    prepared_fs = FakeFileSystem(
        binary_files={_PARTITION_PATH: existing.to_csv(index=False).encode('utf-8')}
    )
    collector = PreparedBatchCollector()
    collector.add_weather(
        _SAMPLE,
        _START,
        _END,
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]]),
    )

    assemble_prepared_weather(
        prepared_fs, collector, _SAMPLE, _START, _END, _GRID_VERSION
    )

    result = pd.read_csv(io.StringIO(prepared_fs.read_text(_PARTITION_PATH)))
    assert len(result) == 1
    assert result['temperature'].iloc[0] == 8.5  # freshly prepared value wins


def test_merges_partition_with_mixed_time_formats() -> None:
    """Regression: a partition file's 'time' column can mix bare-date and
    full-datetime strings — to_csv renders a midnight Timestamp as a bare
    date but one with a time component in full. The merge must parse every
    row as ISO 8601 rather than inferring one format from the first row."""
    partition_csv = (
        'latitude,longitude,elevation,time,temperature,'
        'direct_normal_irradiance,diffuse_radiation\n'
        '50.49,-3.54,120.0,2026-01-15 00:00:00,7.0,180.0,55.0\n'
        '50.49,-3.54,120.0,2026-01-16,7.5,185.0,57.0\n'
    )
    prepared_fs = FakeFileSystem(
        binary_files={_PARTITION_PATH: partition_csv.encode('utf-8')}
    )
    collector = PreparedBatchCollector()
    collector.add_weather(
        _SAMPLE,
        _START,
        _END,
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-17', 9.0, 210.0, 65.0]]),
    )

    assemble_prepared_weather(
        prepared_fs, collector, _SAMPLE, _START, _END, _GRID_VERSION
    )

    result = pd.read_csv(io.StringIO(prepared_fs.read_text(_PARTITION_PATH)))
    assert sorted(result['time'].tolist()) == [
        '2026-01-15',
        '2026-01-16',
        '2026-01-17',
    ]


def test_no_frames_for_requested_slice_is_noop() -> None:
    """When the collector has no rows for the requested (sample, window)
    the assemble call leaves the corpus untouched."""
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(
        prepared_fs,
        PreparedBatchCollector(),
        _SAMPLE,
        _START,
        _END,
        _GRID_VERSION,
    )

    assert prepared_fs.list_files('weather', '*.csv') == []
