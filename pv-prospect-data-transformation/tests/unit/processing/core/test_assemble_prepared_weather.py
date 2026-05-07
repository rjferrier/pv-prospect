"""Tests for assemble_prepared_weather."""

import io

import pandas as pd
from pv_prospect.data_transformation.processing import (
    WEATHER_COLUMNS,
    assemble_prepared_weather,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem


def _batch_csv(rows: list[list[object]]) -> str:
    header = 'latitude,longitude,elevation,time,temperature,direct_normal_irradiance,diffuse_radiation'
    data = '\n'.join(','.join(str(v) for v in row) for row in rows)
    return header + '\n' + data + '\n'


def test_merges_multiple_batches_into_master() -> None:
    batches_fs = FakeFileSystem(
        files={
            'weather/loc1_20260115.csv': _batch_csv(
                [[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]]
            ),
            'weather/loc1_20260116.csv': _batch_csv(
                [[50.49, -3.54, 120.0, '2026-01-16', 9.0, 210.0, 65.0]]
            ),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(batches_fs, prepared_fs)

    assert prepared_fs.exists('weather.csv')
    result = pd.read_csv(io.StringIO(prepared_fs.read_text('weather.csv')))
    assert len(result) == 2
    assert list(result.columns) == WEATHER_COLUMNS


def test_appends_to_existing_master() -> None:
    existing_master = pd.DataFrame(
        {
            'latitude': [50.49],
            'longitude': [-3.54],
            'elevation': [120.0],
            'time': ['2026-01-14'],
            'temperature': [7.0],
            'direct_normal_irradiance': [180.0],
            'diffuse_radiation': [55.0],
        }
    )
    prepared_fs = FakeFileSystem(
        binary_files={
            'weather.csv': existing_master.to_csv(index=False).encode('utf-8'),
        }
    )
    batches_fs = FakeFileSystem(
        files={
            'weather/loc1_20260115.csv': _batch_csv(
                [[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]]
            ),
        }
    )

    assemble_prepared_weather(batches_fs, prepared_fs)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('weather.csv')))
    assert len(result) == 2


def test_deduplicates_on_lat_lon_time() -> None:
    existing_master = pd.DataFrame(
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
        binary_files={
            'weather.csv': existing_master.to_csv(index=False).encode('utf-8'),
        }
    )
    batches_fs = FakeFileSystem(
        files={
            'weather/loc1_20260115.csv': _batch_csv(
                [[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]]
            ),
        }
    )

    assemble_prepared_weather(batches_fs, prepared_fs)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('weather.csv')))
    assert len(result) == 1
    assert result['temperature'].iloc[0] == 8.5  # batch value wins (keep='last')


def test_deletes_batches_after_assembly() -> None:
    batches_fs = FakeFileSystem(
        files={
            'weather/loc1_20260115.csv': _batch_csv(
                [[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]]
            ),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(batches_fs, prepared_fs)

    assert not batches_fs.exists('weather/loc1_20260115.csv')


def test_no_batches_is_noop() -> None:
    batches_fs = FakeFileSystem()
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(batches_fs, prepared_fs)

    assert not prepared_fs.exists('weather.csv')
