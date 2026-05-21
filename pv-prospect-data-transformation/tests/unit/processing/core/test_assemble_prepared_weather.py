"""Tests for assemble_prepared_weather."""

import io

import pandas as pd
from pv_prospect.data_transformation.processing import (
    WEATHER_COLUMNS,
    PreparedBatchCollector,
    assemble_prepared_weather,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem


def _batch_csv(rows: list[list[object]]) -> str:
    header = 'latitude,longitude,elevation,time,temperature,direct_normal_irradiance,diffuse_radiation'
    data = '\n'.join(','.join(str(v) for v in row) for row in rows)
    return header + '\n' + data + '\n'


def _batch_frame(rows: list[list[object]]) -> pd.DataFrame:
    """A prepared frame as the in-memory collector receives it from
    prepare_weather: the 'time' column is datetime64, not str."""
    frame = pd.read_csv(io.StringIO(_batch_csv(rows)))
    frame['time'] = pd.to_datetime(frame['time'])
    return frame


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


def test_collector_path_merges_frames_into_master() -> None:
    collector = PreparedBatchCollector()
    collector.add_weather(
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]])
    )
    collector.add_weather(
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-16', 9.0, 210.0, 65.0]])
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(FakeFileSystem(), prepared_fs, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('weather.csv')))
    assert len(result) == 2
    assert list(result.columns) == WEATHER_COLUMNS


def test_collector_path_leaves_batch_files_untouched() -> None:
    collector = PreparedBatchCollector()
    collector.add_weather(
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-16', 9.0, 210.0, 65.0]])
    )
    batches_fs = FakeFileSystem(
        files={
            'weather/loc1_20260115.csv': _batch_csv(
                [[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]]
            ),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(batches_fs, prepared_fs, collector)

    # The GCS batch file is neither merged into the master nor deleted.
    assert batches_fs.exists('weather/loc1_20260115.csv')
    result = pd.read_csv(io.StringIO(prepared_fs.read_text('weather.csv')))
    assert result['time'].tolist() == ['2026-01-16']


def test_collector_path_keeps_master_data_absent_from_collector() -> None:
    """A prepare unit skipped on a re-run is absent from the in-memory
    collector, but assemble still includes it via the cumulative master."""
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
    collector = PreparedBatchCollector()
    collector.add_weather(
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]])
    )

    assemble_prepared_weather(FakeFileSystem(), prepared_fs, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('weather.csv')))
    assert sorted(result['time'].tolist()) == ['2026-01-14', '2026-01-15']


def test_collector_path_deduplicates_against_string_master() -> None:
    """A re-prepared day in the (datetime64) collector must replace the
    matching (string) master row — drop_duplicates only works once both
    'time' columns are normalised to the same dtype."""
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
    collector = PreparedBatchCollector()
    collector.add_weather(
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-15', 8.5, 200.0, 60.0]])
    )

    assemble_prepared_weather(FakeFileSystem(), prepared_fs, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('weather.csv')))
    assert len(result) == 1
    assert result['temperature'].iloc[0] == 8.5  # freshly prepared value wins


def test_collector_path_merges_into_master_with_mixed_time_formats() -> None:
    """Regression: a master's 'time' column can mix bare-date and
    full-datetime strings — to_csv renders a midnight Timestamp as a bare
    date but one with a time component in full, so a master appended to
    over several runs accumulates both. The merge must parse every row as
    ISO 8601 rather than inferring one format from the first row."""
    master_csv = (
        'latitude,longitude,elevation,time,temperature,'
        'direct_normal_irradiance,diffuse_radiation\n'
        '50.49,-3.54,120.0,2026-01-14 00:00:00,7.0,180.0,55.0\n'
        '50.49,-3.54,120.0,2026-01-15,7.5,185.0,57.0\n'
    )
    prepared_fs = FakeFileSystem(
        binary_files={'weather.csv': master_csv.encode('utf-8')},
    )
    collector = PreparedBatchCollector()
    collector.add_weather(
        _batch_frame([[50.49, -3.54, 120.0, '2026-01-16', 9.0, 210.0, 65.0]])
    )

    assemble_prepared_weather(FakeFileSystem(), prepared_fs, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('weather.csv')))
    assert len(result) == 3
    # The merged master is re-healed to a single serialisation.
    assert sorted(result['time'].tolist()) == ['2026-01-14', '2026-01-15', '2026-01-16']


def test_collector_path_with_no_frames_is_noop() -> None:
    prepared_fs = FakeFileSystem()

    assemble_prepared_weather(FakeFileSystem(), prepared_fs, PreparedBatchCollector())

    assert not prepared_fs.exists('weather.csv')
