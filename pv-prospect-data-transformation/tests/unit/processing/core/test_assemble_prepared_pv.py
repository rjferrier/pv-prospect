"""Tests for assemble_prepared_pv."""

import io

import pandas as pd
from pv_prospect.data_transformation.processing import (
    PV_COLUMNS,
    PreparedBatchCollector,
    assemble_prepared_pv,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem


def _batch_csv(rows: list[list[object]]) -> str:
    header = 'time,temperature,plane_of_array_irradiance,power'
    data = '\n'.join(','.join(str(v) for v in row) for row in rows)
    return header + '\n' + data + '\n'


def _batch_frame(rows: list[list[object]]) -> pd.DataFrame:
    """A prepared frame as the in-memory collector receives it from
    prepare_pv: the 'time' column is datetime64, not str."""
    frame = pd.read_csv(io.StringIO(_batch_csv(rows)))
    frame['time'] = pd.to_datetime(frame['time'])
    return frame


def test_merges_batches_into_per_system_master() -> None:
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv([['2026-01-15', 8.5, 300.0, 1500.0]]),
            'pv/89665_20260116.csv': _batch_csv([['2026-01-16', 9.0, 310.0, 1600.0]]),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    assert prepared_fs.exists('pv/89665.csv')
    result = pd.read_csv(io.StringIO(prepared_fs.read_text('pv/89665.csv')))
    assert len(result) == 2
    assert list(result.columns) == PV_COLUMNS


def test_deduplicates_on_time() -> None:
    existing_master = pd.DataFrame(
        {
            'time': ['2026-01-15'],
            'temperature': [7.0],
            'plane_of_array_irradiance': [280.0],
            'power': [1400.0],
        }
    )
    prepared_fs = FakeFileSystem(
        binary_files={
            'pv/89665.csv': existing_master.to_csv(index=False).encode('utf-8'),
        }
    )
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv([['2026-01-15', 8.5, 300.0, 1500.0]]),
        }
    )

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('pv/89665.csv')))
    assert len(result) == 1
    assert result['power'].iloc[0] == 1500.0  # batch value wins


def test_deletes_only_matching_system_batches() -> None:
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv([['2026-01-15', 8.5, 300.0, 1500.0]]),
            'pv/12345_20260115.csv': _batch_csv([['2026-01-15', 9.0, 310.0, 1600.0]]),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    assert not batches_fs.exists('pv/89665_20260115.csv')
    assert batches_fs.exists('pv/12345_20260115.csv')


def test_no_batches_is_noop() -> None:
    batches_fs = FakeFileSystem()
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    assert not prepared_fs.exists('pv/89665.csv')


def test_collector_path_merges_frames_into_master() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(89665, _batch_frame([['2026-01-15', 8.5, 300.0, 1500.0]]))
    collector.add_pv(89665, _batch_frame([['2026-01-16', 9.0, 310.0, 1600.0]]))
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('pv/89665.csv')))
    assert len(result) == 2
    assert list(result.columns) == PV_COLUMNS


def test_collector_path_drains_only_the_requested_system() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(89665, _batch_frame([['2026-01-15', 8.5, 300.0, 1500.0]]))
    collector.add_pv(12345, _batch_frame([['2026-01-15', 9.0, 310.0, 1600.0]]))
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('pv/89665.csv')))
    assert result['power'].tolist() == [1500.0]
    assert not prepared_fs.exists('pv/12345.csv')


def test_collector_path_leaves_batch_files_untouched() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(89665, _batch_frame([['2026-01-16', 9.0, 310.0, 1600.0]]))
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv([['2026-01-15', 8.5, 300.0, 1500.0]]),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(batches_fs, prepared_fs, 89665, collector)

    assert batches_fs.exists('pv/89665_20260115.csv')
    result = pd.read_csv(io.StringIO(prepared_fs.read_text('pv/89665.csv')))
    assert result['time'].tolist() == ['2026-01-16']


def test_collector_path_with_no_frames_is_noop() -> None:
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, PreparedBatchCollector())

    assert not prepared_fs.exists('pv/89665.csv')


def test_collector_path_merges_into_existing_string_master() -> None:
    """Regression: the collector frames carry datetime64 'time' from
    prepare_pv, while the cumulative master is read back from CSV as
    strings. assemble must normalise both, else sort_values raises
    TypeError comparing Timestamp with str."""
    existing_master = pd.DataFrame(
        {
            'time': ['2026-01-14'],
            'temperature': [7.0],
            'plane_of_array_irradiance': [280.0],
            'power': [1400.0],
        }
    )
    prepared_fs = FakeFileSystem(
        binary_files={
            'pv/89665.csv': existing_master.to_csv(index=False).encode('utf-8'),
        }
    )
    collector = PreparedBatchCollector()
    collector.add_pv(89665, _batch_frame([['2026-01-15', 8.5, 300.0, 1500.0]]))

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('pv/89665.csv')))
    assert len(result) == 2
    assert sorted(result['time'].tolist()) == ['2026-01-14', '2026-01-15']


def test_collector_path_merges_into_master_with_mixed_time_formats() -> None:
    """Regression: a master's 'time' column can mix bare-date and
    full-datetime strings — to_csv renders a midnight Timestamp as a bare
    date but one with a time component in full, so a master appended to
    over several runs accumulates both. The merge must parse every row as
    ISO 8601 rather than inferring one format from the first row."""
    master_csv = (
        'time,temperature,plane_of_array_irradiance,power\n'
        '2026-01-14 00:00:00,7.0,280.0,1400.0\n'
        '2026-01-15,7.5,285.0,1450.0\n'
    )
    prepared_fs = FakeFileSystem(
        binary_files={'pv/89665.csv': master_csv.encode('utf-8')},
    )
    collector = PreparedBatchCollector()
    collector.add_pv(89665, _batch_frame([['2026-01-16', 9.0, 310.0, 1600.0]]))

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('pv/89665.csv')))
    assert len(result) == 3
    # The merged master is re-healed to a single serialisation.
    assert sorted(result['time'].tolist()) == ['2026-01-14', '2026-01-15', '2026-01-16']


def test_collector_path_deduplicates_against_string_master() -> None:
    """A re-prepared day in the (datetime64) collector must replace the
    matching (string) master row — drop_duplicates only works once both
    'time' columns are normalised to the same dtype."""
    existing_master = pd.DataFrame(
        {
            'time': ['2026-01-15'],
            'temperature': [7.0],
            'plane_of_array_irradiance': [280.0],
            'power': [1400.0],
        }
    )
    prepared_fs = FakeFileSystem(
        binary_files={
            'pv/89665.csv': existing_master.to_csv(index=False).encode('utf-8'),
        }
    )
    collector = PreparedBatchCollector()
    collector.add_pv(89665, _batch_frame([['2026-01-15', 8.5, 300.0, 1500.0]]))

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, collector)

    result = pd.read_csv(io.StringIO(prepared_fs.read_text('pv/89665.csv')))
    assert len(result) == 1
    assert result['power'].iloc[0] == 1500.0  # freshly prepared value wins
