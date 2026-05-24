"""Tests for assemble_prepared_pv."""

import io

import pandas as pd
from pv_prospect.data_transformation.processing import (
    PV_COLUMNS,
    PreparedBatchCollector,
    assemble_prepared_pv,
    pv_partition_path,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem

# 2026-01-15 (Thu) and 2026-01-16 (Fri) share the ISO week starting
# Mon 2026-01-12; 2026-01-20 (Tue) falls in the next week (Mon 2026-01-19).
_BACKFILL_WINDOW = ('2026-01-15', '2026-02-12')


def _batch_csv(rows: list[list[object]]) -> str:
    header = 'time,temperature,plane_of_array_irradiance,power,power_max'
    data = '\n'.join(','.join(str(v) for v in row) for row in rows)
    return header + '\n' + data + '\n'


def _batch_frame(rows: list[list[object]]) -> pd.DataFrame:
    """A prepared frame as the in-memory collector receives it from
    prepare_pv: the 'time' column is datetime64, not str."""
    frame = pd.read_csv(io.StringIO(_batch_csv(rows)))
    frame['time'] = pd.to_datetime(frame['time'])
    return frame


# ---------------------------------------------------------------------------
# Collector path (the in-container backfill)
# ---------------------------------------------------------------------------


def test_collector_path_writes_one_file_named_for_the_actual_data_span() -> None:
    """The window key is the 28-day backfill unit; the file is named for
    the span the data actually covers, not the nominal window."""
    collector = PreparedBatchCollector()
    collector.add_pv(
        89665,
        *_BACKFILL_WINDOW,
        _batch_frame([['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]),
    )
    collector.add_pv(
        89665,
        *_BACKFILL_WINDOW,
        _batch_frame([['2026-01-16', 9.0, 310.0, 1600.0, 4800.0]]),
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, collector)

    path = pv_partition_path(89665, '2026-01-15', '2026-01-17')
    result = pd.read_csv(io.StringIO(prepared_fs.read_text(path)))
    assert len(result) == 2
    assert list(result.columns) == PV_COLUMNS


def test_collector_path_separate_windows_become_separate_files() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(
        89665,
        '2026-01-15',
        '2026-02-12',
        _batch_frame([['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]),
    )
    collector.add_pv(
        89665,
        '2026-03-01',
        '2026-03-29',
        _batch_frame([['2026-03-02', 9.0, 310.0, 1600.0, 4800.0]]),
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, collector)

    assert prepared_fs.exists(pv_partition_path(89665, '2026-01-15', '2026-01-16'))
    assert prepared_fs.exists(pv_partition_path(89665, '2026-03-02', '2026-03-03'))


def test_collector_path_drains_only_the_requested_system() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(
        89665,
        *_BACKFILL_WINDOW,
        _batch_frame([['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]),
    )
    collector.add_pv(
        12345,
        *_BACKFILL_WINDOW,
        _batch_frame([['2026-01-15', 9.0, 310.0, 1600.0, 4800.0]]),
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, collector)

    assert prepared_fs.exists(pv_partition_path(89665, '2026-01-15', '2026-01-16'))
    assert prepared_fs.list_files('pv/12345', '*.csv') == []


def test_collector_path_with_no_frames_is_noop() -> None:
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665, PreparedBatchCollector())

    assert prepared_fs.list_files('pv/89665', '*.csv') == []


def test_collector_path_leaves_batch_files_untouched() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(
        89665,
        *_BACKFILL_WINDOW,
        _batch_frame([['2026-01-16', 9.0, 310.0, 1600.0, 4800.0]]),
    )
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv(
                [['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]
            )
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(batches_fs, prepared_fs, 89665, collector)

    assert batches_fs.exists('pv/89665_20260115.csv')


# ---------------------------------------------------------------------------
# Batch-CSV path (the daily transform)
# ---------------------------------------------------------------------------


def test_daily_path_merges_batches_into_one_iso_week_file() -> None:
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv(
                [['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]
            ),
            'pv/89665_20260116.csv': _batch_csv(
                [['2026-01-16', 9.0, 310.0, 1600.0, 4800.0]]
            ),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    path = pv_partition_path(89665, '2026-01-15', '2026-01-17')
    result = pd.read_csv(io.StringIO(prepared_fs.read_text(path)))
    assert len(result) == 2
    assert list(result.columns) == PV_COLUMNS


def test_daily_path_grows_and_renames_the_open_week_file() -> None:
    """A new day joins the existing week file, which is then renamed to
    cover the wider span it now holds; the old name is removed."""
    existing = pd.DataFrame(
        {
            'time': ['2026-01-12'],
            'temperature': [7.0],
            'plane_of_array_irradiance': [280.0],
            'power': [1400.0],
        }
    )
    old_path = pv_partition_path(89665, '2026-01-12', '2026-01-13')
    prepared_fs = FakeFileSystem(
        binary_files={old_path: existing.to_csv(index=False).encode('utf-8')}
    )
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv(
                [['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]
            )
        }
    )

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    new_path = pv_partition_path(89665, '2026-01-12', '2026-01-16')
    assert prepared_fs.exists(new_path)
    assert not prepared_fs.exists(old_path)
    result = pd.read_csv(io.StringIO(prepared_fs.read_text(new_path)))
    assert len(result) == 2


def test_daily_path_splits_batches_across_iso_weeks() -> None:
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv(
                [['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]
            ),
            'pv/89665_20260120.csv': _batch_csv(
                [['2026-01-20', 9.0, 310.0, 1600.0, 4800.0]]
            ),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    assert prepared_fs.exists(pv_partition_path(89665, '2026-01-15', '2026-01-16'))
    assert prepared_fs.exists(pv_partition_path(89665, '2026-01-20', '2026-01-21'))


def test_daily_path_deduplicates_on_time_with_fresh_rows_winning() -> None:
    old_path = pv_partition_path(89665, '2026-01-12', '2026-01-16')
    existing = pd.DataFrame(
        {
            'time': ['2026-01-15'],
            'temperature': [7.0],
            'plane_of_array_irradiance': [280.0],
            'power': [1400.0],
        }
    )
    prepared_fs = FakeFileSystem(
        binary_files={old_path: existing.to_csv(index=False).encode('utf-8')}
    )
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv(
                [['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]
            )
        }
    )

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    new_path = pv_partition_path(89665, '2026-01-15', '2026-01-16')
    result = pd.read_csv(io.StringIO(prepared_fs.read_text(new_path)))
    assert len(result) == 1
    assert result['power'].iloc[0] == 1500.0  # freshly prepared value wins


def test_daily_path_deletes_only_the_systems_consumed_batches() -> None:
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv(
                [['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]
            ),
            'pv/12345_20260115.csv': _batch_csv(
                [['2026-01-15', 9.0, 310.0, 1600.0, 4800.0]]
            ),
        }
    )
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    assert not batches_fs.exists('pv/89665_20260115.csv')
    assert batches_fs.exists('pv/12345_20260115.csv')


def test_daily_path_no_batches_is_noop() -> None:
    prepared_fs = FakeFileSystem()

    assemble_prepared_pv(FakeFileSystem(), prepared_fs, 89665)

    assert prepared_fs.list_files('pv/89665', '*.csv') == []


def test_daily_path_ignores_a_stray_non_partition_file() -> None:
    """A file in the system directory that does not match the partition
    name pattern is skipped, not a crash, during open-file discovery."""
    prepared_fs = FakeFileSystem(files={'pv/89665/readme.csv': 'not a partition\n'})
    batches_fs = FakeFileSystem(
        files={
            'pv/89665_20260115.csv': _batch_csv(
                [['2026-01-15', 8.5, 300.0, 1500.0, 4500.0]]
            )
        }
    )

    assemble_prepared_pv(batches_fs, prepared_fs, 89665)

    assert prepared_fs.exists(pv_partition_path(89665, '2026-01-15', '2026-01-16'))
