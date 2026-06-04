"""Tests for read_prepared_pv."""

from pv_prospect.data_transformation.processing.validation_window import (
    WINDOW_COLUMNS,
    read_prepared_pv,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem


def _partition_csv(rows: list[list]) -> str:
    header = 'time,temperature,plane_of_array_irradiance,power,power_max'
    data = '\n'.join(','.join(str(v) for v in r) for r in rows)
    return header + '\n' + data + '\n'


def test_returns_window_columns_with_system_id() -> None:
    prepared_fs = FakeFileSystem(
        files={
            'pv/89665/pv_89665_2026-01-01_2026-01-29.csv': _partition_csv(
                [['2026-01-15', 10.0, 100.0, 500.0, 4500.0]]
            ),
        }
    )
    result = read_prepared_pv(prepared_fs)
    assert list(result.columns) == WINDOW_COLUMNS
    assert result.iloc[0]['system_id'] == 89665


def test_concatenates_multiple_sites() -> None:
    prepared_fs = FakeFileSystem(
        files={
            'pv/89665/pv_89665_2026-01-01_2026-01-29.csv': _partition_csv(
                [['2026-01-15', 10.0, 100.0, 500.0, 4500.0]]
            ),
            'pv/12345/pv_12345_2026-01-01_2026-01-29.csv': _partition_csv(
                [['2026-01-16', 11.0, 110.0, 600.0, 5000.0]]
            ),
        }
    )
    result = read_prepared_pv(prepared_fs)
    assert set(result['system_id']) == {89665, 12345}


def test_skips_stray_aggregate_files() -> None:
    # Stray aggregates at pv/<id>.csv (name lacks pv_ prefix) are excluded.
    prepared_fs = FakeFileSystem(
        files={
            'pv/89665.csv': _partition_csv(
                [['2026-01-15', 10.0, 100.0, 500.0, 4500.0]]
            ),
            'pv/89665/pv_89665_2026-01-01_2026-01-29.csv': _partition_csv(
                [['2026-01-15', 10.0, 100.0, 500.0, 4500.0]]
            ),
        }
    )
    result = read_prepared_pv(prepared_fs)
    assert len(result) == 1


def test_empty_filesystem_returns_empty_frame() -> None:
    result = read_prepared_pv(FakeFileSystem())
    assert result.empty
    assert list(result.columns) == WINDOW_COLUMNS
