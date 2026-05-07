"""Tests for assemble_prepared_pv."""

import io

import pandas as pd
from pv_prospect.data_transformation.processing import PV_COLUMNS, assemble_prepared_pv

from tests.unit.helpers.fake_file_system import FakeFileSystem


def _batch_csv(rows: list[list[object]]) -> str:
    header = 'time,temperature,plane_of_array_irradiance,power'
    data = '\n'.join(','.join(str(v) for v in row) for row in rows)
    return header + '\n' + data + '\n'


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
