"""Tests for run_maintain_validation_window."""

import json
from datetime import datetime, timezone

import pytest
from pv_prospect.data_transformation.processing.validation_window import (
    WINDOW_COLUMNS,
    WINDOW_CSV,
    WINDOW_MANIFEST,
    ValidationWindowNotSeededError,
    build_manifest,
    run_maintain_validation_window,
    write_window,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem

_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)


def _prepared_fs(rows: list[list]) -> FakeFileSystem:
    """FakeFileSystem with one PV partition containing the given rows."""
    header = 'time,temperature,plane_of_array_irradiance,power,power_max'
    data = '\n'.join(','.join(str(v) for v in r) for r in rows)
    csv = header + '\n' + data + '\n'
    return FakeFileSystem(
        files={'pv/89665/pv_89665_2026-01-01_2026-06-04.csv': csv}
    )


def _seed_window_fs(rows: list[list]) -> FakeFileSystem:
    """FakeFileSystem pre-seeded with a window artifact."""
    import pandas as pd

    df = pd.DataFrame(rows, columns=WINDOW_COLUMNS)
    df['time'] = pd.to_datetime(df['time'])
    window_fs = FakeFileSystem()
    manifest = build_manifest(df, 90, now=_NOW)
    write_window(window_fs, df, manifest)
    return window_fs


def test_merges_incr_into_existing_window() -> None:
    window_fs = _seed_window_fs([
        [89665, '2026-04-01', 10.0, 100.0, 500.0, 4500.0],
    ])
    prepared_fs = _prepared_fs([
        ['2026-04-01', 10.0, 100.0, 500.0, 4500.0],  # already in window
        ['2026-04-02', 11.0, 110.0, 510.0, 4500.0],  # new row
    ])
    run_maintain_validation_window(prepared_fs, window_fs, 90, now=_NOW)

    import io

    import pandas as pd

    result = pd.read_csv(io.StringIO(window_fs.read_text(WINDOW_CSV)))
    assert len(result) == 2
    assert list(result.columns) == WINDOW_COLUMNS


def test_writes_manifest_with_correct_fields() -> None:
    window_fs = _seed_window_fs([
        [89665, '2026-04-01', 10.0, 100.0, 500.0, 4500.0],
    ])
    run_maintain_validation_window(window_fs=window_fs, prepared_fs=FakeFileSystem(),
                                   days=90, now=_NOW)

    manifest = json.loads(window_fs.read_text(WINDOW_MANIFEST))
    assert manifest['window_days'] == 90
    assert manifest['updated_at'] == '2026-06-04T12:00:00+00:00'
    assert '89665' in manifest['row_counts']


def test_raises_if_not_seeded() -> None:
    with pytest.raises(ValidationWindowNotSeededError):
        run_maintain_validation_window(
            FakeFileSystem(), FakeFileSystem(), days=90
        )
