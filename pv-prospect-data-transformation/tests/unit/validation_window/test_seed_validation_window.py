"""Tests for seed_validation_window."""

import json
from datetime import datetime, timezone

import pandas as pd
from pv_prospect.data_transformation.processing.validation_window import (
    WINDOW_COLUMNS,
    WINDOW_CSV,
    WINDOW_MANIFEST,
    read_window,
    seed_validation_window,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem

_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)


def _prepared_fs(rows: list[list]) -> FakeFileSystem:
    header = 'time,temperature,plane_of_array_irradiance,power,power_max'
    data = '\n'.join(','.join(str(v) for v in r) for r in rows)
    csv = header + '\n' + data + '\n'
    return FakeFileSystem(
        files={'pv/89665/pv_89665_2026-01-01_2026-06-04.csv': csv}
    )


def test_written_artifact_has_correct_schema() -> None:
    prepared_fs = _prepared_fs([
        ['2026-04-01', 10.0, 100.0, 500.0, 4500.0],
        ['2026-04-02', 11.0, 110.0, 510.0, 4500.0],
    ])
    window_fs = FakeFileSystem()
    seed_validation_window(prepared_fs, window_fs, 90, now=_NOW)

    assert window_fs.exists(WINDOW_CSV)
    result = pd.read_csv(pd.io.common.StringIO(window_fs.read_text(WINDOW_CSV)))
    assert list(result.columns) == WINDOW_COLUMNS


def test_written_window_is_readable_by_read_window() -> None:
    prepared_fs = _prepared_fs([['2026-04-01', 10.0, 100.0, 500.0, 4500.0]])
    window_fs = FakeFileSystem()
    seed_validation_window(prepared_fs, window_fs, 90, now=_NOW)

    window = read_window(window_fs)
    assert window is not None
    assert list(window.columns) == WINDOW_COLUMNS
    assert len(window) == 1


def test_trims_to_window_days() -> None:
    # max row is 2026-06-04; cutoff = 2026-06-04 - 2d = 2026-06-02
    prepared_fs = _prepared_fs([
        ['2026-06-01', 10.0, 100.0, 500.0, 4500.0],  # outside 2-day window → dropped
        ['2026-06-03', 10.0, 100.0, 500.0, 4500.0],  # within 2-day window → kept
        ['2026-06-04', 10.0, 100.0, 500.0, 4500.0],  # max → kept
    ])
    window_fs = FakeFileSystem()
    seed_validation_window(prepared_fs, window_fs, 2, now=_NOW)

    result = pd.read_csv(pd.io.common.StringIO(window_fs.read_text(WINDOW_CSV)))
    assert len(result) == 2


def test_manifest_is_valid_json() -> None:
    prepared_fs = _prepared_fs([['2026-04-01', 10.0, 100.0, 500.0, 4500.0]])
    window_fs = FakeFileSystem()
    seed_validation_window(prepared_fs, window_fs, 90, now=_NOW)

    manifest = json.loads(window_fs.read_text(WINDOW_MANIFEST))
    assert manifest['window_days'] == 90
    assert '89665' in manifest['row_counts']
