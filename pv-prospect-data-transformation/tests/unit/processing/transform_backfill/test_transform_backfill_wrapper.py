"""Tests for the transform-backfill wrapper.

The cursor / plan-commit logic itself is tested in pv-prospect-etl. These
tests cover only what the wrapper adds: the right paths are used per
scope and the scope-specific default window is applied when none is given.
"""

from datetime import date, timedelta

import pytest
from pv_prospect.data_transformation.processing import (
    commit_transform_backfill,
    paths_for,
    plan_transform_backfill,
)
from pv_prospect.etl import BackfillScope, deserialize_plan, load_cursor

from tests.unit.helpers.fake_file_system import FakeFileSystem


@pytest.mark.parametrize(
    'scope, expected_window_days',
    [
        (BackfillScope.PV_SITES, 28),
        (BackfillScope.WEATHER_GRID, 14),
    ],
)
def test_plan_uses_default_window_for_scope(
    scope: BackfillScope, expected_window_days: int
) -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan = plan_transform_backfill(scope, today, fs)

    assert plan.start_date == today - timedelta(days=expected_window_days)
    assert plan.end_date == today


def test_paths_differ_per_scope() -> None:
    pv_paths = paths_for(BackfillScope.PV_SITES)
    weather_paths = paths_for(BackfillScope.WEATHER_GRID)

    assert pv_paths.cursor != weather_paths.cursor
    assert pv_paths.manifest != weather_paths.manifest


def test_plan_writes_manifest_at_scope_specific_path() -> None:
    fs = FakeFileSystem()

    plan_transform_backfill(BackfillScope.PV_SITES, date(2026, 5, 6), fs)

    assert fs.exists(paths_for(BackfillScope.PV_SITES).manifest)
    assert not fs.exists(paths_for(BackfillScope.WEATHER_GRID).manifest)


def test_commit_advances_cursor_at_scope_specific_path() -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_transform_backfill(BackfillScope.WEATHER_GRID, today, fs)
    commit_transform_backfill(BackfillScope.WEATHER_GRID, fs)

    cursor = load_cursor(fs, paths_for(BackfillScope.WEATHER_GRID), today)
    assert cursor.next_end_date == today - timedelta(days=14)


def test_scopes_have_independent_cursors() -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_transform_backfill(BackfillScope.PV_SITES, today, fs)
    commit_transform_backfill(BackfillScope.PV_SITES, fs)

    pv_cursor = load_cursor(fs, paths_for(BackfillScope.PV_SITES), today)
    weather_cursor = load_cursor(fs, paths_for(BackfillScope.WEATHER_GRID), today)

    assert pv_cursor.next_end_date == today - timedelta(days=28)
    assert weather_cursor.next_end_date == today


def test_plan_round_trips_through_manifest() -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    returned = plan_transform_backfill(BackfillScope.PV_SITES, today, fs)
    persisted, _ = deserialize_plan(
        fs.read_text(paths_for(BackfillScope.PV_SITES).manifest)
    )

    assert returned == persisted
