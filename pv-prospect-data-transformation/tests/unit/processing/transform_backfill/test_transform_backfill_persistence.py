"""Tests for transform-backfill cursor and plan persistence."""

from datetime import date, timedelta

import pytest
from pv_prospect.data_transformation.processing import (
    TransformBackfillCursor,
    TransformBackfillPlan,
    TransformBackfillScope,
    commit_transform_backfill,
    cursor_path,
    deserialize_cursor,
    deserialize_plan,
    load_cursor,
    manifest_path,
    plan_transform_backfill,
    serialize_cursor,
    serialize_plan,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem

_PLAN = TransformBackfillPlan(
    start_date=date(2026, 4, 8),
    end_date=date(2026, 5, 6),
)

_NEXT_CURSOR = TransformBackfillCursor(next_end_date=date(2026, 4, 8))


@pytest.fixture(params=list(TransformBackfillScope))
def scope(request: pytest.FixtureRequest) -> TransformBackfillScope:
    return request.param  # type: ignore[no-any-return]


def test_serialize_cursor_roundtrip() -> None:
    cursor = TransformBackfillCursor(next_end_date=date(2026, 5, 6))

    text = serialize_cursor(cursor)
    result = deserialize_cursor(text)

    assert result == cursor


def test_serialize_plan_roundtrip() -> None:
    text = serialize_plan(_PLAN, _NEXT_CURSOR)
    plan, next_cursor = deserialize_plan(text)

    assert plan == _PLAN
    assert next_cursor == _NEXT_CURSOR


def test_cursor_paths_differ_per_scope() -> None:
    pv_path = cursor_path(TransformBackfillScope.PV_SITES)
    weather_path = cursor_path(TransformBackfillScope.WEATHER_GRID)

    assert pv_path != weather_path


def test_manifest_paths_differ_per_scope() -> None:
    pv_path = manifest_path(TransformBackfillScope.PV_SITES)
    weather_path = manifest_path(TransformBackfillScope.WEATHER_GRID)

    assert pv_path != weather_path


def test_load_cursor_returns_initial_when_no_file(
    scope: TransformBackfillScope,
) -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    cursor = load_cursor(fs, scope, today)

    assert cursor.next_end_date == today


def test_load_cursor_returns_persisted_cursor(
    scope: TransformBackfillScope,
) -> None:
    fs = FakeFileSystem()
    fs.write_text(
        cursor_path(scope),
        serialize_cursor(TransformBackfillCursor(next_end_date=date(2026, 3, 1))),
    )

    cursor = load_cursor(fs, scope, date(2026, 5, 6))

    assert cursor.next_end_date == date(2026, 3, 1)


def test_plan_transform_backfill_writes_manifest(
    scope: TransformBackfillScope,
) -> None:
    fs = FakeFileSystem()

    plan_transform_backfill(scope, date(2026, 5, 6), fs)

    assert fs.exists(manifest_path(scope))


def test_plan_transform_backfill_does_not_advance_live_cursor(
    scope: TransformBackfillScope,
) -> None:
    fs = FakeFileSystem()

    plan_transform_backfill(scope, date(2026, 5, 6), fs)

    assert not fs.exists(cursor_path(scope))


def test_plan_transform_backfill_returns_plan_consistent_with_file(
    scope: TransformBackfillScope,
) -> None:
    fs = FakeFileSystem()

    returned = plan_transform_backfill(scope, date(2026, 5, 6), fs)
    persisted, _ = deserialize_plan(fs.read_text(manifest_path(scope)))

    assert returned == persisted


def test_commit_transform_backfill_promotes_next_cursor(
    scope: TransformBackfillScope,
) -> None:
    fs = FakeFileSystem()
    fs.write_text(manifest_path(scope), serialize_plan(_PLAN, _NEXT_CURSOR))

    committed = commit_transform_backfill(scope, fs)

    assert committed == _NEXT_CURSOR
    assert load_cursor(fs, scope, date(2026, 5, 6)) == _NEXT_CURSOR


def test_plan_then_commit_advances_live_cursor_by_pv_sites_window() -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_transform_backfill(TransformBackfillScope.PV_SITES, today, fs)
    commit_transform_backfill(TransformBackfillScope.PV_SITES, fs)

    cursor = load_cursor(fs, TransformBackfillScope.PV_SITES, today)
    assert cursor.next_end_date == today - timedelta(days=28)


def test_plan_then_commit_advances_live_cursor_by_weather_grid_window() -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_transform_backfill(TransformBackfillScope.WEATHER_GRID, today, fs)
    commit_transform_backfill(TransformBackfillScope.WEATHER_GRID, fs)

    cursor = load_cursor(fs, TransformBackfillScope.WEATHER_GRID, today)
    assert cursor.next_end_date == today - timedelta(days=14)


def test_scopes_have_independent_cursors() -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_transform_backfill(TransformBackfillScope.PV_SITES, today, fs)
    commit_transform_backfill(TransformBackfillScope.PV_SITES, fs)

    pv_cursor = load_cursor(fs, TransformBackfillScope.PV_SITES, today)
    weather_cursor = load_cursor(fs, TransformBackfillScope.WEATHER_GRID, today)

    assert pv_cursor.next_end_date == today - timedelta(days=28)
    assert weather_cursor.next_end_date == today
