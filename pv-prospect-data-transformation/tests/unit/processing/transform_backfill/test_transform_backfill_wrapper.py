"""Tests for the transform-backfill wrapper.

The cursor / plan-commit logic itself is tested in pv-prospect-etl. These
tests cover only what the wrapper adds: the right paths are used per
scope and the scope-specific default window is applied when none is given.
"""

from datetime import date, timedelta

import pytest
from pv_prospect.data_transformation.processing import (
    commit_transform_backfill,
    plan_transform_backfill,
    workflow_name_for,
)
from pv_prospect.etl import (
    BackfillScope,
    cursor_filename,
    deserialize_plan,
    load_cursor,
    manifest_filename,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem

_RUN_DATE = '2026-05-06'


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
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan = plan_transform_backfill(scope, today, _RUN_DATE, cursors_fs, manifests_fs)

    assert plan.start_date == today - timedelta(days=expected_window_days)
    assert plan.end_date == today


def test_workflow_name_differs_per_scope() -> None:
    pv_wf = workflow_name_for(BackfillScope.PV_SITES)
    weather_wf = workflow_name_for(BackfillScope.WEATHER_GRID)

    assert pv_wf != weather_wf


def test_plan_writes_manifest_at_scope_specific_path() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()

    plan_transform_backfill(
        BackfillScope.PV_SITES,
        date(2026, 5, 6),
        _RUN_DATE,
        cursors_fs,
        manifests_fs,
    )

    pv_manifest = manifest_filename(
        workflow_name_for(BackfillScope.PV_SITES), _RUN_DATE
    )
    weather_manifest = manifest_filename(
        workflow_name_for(BackfillScope.WEATHER_GRID), _RUN_DATE
    )
    assert manifests_fs.exists(pv_manifest)
    assert not manifests_fs.exists(weather_manifest)


def test_commit_advances_cursor_at_scope_specific_path() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_transform_backfill(
        BackfillScope.WEATHER_GRID, today, _RUN_DATE, cursors_fs, manifests_fs
    )
    commit_transform_backfill(
        BackfillScope.WEATHER_GRID, _RUN_DATE, cursors_fs, manifests_fs
    )

    cursor = load_cursor(
        cursors_fs, workflow_name_for(BackfillScope.WEATHER_GRID), today
    )
    assert cursor.next_end_date == today - timedelta(days=14)


def test_scopes_have_independent_cursors() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_transform_backfill(
        BackfillScope.PV_SITES, today, _RUN_DATE, cursors_fs, manifests_fs
    )
    commit_transform_backfill(
        BackfillScope.PV_SITES, _RUN_DATE, cursors_fs, manifests_fs
    )

    pv_cursor = load_cursor(
        cursors_fs, workflow_name_for(BackfillScope.PV_SITES), today
    )
    weather_cursor = load_cursor(
        cursors_fs, workflow_name_for(BackfillScope.WEATHER_GRID), today
    )

    assert pv_cursor.next_end_date == today - timedelta(days=28)
    assert weather_cursor.next_end_date == today


def test_plan_round_trips_through_manifest() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()
    today = date(2026, 5, 6)

    returned = plan_transform_backfill(
        BackfillScope.PV_SITES, today, _RUN_DATE, cursors_fs, manifests_fs
    )
    pv_manifest = manifest_filename(
        workflow_name_for(BackfillScope.PV_SITES), _RUN_DATE
    )
    persisted, _ = deserialize_plan(manifests_fs.read_text(pv_manifest))

    assert returned == persisted


def test_cursor_filename_is_workflow_scoped() -> None:
    assert cursor_filename(workflow_name_for(BackfillScope.PV_SITES)) != (
        cursor_filename(workflow_name_for(BackfillScope.WEATHER_GRID))
    )
