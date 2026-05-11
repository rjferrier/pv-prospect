"""Daily backfill cursors for the data-transformation pipeline.

The cursor / plan-commit machinery lives in :mod:`pv_prospect.etl.backfill`
and is shared with the extraction pipeline. This module is a thin wrapper
that pins the workflow name for each :class:`BackfillScope` so the
transform backfill can advance independently of the extraction backfill it
trails.

Two scopes are tracked independently — one for the PV-site transform
backfill, one for the weather-grid transform backfill — so each can
advance at the cadence of its corresponding extraction backfill without
being held back by the other. Window lengths come from
:func:`default_window_days` and so always match the extraction side.
"""

from datetime import date

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPlan,
    BackfillScope,
    commit_backfill,
    default_window_days,
    plan_backfill,
)
from pv_prospect.etl.storage import FileSystem

_WORKFLOW_NAME_BY_SCOPE: dict[BackfillScope, str] = {
    BackfillScope.PV_SITES: 'pv-prospect-transform-pv-sites-backfill',
    BackfillScope.WEATHER_GRID: 'pv-prospect-transform-weather-grid-backfill',
}


def workflow_name_for(scope: BackfillScope) -> str:
    """Return the workflow name for the transform backfill of *scope*."""
    return _WORKFLOW_NAME_BY_SCOPE[scope]


def plan_transform_backfill(
    scope: BackfillScope,
    today: date,
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
    window_days: int | None = None,
) -> BackfillPlan:
    """Plan today's transform-backfill window for *scope* and persist the manifest.

    The live cursor is not advanced — that happens later via
    :func:`commit_transform_backfill`, after the transform jobs succeed.
    When ``window_days`` is ``None`` the scope's default window is used.
    """
    if window_days is None:
        window_days = default_window_days(scope)
    return plan_backfill(
        cursors_fs,
        manifests_fs,
        workflow_name_for(scope),
        run_date,
        today,
        window_days,
    )


def commit_transform_backfill(
    scope: BackfillScope,
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> BackfillCursor:
    """Promote *scope*'s manifest next-cursor to the live cursor."""
    return commit_backfill(cursors_fs, manifests_fs, workflow_name_for(scope), run_date)
