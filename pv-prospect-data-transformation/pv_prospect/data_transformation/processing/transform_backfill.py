"""Daily backfill cursors for the data-transformation pipeline.

The cursor / plan-commit machinery lives in :mod:`pv_prospect.etl.backfill`
and is shared with the extraction pipeline. This module is a thin wrapper
that pins the file paths for each :class:`BackfillScope` so the transform
backfill can advance independently of the extraction backfill it trails.

Two scopes are tracked independently — one for the PV-site transform
backfill, one for the weather-grid transform backfill — so each can
advance at the cadence of its corresponding extraction backfill without
being held back by the other. Window lengths come from
:func:`default_window_days` and so always match the extraction side.
"""

from datetime import date

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPaths,
    BackfillPlan,
    BackfillScope,
    commit_backfill,
    default_window_days,
    plan_backfill,
)
from pv_prospect.etl.storage import FileSystem

_PATHS_BY_SCOPE: dict[BackfillScope, BackfillPaths] = {
    BackfillScope.PV_SITES: BackfillPaths(
        cursor='manifests/pv_sites_transform_backfill_cursor.json',
        manifest='manifests/todays_pv_sites_transform_backfill_manifest.json',
    ),
    BackfillScope.WEATHER_GRID: BackfillPaths(
        cursor='manifests/weather_grid_transform_backfill_cursor.json',
        manifest='manifests/todays_weather_grid_transform_backfill_manifest.json',
    ),
}


def paths_for(scope: BackfillScope) -> BackfillPaths:
    """Return the cursor/manifest paths for the transform backfill of *scope*."""
    return _PATHS_BY_SCOPE[scope]


def plan_transform_backfill(
    scope: BackfillScope,
    today: date,
    fs: FileSystem,
    window_days: int | None = None,
) -> BackfillPlan:
    """Plan today's transform-backfill window for *scope* and persist the manifest.

    The live cursor is not advanced — that happens later via
    :func:`commit_transform_backfill`, after the transform jobs succeed.
    When ``window_days`` is ``None`` the scope's default window is used.
    """
    if window_days is None:
        window_days = default_window_days(scope)
    return plan_backfill(fs, paths_for(scope), today, window_days)


def commit_transform_backfill(
    scope: BackfillScope,
    fs: FileSystem,
) -> BackfillCursor:
    """Promote *scope*'s manifest next-cursor to the live cursor."""
    return commit_backfill(fs, paths_for(scope))
