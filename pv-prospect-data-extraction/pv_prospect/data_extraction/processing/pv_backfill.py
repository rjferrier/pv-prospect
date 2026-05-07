"""Daily PV backfill for PV sites.

Computes the work plan for a single day's PV-site backfill budget:

- Backfill PV output data for all PV sites over a 28-day window, marching
  backwards through history one window per day.
- Backfill weather data for the same PV sites over the same 28-day window
  (covered as two 14-day sub-windows, matching the OpenMeteo API's natural
  granularity).

The cursor / plan-commit machinery lives in :mod:`pv_prospect.etl.backfill`
and is shared with the transformation pipeline. This module is just a
thin wrapper that pins the file paths and the :class:`BackfillScope`
appropriate to the extraction-side PV-site backfill.

The 28-day window is the largest round-week multiple compatible with the
PVOutput rate limit. PVOutput's ``getstatus.jsp`` endpoint is single-date
only — one HTTP call per (system, day) — and the API rate-limits at 300
requests per hour. With 10 PV sites and 28 days per site the daily budget
is 280 calls, safely under the cap when dispatched sequentially from the
Cloud Workflow and leaving headroom for retries.
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

_PATHS = BackfillPaths(
    cursor='manifests/pv_backfill_cursor.json',
    manifest='manifests/todays_pv_backfill_manifest.json',
)


def plan_pv_site_backfill(
    today: date,
    fs: FileSystem,
    window_days: int | None = None,
) -> BackfillPlan:
    """Plan today's PV-site backfill window and persist the manifest.

    The live cursor is not advanced — that happens later via
    :func:`commit_pv_site_backfill`, after the extraction jobs succeed.
    """
    if window_days is None:
        window_days = default_window_days(BackfillScope.PV_SITES)
    return plan_backfill(fs, _PATHS, today, window_days)


def commit_pv_site_backfill(fs: FileSystem) -> BackfillCursor:
    """Promote the manifest's next cursor to the live cursor.

    Called after all PV + weather extraction jobs for today have completed.
    """
    return commit_backfill(fs, _PATHS)
