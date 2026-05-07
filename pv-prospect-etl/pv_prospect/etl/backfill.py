"""Shared cursor / manifest / plan-commit primitives for daily backfills.

Both extraction and transformation pipelines run daily backfills that march
backwards through history one window at a time. They share a common shape:

  * a *cursor* records where the last run left off (the exclusive end of
    the next window to process);
  * a *plan* describes today's window;
  * the *plan-commit* dance writes today's plan and the *next* cursor into
    a manifest file, but only promotes the next-cursor to the live cursor
    after the run succeeds (so a failed run is safely re-tried tomorrow).

The window length is scope-specific: see :class:`BackfillScope` and
:func:`default_window_days`. Centralising the configuration here means
extraction and transformation can never disagree on cadence — change a
value in this module and both sides pick it up.
"""

import json
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum

from pv_prospect.etl.storage import FileSystem


class BackfillScope(Enum):
    """Independent backfill scopes with their own progress cursors.

    The two natural axes of historical-data backfill in this project:
    PV-output (and paired weather) data for the fixed set of monitored PV
    systems, and weather data over the broader UK grid (sample-file
    driven). Both extraction and transformation pipelines maintain their
    own independent cursors per scope but share the window-length
    configuration via this enum.
    """

    PV_SITES = 'pv_sites'
    WEATHER_GRID = 'weather_grid'


# Default window length per scope. Both extraction and transformation
# advance at this cadence, so changing the value here updates both sides.
#
#   - PV_SITES: 28 days. PVOutput's ``getstatus.jsp`` is single-date only
#     (one HTTP call per system per day) and rate-limits at 300/hour;
#     10 sites × 28 days = 280 calls is the largest round-week multiple
#     that fits the rate limit with retry headroom.
#   - WEATHER_GRID: 14 days. OpenMeteo's natural single-request window
#     for the historical-forecast endpoint.
_DEFAULT_WINDOW_DAYS_BY_SCOPE: dict[BackfillScope, int] = {
    BackfillScope.PV_SITES: 28,
    BackfillScope.WEATHER_GRID: 14,
}


def default_window_days(scope: BackfillScope) -> int:
    """Return the default backfill window length (in days) for *scope*."""
    return _DEFAULT_WINDOW_DAYS_BY_SCOPE[scope]


@dataclass(frozen=True)
class BackfillCursor:
    """Tracks where a backfill left off.

    ``next_end_date`` is the *exclusive* end of the next window to process
    (so the window is ``[next_end_date - window_days, next_end_date)``).
    """

    next_end_date: date


@dataclass(frozen=True)
class BackfillPlan:
    """A date window: ``start_date`` inclusive, ``end_date`` exclusive."""

    start_date: date
    end_date: date


def initial_backfill_cursor(today: date) -> BackfillCursor:
    """Cursor for a first-ever run (no prior backfill)."""
    return BackfillCursor(next_end_date=today)


def build_backfill_plan(
    cursor: BackfillCursor,
    window_days: int,
) -> tuple[BackfillPlan, BackfillCursor]:
    """Build a plan for the next window and return the updated cursor.

    Returns ``(plan, next_cursor)``. ``next_cursor`` points at the
    exclusive end of the *following* window (i.e. ``window_days`` further
    into the past).
    """
    end_date = cursor.next_end_date
    start_date = end_date - timedelta(days=window_days)
    plan = BackfillPlan(start_date=start_date, end_date=end_date)
    next_cursor = BackfillCursor(next_end_date=start_date)
    return plan, next_cursor


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def serialize_cursor(cursor: BackfillCursor) -> str:
    return json.dumps({'next_end_date': cursor.next_end_date.isoformat()})


def deserialize_cursor(text: str) -> BackfillCursor:
    data = json.loads(text)
    return BackfillCursor(
        next_end_date=date.fromisoformat(data['next_end_date']),
    )


def serialize_plan(plan: BackfillPlan, next_cursor: BackfillCursor) -> str:
    """Serialize a plan plus its next cursor to JSON.

    The format is consumed by the Cloud Workflow dispatcher (which needs
    the plan dates) and by :func:`commit_backfill` (which uses the next
    cursor to advance the live cursor).
    """
    return json.dumps(
        {
            'start_date': plan.start_date.isoformat(),
            'end_date': plan.end_date.isoformat(),
            'next_cursor': {
                'next_end_date': next_cursor.next_end_date.isoformat(),
            },
        }
    )


def deserialize_plan(text: str) -> tuple[BackfillPlan, BackfillCursor]:
    data = json.loads(text)
    plan = BackfillPlan(
        start_date=date.fromisoformat(data['start_date']),
        end_date=date.fromisoformat(data['end_date']),
    )
    next_cursor = BackfillCursor(
        next_end_date=date.fromisoformat(data['next_cursor']['next_end_date']),
    )
    return plan, next_cursor


# ---------------------------------------------------------------------------
# Persistence + plan-commit (parameterised by file paths)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BackfillPaths:
    """File-system paths that identify a particular backfill instance.

    Each daily backfill (extraction-side PV, extraction-side weather grid,
    transformation-side PV-sites, transformation-side weather grid, ...)
    has its own cursor and manifest files and so its own ``BackfillPaths``.
    """

    cursor: str
    manifest: str


def load_cursor(fs: FileSystem, paths: BackfillPaths, today: date) -> BackfillCursor:
    """Load the cursor at *paths*, or return an initial cursor (== today)."""
    if fs.exists(paths.cursor):
        return deserialize_cursor(fs.read_text(paths.cursor))
    return initial_backfill_cursor(today)


def save_cursor(fs: FileSystem, paths: BackfillPaths, cursor: BackfillCursor) -> None:
    fs.write_text(paths.cursor, serialize_cursor(cursor))


def plan_backfill(
    fs: FileSystem,
    paths: BackfillPaths,
    today: date,
    window_days: int,
) -> BackfillPlan:
    """Plan today's window and write the manifest. Live cursor unchanged.

    The manifest at ``paths.manifest`` carries both the plan and the
    *next* cursor — the latter is later promoted to the live cursor by
    :func:`commit_backfill` once the run succeeds.
    """
    cursor = load_cursor(fs, paths, today)
    plan, next_cursor = build_backfill_plan(cursor, window_days)
    fs.write_text(paths.manifest, serialize_plan(plan, next_cursor))
    return plan


def commit_backfill(fs: FileSystem, paths: BackfillPaths) -> BackfillCursor:
    """Promote the manifest's next-cursor to the live cursor."""
    _, next_cursor = deserialize_plan(fs.read_text(paths.manifest))
    save_cursor(fs, paths, next_cursor)
    return next_cursor
