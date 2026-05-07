"""Daily backfill cursor for the data-transformation pipeline.

Mirrors the plan-commit cursor pattern in pv-prospect-data-extraction's
``pv_backfill`` and ``weather_grid_backfill`` modules: a cursor advances
one window backwards through history per daily run, and the live cursor
is only advanced after the run succeeds.

Two scopes are tracked independently — one for the PV-site transform
backfill, one for the weather-grid transform backfill — so each can advance
at the cadence of its corresponding extraction backfill without being held
back by the other.

The window length is scope-specific (see :func:`default_window_days`) and
mirrors the extraction-side window for that scope, since the transform
backfill consumes raw data produced by the extraction backfill.
"""

import json
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum

from pv_prospect.etl.storage import FileSystem


class TransformBackfillScope(Enum):
    """The two independent scopes of the transform backfill."""

    PV_SITES = 'pv_sites'
    WEATHER_GRID = 'weather_grid'


# Mirror the extraction-side window constants. PV_SITES uses 28 days because
# PVOutput's getstatus.jsp is single-date only (one API call per day per site)
# and 10 sites × 28 days = 280 calls fits within PVOutput's 300-requests/hour
# rate limit. WEATHER_GRID uses 14 days because that's OpenMeteo's natural
# single-request window for the historical-forecast endpoint.
_DEFAULT_WINDOW_DAYS_BY_SCOPE: dict[TransformBackfillScope, int] = {
    TransformBackfillScope.PV_SITES: 28,
    TransformBackfillScope.WEATHER_GRID: 14,
}


def default_window_days(scope: TransformBackfillScope) -> int:
    """Return the default backfill window length (in days) for *scope*."""
    return _DEFAULT_WINDOW_DAYS_BY_SCOPE[scope]


@dataclass(frozen=True)
class TransformBackfillCursor:
    """Tracks where a transform backfill left off.

    ``next_end_date`` is the *exclusive* end of the next window to process
    (so the window is ``[next_end_date - window_days, next_end_date)``).
    The window length is scope-specific — see :func:`default_window_days`.
    """

    next_end_date: date


@dataclass(frozen=True)
class TransformBackfillPlan:
    """The date window to transform today.

    ``start_date`` is inclusive, ``end_date`` is exclusive.
    """

    start_date: date
    end_date: date


def cursor_path(scope: TransformBackfillScope) -> str:
    """Path to the persisted cursor file for *scope*."""
    return f'manifests/{scope.value}_transform_backfill_cursor.json'


def manifest_path(scope: TransformBackfillScope) -> str:
    """Path to today's planned-but-not-yet-committed manifest for *scope*."""
    return f'manifests/todays_{scope.value}_transform_backfill_manifest.json'


def initial_transform_backfill_cursor(today: date) -> TransformBackfillCursor:
    """Cursor for a first-ever run (no prior backfill)."""
    return TransformBackfillCursor(next_end_date=today)


def build_transform_backfill_plan(
    cursor: TransformBackfillCursor,
    window_days: int,
) -> tuple[TransformBackfillPlan, TransformBackfillCursor]:
    """Build today's plan and return the updated cursor.

    Returns a ``(plan, next_cursor)`` tuple. The plan describes the date
    window to transform; the next cursor points ``window_days`` further
    into the past.
    """
    end_date = cursor.next_end_date
    start_date = end_date - timedelta(days=window_days)
    plan = TransformBackfillPlan(start_date=start_date, end_date=end_date)
    next_cursor = TransformBackfillCursor(next_end_date=start_date)
    return plan, next_cursor


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def serialize_cursor(cursor: TransformBackfillCursor) -> str:
    return json.dumps({'next_end_date': cursor.next_end_date.isoformat()})


def deserialize_cursor(text: str) -> TransformBackfillCursor:
    data = json.loads(text)
    return TransformBackfillCursor(
        next_end_date=date.fromisoformat(data['next_end_date']),
    )


def serialize_plan(
    plan: TransformBackfillPlan, next_cursor: TransformBackfillCursor
) -> str:
    """Serialize a plan plus its next cursor.

    The JSON is consumed by the Cloud Workflow dispatcher and by
    :func:`commit_transform_backfill` to advance the live cursor.
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


def deserialize_plan(
    text: str,
) -> tuple[TransformBackfillPlan, TransformBackfillCursor]:
    data = json.loads(text)
    plan = TransformBackfillPlan(
        start_date=date.fromisoformat(data['start_date']),
        end_date=date.fromisoformat(data['end_date']),
    )
    next_cursor = TransformBackfillCursor(
        next_end_date=date.fromisoformat(data['next_cursor']['next_end_date']),
    )
    return plan, next_cursor


# ---------------------------------------------------------------------------
# Cursor persistence
# ---------------------------------------------------------------------------


def load_cursor(
    fs: FileSystem, scope: TransformBackfillScope, today: date
) -> TransformBackfillCursor:
    """Load the cursor for *scope* from storage, or create an initial one."""
    path = cursor_path(scope)
    if fs.exists(path):
        return deserialize_cursor(fs.read_text(path))
    return initial_transform_backfill_cursor(today)


def save_cursor(
    fs: FileSystem, scope: TransformBackfillScope, cursor: TransformBackfillCursor
) -> None:
    """Persist the cursor for *scope* to storage."""
    fs.write_text(cursor_path(scope), serialize_cursor(cursor))


# ---------------------------------------------------------------------------
# Plan / commit entry points (called from the Cloud Run entrypoint)
# ---------------------------------------------------------------------------


def plan_transform_backfill(
    scope: TransformBackfillScope,
    today: date,
    fs: FileSystem,
    window_days: int | None = None,
) -> TransformBackfillPlan:
    """Compute today's transform backfill plan for *scope* and persist it.

    Reads the current cursor for *scope*, builds the plan, and writes both
    the plan and the *next* cursor to the manifest path. The live cursor is
    not advanced — that happens later via :func:`commit_transform_backfill`,
    after the transform jobs succeed.

    When ``window_days`` is ``None`` the scope's default window is used
    (see :func:`default_window_days`).
    """
    if window_days is None:
        window_days = default_window_days(scope)
    cursor = load_cursor(fs, scope, today)
    plan, next_cursor = build_transform_backfill_plan(cursor, window_days=window_days)
    fs.write_text(manifest_path(scope), serialize_plan(plan, next_cursor))
    return plan


def commit_transform_backfill(
    scope: TransformBackfillScope,
    fs: FileSystem,
) -> TransformBackfillCursor:
    """Promote *scope*'s manifest next cursor to the live cursor.

    Called after all transform jobs dispatched by the workflow for *scope*
    have completed.
    """
    _, next_cursor = deserialize_plan(fs.read_text(manifest_path(scope)))
    save_cursor(fs, scope, next_cursor)
    return next_cursor
