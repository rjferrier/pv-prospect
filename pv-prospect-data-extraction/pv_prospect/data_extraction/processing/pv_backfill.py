"""Daily PV backfill for PV sites.

Computes the work plan for a single day's PV-site backfill budget:

- Backfill PV output data for all PV sites over a 28-day window, marching
  backwards through history one window per day.
- Backfill weather data for the same PV sites over the same 28-day window
  (covered as two 14-day sub-windows, matching the OpenMeteo API's natural
  granularity).

A :class:`PvBackfillCursor` tracks progress between daily runs.  The cursor
stores the exclusive end date of the *next* window to process; each run
walks 28 days further into the past.

The 28-day window is the largest round-week multiple compatible with the
PVOutput rate limit. PVOutput's ``getstatus.jsp`` endpoint is single-date
only — one HTTP call per (system, day) — and the API rate-limits at 300
requests per hour. With 10 PV sites and 28 days per site the daily budget
is 280 calls, safely under the cap when dispatched sequentially from the
Cloud Workflow and leaving headroom for retries.
"""

import json
from dataclasses import dataclass
from datetime import date, timedelta

from pv_prospect.etl.storage import FileSystem

PV_BACKFILL_WINDOW_DAYS = 28

PV_BACKFILL_CURSOR_PATH = 'manifests/pv_backfill_cursor.json'
PV_BACKFILL_MANIFEST_PATH = 'manifests/todays_pv_backfill_manifest.json'


@dataclass(frozen=True)
class PvBackfillPlan:
    """The 28-day window to process today.

    ``start_date`` is inclusive, ``end_date`` is exclusive.
    """

    start_date: date
    end_date: date


@dataclass(frozen=True)
class PvBackfillCursor:
    """Tracks where the PV backfill left off.

    ``next_end_date`` is the exclusive end of the *next* 28-day window to
    process (i.e. the window is ``[next_end_date - 28, next_end_date)``).
    """

    next_end_date: date


def initial_pv_backfill_cursor(today: date) -> PvBackfillCursor:
    """Return the cursor for a first-ever run (no prior PV backfill)."""
    return PvBackfillCursor(next_end_date=today)


def build_pv_backfill_plan(
    cursor: PvBackfillCursor,
    window_days: int = PV_BACKFILL_WINDOW_DAYS,
) -> tuple[PvBackfillPlan, PvBackfillCursor]:
    """Build today's PV backfill plan and return the updated cursor.

    Returns:
        A ``(plan, next_cursor)`` tuple.  The plan describes the date window
        to extract; the next cursor points 28 days further into the past.
    """
    end_date = cursor.next_end_date
    start_date = end_date - timedelta(days=window_days)

    plan = PvBackfillPlan(start_date=start_date, end_date=end_date)
    next_cursor = PvBackfillCursor(next_end_date=start_date)

    return plan, next_cursor


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def serialize_cursor(cursor: PvBackfillCursor) -> str:
    """Serialize a cursor to a JSON string."""
    return json.dumps({'next_end_date': cursor.next_end_date.isoformat()})


def deserialize_cursor(text: str) -> PvBackfillCursor:
    """Deserialize a cursor from a JSON string."""
    data = json.loads(text)
    return PvBackfillCursor(
        next_end_date=date.fromisoformat(data['next_end_date']),
    )


def serialize_plan(plan: PvBackfillPlan, next_cursor: PvBackfillCursor) -> str:
    """Serialize a plan (plus next cursor) to a JSON string.

    The JSON is consumed by the Cloud Workflow dispatcher and by
    ``commit_pv_backfill`` to advance the live cursor.
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


def deserialize_plan(text: str) -> tuple[PvBackfillPlan, PvBackfillCursor]:
    """Deserialize a plan (and its next cursor) from a JSON string."""
    data = json.loads(text)
    plan = PvBackfillPlan(
        start_date=date.fromisoformat(data['start_date']),
        end_date=date.fromisoformat(data['end_date']),
    )
    next_cursor = PvBackfillCursor(
        next_end_date=date.fromisoformat(data['next_cursor']['next_end_date']),
    )
    return plan, next_cursor


# ---------------------------------------------------------------------------
# Cursor persistence
# ---------------------------------------------------------------------------


def load_cursor(fs: FileSystem, today: date) -> PvBackfillCursor:
    """Load the PV backfill cursor from storage, or create an initial one."""
    if fs.exists(PV_BACKFILL_CURSOR_PATH):
        return deserialize_cursor(fs.read_text(PV_BACKFILL_CURSOR_PATH))
    return initial_pv_backfill_cursor(today)


def save_cursor(fs: FileSystem, cursor: PvBackfillCursor) -> None:
    """Persist the PV backfill cursor to storage."""
    fs.write_text(PV_BACKFILL_CURSOR_PATH, serialize_cursor(cursor))


# ---------------------------------------------------------------------------
# Plan / commit entry points (called from the Cloud Run entrypoint)
# ---------------------------------------------------------------------------


def plan_pv_site_backfill(
    today: date,
    fs: FileSystem,
    window_days: int = PV_BACKFILL_WINDOW_DAYS,
) -> PvBackfillPlan:
    """Compute today's PV-site backfill plan and persist it to storage.

    Reads the current cursor, builds the plan, and writes both the plan and
    the *next* cursor to :data:`PV_BACKFILL_MANIFEST_PATH`.  The live cursor
    at :data:`PV_BACKFILL_CURSOR_PATH` is **not** advanced — that happens
    later via :func:`commit_pv_site_backfill`, after the extraction jobs
    succeed.
    """
    cursor = load_cursor(fs, today)
    plan, next_cursor = build_pv_backfill_plan(cursor, window_days=window_days)
    fs.write_text(PV_BACKFILL_MANIFEST_PATH, serialize_plan(plan, next_cursor))
    return plan


def commit_pv_site_backfill(fs: FileSystem) -> PvBackfillCursor:
    """Promote the manifest's next cursor to the live cursor.

    Called after all PV + weather extraction jobs for today have completed.
    """
    _, next_cursor = deserialize_plan(fs.read_text(PV_BACKFILL_MANIFEST_PATH))
    save_cursor(fs, next_cursor)
    return next_cursor
