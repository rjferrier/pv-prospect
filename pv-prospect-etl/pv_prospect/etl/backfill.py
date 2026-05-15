"""Shared cursor / manifest / plan-commit primitives for daily backfills.

Both extraction and transformation pipelines run daily backfills that march
backwards through history one window at a time. They share a common shape:

  * a *cursor* records where the last run left off (the exclusive end of
    the next window to process). Cursors are singleton committed state,
    stored at ``<workflow_name>.json`` on the *cursors filesystem*.
  * a *plan* describes the next window's work.
  * the *plan-commit* dance writes today's plan and the *next* cursor into
    a manifest at ``<run_date>/<workflow_name>.json`` on the *manifests
    filesystem*, but only promotes the next-cursor to the live cursor
    after the run succeeds (so a failed run is safely re-tried tomorrow).

The window length is scope-specific: see :class:`BackfillScope` and
:func:`default_window_days`. Centralising the configuration here means
extraction and transformation can never disagree on cadence — change a
value in this module and both sides pick it up.
"""

import json
from collections.abc import Callable
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


def serialize_plan(
    plan: BackfillPlan,
    next_cursor: BackfillCursor,
    phases: list[list[list[dict[str, str]]]] | None = None,
) -> str:
    """Serialize a plan plus its next cursor (and optional phases) to JSON.

    The format is consumed by the Cloud Workflow dispatcher (which needs
    the plan dates and, when present, the phased task list) and by
    :func:`commit_backfill` (which uses the next cursor to advance the
    live cursor). Extra fields are ignored on deserialization, so
    backfill workflows whose plan job emits ``phases`` can share a single
    manifest file with :func:`commit_backfill`.
    """
    data: dict[str, object] = {
        'start_date': plan.start_date.isoformat(),
        'end_date': plan.end_date.isoformat(),
        'next_cursor': {
            'next_end_date': next_cursor.next_end_date.isoformat(),
        },
    }
    if phases is not None:
        data['phases'] = phases
    return json.dumps(data)


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
# Persistence + plan-commit
# ---------------------------------------------------------------------------


def cursor_filename(workflow_name: str) -> str:
    """Return the cursor filename (relative to the cursors filesystem) for
    *workflow_name*."""
    return f'{workflow_name}.json'


def manifest_filename(workflow_name: str, run_date: str) -> str:
    """Return the per-run-date backfill-plan manifest filename (relative to
    the manifests filesystem) for *workflow_name* on *run_date*.

    The file carries the date window, the next cursor, and the phased
    task list — extract-backfill workflows embed phases here directly
    rather than emitting a separate ``WorkflowOrchestrator.write_manifest``
    file, so there is no name collision to disambiguate."""
    return f'{run_date}/{workflow_name}.json'


def load_cursor(
    cursors_fs: FileSystem,
    workflow_name: str,
    today: date,
) -> BackfillCursor:
    """Load the cursor for *workflow_name*, or return an initial cursor."""
    path = cursor_filename(workflow_name)
    if cursors_fs.exists(path):
        return deserialize_cursor(cursors_fs.read_text(path))
    return initial_backfill_cursor(today)


def save_cursor(
    cursors_fs: FileSystem,
    workflow_name: str,
    cursor: BackfillCursor,
) -> None:
    """Persist *cursor* as the live cursor for *workflow_name*."""
    cursors_fs.write_text(cursor_filename(workflow_name), serialize_cursor(cursor))


PhasesFn = Callable[[BackfillPlan], list[list[list[dict[str, str]]]]]


def plan_backfill(
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
    workflow_name: str,
    run_date: str,
    today: date,
    window_days: int,
    phases_fn: PhasesFn | None = None,
) -> BackfillPlan:
    """Plan today's window and write the manifest. Live cursor unchanged.

    Writes ``<run_date>/<workflow_name>.json`` on *manifests_fs* carrying
    the plan, the *next* cursor (later promoted by
    :func:`commit_backfill` once the run succeeds), and — when
    *phases_fn* is supplied — the phased task list the Cloud Workflow
    dispatches. *phases_fn* is called with the freshly-built plan, so
    callers can enumerate per-task env lists keyed to the chosen window
    without re-computing the cursor themselves.
    """
    cursor = load_cursor(cursors_fs, workflow_name, today)
    plan, next_cursor = build_backfill_plan(cursor, window_days)
    phases = phases_fn(plan) if phases_fn is not None else None
    manifests_fs.write_text(
        manifest_filename(workflow_name, run_date),
        serialize_plan(plan, next_cursor, phases),
    )
    return plan


def commit_backfill(
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
    workflow_name: str,
    run_date: str,
) -> BackfillCursor:
    """Promote the manifest's next-cursor to the live cursor."""
    _, next_cursor = deserialize_plan(
        manifests_fs.read_text(manifest_filename(workflow_name, run_date))
    )
    save_cursor(cursors_fs, workflow_name, next_cursor)
    return next_cursor
