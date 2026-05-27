"""Daily extraction manifest for grid-point weather backfill.

This module is the extraction-side wrapper around the shared
:mod:`pv_prospect.etl.slice_schedule` module: it converts each
:class:`WeatherSlice` produced by :func:`weather_grid_schedule` into
one ``extract_and_load`` task env, persists the resulting phased
manifest, and runs the plan-commit dance for the cursor.

The schedule itself (today's anchor + step-3 historical march), the
slice value type, and the cursor type live in
:mod:`pv_prospect.etl.slice_schedule` so the transformation pipeline
can consume the same source of truth for slice boundaries.

The persisted manifest carries a ``phases`` list in the same shape as
the daily-extract orchestrator manifest. Each slice becomes one
``extract_and_load`` task env carrying the concrete list of
grid-point locations (``LOCATIONS``) the container should process,
plus the slice's ``GRID_POINT_SAMPLE_INDEX`` so each per-site ledger
entry's descriptor records which sample file the window belongs to.
Per-site task identity is computed inside the container via
:func:`pv_prospect.etl.compute_task_hash` so the workflow ledger
records one entry per ``(site, window)`` pair rather than per slice.
"""

import json
from datetime import date

from pv_prospect.etl import (
    WeatherGridBackfillCursor,
    WeatherSlice,
    build_env_list,
    initial_weather_grid_backfill_cursor,
    inject_task_hash,
    read_sample_file,
    sample_file_path,
    weather_grid_schedule,
)
from pv_prospect.etl.storage import FileSystem

WORKFLOW_NAME = 'pv-prospect-extract-weather-grid-backfill'


def _cursor_path() -> str:
    return f'{WORKFLOW_NAME}.json'


def _manifest_path(run_date: str) -> str:
    return f'{run_date}/{WORKFLOW_NAME}.json'


def serialize_cursor(cursor: WeatherGridBackfillCursor) -> str:
    """Serialize a cursor to a JSON string."""
    return json.dumps(
        {
            'next_end_date': cursor.next_end_date.isoformat(),
            'next_sample_offset': cursor.next_sample_offset,
        }
    )


def deserialize_cursor(text: str) -> WeatherGridBackfillCursor:
    """Deserialize a cursor from a JSON string."""
    data = json.loads(text)
    return WeatherGridBackfillCursor(
        next_end_date=date.fromisoformat(data['next_end_date']),
        next_sample_offset=data['next_sample_offset'],
    )


def load_cursor(cursors_fs: FileSystem, today: date) -> WeatherGridBackfillCursor:
    """Load the backfill cursor from storage, or create an initial one."""
    path = _cursor_path()
    if cursors_fs.exists(path):
        return deserialize_cursor(cursors_fs.read_text(path))
    return initial_weather_grid_backfill_cursor(today)


def save_cursor(cursors_fs: FileSystem, cursor: WeatherGridBackfillCursor) -> None:
    """Persist the backfill cursor to storage."""
    cursors_fs.write_text(_cursor_path(), serialize_cursor(cursor))


def _weather_slice_to_task_env(
    weather_slice: WeatherSlice,
    locations: list[str],
    data_source: str,
    dry_run: str,
    run_date: str,
) -> list[dict[str, str]]:
    """Build the ``extract_and_load`` env list for *weather_slice*.

    The env list is what the Cloud Workflow dispatches as
    ``containerOverrides.env`` for one Cloud Run Job task. The
    sample-file index is resolved to its concrete list of ``lat,lon``
    strings at plan time and passed as the ``LOCATIONS`` env var (a
    JSON array). The index itself rides along as
    ``GRID_POINT_SAMPLE_INDEX`` so the container can record it in
    each per-site ledger descriptor; it does not drive resolution
    (``LOCATIONS`` does) and is excluded from the per-site task hash.

    The injected ``TASK_HASH`` here identifies the *whole slice* —
    the container uses it as the filename for the one per-task
    scratch ledger/log file it flushes at end of run. Per-*site*
    hashes recorded in each ledger entry are still computed inside
    the container.
    """
    return inject_task_hash(
        build_env_list(
            JOB_TYPE='extract_and_load',
            DATA_SOURCE=data_source,
            START_DATE=weather_slice.start_date.isoformat(),
            END_DATE=weather_slice.end_date.isoformat(),
            LOCATIONS=json.dumps(locations),
            GRID_POINT_SAMPLE_INDEX=str(weather_slice.grid_point_sample_index),
            DRY_RUN=dry_run,
            WORKFLOW_NAME=WORKFLOW_NAME,
            RUN_DATE=run_date,
        )
    )


def build_phases(
    slices: list[WeatherSlice],
    resources_fs: FileSystem,
    data_source: str,
    dry_run: str,
    run_date: str,
) -> list[list[list[dict[str, str]]]]:
    """Convert *slices* into the orchestrator ``phases`` shape.

    Each slice's ``grid_point_sample_index`` is resolved to its
    concrete list of grid-point ``lat,lon`` strings via
    *resources_fs*. All slices go into a single phase. Within-phase
    order is preserved so the Cloud Workflow's pacing loop can
    dispatch them in sequence under the OpenMeteo rate limit.
    """
    tasks = [
        _weather_slice_to_task_env(
            s,
            read_sample_file(resources_fs, sample_file_path(s.grid_point_sample_index)),
            data_source,
            dry_run,
            run_date,
        )
        for s in slices
    ]
    return [tasks]


def serialize_manifest(
    phases: list[list[list[dict[str, str]]]],
    next_cursor: WeatherGridBackfillCursor,
) -> str:
    """Serialize the phased manifest plus its next cursor to JSON.

    Consumed by the Cloud Workflow dispatcher (which iterates
    ``phases``) and by :func:`commit_weather_grid_backfill` (which
    reads ``next_cursor`` to advance the live cursor).
    """
    return json.dumps(
        {
            'phases': phases,
            'next_cursor': {
                'next_end_date': next_cursor.next_end_date.isoformat(),
                'next_sample_offset': next_cursor.next_sample_offset,
            },
        }
    )


def deserialize_next_cursor(text: str) -> WeatherGridBackfillCursor:
    """Extract the ``next_cursor`` from a serialized manifest.

    The phases are opaque env-list data the Cloud Workflow consumes
    directly; only the cursor is needed Python-side (by
    :func:`commit_weather_grid_backfill`).
    """
    data = json.loads(text)
    return WeatherGridBackfillCursor(
        next_end_date=date.fromisoformat(data['next_cursor']['next_end_date']),
        next_sample_offset=int(data['next_cursor']['next_sample_offset']),
    )


def plan_weather_grid_backfill(
    today: date,
    run_date: str,
    num_sample_files: int,
    data_source: str,
    dry_run: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
    resources_fs: FileSystem,
    step3_batch_count: int = 8,
) -> list[WeatherSlice]:
    """Compute today's manifest and persist it to storage.

    Reads the current cursor, asks the shared slice schedule for
    today's slice list and the updated cursor, resolves each slice's
    sample-file index to its concrete list of grid-point locations
    via *resources_fs*, and writes both the phased manifest and the
    *next* cursor to ``<run_date>/<workflow_name>.json`` on the
    manifests filesystem. The live cursor at
    ``<workflow_name>.json`` on the cursors filesystem is **not**
    advanced — that happens later via
    :func:`commit_weather_grid_backfill`, after the slices have been
    dispatched successfully.

    The planner does not consult the ledger to skip already-completed
    sites: the workflow's role is to advance the cursor at a predictable
    cadence, accepting that transiently-failed sites become small holes
    rather than perpetual retries.
    """
    cursor = load_cursor(cursors_fs, today)
    slices, next_cursor = weather_grid_schedule(
        today, num_sample_files, cursor, step3_batch_count=step3_batch_count
    )
    phases = build_phases(slices, resources_fs, data_source, dry_run, run_date)
    manifests_fs.write_text(
        _manifest_path(run_date), serialize_manifest(phases, next_cursor)
    )
    return slices


def commit_weather_grid_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> WeatherGridBackfillCursor:
    """Promote the manifest's next cursor to the live cursor.

    Called after all slices dispatched by the workflow have completed.
    """
    next_cursor = deserialize_next_cursor(
        manifests_fs.read_text(_manifest_path(run_date))
    )
    save_cursor(cursors_fs, next_cursor)
    return next_cursor
