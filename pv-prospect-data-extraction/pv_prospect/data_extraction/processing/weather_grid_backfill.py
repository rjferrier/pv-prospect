"""Daily extraction manifest for grid-point weather backfill.

Computes the work plan for a single day's API budget, consisting of:

- **Step 1** — query yesterday's weather for each PV site.
- **Step 2** — query the trailing 14 days for one sample file (chosen by
  ``today_epoch_days % num_sample_files``).
- **Step 3** — resume the historical backfill from where yesterday left off,
  processing up to ``step3_batch_count`` further sample-file batches, each
  covering a 14-day window and marching backwards in time.

A :class:`WeatherGridBackfillCursor` tracks Step 3 progress between runs.

The persisted manifest carries a ``phases`` list in the same shape as the
daily-extract orchestrator manifest. Each batch becomes an
``extract_and_load`` task env carrying the concrete list of grid-point
locations (``LOCATIONS``) the container should process. Per-site task
identity is computed inside the container via
:func:`pv_prospect.etl.compute_task_hash` so the workflow ledger records
one entry per ``(site, window)`` pair rather than per batch.
"""

import json
from dataclasses import dataclass
from datetime import date, timedelta

from pv_prospect.data_extraction.processing.sample_file import (
    read_sample_file,
    sample_file_path,
)
from pv_prospect.etl import (
    BackfillScope,
    build_env_list,
    default_window_days,
)
from pv_prospect.etl.storage import FileSystem

_EPOCH = date(1970, 1, 1)

# 14-day window. Matches OpenMeteo's natural single-request window for the
# historical-forecast endpoint. The constant is sourced from the shared
# backfill-config in pv_prospect.etl so it cannot drift from the
# transformation-side weather-grid backfill.
WEATHER_GRID_BACKFILL_WINDOW_DAYS = default_window_days(BackfillScope.WEATHER_GRID)


def _epoch_days(d: date) -> int:
    return (d - _EPOCH).days


@dataclass(frozen=True)
class Batch:
    """A unit of work: one sample file queried over one date range."""

    sample_file_index: int
    start_date: date
    end_date: date

    @property
    def sample_file_name(self) -> str:
        return f'sample_{self.sample_file_index:03d}.csv'


@dataclass(frozen=True)
class WeatherGridManifest:
    """The complete daily work plan."""

    step2_batch: Batch
    step3_batches: list[Batch]


@dataclass(frozen=True)
class WeatherGridBackfillCursor:
    """Tracks where the historical backfill left off.

    ``next_end_date`` is the *exclusive* end of the next 14-day window to
    query (i.e. the start of the window is ``next_end_date - 14 days``).
    ``next_sample_offset`` is the offset (from the Step 2 anchor) used to
    choose the next sample file.
    """

    next_end_date: date
    next_sample_offset: int


def initial_weather_grid_backfill_cursor(today: date) -> WeatherGridBackfillCursor:
    """Return the cursor for a first-ever run (no prior backfill)."""
    return WeatherGridBackfillCursor(
        next_end_date=today - timedelta(days=WEATHER_GRID_BACKFILL_WINDOW_DAYS),
        next_sample_offset=1,
    )


def build_weather_grid_manifest(
    today: date,
    num_sample_files: int,
    cursor: WeatherGridBackfillCursor,
    step3_batch_count: int = 8,
) -> tuple[WeatherGridManifest, WeatherGridBackfillCursor]:
    """Build today's extraction manifest and return the updated cursor.

    Args:
        today: The current date.
        num_sample_files: Total number of ``sample_NNN.csv`` files.
        cursor: The backfill cursor from the previous run.
        step3_batch_count: How many backward batches to include in Step 3.

    Returns:
        A ``(manifest, updated_cursor)`` tuple.
    """
    anchor = _epoch_days(today) % num_sample_files

    # Step 2: trailing 14 days for today's sample file
    step2_batch = Batch(
        sample_file_index=anchor,
        start_date=today - timedelta(days=WEATHER_GRID_BACKFILL_WINDOW_DAYS),
        end_date=today,
    )

    # Step 3: resume from cursor, marching backwards
    step3_batches: list[Batch] = []
    end_date = cursor.next_end_date
    sample_offset = cursor.next_sample_offset

    for _ in range(step3_batch_count):
        start_date = end_date - timedelta(days=WEATHER_GRID_BACKFILL_WINDOW_DAYS)
        sample_index = (anchor - sample_offset) % num_sample_files
        step3_batches.append(
            Batch(
                sample_file_index=sample_index,
                start_date=start_date,
                end_date=end_date,
            )
        )
        end_date = start_date
        sample_offset += 1

    updated_cursor = WeatherGridBackfillCursor(
        next_end_date=end_date,
        next_sample_offset=sample_offset,
    )

    manifest = WeatherGridManifest(
        step2_batch=step2_batch,
        step3_batches=step3_batches,
    )

    return manifest, updated_cursor


WORKFLOW_NAME = 'pv-prospect-extract-weather-grid-backfill'


def _cursor_path() -> str:
    return f'{WORKFLOW_NAME}.json'


def _manifest_path(run_date: str) -> str:
    return f'{run_date}/{WORKFLOW_NAME}.backfill.json'


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


def batch_to_task_env(
    batch: Batch,
    locations: list[str],
    data_source: str,
    dry_run: str,
    run_date: str,
) -> list[dict[str, str]]:
    """Build the ``extract_and_load`` env list for *batch*.

    The env list is what the Cloud Workflow dispatches as
    ``containerOverrides.env`` for one Cloud Run Job task. The
    sample-file index is resolved to its concrete list of ``lat,lon``
    strings at plan time and passed as the ``LOCATIONS`` env var (a
    JSON array). Per-site ``TASK_HASH`` values are computed inside the
    container; nothing is injected here.
    """
    return build_env_list(
        JOB_TYPE='extract_and_load',
        DATA_SOURCE=data_source,
        START_DATE=batch.start_date.isoformat(),
        END_DATE=batch.end_date.isoformat(),
        LOCATIONS=json.dumps(locations),
        DRY_RUN=dry_run,
        WORKFLOW_NAME=WORKFLOW_NAME,
        RUN_DATE=run_date,
    )


def build_phases(
    manifest: WeatherGridManifest,
    resources_fs: FileSystem,
    data_source: str,
    dry_run: str,
    run_date: str,
) -> list[list[list[dict[str, str]]]]:
    """Convert *manifest*'s batches into the orchestrator ``phases`` shape.

    Each sample-file index in *manifest* is resolved to its concrete
    list of grid-point ``lat,lon`` strings via *resources_fs*. All
    batches go into a single phase. Within-phase order is preserved so
    the Cloud Workflow's pacing loop can dispatch them in sequence
    under the OpenMeteo rate limit.
    """
    batches = [manifest.step2_batch, *manifest.step3_batches]
    tasks = [
        batch_to_task_env(
            b,
            read_sample_file(resources_fs, sample_file_path(b.sample_file_index)),
            data_source,
            dry_run,
            run_date,
        )
        for b in batches
    ]
    return [tasks]


def serialize_manifest(
    phases: list[list[list[dict[str, str]]]],
    next_cursor: WeatherGridBackfillCursor,
) -> str:
    """Serialize the phased manifest plus its next cursor to JSON.

    Consumed by the Cloud Workflow dispatcher (which iterates ``phases``)
    and by :func:`commit_weather_grid_backfill` (which reads
    ``next_cursor`` to advance the live cursor).
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
) -> WeatherGridManifest:
    """Compute today's manifest and persist it to storage.

    Reads the current cursor, builds the batch plan, resolves each
    sample-file index to its concrete list of grid-point locations via
    *resources_fs*, and writes both the phased manifest and the *next*
    cursor to ``<run_date>/<workflow_name>.backfill.json`` on the
    manifests filesystem. The live cursor at
    ``<workflow_name>.json`` on the cursors filesystem is **not**
    advanced — that happens later via
    :func:`commit_weather_grid_backfill`, after the batches have been
    dispatched successfully.

    The planner does not consult the ledger to skip already-completed
    sites: the workflow's role is to advance the cursor at a predictable
    cadence, accepting that transiently-failed sites become small holes
    rather than perpetual retries.
    """
    cursor = load_cursor(cursors_fs, today)
    manifest, next_cursor = build_weather_grid_manifest(
        today, num_sample_files, cursor, step3_batch_count=step3_batch_count
    )
    phases = build_phases(manifest, resources_fs, data_source, dry_run, run_date)
    manifests_fs.write_text(
        _manifest_path(run_date), serialize_manifest(phases, next_cursor)
    )
    return manifest


def commit_weather_grid_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> WeatherGridBackfillCursor:
    """Promote the manifest's next cursor to the live cursor.

    Called after all batches dispatched by the workflow have completed.
    """
    next_cursor = deserialize_next_cursor(
        manifests_fs.read_text(_manifest_path(run_date))
    )
    save_cursor(cursors_fs, next_cursor)
    return next_cursor
