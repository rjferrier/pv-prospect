"""Daily extraction manifest for grid-point weather backfill.

Computes the work plan for a single day's API budget, consisting of:

- **Step 1** — query yesterday's weather for each PV site.
- **Step 2** — query the trailing 14 days for one sample file (chosen by
  ``today_epoch_days % num_sample_files``).
- **Step 3** — resume the historical backfill from where yesterday left off,
  processing up to ``step3_batch_count`` further sample-file batches, each
  covering a 14-day window and marching backwards in time.

A :class:`BackfillCursor` tracks Step 3 progress between runs.
"""

import json
from dataclasses import dataclass
from datetime import date, timedelta

from pv_prospect.etl.storage import FileSystem

_EPOCH = date(1970, 1, 1)
BACKFILL_WINDOW_DAYS = 14


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
class Manifest:
    """The complete daily work plan."""

    step2_batch: Batch
    step3_batches: list[Batch]


@dataclass(frozen=True)
class BackfillCursor:
    """Tracks where the historical backfill left off.

    ``next_end_date`` is the *exclusive* end of the next 14-day window to
    query (i.e. the start of the window is ``next_end_date - 14 days``).
    ``next_sample_offset`` is the offset (from the Step 2 anchor) used to
    choose the next sample file.
    """

    next_end_date: date
    next_sample_offset: int


def initial_cursor(today: date) -> BackfillCursor:
    """Return the cursor for a first-ever run (no prior backfill)."""
    return BackfillCursor(
        next_end_date=today - timedelta(days=BACKFILL_WINDOW_DAYS),
        next_sample_offset=1,
    )


def build_manifest(
    today: date,
    num_sample_files: int,
    cursor: BackfillCursor,
    step3_batch_count: int = 8,
) -> tuple[Manifest, BackfillCursor]:
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
        start_date=today - timedelta(days=BACKFILL_WINDOW_DAYS),
        end_date=today,
    )

    # Step 3: resume from cursor, marching backwards
    step3_batches: list[Batch] = []
    end_date = cursor.next_end_date
    sample_offset = cursor.next_sample_offset

    for _ in range(step3_batch_count):
        start_date = end_date - timedelta(days=BACKFILL_WINDOW_DAYS)
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

    updated_cursor = BackfillCursor(
        next_end_date=end_date,
        next_sample_offset=sample_offset,
    )

    manifest = Manifest(
        step2_batch=step2_batch,
        step3_batches=step3_batches,
    )

    return manifest, updated_cursor


CURSOR_PATH = 'backfill_cursor.json'


def serialize_cursor(cursor: BackfillCursor) -> str:
    """Serialize a cursor to a JSON string."""
    return json.dumps(
        {
            'next_end_date': cursor.next_end_date.isoformat(),
            'next_sample_offset': cursor.next_sample_offset,
        }
    )


def deserialize_cursor(text: str) -> BackfillCursor:
    """Deserialize a cursor from a JSON string."""
    data = json.loads(text)
    return BackfillCursor(
        next_end_date=date.fromisoformat(data['next_end_date']),
        next_sample_offset=data['next_sample_offset'],
    )


def load_cursor(fs: FileSystem, today: date) -> BackfillCursor:
    """Load the backfill cursor from storage, or create an initial one."""
    if fs.exists(CURSOR_PATH):
        return deserialize_cursor(fs.read_text(CURSOR_PATH))
    return initial_cursor(today)


def save_cursor(fs: FileSystem, cursor: BackfillCursor) -> None:
    """Persist the backfill cursor to storage."""
    fs.write_text(CURSOR_PATH, serialize_cursor(cursor))


MANIFEST_PATH = 'todays_manifest.json'


def _serialize_batch(batch: Batch) -> dict[str, str | int]:
    return {
        'sample_file_index': batch.sample_file_index,
        'start_date': batch.start_date.isoformat(),
        'end_date': batch.end_date.isoformat(),
    }


def _deserialize_batch(data: dict[str, str | int]) -> Batch:
    return Batch(
        sample_file_index=int(data['sample_file_index']),
        start_date=date.fromisoformat(str(data['start_date'])),
        end_date=date.fromisoformat(str(data['end_date'])),
    )


def serialize_manifest(manifest: Manifest, next_cursor: BackfillCursor) -> str:
    """Serialize a manifest (plus next cursor) to a JSON string.

    The JSON format is consumed by both the Cloud Workflow dispatcher and
    the ``commit_grid_point_backfill`` job that promotes ``next_cursor`` to
    the live cursor after a successful run.
    """
    return json.dumps(
        {
            'step2_batch': _serialize_batch(manifest.step2_batch),
            'step3_batches': [_serialize_batch(b) for b in manifest.step3_batches],
            'next_cursor': {
                'next_end_date': next_cursor.next_end_date.isoformat(),
                'next_sample_offset': next_cursor.next_sample_offset,
            },
        }
    )


def deserialize_manifest(text: str) -> tuple[Manifest, BackfillCursor]:
    """Deserialize a manifest (and its next cursor) from a JSON string."""
    data = json.loads(text)
    manifest = Manifest(
        step2_batch=_deserialize_batch(data['step2_batch']),
        step3_batches=[_deserialize_batch(b) for b in data['step3_batches']],
    )
    next_cursor = BackfillCursor(
        next_end_date=date.fromisoformat(data['next_cursor']['next_end_date']),
        next_sample_offset=int(data['next_cursor']['next_sample_offset']),
    )
    return manifest, next_cursor


def plan_backfill(
    today: date,
    num_sample_files: int,
    fs: FileSystem,
    step3_batch_count: int = 8,
) -> Manifest:
    """Compute today's manifest and persist it to storage.

    Reads the current cursor, builds the manifest, and writes both the
    manifest and the *next* cursor to :data:`MANIFEST_PATH`. The live
    cursor at :data:`CURSOR_PATH` is **not** advanced — that happens later
    via :func:`commit_backfill`, after the batches have been dispatched
    successfully.
    """
    cursor = load_cursor(fs, today)
    manifest, next_cursor = build_manifest(
        today, num_sample_files, cursor, step3_batch_count=step3_batch_count
    )
    fs.write_text(MANIFEST_PATH, serialize_manifest(manifest, next_cursor))
    return manifest


def commit_backfill(fs: FileSystem) -> BackfillCursor:
    """Promote the manifest's next cursor to the live cursor.

    Called after all batches dispatched by the workflow have completed.
    """
    _, next_cursor = deserialize_manifest(fs.read_text(MANIFEST_PATH))
    save_cursor(fs, next_cursor)
    return next_cursor
