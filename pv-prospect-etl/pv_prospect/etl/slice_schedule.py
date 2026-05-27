"""Pure slice-list generation for backfill schedules.

A *slice* is the unit of work both the extraction and transformation
backfill pipelines plan around: a ``(system or sample-file, date
window)`` tuple that maps to one prepared partition file on the
transformation side and to a small bundle of raw extraction tasks on
the extraction side. Centralising slice generation here keeps the
two pipelines in sync by construction — they can't disagree on
slice boundaries because they don't each compute them.

Pure functions only — no I/O, no clock reads. Callers pass today's
date, the live cursor, and any pipeline-specific configuration; the
functions return the slice list (plus, for stateful schedules, the
updated cursor).
"""

from dataclasses import dataclass
from datetime import date, timedelta

from pv_prospect.etl.backfill import (
    BackfillCursor,
    BackfillPlan,
    BackfillScope,
    build_backfill_plan,
    default_window_days,
)

_EPOCH = date(1970, 1, 1)


@dataclass(frozen=True)
class PVSlice:
    """One PV-site backfill slice: a ``(system, window)`` pair.

    ``start_date`` is inclusive and ``end_date`` exclusive. The
    extraction backfill emits a PV-data and a weather-data raw-fetch
    task for this slice; the transformation backfill produces the
    one prepared PV partition file aggregating them.
    """

    pv_system_id: int
    start_date: date
    end_date: date


@dataclass(frozen=True)
class WeatherSlice:
    """One weather-grid backfill slice: a ``(sample file, window)`` pair.

    ``start_date`` is inclusive and ``end_date`` exclusive.
    ``grid_point_sample_index`` selects the sample file (a list of
    grid points) the slice belongs to. The extraction backfill emits
    one raw-fetch task per grid point in the sample file for this
    window; the transformation backfill produces the one prepared
    weather partition file aggregating them.
    """

    grid_point_sample_index: int
    start_date: date
    end_date: date


@dataclass(frozen=True)
class WeatherGridBackfillCursor:
    """Where the weather-grid schedule's historical march left off.

    ``next_end_date`` is the *exclusive* end of the next backfill
    window. ``next_sample_offset`` is the sample-file offset to use
    next — see :func:`weather_grid_schedule` for how it's combined
    with today's anchor to pick a sample file.
    """

    next_end_date: date
    next_sample_offset: int


def initial_weather_grid_backfill_cursor(today: date) -> WeatherGridBackfillCursor:
    """Cursor value for a first-ever weather-grid run.

    Places the next backfill window one full window-length before
    *today*, leaving the trailing window for the schedule's Step 2
    sample (today's recent-history slot).
    """
    return WeatherGridBackfillCursor(
        next_end_date=today
        - timedelta(days=default_window_days(BackfillScope.WEATHER_GRID)),
        next_sample_offset=1,
    )


def pv_sites_schedule(plan: BackfillPlan, pv_system_ids: list[int]) -> list[PVSlice]:
    """Enumerate today's PV-sites slices: one per ``(system, window)``.

    *plan* is the freshly-built backfill plan whose ``[start, end)``
    window applies to every slice; *pv_system_ids* is the universe of
    systems the backfill covers.
    """
    return [
        PVSlice(pv_system_id=sid, start_date=plan.start_date, end_date=plan.end_date)
        for sid in pv_system_ids
    ]


def pv_sites_plan(
    cursor: BackfillCursor,
    window_days: int | None = None,
) -> tuple[BackfillPlan, BackfillCursor]:
    """Build today's PV-sites backfill plan and return the next cursor.

    Thin wrapper around :func:`build_backfill_plan` that pins
    *window_days* to the PV-sites default when none is given. Returned
    alongside the plan, the *next* cursor is what
    :func:`commit_backfill` later promotes — kept distinct from the
    cursor passed in.
    """
    if window_days is None:
        window_days = default_window_days(BackfillScope.PV_SITES)
    return build_backfill_plan(cursor, window_days)


def weather_grid_schedule(
    today: date,
    num_sample_files: int,
    cursor: WeatherGridBackfillCursor,
    step3_batch_count: int = 8,
) -> tuple[list[WeatherSlice], WeatherGridBackfillCursor]:
    """Today's weather-grid slices and the updated cursor.

    Emits one Step-2 slice (today's trailing window on the sample
    file picked by ``today_epoch_days % num_sample_files``) followed
    by *step3_batch_count* Step-3 slices that march backwards in time
    from *cursor*. The Step-2 slice is always first. The cursor
    advances based on the Step-3 slices only — Step 2 is independent
    of the historical-march progress.
    """
    window_days = default_window_days(BackfillScope.WEATHER_GRID)
    anchor = (today - _EPOCH).days % num_sample_files

    step2_slice = WeatherSlice(
        grid_point_sample_index=anchor,
        start_date=today - timedelta(days=window_days),
        end_date=today,
    )

    step3_slices: list[WeatherSlice] = []
    end_date = cursor.next_end_date
    sample_offset = cursor.next_sample_offset
    for _ in range(step3_batch_count):
        start_date = end_date - timedelta(days=window_days)
        sample_index = (anchor - sample_offset) % num_sample_files
        step3_slices.append(
            WeatherSlice(
                grid_point_sample_index=sample_index,
                start_date=start_date,
                end_date=end_date,
            )
        )
        end_date = start_date
        sample_offset += 1

    next_cursor = WeatherGridBackfillCursor(
        next_end_date=end_date,
        next_sample_offset=sample_offset,
    )
    return [step2_slice, *step3_slices], next_cursor
