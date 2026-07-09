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
    MIN_ARCHIVE_DATE,
    BackfillCursor,
    BackfillPlan,
    BackfillScope,
    build_backfill_plan,
    default_window_days,
)

_EPOCH = date(1970, 1, 1)

# The weather-grid historical march runs on a *fixed* window grid: every
# Step-3 window is one cell of a 14-day tiling of ``[MIN_ARCHIVE_DATE,
# today]``, indexed upwards from the bottom. This date is the exclusive end
# of the bottom-most cell (index 0), whose start is truncated to
# ``MIN_ARCHIVE_DATE`` — it is the first 14-day boundary above the floor.
#
# The phase is not arbitrary: every window the march has already fetched is
# congruent to this date modulo 14 days, so the grid re-uses the as-built
# tiling rather than laying a second, offset one over the same corpus.
# Fixing the grid is what makes density passes safe. Were each pass instead
# re-anchored at its own start date (~34 daily runs apart, and 34 is not a
# multiple of 14), successive passes would tile the same history with
# *overlapping* windows, and a grid point drawn twice for overlapping date
# ranges would duplicate rows in the prepared corpus.
_BOTTOM_WINDOW_END = date(2016, 1, 8)

# How many density passes the historical march makes before going quiescent.
# One pass lays a single sample file over each window of the grid; after `k`
# passes each window holds `k` distinct sample files, i.e. a relative spatial
# density of `k / num_sample_files` (4/32 = 12.5%).
#
# The cap is not optional. A pass over [2016, today] is ~274 windows at 8
# windows/day ≈ 34 daily runs, so full density (32 passes) would take ~3
# years of running. Densification is monotonic, so this can be raised later
# without rework or re-fetching.
TARGET_DENSITY_PASSES = 4


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

    The march is two-dimensional. ``next_end_date`` is the *exclusive*
    end of the next backfill window — the fast, inner axis, bounded
    below by ``MIN_ARCHIVE_DATE``. ``density_pass`` is the slow, outer
    axis: how many times the march has already swept the whole grid.
    Reaching the floor rolls the cursor to the next pass rather than
    ending the march, and shifts every window's sample file by one, so
    each pass adds a distinct sample file to every window.
    """

    next_end_date: date
    density_pass: int


def _window_index(end_date: date, window_days: int) -> int:
    """Index of the grid window ending at *end_date*, counting up from 0.

    Window 0 is the bottom-most (truncated) window; a negative index
    means *end_date* has fallen below the archive floor, i.e. the
    current density pass has swept the whole grid.
    """
    return (end_date - _BOTTOM_WINDOW_END).days // window_days


def _window_start(end_date: date, window_days: int) -> date:
    """Inclusive start of the grid window ending at *end_date*.

    The bottom window is truncated to ``MIN_ARCHIVE_DATE`` — the source
    rejects the whole request when ``start_date`` falls below the floor,
    so an untruncated bottom window would throw away its valid days too.
    """
    if _window_index(end_date, window_days) == 0:
        return MIN_ARCHIVE_DATE
    return end_date - timedelta(days=window_days)


def _top_window_end(today: date, window_days: int) -> date:
    """Exclusive end of the topmost grid window a pass may start from.

    One full window below *today*, snapped down onto the grid, so the
    historical march never overlaps the trailing window Step 2 samples.
    """
    latest = today - timedelta(days=window_days)
    return _BOTTOM_WINDOW_END + timedelta(
        days=_window_index(latest, window_days) * window_days
    )


def initial_weather_grid_backfill_cursor(today: date) -> WeatherGridBackfillCursor:
    """Cursor value for a first-ever weather-grid run.

    Starts the first density pass at the top of the grid, leaving the
    trailing window for the schedule's Step 2 sample (today's
    recent-history slot).
    """
    return WeatherGridBackfillCursor(
        next_end_date=_top_window_end(
            today, default_window_days(BackfillScope.WEATHER_GRID)
        ),
        density_pass=0,
    )


def pv_sites_schedule(plan: BackfillPlan, pv_system_ids: list[int]) -> list[PVSlice]:
    """Enumerate today's PV-sites slices: one per ``(system, window)``.

    *plan* is the freshly-built backfill plan whose ``[start, end)``
    window applies to every slice; *pv_system_ids* is the universe of
    systems the backfill covers.

    An empty window means the march has reached the archive floor: no
    slices, so the workflow dispatches nothing and commits an unchanged
    cursor.
    """
    if plan.start_date >= plan.end_date:
        return []
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
    target_passes: int = TARGET_DENSITY_PASSES,
) -> tuple[list[WeatherSlice], WeatherGridBackfillCursor]:
    """Today's weather-grid slices and the updated cursor.

    Emits one Step-2 slice (today's trailing window on the sample file
    picked by ``today_epoch_days % num_sample_files``) followed by up to
    *step3_batch_count* Step-3 slices marching backwards down the fixed
    window grid from *cursor*. The Step-2 slice is always first. The
    cursor advances on the Step-3 slices only — Step 2 is independent of
    the historical march.

    A Step-3 slice's sample file is a pure function of its window and
    the density pass, ``(density_pass + window_index) % num_sample_files``.
    Consecutive windows within a pass therefore draw consecutive sample
    files, and a given window draws a different sample file on every
    pass. When a pass reaches the archive floor the cursor rolls to the
    next pass at the top of the grid; after *target_passes* the march
    goes quiescent and only Step 2 keeps sampling.
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
    density_pass = cursor.density_pass
    for _ in range(step3_batch_count):
        if density_pass >= target_passes:
            break
        if _window_index(end_date, window_days) < 0:
            density_pass += 1
            if density_pass >= target_passes:
                break
            end_date = _top_window_end(today, window_days)
            if _window_index(end_date, window_days) < 0:
                break
        start_date = _window_start(end_date, window_days)
        step3_slices.append(
            WeatherSlice(
                grid_point_sample_index=(
                    density_pass + _window_index(end_date, window_days)
                )
                % num_sample_files,
                start_date=start_date,
                end_date=end_date,
            )
        )
        end_date = start_date

    next_cursor = WeatherGridBackfillCursor(
        next_end_date=end_date,
        density_pass=density_pass,
    )
    return [step2_slice, *step3_slices], next_cursor
