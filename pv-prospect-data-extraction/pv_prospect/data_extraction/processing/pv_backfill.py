"""Daily PV backfill for PV sites.

This module is the extraction-side wrapper around the shared
:mod:`pv_prospect.etl.slice_schedule` module: it asks the schedule
for today's :class:`PVSlice` list, builds per-site PV and weather
``extract_and_load`` task envs, and runs the plan-commit dance for
the cursor.

The slice value type and the schedule itself (today's window from
the cursor × the universe of pv_system_ids) live in
:mod:`pv_prospect.etl.slice_schedule` so the transformation pipeline
can consume the same source of truth for slice boundaries.

The 28-day window is the largest round-week multiple compatible with
the PVOutput rate limit. PVOutput's ``getstatus.jsp`` endpoint is
single-date only — one HTTP call per (system, day) — and the API
rate-limits at 300 requests per hour. With 10 PV sites and 28 days
per site the daily budget is 280 calls, safely under the cap when
dispatched sequentially from the Cloud Workflow and leaving headroom
for retries.

A per-task ``TASK_HASH`` is pre-injected so each container has a
stable identity for naming its per-task scratch ledger/log file at
flush time; the per-*site* hashes recorded in each ledger entry are
still computed inside the container.
"""

from datetime import date

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPlan,
    PVSlice,
    build_env_list,
    commit_backfill,
    inject_task_hash,
    load_cursor,
    manifest_filename,
    pv_sites_plan,
    pv_sites_schedule,
    serialize_plan,
)
from pv_prospect.etl.storage import FileSystem

WORKFLOW_NAME = 'pv-prospect-extract-pv-sites-backfill'


def _pv_task_env(
    pv_slice: PVSlice,
    pv_data_source: str,
    dry_run: str,
    run_date: str,
) -> list[dict[str, str]]:
    return build_env_list(
        JOB_TYPE='extract_and_load',
        DATA_SOURCE=pv_data_source,
        PV_SYSTEM_ID=str(pv_slice.pv_system_id),
        START_DATE=pv_slice.start_date.isoformat(),
        END_DATE=pv_slice.end_date.isoformat(),
        DRY_RUN=dry_run,
        SPLIT_BY='day',
        WORKFLOW_NAME=WORKFLOW_NAME,
        RUN_DATE=run_date,
    )


def _weather_task_env(
    pv_slice: PVSlice,
    weather_data_source: str,
    run_date: str,
) -> list[dict[str, str]]:
    return build_env_list(
        JOB_TYPE='extract_and_load',
        DATA_SOURCE=weather_data_source,
        PV_SYSTEM_ID=str(pv_slice.pv_system_id),
        START_DATE=pv_slice.start_date.isoformat(),
        END_DATE=pv_slice.end_date.isoformat(),
        WORKFLOW_NAME=WORKFLOW_NAME,
        RUN_DATE=run_date,
    )


def build_phases(
    slices: list[PVSlice],
    pv_data_source: str,
    weather_data_source: str,
    dry_run: str,
    run_date: str,
) -> list[list[list[dict[str, str]]]]:
    """Enumerate per-slice PV and weather tasks into orchestrator phases.

    Phase 0 holds the per-slice PV ``extract_and_load`` envs (one per
    slice) and is dispatched sequentially by the Cloud Workflow to
    stay under the PVOutput rate limit. Phase 1 holds the per-slice
    weather ``extract_and_load`` envs and is dispatched in parallel
    since OpenMeteo's limits are generous enough.
    """
    pv_phase = [
        inject_task_hash(_pv_task_env(s, pv_data_source, dry_run, run_date))
        for s in slices
    ]
    weather_phase = [
        inject_task_hash(_weather_task_env(s, weather_data_source, run_date))
        for s in slices
    ]
    return [pv_phase, weather_phase]


def plan_pv_site_backfill(
    today: date,
    run_date: str,
    pv_system_ids: list[int],
    pv_data_source: str,
    weather_data_source: str,
    dry_run: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
    window_days: int | None = None,
) -> BackfillPlan:
    """Plan today's PV-site backfill window and persist the manifest.

    The persisted manifest includes the date window, the next cursor,
    and a ``phases`` list — the latter giving the Cloud Workflow one
    env list per dispatched ``extract_and_load`` task.

    The live cursor is not advanced — that happens later via
    :func:`commit_pv_site_backfill`, after the extraction jobs succeed.
    """
    cursor = load_cursor(cursors_fs, WORKFLOW_NAME, today)
    plan, next_cursor = pv_sites_plan(cursor, window_days=window_days)
    slices = pv_sites_schedule(plan, pv_system_ids)
    phases = build_phases(
        slices, pv_data_source, weather_data_source, dry_run, run_date
    )
    manifests_fs.write_text(
        manifest_filename(WORKFLOW_NAME, run_date),
        serialize_plan(plan, next_cursor, phases),
    )
    return plan


def commit_pv_site_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> BackfillCursor:
    """Promote the manifest's next cursor to the live cursor.

    Called after all PV + weather extraction jobs for today have completed.
    """
    return commit_backfill(cursors_fs, manifests_fs, WORKFLOW_NAME, run_date)
