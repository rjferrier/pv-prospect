"""Daily PV backfill for PV sites.

Computes the work plan for a single day's PV-site backfill budget:

- Backfill PV output data for all PV sites over a 28-day window, marching
  backwards through history one window per day.
- Backfill weather data for the same PV sites over the same 28-day window
  (covered as two 14-day sub-windows, matching the OpenMeteo API's natural
  granularity).

The cursor / plan-commit machinery lives in :mod:`pv_prospect.etl.backfill`
and is shared with the transformation pipeline. This module is the
extraction-side wrapper that pins the workflow name and the
:class:`BackfillScope`, and additionally enumerates the per-site PV and
weather tasks into the orchestrator ``phases`` shape so the Cloud
Workflow dispatcher gets an env list for every task. Per-site task
identity is computed inside the container by
:func:`pv_prospect.etl.compute_task_hash` against each task's env, so
no ``TASK_HASH`` is pre-injected here.

The 28-day window is the largest round-week multiple compatible with the
PVOutput rate limit. PVOutput's ``getstatus.jsp`` endpoint is single-date
only — one HTTP call per (system, day) — and the API rate-limits at 300
requests per hour. With 10 PV sites and 28 days per site the daily budget
is 280 calls, safely under the cap when dispatched sequentially from the
Cloud Workflow and leaving headroom for retries.
"""

from datetime import date

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPlan,
    BackfillScope,
    build_env_list,
    commit_backfill,
    default_window_days,
    plan_backfill,
)
from pv_prospect.etl.storage import FileSystem

WORKFLOW_NAME = 'pv-prospect-extract-pv-sites-backfill'


def _pv_task_env(
    pv_system_id: int,
    pv_data_source: str,
    start_date: date,
    end_date: date,
    dry_run: str,
    run_date: str,
) -> list[dict[str, str]]:
    return build_env_list(
        JOB_TYPE='extract_and_load',
        DATA_SOURCE=pv_data_source,
        PV_SYSTEM_ID=str(pv_system_id),
        START_DATE=start_date.isoformat(),
        END_DATE=end_date.isoformat(),
        DRY_RUN=dry_run,
        SPLIT_BY='day',
        WORKFLOW_NAME=WORKFLOW_NAME,
        RUN_DATE=run_date,
    )


def _weather_task_env(
    pv_system_id: int,
    weather_data_source: str,
    start_date: date,
    end_date: date,
    run_date: str,
) -> list[dict[str, str]]:
    return build_env_list(
        JOB_TYPE='extract_and_load',
        DATA_SOURCE=weather_data_source,
        PV_SYSTEM_ID=str(pv_system_id),
        START_DATE=start_date.isoformat(),
        END_DATE=end_date.isoformat(),
        WORKFLOW_NAME=WORKFLOW_NAME,
        RUN_DATE=run_date,
    )


def build_phases(
    plan: BackfillPlan,
    pv_system_ids: list[int],
    pv_data_source: str,
    weather_data_source: str,
    dry_run: str,
    run_date: str,
) -> list[list[list[dict[str, str]]]]:
    """Enumerate per-site PV and weather tasks into orchestrator phases.

    Phase 0 holds the per-site PV ``extract_and_load`` envs (one per
    system) and is dispatched sequentially by the Cloud Workflow to stay
    under the PVOutput rate limit. Phase 1 holds the per-site weather
    ``extract_and_load`` envs and is dispatched in parallel since
    OpenMeteo's limits are generous enough.
    """
    pv_phase = [
        _pv_task_env(
            sid,
            pv_data_source,
            plan.start_date,
            plan.end_date,
            dry_run,
            run_date,
        )
        for sid in pv_system_ids
    ]
    weather_phase = [
        _weather_task_env(
            sid,
            weather_data_source,
            plan.start_date,
            plan.end_date,
            run_date,
        )
        for sid in pv_system_ids
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
    if window_days is None:
        window_days = default_window_days(BackfillScope.PV_SITES)
    return plan_backfill(
        cursors_fs,
        manifests_fs,
        WORKFLOW_NAME,
        run_date,
        today,
        window_days,
        phases_fn=lambda plan: build_phases(
            plan,
            pv_system_ids,
            pv_data_source,
            weather_data_source,
            dry_run,
            run_date,
        ),
    )


def commit_pv_site_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> BackfillCursor:
    """Promote the manifest's next cursor to the live cursor.

    Called after all PV + weather extraction jobs for today have completed.
    """
    return commit_backfill(cursors_fs, manifests_fs, WORKFLOW_NAME, run_date)
