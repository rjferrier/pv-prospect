"""Compare manifests vs ledgers for all workflows on a given date.

Usage::

    python -m check_workflow [<date>]

<date>
    Run date as ``YYYY-MM-DD`` (default: today UTC).

Workflows are discovered by listing
``tracking/manifests/<date>/`` — every ``<workflow>.json`` file found
there is analysed in turn.

Output
------
For *batch-style* workflows whose manifest tasks carry a ``LOCATIONS``
env var (WGB, any future batched workflow), a per-batch table is shown:
sample index, date window, planned-site count, completed, partial,
failed-only, and missing counts.

For *direct-task* workflows (daily extract, pv-sites backfill) each
manifest task maps to exactly one site, so only a summary is shown plus
the first few non-completed task descriptors.

Tasks are matched to ledger entries by descriptor (data_source,
start_date, end_date, site identity) rather than by TASK_HASH, so
the script is robust to whether the running image pre-dates
inject_task_hash.

Ledger sources checked:

* Consolidated  ``tracking/ledger/<date>/*-<workflow>.jsonl``
* Scratch        ``tracking/ledger/<date>/<workflow>/*.jsonl``

If scratch files are still present (consolidation has not yet run),
they are merged with any consolidated file so the counts remain
accurate mid-run.
"""

import argparse
import json
import subprocess  # nosec B404
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

BUCKET = 'pv-prospect-staging'

# Higher rank = better outcome.  'partial' beats 'failed' because some output
# was written; 'completed' beats both.  The resume filter skips only
# 'completed', so 'partial' tasks are re-attempted.
_STATUS_RANK: dict[str, int] = {'failed': 0, 'partial': 1, 'completed': 2}

# Env-var keys to display when listing non-completed tasks.
_SHOW_KEYS = ('DATA_SOURCE', 'START_DATE', 'END_DATE', 'PV_SYSTEM_ID', 'LOCATION')


# ---------------------------------------------------------------------------
# GCS helpers
# ---------------------------------------------------------------------------


def _gsutil_cat(uri: str) -> str | None:
    result = subprocess.run(['gsutil', 'cat', uri], capture_output=True, text=True)  # nosec B603, B607
    if result.returncode != 0:
        return None
    return result.stdout


def _gsutil_ls(pattern: str) -> list[str]:
    result = subprocess.run(['gsutil', 'ls', pattern], capture_output=True, text=True)  # nosec B603, B607
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def _fetch_manifest(date_str: str, workflow: str) -> dict | None:
    uri = f'gs://{BUCKET}/tracking/manifests/{date_str}/{workflow}.json'
    content = _gsutil_cat(uri)
    if content is None:
        print(f'No manifest found: {uri}', file=sys.stderr)
        return None
    return json.loads(content)


def _env_dict(task: list[dict[str, str]]) -> dict[str, str]:
    return {e['name']: e['value'] for e in task}


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------


def _fetch_ledger_entries(date_str: str, workflow: str) -> list[dict]:
    lines: list[str] = []

    # Consolidated: <date_str>/<date_str>-<HHMMSS>-<workflow>.jsonl
    for uri in _gsutil_ls(
        f'gs://{BUCKET}/tracking/ledger/{date_str}/*-{workflow}.jsonl'
    ):
        content = _gsutil_cat(uri)
        if content:
            lines.extend(content.splitlines())

    # Scratch (per-task): <date_str>/<workflow>/<task_hash>.jsonl
    for uri in _gsutil_ls(
        f'gs://{BUCKET}/tracking/ledger/{date_str}/{workflow}/*.jsonl'
    ):
        content = _gsutil_cat(uri)
        if content:
            lines.extend(content.splitlines())

    entries: list[dict] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            entries.append(rec)
    return entries


# ---------------------------------------------------------------------------
# Descriptor-key helpers
# ---------------------------------------------------------------------------

# (data_source, start_date, end_date, pv_system_id, location)
DescKey = tuple[str, str, str, str, str]


def _task_desc_key(env: dict[str, str]) -> DescKey:
    """Descriptor key derived from a manifest task's env vars."""
    return (
        env.get('DATA_SOURCE', ''),
        env.get('START_DATE', env.get('DATE', '')),
        env.get('END_DATE', ''),
        env.get('PV_SYSTEM_ID', ''),
        env.get('LOCATION', ''),
    )


def _entry_desc_key(desc: dict) -> DescKey:
    """Descriptor key derived from a ledger entry's descriptor dict."""
    return (
        desc.get('data_source', ''),
        desc.get('start_date', ''),
        desc.get('end_date', ''),
        desc.get('pv_system_id', ''),
        desc.get('location', ''),
    )


def _best_status_by_desc_key(entries: list[dict]) -> dict[DescKey, str]:
    """Return the best status achieved per descriptor key across all entries."""
    result: dict[DescKey, str] = {}
    for entry in entries:
        desc = entry.get('descriptor') or {}
        key = _entry_desc_key(desc)
        s = entry.get('status', '')
        if s in _STATUS_RANK:
            cur = result.get(key, '')
            if not cur or _STATUS_RANK[s] > _STATUS_RANK.get(cur, -1):
                result[key] = s
    return result


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------


@dataclass
class WorkflowStats:
    workflow: str
    planned: int
    completed: int
    partial: int
    failed: int
    missing: int


def _analyse_batch(
    tasks: list[list[dict[str, str]]],
    entries: list[dict],
    workflow: str,
    date_str: str,
) -> WorkflowStats:
    """Per-batch table for workflows that dispatch many sites per Cloud Run task."""
    # (start_date, end_date, grid_point_sample_index)
    BatchKey = tuple[str, str, str]

    # -- Planned batches ---------------------------------------------------
    planned: list[tuple[dict[str, str], int]] = []
    total_planned = 0
    for task in tasks:
        env = _env_dict(task)
        locations = json.loads(env.get('LOCATIONS', '[]'))
        n = len(locations)
        planned.append((env, n))
        total_planned += n

    # -- Ledger: best status per descriptor key, grouped by batch key ------
    # batch key = (start_date, end_date, grid_point_sample_index) from descriptor
    best_by_desc = _best_status_by_desc_key(entries)

    batch_counts: dict[BatchKey, dict[str, int]] = defaultdict(
        lambda: {'completed': 0, 'partial': 0, 'failed': 0}
    )

    # Build batch_key per descriptor key by consulting the raw entries.
    desc_to_batch: dict[DescKey, BatchKey] = {}
    for entry in entries:
        desc = entry.get('descriptor') or {}
        dk = _entry_desc_key(desc)
        bk: BatchKey = (
            desc.get('start_date', ''),
            desc.get('end_date', ''),
            desc.get('grid_point_sample_index', ''),
        )
        desc_to_batch.setdefault(dk, bk)

    for dk, status in best_by_desc.items():
        matched_bk = desc_to_batch.get(dk)
        if matched_bk is not None:
            batch_counts[matched_bk][status] += 1

    # -- Print ------------------------------------------------------------
    col = 84
    header = (
        f'  {"#":>3}  {"Sample":>6}  {"Start":>10}  {"End":>10}  '
        f'{"Planned":>7}  {"Done":>7}  {"~Part":>7}  {"Failed":>7}  {"Missing":>7}'
    )

    print(f'Workflow : {workflow}')
    print(f'Date     : {date_str}')
    print(f'Batches  : {len(tasks)}')
    print(f'Planned  : {total_planned} sites')
    print()
    print(header)
    print('-' * col)

    total_done = total_partial = total_failed = total_missing = 0
    for i, (env, n_planned) in enumerate(planned, 1):
        sample_idx = env.get('GRID_POINT_SAMPLE_INDEX', '')
        start = env.get('START_DATE', env.get('DATE', ''))
        end = env.get('END_DATE', '')
        bk = (start, end, sample_idx)
        counts = batch_counts.get(bk, {'completed': 0, 'partial': 0, 'failed': 0})
        n_done = counts['completed']
        n_partial = counts['partial']
        n_failed = counts['failed']
        n_missing = n_planned - n_done - n_partial - n_failed
        total_done += n_done
        total_partial += n_partial
        total_failed += n_failed
        total_missing += n_missing
        flag = (
            '  !'
            if (n_missing > 0 or n_failed > 0)
            else ('  ~' if n_partial > 0 else '')
        )
        print(
            f'  {i:>3}  {sample_idx:>6}  {start:>10}  {end:>10}  '
            f'{n_planned:>7}  {n_done:>7}  {n_partial:>7}  '
            f'{n_failed:>7}  {n_missing:>7}{flag}'
        )

    print('-' * col)
    print(
        f'  {"":>3}  {"Total":>6}  {"":>10}  {"":>10}  '
        f'{total_planned:>7}  {total_done:>7}  {total_partial:>7}  '
        f'{total_failed:>7}  {total_missing:>7}'
    )
    return WorkflowStats(
        workflow, total_planned, total_done, total_partial, total_failed, total_missing
    )


def _analyse_direct(
    tasks: list[list[dict[str, str]]],
    entries: list[dict],
    workflow: str,
    date_str: str,
) -> WorkflowStats:
    """Summary for workflows where each manifest task is a single site.

    Matches manifest tasks to ledger entries by descriptor key rather than
    TASK_HASH so results are correct whether or not the running image
    pre-dates inject_task_hash.
    """
    best_by_desc = _best_status_by_desc_key(entries)

    n_completed = n_partial = n_failed = n_missing = 0
    for task in tasks:
        env = _env_dict(task)
        k = _task_desc_key(env)
        s = best_by_desc.get(k, '')
        if s == 'completed':
            n_completed += 1
        elif s == 'partial':
            n_partial += 1
        elif s == 'failed':
            n_failed += 1
        else:
            n_missing += 1

    print(f'Workflow : {workflow}')
    print(f'Date     : {date_str}')
    print(f'Planned  : {len(tasks)}')
    print(f'Completed: {n_completed}')
    if n_partial > 0:
        print(f'Partial  : {n_partial}')
    print(f'Failed   : {n_failed}')
    print(f'Missing  : {n_missing}')

    def _task_parts(env: dict[str, str]) -> str:
        return ' '.join(f'{k}={env[k]}' for k in _SHOW_KEYS if k in env)

    if n_partial > 0:
        print('\nPartial tasks (first 10):')
        shown = 0
        for task in tasks:
            env = _env_dict(task)
            if best_by_desc.get(_task_desc_key(env)) == 'partial':
                print(f'  {_task_parts(env)}')
                shown += 1
                if shown >= 10:
                    break

    if n_failed > 0:
        print('\nFailed tasks (first 10):')
        shown = 0
        for task in tasks:
            env = _env_dict(task)
            if best_by_desc.get(_task_desc_key(env)) == 'failed':
                print(f'  {_task_parts(env)}')
                shown += 1
                if shown >= 10:
                    break

    if n_missing > 0:
        print('\nMissing tasks (first 10 — no ledger entry at all):')
        shown = 0
        for task in tasks:
            env = _env_dict(task)
            if _task_desc_key(env) not in best_by_desc:
                print(f'  {_task_parts(env)}')
                shown += 1
                if shown >= 10:
                    break

    return WorkflowStats(
        workflow, len(tasks), n_completed, n_partial, n_failed, n_missing
    )


def _discover_workflows(date_str: str) -> list[str]:
    """Return sorted workflow names that have a manifest for *date_str*."""
    uris = _gsutil_ls(f'gs://{BUCKET}/tracking/manifests/{date_str}/')
    workflows = []
    for uri in uris:
        name = uri.rstrip('/').rsplit('/', 1)[-1]
        if name.endswith('.json'):
            workflows.append(name[:-5])
    return sorted(workflows)


def _analyse_one(date_str: str, workflow: str) -> WorkflowStats | None:
    manifest = _fetch_manifest(date_str, workflow)
    if manifest is None:
        return None

    tasks: list[list[dict[str, str]]] = [
        task for phase in manifest.get('phases', []) for task in phase
    ]
    if not tasks:
        print(f'Workflow : {workflow}')
        print(f'Date     : {date_str}')
        print('Manifest contains no tasks.')
        return WorkflowStats(workflow, 0, 0, 0, 0, 0)

    entries = _fetch_ledger_entries(date_str, workflow)

    is_batch = any(any(e['name'] == 'LOCATIONS' for e in task) for task in tasks)
    if is_batch:
        return _analyse_batch(tasks, entries, workflow, date_str)
    else:
        return _analyse_direct(tasks, entries, workflow, date_str)


def _print_summary_table(all_stats: list[WorkflowStats]) -> None:
    name_width = max(len(s.workflow) for s in all_stats)
    header = (
        f'  {"Workflow":<{name_width}}  {"Planned":>9}  {"Done":>9}'
        f'  {"~Partial":>9}  {"Failed":>9}  {"Missing":>9}'
    )
    sep = '-' * len(header)
    print(header)
    print(sep)
    for s in all_stats:
        flag = (
            '  !'
            if (s.failed > 0 or s.missing > 0)
            else ('  ~' if s.partial > 0 else '')
        )
        print(
            f'  {s.workflow:<{name_width}}  {s.planned:>9}  {s.completed:>9}'
            f'  {s.partial:>9}  {s.failed:>9}  {s.missing:>9}{flag}'
        )


def analyse_all(date_str: str) -> None:
    workflows = _discover_workflows(date_str)
    if not workflows:
        print(f'No manifests found under tracking/manifests/{date_str}/')
        return

    all_stats: list[WorkflowStats] = []
    for i, workflow in enumerate(workflows):
        if i > 0:
            print()
        stats = _analyse_one(date_str, workflow)
        if stats is not None:
            all_stats.append(stats)

    if len(all_stats) > 1:
        print()
        print(f'Summary for {date_str}')
        print()
        _print_summary_table(all_stats)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Compare manifests vs ledgers for all workflows on a date.'
    )
    parser.add_argument(
        'date',
        nargs='?',
        default=date.today().strftime('%Y-%m-%d'),
        help='Run date YYYY-MM-DD (default: today UTC)',
    )
    args = parser.parse_args()
    analyse_all(args.date)
