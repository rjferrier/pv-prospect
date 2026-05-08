"""Tests for inject_task_hash."""

from pv_prospect.etl.orchestration import compute_task_hash, inject_task_hash


def test_appends_task_hash_entry() -> None:
    env = [{'name': 'JOB_TYPE', 'value': 'extract'}]

    result = inject_task_hash(env)

    hash_entries = [e for e in result if e['name'] == 'TASK_HASH']
    assert len(hash_entries) == 1
    assert hash_entries[0]['value'] == compute_task_hash(env)


def test_replaces_existing_task_hash() -> None:
    env = [
        {'name': 'JOB_TYPE', 'value': 'extract'},
        {'name': 'TASK_HASH', 'value': 'stale'},
    ]

    result = inject_task_hash(env)

    hash_entries = [e for e in result if e['name'] == 'TASK_HASH']
    assert len(hash_entries) == 1
    assert hash_entries[0]['value'] != 'stale'


def test_does_not_mutate_input() -> None:
    env = [{'name': 'JOB_TYPE', 'value': 'extract'}]

    inject_task_hash(env)

    assert env == [{'name': 'JOB_TYPE', 'value': 'extract'}]


def test_preserves_non_hash_entries() -> None:
    env = [
        {'name': 'JOB_TYPE', 'value': 'extract'},
        {'name': 'PV_SYSTEM_ID', 'value': '89665'},
    ]

    result = inject_task_hash(env)

    non_hash = [e for e in result if e['name'] != 'TASK_HASH']
    assert non_hash == env
