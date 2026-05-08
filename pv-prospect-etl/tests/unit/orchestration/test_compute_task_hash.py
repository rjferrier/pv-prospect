"""Tests for compute_task_hash."""

from pv_prospect.etl.orchestration import compute_task_hash


def test_returns_sha256_hex_digest() -> None:
    digest = compute_task_hash([{'name': 'JOB_TYPE', 'value': 'extract'}])

    assert len(digest) == 64
    assert all(c in '0123456789abcdef' for c in digest)


def test_deterministic_for_same_input() -> None:
    env = [
        {'name': 'JOB_TYPE', 'value': 'extract'},
        {'name': 'PV_SYSTEM_ID', 'value': '89665'},
    ]

    assert compute_task_hash(env) == compute_task_hash(env)


def test_order_independent() -> None:
    env_a = [
        {'name': 'JOB_TYPE', 'value': 'extract'},
        {'name': 'PV_SYSTEM_ID', 'value': '89665'},
    ]
    env_b = [
        {'name': 'PV_SYSTEM_ID', 'value': '89665'},
        {'name': 'JOB_TYPE', 'value': 'extract'},
    ]

    assert compute_task_hash(env_a) == compute_task_hash(env_b)


def test_excludes_existing_task_hash_from_computation() -> None:
    base = [{'name': 'JOB_TYPE', 'value': 'extract'}]
    with_hash = base + [{'name': 'TASK_HASH', 'value': 'spurious'}]

    assert compute_task_hash(base) == compute_task_hash(with_hash)


def test_different_inputs_produce_different_hashes() -> None:
    a = compute_task_hash([{'name': 'PV_SYSTEM_ID', 'value': '89665'}])
    b = compute_task_hash([{'name': 'PV_SYSTEM_ID', 'value': '12345'}])

    assert a != b
