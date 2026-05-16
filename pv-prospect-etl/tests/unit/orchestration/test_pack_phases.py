"""Tests for pack_phases."""

from pv_prospect.etl import pack_phases


def test_empty_phase_packs_to_empty_descriptor() -> None:
    phase_entries, phase_docs = pack_phases([[]])

    assert phase_entries == [{'common_env': [], 'task_keys': []}]
    assert phase_docs == [{'rows': []}]


def test_single_task_hoists_every_env_to_common() -> None:
    task = [{'name': 'A', 'value': '1'}, {'name': 'B', 'value': '2'}]

    phase_entries, phase_docs = pack_phases([[task]])

    assert phase_entries[0]['common_env'] == [
        {'name': 'A', 'value': '1'},
        {'name': 'B', 'value': '2'},
    ]
    assert phase_entries[0]['task_keys'] == []
    assert phase_docs == [{'rows': [[]]}]


def test_constant_keys_hoist_varying_keys_become_rows() -> None:
    task1 = [
        {'name': 'STEP', 'value': 'clean'},
        {'name': 'LOCATION', 'value': '50.0,-5.0'},
        {'name': 'TASK_HASH', 'value': 'h1'},
    ]
    task2 = [
        {'name': 'STEP', 'value': 'clean'},
        {'name': 'LOCATION', 'value': '51.0,-4.0'},
        {'name': 'TASK_HASH', 'value': 'h2'},
    ]

    phase_entries, phase_docs = pack_phases([[task1, task2]])

    assert phase_entries[0]['common_env'] == [{'name': 'STEP', 'value': 'clean'}]
    assert phase_entries[0]['task_keys'] == ['LOCATION', 'TASK_HASH']
    assert phase_docs[0]['rows'] == [
        ['50.0,-5.0', 'h1'],
        ['51.0,-4.0', 'h2'],
    ]


def test_each_phase_is_packed_independently() -> None:
    phase0 = [
        [
            {'name': 'STEP', 'value': 'clean'},
            {'name': 'LOCATION', 'value': '50,0'},
        ],
    ]
    phase1 = [
        [
            {'name': 'STEP', 'value': 'prepare'},
            {'name': 'LOCATION', 'value': '51,0'},
        ],
    ]

    phase_entries, _ = pack_phases([phase0, phase1])

    assert phase_entries[0]['common_env'] == [
        {'name': 'LOCATION', 'value': '50,0'},
        {'name': 'STEP', 'value': 'clean'},
    ]
    assert phase_entries[1]['common_env'] == [
        {'name': 'LOCATION', 'value': '51,0'},
        {'name': 'STEP', 'value': 'prepare'},
    ]


def test_task_keys_are_sorted_for_determinism() -> None:
    tasks = [
        [
            {'name': 'Z', 'value': 'z1'},
            {'name': 'A', 'value': 'a1'},
            {'name': 'M', 'value': 'm1'},
        ],
        [
            {'name': 'Z', 'value': 'z2'},
            {'name': 'A', 'value': 'a2'},
            {'name': 'M', 'value': 'm2'},
        ],
    ]

    phase_entries, phase_docs = pack_phases([tasks])

    assert phase_entries[0]['task_keys'] == ['A', 'M', 'Z']
    assert phase_docs[0]['rows'] == [['a1', 'm1', 'z1'], ['a2', 'm2', 'z2']]


def test_heterogeneous_keys_become_varying_with_null_for_missing() -> None:
    """A key present in some tasks but absent in others lives in
    ``task_keys`` with ``None`` for the rows that don't carry it. The
    YAML expander treats ``None`` as 'omit this env-var', so the Cloud
    Run container sees the key as absent rather than empty-string."""
    task_with_id = [
        {'name': 'STEP', 'value': 'assemble'},
        {'name': 'PV_SYSTEM_ID', 'value': '42'},
    ]
    task_without_id = [{'name': 'STEP', 'value': 'assemble'}]

    phase_entries, phase_docs = pack_phases([[task_with_id, task_without_id]])

    assert phase_entries[0]['common_env'] == [{'name': 'STEP', 'value': 'assemble'}]
    assert phase_entries[0]['task_keys'] == ['PV_SYSTEM_ID']
    assert phase_docs[0]['rows'] == [['42'], [None]]


def test_empty_phases_list_returns_empty_outputs() -> None:
    phase_entries, phase_docs = pack_phases([])

    assert phase_entries == []
    assert phase_docs == []


def test_reconstruction_matches_original_env_set() -> None:
    """The packer is lossless: hoisting + zipping reproduces each task's env."""
    original = [
        [
            {'name': 'STEP', 'value': 'clean'},
            {'name': 'WORKFLOW', 'value': 'wf'},
            {'name': 'LOCATION', 'value': '50,0'},
            {'name': 'TASK_HASH', 'value': 'h1'},
        ],
        [
            {'name': 'STEP', 'value': 'clean'},
            {'name': 'WORKFLOW', 'value': 'wf'},
            {'name': 'LOCATION', 'value': '51,0'},
            {'name': 'TASK_HASH', 'value': 'h2'},
        ],
    ]

    phase_entries, phase_docs = pack_phases([original])

    common_env = phase_entries[0]['common_env']
    task_keys = phase_entries[0]['task_keys']
    rows = phase_docs[0]['rows']
    assert isinstance(common_env, list)
    assert isinstance(task_keys, list)
    assert isinstance(rows, list)
    reconstructed = [
        common_env
        + [{'name': k, 'value': v} for k, v in zip(task_keys, row, strict=False)]
        for row in rows
    ]
    reconstructed_as_dicts = [{e['name']: e['value'] for e in t} for t in reconstructed]
    original_as_dicts = [{e['name']: e['value'] for e in t} for t in original]
    assert reconstructed_as_dicts == original_as_dicts
