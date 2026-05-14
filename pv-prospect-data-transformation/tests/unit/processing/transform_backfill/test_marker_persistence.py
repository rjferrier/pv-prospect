"""Tests for the consumed-through marker: round-trip and load/save."""

from pv_prospect.data_transformation.processing import (
    ConsumedMarker,
    deserialize_marker,
    load_marker,
    save_marker,
    serialize_marker,
)

from tests.unit.helpers.fake_file_system import FakeFileSystem

_WORKFLOW = 'pv-prospect-transform-pv-sites-backfill'


def test_marker_round_trips_through_serialization() -> None:
    marker = ConsumedMarker(consumed_through='2026-05-14-070741-wf.jsonl')

    assert deserialize_marker(serialize_marker(marker)) == marker


def test_load_marker_returns_initial_empty_when_file_absent() -> None:
    assert load_marker(FakeFileSystem(), _WORKFLOW) == ConsumedMarker(
        consumed_through=''
    )


def test_initial_marker_sorts_before_every_ledger_name() -> None:
    """The empty initial marker must sort below any real ledger filename
    so the first run consumes from the oldest extraction ledger."""
    assert ConsumedMarker().consumed_through < '2020-01-01-000000-wf.jsonl'


def test_save_then_load_marker_round_trips_through_filesystem() -> None:
    cursors_fs = FakeFileSystem()
    marker = ConsumedMarker(consumed_through='2026-05-14-024844-extract.jsonl')

    save_marker(cursors_fs, _WORKFLOW, marker)

    assert load_marker(cursors_fs, _WORKFLOW) == marker


def test_marker_lives_at_the_workflow_scoped_path() -> None:
    cursors_fs = FakeFileSystem()

    save_marker(cursors_fs, _WORKFLOW, ConsumedMarker(consumed_through='x'))

    assert cursors_fs.exists(f'{_WORKFLOW}.json')
