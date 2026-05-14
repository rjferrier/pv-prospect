"""Tests for commit_transform_backfill."""

import json

from pv_prospect.data_transformation.processing import (
    ConsumedMarker,
    commit_transform_backfill,
    load_marker,
    save_marker,
    workflow_name_for,
)
from pv_prospect.etl import BackfillScope

from tests.unit.helpers.fake_file_system import FakeFileSystem

_RUN_DATE = '2026-05-15'


def _write_sidecar(
    manifests_fs: FakeFileSystem, scope: BackfillScope, next_marker: str
) -> None:
    manifests_fs.write_text(
        f'{_RUN_DATE}/{workflow_name_for(scope)}.marker.json',
        json.dumps({'next_marker': next_marker}),
    )


def test_promotes_sidecar_next_marker_to_the_live_marker() -> None:
    scope = BackfillScope.PV_SITES
    manifests_fs = FakeFileSystem()
    cursors_fs = FakeFileSystem()
    next_marker = '2026-05-14-024844-pv-prospect-extract-pv-sites-backfill.jsonl'
    _write_sidecar(manifests_fs, scope, next_marker)

    returned = commit_transform_backfill(scope, _RUN_DATE, manifests_fs, cursors_fs)

    assert returned == next_marker
    assert load_marker(cursors_fs, workflow_name_for(scope)) == ConsumedMarker(
        consumed_through=next_marker
    )


def test_commit_overwrites_a_prior_live_marker() -> None:
    scope = BackfillScope.WEATHER_GRID
    manifests_fs = FakeFileSystem()
    cursors_fs = FakeFileSystem()
    save_marker(
        cursors_fs, workflow_name_for(scope), ConsumedMarker('old-marker.jsonl')
    )
    new_marker = '2026-05-14-063130-pv-prospect-extract-weather-grid-backfill.jsonl'
    _write_sidecar(manifests_fs, scope, new_marker)

    commit_transform_backfill(scope, _RUN_DATE, manifests_fs, cursors_fs)

    assert (
        load_marker(cursors_fs, workflow_name_for(scope)).consumed_through == new_marker
    )
