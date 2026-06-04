"""Tests for build_manifest."""

import json
from datetime import datetime, timezone

import pandas as pd
from pv_prospect.data_transformation.processing.validation_window import (
    WINDOW_COLUMNS,
    build_manifest,
)

_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)


def _make_window(rows: list[list]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=WINDOW_COLUMNS)
    df['time'] = pd.to_datetime(df['time'])
    return df


def test_manifest_contents() -> None:
    window = _make_window([
        [1, '2026-01-15', 10.0, 100.0, 500.0, 4500.0],
        [1, '2026-03-31', 10.0, 100.0, 500.0, 4500.0],
        [2, '2026-02-01', 10.0, 100.0, 500.0, 4500.0],
    ])
    manifest = build_manifest(window, 90, now=_NOW)
    assert manifest['updated_at'] == '2026-06-04T12:00:00+00:00'
    assert manifest['window_days'] == 90
    assert manifest['window_start'] == '2026-01-15'
    assert manifest['window_end'] == '2026-03-31'
    assert manifest['row_counts'] == {'1': 2, '2': 1}


def test_json_round_trip() -> None:
    # Verifies no numpy int types sneak into the manifest and break json.dumps.
    window = _make_window([
        [89665, '2026-01-15', 10.0, 100.0, 500.0, 4500.0],
        [89665, '2026-01-16', 10.0, 100.0, 500.0, 4500.0],
    ])
    manifest = build_manifest(window, 90, now=_NOW)
    reloaded = json.loads(json.dumps(manifest))
    assert reloaded['row_counts'] == {'89665': 2}


def test_empty_window() -> None:
    df = pd.DataFrame(columns=WINDOW_COLUMNS)
    manifest = build_manifest(df, 90, now=_NOW)
    assert manifest['window_start'] is None
    assert manifest['window_end'] is None
    assert manifest['row_counts'] == {}
