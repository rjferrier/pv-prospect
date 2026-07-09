"""Tests for build_match_glob.

The expressions asserted here were verified against a real GCS bucket:
``<prefix>/**/<pattern>`` returns objects sitting directly under the
prefix as well as nested ones, and pages over the matches rather than
over the whole subtree.
"""

from pv_prospect.etl.storage.backends.gcs import build_match_glob


def test_match_everything_pattern_needs_no_glob() -> None:
    assert build_match_glob('tracking/ledger/', '*') is None


def test_pattern_is_anchored_under_the_prefix() -> None:
    assert (
        build_match_glob('tracking/ledger/', '*.jsonl') == 'tracking/ledger/**/*.jsonl'
    )


def test_empty_prefix_globs_from_the_bucket_root() -> None:
    assert build_match_glob('', '*.csv') == '**/*.csv'


def test_workflow_suffix_pattern_is_preserved() -> None:
    """The consolidated-ledger matcher must survive translation intact —
    it is what keeps ``pv-prospect-extract`` from pulling in
    ``pv-prospect-extract-pv-sites-backfill``."""
    assert build_match_glob('tracking/ledger/', '*-pv-prospect-extract.jsonl') == (
        'tracking/ledger/**/*-pv-prospect-extract.jsonl'
    )
