"""Unit tests for the site ID obfuscation mapping."""

from pv_prospect.app.main import SITE_DISPLAY_ID, SITE_REAL_ID


def test_display_ids_are_one_through_ten() -> None:
    assert set(SITE_DISPLAY_ID.values()) == set(range(1, 11))


def test_real_ids_are_all_distinct() -> None:
    assert len(SITE_DISPLAY_ID) == len(set(SITE_DISPLAY_ID.keys()))


def test_mappings_are_mutual_inverses() -> None:
    for real_id, display_id in SITE_DISPLAY_ID.items():
        assert SITE_REAL_ID[display_id] == real_id


def test_real_pvoutput_ids_not_in_display_mapping() -> None:
    for real_id in SITE_DISPLAY_ID:
        assert real_id not in SITE_REAL_ID, (
            f'PVOutput ID {real_id} appears as a display ID — '
            'the obfuscation maps the wrong direction'
        )
