"""Tests for pv_sites_schedule."""

from datetime import date

from pv_prospect.etl import BackfillPlan, PVSlice, pv_sites_schedule

_PLAN = BackfillPlan(start_date=date(2026, 4, 1), end_date=date(2026, 4, 29))


def test_emits_one_slice_per_pv_system() -> None:
    pv_system_ids = [89665, 12345, 67890]

    slices = pv_sites_schedule(_PLAN, pv_system_ids)

    assert len(slices) == len(pv_system_ids)


def test_every_slice_carries_the_plan_window() -> None:
    slices = pv_sites_schedule(_PLAN, [89665, 12345])

    for pv_slice in slices:
        assert pv_slice.start_date == _PLAN.start_date
        assert pv_slice.end_date == _PLAN.end_date


def test_slices_preserve_pv_system_id_order() -> None:
    pv_system_ids = [67890, 89665, 12345]

    slices = pv_sites_schedule(_PLAN, pv_system_ids)

    assert [s.pv_system_id for s in slices] == pv_system_ids


def test_empty_pv_system_ids_yields_no_slices() -> None:
    assert pv_sites_schedule(_PLAN, []) == []


def test_slice_values_are_pv_slices() -> None:
    slices = pv_sites_schedule(_PLAN, [89665])

    assert isinstance(slices[0], PVSlice)
