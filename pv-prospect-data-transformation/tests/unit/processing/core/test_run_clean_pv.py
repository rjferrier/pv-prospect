"""Tests for run_clean_pv."""

import logging
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest
from pv_prospect.common.domain import (
    DateRange,
    Location,
    PanelGeometry,
    PVSite,
    Shading,
    System,
)
from pv_prospect.data_sources import DataSource, build_time_series_csv_file_path
from pv_prospect.data_transformation.processing import run_clean_pv
from pv_prospect.etl import TIMESERIES_FOLDER

from tests.unit.helpers.fake_file_system import FakeFileSystem

_PV_SOURCE = DataSource.PVOUTPUT


@pytest.fixture
def pv_site() -> PVSite:
    return PVSite(
        pvo_sys_id=25724,
        name='Test Site',
        location=Location(latitude=Decimal('50.4900'), longitude=Decimal('-3.5400')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=(PanelGeometry(tilt=35, azimuth=180, area_fraction=1.0),),
        inverter_system=System(brand='Test', capacity=3600),
    )


def _day_path(pv_site: PVSite, day: date) -> str:
    return build_time_series_csv_file_path(
        TIMESERIES_FOLDER, _PV_SOURCE, pv_site, DateRange.of_single_day(day)
    )


def _raw_pv_csv(day: date) -> bytes:
    """A minimal valid PVOutput CSV (one reading) for *day*."""
    df = pd.DataFrame(
        {
            'date': [day.strftime('%Y-%m-%d')],
            'time': ['12:00'],
            'power': [1234.0],
        }
    )
    return df.to_csv(index=False).encode('utf-8')


def _empty_pv_csv() -> bytes:
    """An empty CSV with just the PVOutput header row — what the extractor
    writes when the API returns no readings for a day."""
    return b'date,time,power\n'


def test_each_non_empty_day_produces_a_cleaned_csv(pv_site: PVSite) -> None:
    """Sanity: a well-populated window writes one cleaned CSV per day."""
    raw_fs = FakeFileSystem()
    cleaned_fs = FakeFileSystem()
    days = [date(2026, 1, 15), date(2026, 1, 16), date(2026, 1, 17)]
    for day in days:
        raw_fs.write_bytes(_day_path(pv_site, day), _raw_pv_csv(day))

    run_clean_pv(
        raw_fs, cleaned_fs, _PV_SOURCE, pv_site, DateRange(days[0], date(2026, 1, 18))
    )

    for day in days:
        assert cleaned_fs.exists(_day_path(pv_site, day))


def test_empty_per_day_csv_is_skipped_with_a_warning(
    pv_site: PVSite, caplog: pytest.LogCaptureFixture
) -> None:
    """An empty per-day CSV (PVOutput returned no readings) is a real data
    point — the system was offline that day — not a corruption. Skip it
    with a warning so the rest of the window still cleans; matches
    ``run_clean_weather``'s behaviour for empty days within a window.
    """
    raw_fs = FakeFileSystem()
    cleaned_fs = FakeFileSystem()
    populated_day = date(2026, 1, 15)
    empty_day = date(2026, 1, 16)
    raw_fs.write_bytes(_day_path(pv_site, populated_day), _raw_pv_csv(populated_day))
    raw_fs.write_bytes(_day_path(pv_site, empty_day), _empty_pv_csv())

    with caplog.at_level(logging.WARNING):
        run_clean_pv(
            raw_fs,
            cleaned_fs,
            _PV_SOURCE,
            pv_site,
            DateRange(populated_day, date(2026, 1, 17)),
        )

    assert cleaned_fs.exists(_day_path(pv_site, populated_day))
    assert not cleaned_fs.exists(_day_path(pv_site, empty_day))
    assert any('No data for 20260116' in record.message for record in caplog.records)


def test_one_empty_day_does_not_block_subsequent_days(pv_site: PVSite) -> None:
    """A hole in the middle of a window doesn't abort the run — days
    either side of the empty day still produce cleaned outputs."""
    raw_fs = FakeFileSystem()
    cleaned_fs = FakeFileSystem()
    before = date(2026, 1, 15)
    hole = date(2026, 1, 16)
    after = date(2026, 1, 17)
    raw_fs.write_bytes(_day_path(pv_site, before), _raw_pv_csv(before))
    raw_fs.write_bytes(_day_path(pv_site, hole), _empty_pv_csv())
    raw_fs.write_bytes(_day_path(pv_site, after), _raw_pv_csv(after))

    run_clean_pv(
        raw_fs, cleaned_fs, _PV_SOURCE, pv_site, DateRange(before, date(2026, 1, 18))
    )

    assert cleaned_fs.exists(_day_path(pv_site, before))
    assert cleaned_fs.exists(_day_path(pv_site, after))
    assert not cleaned_fs.exists(_day_path(pv_site, hole))
