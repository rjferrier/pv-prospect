"""Tests for produce_pv_slice."""

import io
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
from pv_prospect.data_transformation.processing import (
    SliceOutcome,
    produce_pv_slice,
    pv_partition_path,
)
from pv_prospect.etl import TIMESERIES_FOLDER, PVSlice

from tests.unit.helpers.fake_file_system import FakeFileSystem

_PV_SOURCE = DataSource.PVOUTPUT
_WEATHER_SOURCE = DataSource.OPENMETEO_HISTORICAL
_SYSTEM_ID = 89665
_SLICE = PVSlice(
    pv_system_id=_SYSTEM_ID,
    start_date=date(2026, 6, 21),
    end_date=date(2026, 6, 23),
)


@pytest.fixture
def pv_site() -> PVSite:
    return PVSite(
        pvo_sys_id=_SYSTEM_ID,
        name='Test Site',
        location=Location(latitude=Decimal('50.4900'), longitude=Decimal('-3.5400')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=(PanelGeometry(tilt=35, azimuth=180, area_fraction=1.0),),
        inverter_system=System(brand='Test', capacity=3600),
    )


def _weather_raw_csv(window: DateRange) -> bytes:
    """Hourly OpenMeteo raw weather for one window-spanning file."""
    periods = len(window) * 24
    df = pd.DataFrame(
        {
            'time': pd.date_range(window.start, periods=periods, freq='h'),
            'temperature_best_match': [15.0] * periods,
            'direct_normal_irradiance_best_match': [500.0] * periods,
            'diffuse_radiation_best_match': [100.0] * periods,
        }
    )
    return df.to_csv(index=False).encode('utf-8')


def _pv_raw_csv(day: date) -> bytes:
    """Hourly PVOutput readings across one day.

    prepare_pv time-weighted-averages PV power onto the weather cadence
    and then downsamples per ``timescale_days``; both steps need enough
    sub-period samples to produce a non-empty output row. One reading
    per hour matches the cadence of the existing prepare_pv unit-test
    fixtures.
    """
    times = [f'{h:02d}:00' for h in range(24)]
    df = pd.DataFrame(
        {
            'date': [day.strftime('%Y-%m-%d')] * 24,
            'time': times,
            'power': [1234.0] * 24,
        }
    )
    return df.to_csv(index=False).encode('utf-8')


def _empty_pv_csv() -> bytes:
    return b'date,time,power\n'


def _write_weather_window(
    raw_fs: FakeFileSystem, pv_site: PVSite, window: DateRange
) -> None:
    path = build_time_series_csv_file_path(
        TIMESERIES_FOLDER, _WEATHER_SOURCE, pv_site, window
    )
    raw_fs.write_bytes(path, _weather_raw_csv(window))


def _write_pv_days(raw_fs: FakeFileSystem, pv_site: PVSite, days: list[date]) -> None:
    for day in days:
        path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER, _PV_SOURCE, pv_site, DateRange.of_single_day(day)
        )
        raw_fs.write_bytes(path, _pv_raw_csv(day))


def _slice_window(pv_slice: PVSlice) -> DateRange:
    return DateRange(pv_slice.start_date, pv_slice.end_date)


def _partition_path(pv_slice: PVSlice) -> str:
    return pv_partition_path(
        pv_slice.pv_system_id,
        pv_slice.start_date.isoformat(),
        pv_slice.end_date.isoformat(),
    )


def test_all_inputs_present_yields_completed(pv_site: PVSite) -> None:
    window = _slice_window(_SLICE)
    raw_fs = FakeFileSystem()
    _write_weather_window(raw_fs, pv_site, window)
    _write_pv_days(raw_fs, pv_site, [date(2026, 6, 21), date(2026, 6, 22)])
    prepared_fs = FakeFileSystem()

    outcome = produce_pv_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _PV_SOURCE,
        _WEATHER_SOURCE,
        lambda _: pv_site,
    )

    assert outcome == SliceOutcome(status='completed', missing_inputs_count=0)


def test_completed_outcome_writes_one_partition_file(pv_site: PVSite) -> None:
    window = _slice_window(_SLICE)
    raw_fs = FakeFileSystem()
    _write_weather_window(raw_fs, pv_site, window)
    _write_pv_days(raw_fs, pv_site, [date(2026, 6, 21), date(2026, 6, 22)])
    prepared_fs = FakeFileSystem()

    produce_pv_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _PV_SOURCE,
        _WEATHER_SOURCE,
        lambda _: pv_site,
    )

    assert prepared_fs.exists(_partition_path(_SLICE))


def test_missing_weather_window_yields_partial_one_and_writes_nothing(
    pv_site: PVSite,
) -> None:
    raw_fs = FakeFileSystem()
    # Stage PV days but not weather.
    _write_pv_days(raw_fs, pv_site, [date(2026, 6, 21), date(2026, 6, 22)])
    prepared_fs = FakeFileSystem()

    outcome = produce_pv_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _PV_SOURCE,
        _WEATHER_SOURCE,
        lambda _: pv_site,
    )

    assert outcome == SliceOutcome(status='partial', missing_inputs_count=1)
    assert not prepared_fs.exists(_partition_path(_SLICE))


def test_one_missing_pv_day_yields_partial_one(pv_site: PVSite) -> None:
    window = _slice_window(_SLICE)
    raw_fs = FakeFileSystem()
    _write_weather_window(raw_fs, pv_site, window)
    # Stage only the first of two days.
    _write_pv_days(raw_fs, pv_site, [date(2026, 6, 21)])
    prepared_fs = FakeFileSystem()

    outcome = produce_pv_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _PV_SOURCE,
        _WEATHER_SOURCE,
        lambda _: pv_site,
    )

    assert outcome == SliceOutcome(status='partial', missing_inputs_count=1)


def test_empty_pv_file_does_not_count_as_missing(pv_site: PVSite) -> None:
    """PVOutput records the API call as completed even when no readings
    were returned; the resulting empty file is the truth about that day,
    not a missing input. The slice should still produce a completed
    outcome (no missing inputs)."""
    window = _slice_window(_SLICE)
    raw_fs = FakeFileSystem()
    _write_weather_window(raw_fs, pv_site, window)
    _write_pv_days(raw_fs, pv_site, [date(2026, 6, 21)])
    # Empty PV file for day 2.
    empty_path = build_time_series_csv_file_path(
        TIMESERIES_FOLDER,
        _PV_SOURCE,
        pv_site,
        DateRange.of_single_day(date(2026, 6, 22)),
    )
    raw_fs.write_bytes(empty_path, _empty_pv_csv())
    prepared_fs = FakeFileSystem()

    outcome = produce_pv_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _PV_SOURCE,
        _WEATHER_SOURCE,
        lambda _: pv_site,
    )

    assert outcome == SliceOutcome(status='completed', missing_inputs_count=0)


def test_partition_file_carries_rows_from_present_days(pv_site: PVSite) -> None:
    window = _slice_window(_SLICE)
    raw_fs = FakeFileSystem()
    _write_weather_window(raw_fs, pv_site, window)
    _write_pv_days(raw_fs, pv_site, [date(2026, 6, 21), date(2026, 6, 22)])
    prepared_fs = FakeFileSystem()

    produce_pv_slice(
        _SLICE,
        raw_fs,
        prepared_fs,
        _PV_SOURCE,
        _WEATHER_SOURCE,
        lambda _: pv_site,
    )

    df = pd.read_csv(io.StringIO(prepared_fs.read_text(_partition_path(_SLICE))))
    # One row per day (downsampled to daily) from the two PV days.
    assert len(df) == 2


def test_rerun_with_missing_day_filled_in_upgrades_to_completed(
    pv_site: PVSite,
) -> None:
    window = _slice_window(_SLICE)
    prepared_fs = FakeFileSystem()

    # Run 1: only day 1's PV present.
    raw_fs_1 = FakeFileSystem()
    _write_weather_window(raw_fs_1, pv_site, window)
    _write_pv_days(raw_fs_1, pv_site, [date(2026, 6, 21)])
    produce_pv_slice(
        _SLICE,
        raw_fs_1,
        prepared_fs,
        _PV_SOURCE,
        _WEATHER_SOURCE,
        lambda _: pv_site,
    )

    # Run 2: both days present. The existing partition is merged in.
    raw_fs_2 = FakeFileSystem()
    _write_weather_window(raw_fs_2, pv_site, window)
    _write_pv_days(raw_fs_2, pv_site, [date(2026, 6, 21), date(2026, 6, 22)])
    outcome = produce_pv_slice(
        _SLICE,
        raw_fs_2,
        prepared_fs,
        _PV_SOURCE,
        _WEATHER_SOURCE,
        lambda _: pv_site,
    )

    assert outcome.status == 'completed'
    df = pd.read_csv(io.StringIO(prepared_fs.read_text(_partition_path(_SLICE))))
    assert len(df) == 2
