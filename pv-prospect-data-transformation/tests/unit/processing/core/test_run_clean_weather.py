"""Tests for run_clean_weather."""

import io
import json
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest
from pv_prospect.common.domain import (
    AnySite,
    ArbitrarySite,
    DateRange,
    Location,
    PanelGeometry,
    PVSite,
    Shading,
    System,
)
from pv_prospect.data_sources import (
    DataSource,
    build_time_series_csv_file_path,
    csv_path_to_metadata_path,
)
from pv_prospect.data_transformation.processing import run_clean_weather
from pv_prospect.etl import TIMESERIES_FOLDER

from tests.unit.helpers.fake_file_system import FakeFileSystem

_WEATHER_SOURCE = DataSource.OPENMETEO_HISTORICAL
_METADATA = {'latitude': 50.49, 'longitude': -3.54, 'elevation': 120.0}


def _raw_weather_csv(date_range: DateRange) -> bytes:
    """Hourly, model-suffixed raw OpenMeteo weather spanning *date_range*."""
    periods = len(date_range) * 24
    df = pd.DataFrame(
        {
            'time': pd.date_range(date_range.start, periods=periods, freq='h'),
            'temperature_best_match': range(periods),
            'direct_normal_irradiance_best_match': range(periods),
            'diffuse_radiation_best_match': range(periods),
        }
    )
    return df.to_csv(index=False).encode('utf-8')


def _write_raw(fs: FakeFileSystem, site: AnySite, date_range: DateRange) -> str:
    """Stage a raw weather file (+ metadata companion) covering *date_range*.

    The path is derived from *date_range*, so a multi-day range produces a
    ``..._YYYYMMDD_YYYYMMDD.csv`` range file and a single day produces a
    ``..._YYYYMMDD.csv`` file — exactly as the extractors name them.
    """
    path = build_time_series_csv_file_path(
        TIMESERIES_FOLDER, _WEATHER_SOURCE, site, date_range
    )
    fs.write_bytes(path, _raw_weather_csv(date_range))
    fs.write_text(csv_path_to_metadata_path(path), json.dumps(_METADATA))
    return path


def _cleaned_day_path(site: AnySite, day: date) -> str:
    return build_time_series_csv_file_path(
        TIMESERIES_FOLDER, _WEATHER_SOURCE, site, DateRange.of_single_day(day)
    )


@pytest.fixture
def grid_point() -> ArbitrarySite:
    return ArbitrarySite.from_id('504900_-35400')


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


def test_range_file_produces_one_cleaned_csv_per_day(
    grid_point: ArbitrarySite,
) -> None:
    """A backfill's window-spanning raw file is read as a single unit and
    fanned out into one cleaned CSV per day in the window."""
    date_range = DateRange(date(2026, 1, 15), date(2026, 1, 18))
    raw_fs = FakeFileSystem()
    range_path = _write_raw(raw_fs, grid_point, date_range)
    cleaned_fs = FakeFileSystem()

    run_clean_weather(raw_fs, cleaned_fs, _WEATHER_SOURCE, grid_point, date_range)

    for day in (date(2026, 1, 15), date(2026, 1, 16), date(2026, 1, 17)):
        assert cleaned_fs.exists(_cleaned_day_path(grid_point, day))
    # The range-shaped path is an input name, never a cleaned output name.
    assert not cleaned_fs.exists(range_path)


def test_each_cleaned_day_holds_only_its_own_rows(
    grid_point: ArbitrarySite,
) -> None:
    date_range = DateRange(date(2026, 1, 15), date(2026, 1, 18))
    raw_fs = FakeFileSystem()
    _write_raw(raw_fs, grid_point, date_range)
    cleaned_fs = FakeFileSystem()

    run_clean_weather(raw_fs, cleaned_fs, _WEATHER_SOURCE, grid_point, date_range)

    for day in (date(2026, 1, 15), date(2026, 1, 16), date(2026, 1, 17)):
        content = cleaned_fs.read_text(_cleaned_day_path(grid_point, day))
        df = pd.read_csv(io.StringIO(content), parse_dates=['time'])
        assert set(df['time'].dt.date) == {day}
        assert len(df) == 24


def test_metadata_companion_written_for_each_cleaned_day(
    grid_point: ArbitrarySite,
) -> None:
    date_range = DateRange(date(2026, 1, 15), date(2026, 1, 17))
    raw_fs = FakeFileSystem()
    _write_raw(raw_fs, grid_point, date_range)
    cleaned_fs = FakeFileSystem()

    run_clean_weather(raw_fs, cleaned_fs, _WEATHER_SOURCE, grid_point, date_range)

    for day in (date(2026, 1, 15), date(2026, 1, 16)):
        meta_path = csv_path_to_metadata_path(_cleaned_day_path(grid_point, day))
        assert json.loads(cleaned_fs.read_text(meta_path)) == _METADATA


def test_single_date_pv_site_file_is_read(pv_site: PVSite) -> None:
    """The daily pipeline stages PV-site-located single-date weather files
    (e.g. openmeteo-historical_25724_20260513.csv); clean_weather must still
    read those — a one-day range is just the degenerate window."""
    day = date(2026, 5, 13)
    date_range = DateRange.of_single_day(day)
    raw_fs = FakeFileSystem()
    raw_path = _write_raw(raw_fs, pv_site, date_range)
    assert raw_path.endswith('openmeteo-historical_25724_20260513.csv')
    cleaned_fs = FakeFileSystem()

    run_clean_weather(raw_fs, cleaned_fs, _WEATHER_SOURCE, pv_site, date_range)

    assert cleaned_fs.exists(_cleaned_day_path(pv_site, day))
