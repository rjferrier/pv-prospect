"""Tests for run_prepare_pv."""

from decimal import Decimal
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pv_prospect.common import Location, PanelGeometry, PVSite, Shading, System
from pv_prospect.data_sources import SourceDescriptor
from pv_prospect.data_sources.ts_descriptors import (
    OpenMeteoTimeSeriesDescriptor,
    PVOutputTimeSeriesDescriptor,
)
from pv_prospect.data_transformation.core import run_prepare_pv

from tests.unit.helpers.fake_file_system import FakeFileSystem

_SYSTEM_ID = 89665
_DATE_STR = '20260621'
_PV_DESCRIPTOR = SourceDescriptor.PVOUTPUT
_WEATHER_DESCRIPTOR = SourceDescriptor.OPENMETEO_HISTORICAL


def _make_location() -> OpenMeteoTimeSeriesDescriptor:
    return OpenMeteoTimeSeriesDescriptor(
        location_id='504900_-35400',
        latitude=Decimal('50.4900'),
        longitude=Decimal('-3.5400'),
    )


def _build_path(descriptor: SourceDescriptor, ts_str: str, date_str: str) -> str:
    source_str = str(descriptor)
    filename = f'{source_str.replace("/", "-")}_{ts_str}_{date_str}.csv'
    return f'timeseries/{source_str}/{ts_str}/{filename}'


@pytest.fixture
def pv_ts() -> PVOutputTimeSeriesDescriptor:
    return PVOutputTimeSeriesDescriptor(_SYSTEM_ID)


@pytest.fixture
def location() -> OpenMeteoTimeSeriesDescriptor:
    return _make_location()


@pytest.fixture
def cleaned_fs(
    pv_ts: PVOutputTimeSeriesDescriptor,
    location: OpenMeteoTimeSeriesDescriptor,
) -> FakeFileSystem:
    times = pd.date_range('2026-06-21 00:00:00', periods=24, freq='h')
    rng = np.random.default_rng(42)
    hours = np.array([t.hour for t in times])
    solar_factor = np.clip(np.sin(np.pi * (hours - 6) / 12), 0, 1)
    weather_df = pd.DataFrame(
        {
            'time': times,
            'temperature': 15.0 + 10.0 * solar_factor + rng.normal(0, 0.5, 24),
            'direct_normal_irradiance': np.clip(
                800.0 * solar_factor + rng.normal(0, 10, 24), 0, None
            ),
            'diffuse_radiation': np.clip(
                100.0 * solar_factor + rng.normal(0, 5, 24), 0, None
            ),
        }
    )
    pv_df = pd.DataFrame(
        {
            'time': times,
            'power': np.clip(4000.0 * solar_factor + rng.normal(0, 50, 24), 0, None),
        }
    )
    fs = FakeFileSystem(
        binary_files={
            _build_path(
                _WEATHER_DESCRIPTOR, str(location), _DATE_STR
            ): weather_df.to_csv(index=False).encode('utf-8'),
            _build_path(_PV_DESCRIPTOR, str(pv_ts), _DATE_STR): pv_df.to_csv(
                index=False
            ).encode('utf-8'),
        }
    )
    return fs


@pytest.fixture
def batches_fs() -> FakeFileSystem:
    return FakeFileSystem()


@pytest.fixture
def mock_pv_site() -> PVSite:
    return PVSite(
        pvo_sys_id=_SYSTEM_ID,
        name='Test Site',
        location=Location(latitude=Decimal('50.4900'), longitude=Decimal('-3.5400')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=[
            PanelGeometry(tilt=35, azimuth=180, area_fraction=1.0),
        ],
        inverter_system=System(brand='Test', capacity=3600),
    )


def test_writes_batch_at_expected_path(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    pv_ts: PVOutputTimeSeriesDescriptor,
    location: OpenMeteoTimeSeriesDescriptor,
    mock_pv_site: PVSite,
) -> None:
    with patch(
        'pv_prospect.data_transformation.core.get_pv_site_by_system_id',
        return_value=mock_pv_site,
    ):
        run_prepare_pv(
            cleaned_fs,
            batches_fs,
            _PV_DESCRIPTOR,
            _WEATHER_DESCRIPTOR,
            pv_ts,
            location,
            _DATE_STR,
        )

    expected_path = f'pv/{_SYSTEM_ID}_{_DATE_STR}.csv'
    assert batches_fs.exists(expected_path)


def test_batch_has_no_header(
    cleaned_fs: FakeFileSystem,
    batches_fs: FakeFileSystem,
    pv_ts: PVOutputTimeSeriesDescriptor,
    location: OpenMeteoTimeSeriesDescriptor,
    mock_pv_site: PVSite,
) -> None:
    with patch(
        'pv_prospect.data_transformation.core.get_pv_site_by_system_id',
        return_value=mock_pv_site,
    ):
        run_prepare_pv(
            cleaned_fs,
            batches_fs,
            _PV_DESCRIPTOR,
            _WEATHER_DESCRIPTOR,
            pv_ts,
            location,
            _DATE_STR,
        )

    content = batches_fs.read_text(f'pv/{_SYSTEM_ID}_{_DATE_STR}.csv')
    lines = content.strip().split('\n')
    first_field = lines[0].split(',')[0]
    # First field should be a timestamp, not a column name like 'time'
    assert first_field != 'time'


def test_skips_when_cleaned_pv_missing(
    batches_fs: FakeFileSystem,
    pv_ts: PVOutputTimeSeriesDescriptor,
    location: OpenMeteoTimeSeriesDescriptor,
    mock_pv_site: PVSite,
) -> None:
    empty_cleaned_fs = FakeFileSystem()
    with patch(
        'pv_prospect.data_transformation.core.get_pv_site_by_system_id',
        return_value=mock_pv_site,
    ):
        run_prepare_pv(
            empty_cleaned_fs,
            batches_fs,
            _PV_DESCRIPTOR,
            _WEATHER_DESCRIPTOR,
            pv_ts,
            location,
            _DATE_STR,
        )

    assert not batches_fs._files
