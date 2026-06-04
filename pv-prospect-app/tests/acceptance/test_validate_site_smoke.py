"""Smoke test: validate_site end-to-end over a real promoted artifact.

Requires a local model store.  Set STORE_DIR to the store directory, or
place the store at /tmp/pv-prospect-models (the default config value).

The window fixture is a handful of real prepared PV rows for site 89665
(Bell's Meadow, April 2026) committed alongside this test.  The site has a
known installation_date so age features are exact.

This test is the only end-to-end exercise of validate_site (the orchestrator
left deliberately unit-untested because it drives the model forward pass).
It verifies that the full code path — feature assembly, model inference,
metric computation — runs without error and produces non-degenerate output.
"""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pv_prospect.app.store import load_store, parse_window
from pv_prospect.app.validation import validate_site
from pv_prospect.common.domain.location import Location
from pv_prospect.common.domain.pv_site import PanelGeometry, PVSite, Shading, System

_FIXTURE = Path(__file__).parent / 'fixtures' / 'window_89665.csv'

_STORE_DIR = os.environ.get('STORE_DIR', '/tmp/pv-prospect-models')


def _site_89665() -> PVSite:
    return PVSite(
        pvo_sys_id=89665,
        name="Bell's Meadow",
        location=Location(Decimal('52.6524'), Decimal('0.7785')),
        shading=Shading.LOW,
        panel_system=System(brand='Trina Vertex S', capacity=7800),
        panel_geometries=(PanelGeometry(azimuth=180, tilt=36, area_fraction=1.0),),
        inverter_system=System(brand='SolarEdge SE8000H', capacity=8000),
        installation_date=date(2021, 11, 21),
    )


@pytest.mark.skipif(
    not Path(_STORE_DIR).exists(),
    reason=f'Model store not present at {_STORE_DIR}; set STORE_DIR to run',
)
def test_validate_site_runs_and_returns_non_degenerate_output() -> None:
    store = load_store(_STORE_DIR)
    pv_site = _site_89665()
    windows = parse_window(_FIXTURE.read_text())
    site_df = windows[89665]

    result = validate_site(site_df, pv_site, store.pv, age_fill=4.5)

    assert len(result.series) == len(site_df), 'one series point per window row'
    for pt in result.series:
        assert pt.predicted_kwh >= 0.0
        assert pt.actual_kwh >= 0.0
        assert pt.predicted_cf >= 0.0

    assert result.error.power_space_r2 is not None, (
        'R² should be non-None for 5 unclipped rows'
    )
