"""FastAPI application: /predict, /healthz, /version."""

from __future__ import annotations

import datetime
import logging
from contextlib import asynccontextmanager
from io import StringIO
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException
from pv_prospect.app.chain import OutsideUKDomainError, check_uk_domain, predict_yield
from pv_prospect.app.config import AppConfig
from pv_prospect.app.elevation import get_grid_cell_elevation
from pv_prospect.app.resources import get_config_dir
from pv_prospect.app.store import (
    ModelStore,
    ValidationWindowCache,
    filesystem_for,
    load_store,
)
from pv_prospect.common import build_pv_site_repo, get_config
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

_store: ModelStore | None = None
_window_cache: ValidationWindowCache | None = None

_VINTAGE_CAVEAT = (
    'Known bias: the trained artifacts (data-v2026-05-31) carry a ~30% '
    'systematic underestimate of annual yield due to a weather/PV corpus '
    'vintage mismatch (different OpenMeteo reanalysis snapshots). '
    'Fix tracked in briefs/weather-pv-vintage-alignment.md.'
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _store, _window_cache
    config = get_config(AppConfig, base_config_dirs=[get_config_dir()])
    logger.info('Loading model store from %s', config.store_dir)
    _store = load_store(config.store_dir)
    logger.info(
        'Models loaded: PV=%s (R²=%.3f), weather=%s',
        _store.pv_version,
        _store.pv_critical_metric,
        _store.weather_version,
    )
    _window_cache = ValidationWindowCache(filesystem_for(config.validation_window_dir))
    try:
        _window_cache.load()
        logger.info('Validation window loaded from %s', config.validation_window_dir)
    except Exception:
        logger.warning(
            'Validation window load failed at startup (will retry on first request)',
            exc_info=True,
        )
    try:
        resources_fs = filesystem_for(config.resources_dir)
        build_pv_site_repo(StringIO(resources_fs.read_text('pv_sites.csv')))
        logger.info('PV site repo built from %s', config.resources_dir)
    except Exception:
        logger.warning('PV site repo load failed at startup', exc_info=True)
    yield
    _store = None
    _window_cache = None


app = FastAPI(
    title='PV Prospect Prediction API',
    description='Climatological energy-yield estimates for UK PV installations.',
    version='0.1.0',
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class PredictRequest(BaseModel):
    latitude: float = Field(
        ..., ge=49.5, le=61.0, description='Site latitude (UK only)'
    )
    longitude: float = Field(
        ..., ge=-9.0, le=2.0, description='Site longitude (UK only)'
    )
    start_date: datetime.date
    end_date: datetime.date
    panels_capacity_w: float = Field(..., gt=0)
    azimuth_deg: int = Field(..., ge=0, lt=360)
    tilt_deg: int = Field(..., ge=0, le=90)
    inverter_capacity_w: float | None = Field(None, gt=0)
    install_age_years: float = Field(0.0, ge=0)

    @model_validator(mode='after')
    def end_after_start(self) -> 'PredictRequest':
        if self.end_date < self.start_date:
            raise ValueError('end_date must be >= start_date')
        return self


class PredictResponse(BaseModel):
    expected_annual_kwh: float
    monthly_kwh: list[float]
    assumptions: dict[str, Any]
    caveats: list[str]


class VersionResponse(BaseModel):
    pv_model_version: str
    weather_model_version: str
    pv_critical_metric_r2: float


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get('/healthz')
def healthz() -> dict[str, str]:
    if _store is None:
        raise HTTPException(status_code=503, detail='Models not loaded')
    return {'status': 'ok'}


@app.get('/version', response_model=VersionResponse)
def version() -> VersionResponse:
    if _store is None:
        raise HTTPException(status_code=503, detail='Models not loaded')
    return VersionResponse(
        pv_model_version=_store.pv_version,
        weather_model_version=_store.weather_version,
        pv_critical_metric_r2=_store.pv_critical_metric,
    )


@app.post('/predict', response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    if _store is None:
        raise HTTPException(status_code=503, detail='Models not loaded')

    try:
        check_uk_domain(request.latitude, request.longitude)
    except OutsideUKDomainError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        elevation = get_grid_cell_elevation(request.latitude, request.longitude)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    inverter_capacity_w = request.inverter_capacity_w or request.panels_capacity_w

    result = predict_yield(
        latitude=request.latitude,
        longitude=request.longitude,
        elevation=elevation,
        start_date=request.start_date,
        end_date=request.end_date,
        panels_capacity_w=request.panels_capacity_w,
        inverter_capacity_w=inverter_capacity_w,
        tilt=request.tilt_deg,
        azimuth=request.azimuth_deg,
        age_years=request.install_age_years,
        pv_artifact=_store.pv,
        weather_artifact=_store.weather,
    )

    return PredictResponse(
        expected_annual_kwh=round(result.expected_annual_kwh, 1),
        monthly_kwh=[round(k, 1) for k in result.monthly_kwh],
        assumptions={
            'climatology': True,
            'elevation_m': elevation,
            'pv_model_version': _store.pv_version,
            'weather_model_version': _store.weather_version,
            'inverter_capacity_w': inverter_capacity_w,
        },
        caveats=[
            'This is a climatological (typical-year) estimate. Actual output will vary year-to-year.',
            'Age degradation: age_years=0 (new install) is a slight extrapolation for most models.',
            _VINTAGE_CAVEAT,
        ],
    )
