"""FastAPI application: /predict, /healthz, /version, /validate."""

from __future__ import annotations

import datetime
import logging
from contextlib import asynccontextmanager
from importlib.resources import files
from io import StringIO
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
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
from pv_prospect.app.validation import validate_site as _validate_site
from pv_prospect.app.validation import window_age_fill
from pv_prospect.common import build_pv_site_repo, get_config, get_pv_site_by_system_id
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

_store: ModelStore | None = None
_window_cache: ValidationWindowCache | None = None
_static_dir = Path(str(files('pv_prospect.app').joinpath('static')))

_VINTAGE_CAVEAT = (
    'Known bias: the trained artifacts (model-v2026-06-10) carry a '
    'systematic overestimate of annual yield, measured at approximately '
    '+100% across 10 validation sites (mean pred/actual ratio 2.0, '
    'year 2025-06-09 to 2026-06-08). Root cause under investigation; '
    'a PV-model calibration issue (the model over-predicts capacity '
    'factor at the low irradiance levels typical of UK climatological '
    'means, where its training data is sparse). '
    'Fix tracked in briefs/weather-pv-vintage-alignment.md.'
)

_VALIDATION_IN_SAMPLE_CAVEAT = (
    'Validation metrics are in-sample: the window sites and dates overlap with '
    'the training corpus.  A low error rate reflects model fit on known data, '
    'not out-of-sample generalisation to new sites or future years.'
)

_VALIDATION_AGE_FILL_CAVEAT = (
    'Sites 61272 and 79336 have no installation_date in pv_sites.csv.  '
    'Their age_years feature is imputed from the window-global median of '
    'known sites — a best-effort approximation of the training fill.'
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
app.mount('/static', StaticFiles(directory=_static_dir), name='static')


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
    window_loaded: bool
    window_updated_at: str | None


class SiteSummary(BaseModel):
    system_id: int
    window_start: datetime.date
    window_end: datetime.date


class ValidateSitesResponse(BaseModel):
    sites: list[SiteSummary]


class SeriesPointModel(BaseModel):
    date: datetime.date
    predicted_kwh: float
    actual_kwh: float
    predicted_cf: float
    actual_cf: float
    clipped: bool


class ErrorModel(BaseModel):
    mape: float | None
    power_space_r2: float | None


class ValidateSiteResponse(BaseModel):
    system_id: int
    series: list[SeriesPointModel]
    error: ErrorModel
    model_version: str
    window_updated_at: str
    caveats: list[str]


class _ErrorDetail(BaseModel):
    detail: str


_503 = {503: {'model': _ErrorDetail, 'description': 'Service not ready'}}
_404 = {404: {'model': _ErrorDetail, 'description': 'Site not found'}}
_502 = {502: {'model': _ErrorDetail, 'description': 'Elevation lookup failed'}}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get('/', include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(_static_dir / 'index.html')


@app.get('/healthz', responses=_503)
def healthz() -> dict[str, str]:
    if _store is None:
        raise HTTPException(status_code=503, detail='Models not loaded')
    return {'status': 'ok'}


@app.get('/version', response_model=VersionResponse, responses=_503)
def version() -> VersionResponse:
    if _store is None:
        raise HTTPException(status_code=503, detail='Models not loaded')
    window_loaded, window_updated_at = (
        _window_cache.snapshot if _window_cache is not None else (False, None)
    )
    return VersionResponse(
        pv_model_version=_store.pv_version,
        weather_model_version=_store.weather_version,
        pv_critical_metric_r2=_store.pv_critical_metric,
        window_loaded=window_loaded,
        window_updated_at=window_updated_at,
    )


@app.post('/predict', response_model=PredictResponse, responses={**_503, **_502})
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


@app.get('/validate/sites', response_model=ValidateSitesResponse, responses=_503)
def validate_sites() -> ValidateSitesResponse:
    if _window_cache is None:
        raise HTTPException(status_code=503, detail='Validation window not loaded')
    window = _window_cache.current()
    if window is None:
        raise HTTPException(status_code=503, detail='Validation window not loaded')
    sites = [
        SiteSummary(
            system_id=sid,
            window_start=window.windows[sid]['time'].min().date(),
            window_end=window.windows[sid]['time'].max().date(),
        )
        for sid in window.system_ids
    ]
    return ValidateSitesResponse(sites=sites)


@app.get(
    '/validate/{system_id}',
    response_model=ValidateSiteResponse,
    responses={**_503, **_404},
)
def validate_site(system_id: int) -> ValidateSiteResponse:
    if _store is None or _window_cache is None:
        raise HTTPException(status_code=503, detail='Service not ready')
    window = _window_cache.current()
    if window is None:
        raise HTTPException(status_code=503, detail='Validation window not loaded')
    site_df = window.for_site(system_id)
    if site_df is None:
        raise HTTPException(
            status_code=404, detail=f'Site {system_id} not in validation window'
        )
    try:
        pv_site = get_pv_site_by_system_id(system_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail=f'Site {system_id} not found in site registry'
        ) from exc

    install_dates = {}
    for sid in window.system_ids:
        try:
            install_dates[sid] = get_pv_site_by_system_id(sid).installation_date
        except KeyError:
            install_dates[sid] = None

    age_fill = window_age_fill(window.windows, install_dates)
    result = _validate_site(site_df, pv_site, _store.pv, age_fill)

    return ValidateSiteResponse(
        system_id=system_id,
        series=[
            SeriesPointModel(
                date=pt.date,
                predicted_kwh=pt.predicted_kwh,
                actual_kwh=pt.actual_kwh,
                predicted_cf=pt.predicted_cf,
                actual_cf=pt.actual_cf,
                clipped=pt.clipped,
            )
            for pt in result.series
        ],
        error=ErrorModel(
            mape=result.error.mape,
            power_space_r2=result.error.power_space_r2,
        ),
        model_version=_store.pv_version,
        window_updated_at=window.updated_at,
        caveats=[_VALIDATION_IN_SAMPLE_CAVEAT, _VALIDATION_AGE_FILL_CAVEAT],
    )
