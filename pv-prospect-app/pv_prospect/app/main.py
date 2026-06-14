"""FastAPI application: /predict, /healthz, /version, /validate."""

from __future__ import annotations

import datetime
import logging
from contextlib import asynccontextmanager
from importlib.resources import files
from io import StringIO
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pv_prospect.app.chain import OutsideUKDomainError, check_uk_domain, predict_yield
from pv_prospect.app.config import AppConfig
from pv_prospect.app.elevation import get_grid_cell_elevation
from pv_prospect.app.rate_limit import (
    SETTINGS,
    limiter,
    predict_limit,
    rate_limit_exceeded_handler,
    validate_limit,
)
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
from slowapi.errors import RateLimitExceeded

logger = logging.getLogger(__name__)

_store: ModelStore | None = None
_window_cache: ValidationWindowCache | None = None
_static_dir = Path(str(files('pv_prospect.app').joinpath('static')))

# Prospect yield uncertainty band: the 1σ fractional margin on the expected
# annual estimate, calibrated by the Phase 2 LOSO cross-site eval (the spread of
# per-site "level" across the 10 self-selected training sites). Framed as a
# floor — it captures per-site level, not weather-model or single-year noise.
# See reports/pv-age-feature.md §6 and briefs/prospect-uncertainty-band.md.
PROSPECT_BAND_1SIGMA_FRAC = 0.17

SITE_DISPLAY_ID: dict[int, int] = {
    89665: 1,
    61272: 2,
    25724: 3,
    79336: 4,
    4708: 5,
    82517: 6,
    36019: 7,
    42248: 8,
    24667: 9,
    56874: 10,
}
SITE_REAL_ID: dict[int, int] = {v: k for k, v in SITE_DISPLAY_ID.items()}

_VINTAGE_CAVEAT = (
    'Model limitation: predictions carry per-site level uncertainty due to '
    'self-selection bias in the training corpus (10 well-maintained PVOutput '
    'sites). Cross-site generalisation testing shows a floor of '
    f'±{PROSPECT_BAND_1SIGMA_FRAC * 100:.0f} % (1σ) yield uncertainty for an '
    'arbitrary prospect (see pv-age-feature report). The model represents an '
    'optimistic population; actual installations on less-ideal roofs will fall '
    'below the estimate.'
)

_VALIDATION_IN_SAMPLE_CAVEAT = (
    'Validation metrics are in-sample: the window sites and dates overlap with '
    'the training corpus.  A low error rate reflects model fit on known data, '
    'not out-of-sample generalisation to new sites or future years.'
)

_VALIDATION_AGE_FILL_CAVEAT = (
    'Sites 2 and 4 have no known installation date.  '
    'Their age_years feature is imputed from the window-global median of '
    'known sites — a best-effort approximation of the training fill.'
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _store, _window_cache
    config = get_config(AppConfig, base_config_dirs=[get_config_dir()])
    SETTINGS.predict = config.rate_limit_predict
    SETTINGS.validate = config.rate_limit_validate
    SETTINGS.trusted_hops = config.rate_limit_trusted_hops
    limiter.enabled = config.rate_limit_enabled
    logger.info(
        'Rate limiting %s: predict=%s validate=%s trusted_hops=%d',
        'enabled' if limiter.enabled else 'disabled',
        SETTINGS.predict,
        SETTINGS.validate,
        SETTINGS.trusted_hops,
    )
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
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
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


class UncertaintyBand(BaseModel):
    sigma_frac: float
    annual_kwh_low: float
    annual_kwh_high: float


class PredictResponse(BaseModel):
    expected_annual_kwh: float
    monthly_kwh: list[float]
    uncertainty: UncertaintyBand
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
    latitude: float | None = None
    longitude: float | None = None


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


_Responses = dict[int | str, dict[str, Any]]
_503: _Responses = {503: {'model': _ErrorDetail, 'description': 'Service not ready'}}
_404: _Responses = {404: {'model': _ErrorDetail, 'description': 'Site not found'}}
_502: _Responses = {
    502: {'model': _ErrorDetail, 'description': 'Elevation lookup failed'}
}
_429: _Responses = {429: {'model': _ErrorDetail, 'description': 'Rate limit exceeded'}}


def prospect_uncertainty_band(
    expected_annual_kwh: float,
    sigma_frac: float = PROSPECT_BAND_1SIGMA_FRAC,
) -> UncertaintyBand:
    """Annual-yield uncertainty band for a prospect.

    A uniform fractional margin on the expected estimate (the LOSO-calibrated
    per-site level spread), exposed as a 1σ band. It is a *floor*: it omits
    weather-model and single-year noise — see PROSPECT_BAND_1SIGMA_FRAC.
    """
    return UncertaintyBand(
        sigma_frac=sigma_frac,
        annual_kwh_low=round(expected_annual_kwh * (1 - sigma_frac), 1),
        annual_kwh_high=round(expected_annual_kwh * (1 + sigma_frac), 1),
    )


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


@app.post(
    '/predict', response_model=PredictResponse, responses={**_503, **_502, **_429}
)
@limiter.limit(predict_limit)
def predict(
    request: Request, response: Response, predict_request: PredictRequest
) -> PredictResponse:
    if _store is None:
        raise HTTPException(status_code=503, detail='Models not loaded')

    try:
        check_uk_domain(predict_request.latitude, predict_request.longitude)
    except OutsideUKDomainError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        elevation = get_grid_cell_elevation(
            predict_request.latitude, predict_request.longitude
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    inverter_capacity_w = (
        predict_request.inverter_capacity_w or predict_request.panels_capacity_w
    )

    result = predict_yield(
        latitude=predict_request.latitude,
        longitude=predict_request.longitude,
        elevation=elevation,
        start_date=predict_request.start_date,
        end_date=predict_request.end_date,
        panels_capacity_w=predict_request.panels_capacity_w,
        inverter_capacity_w=inverter_capacity_w,
        tilt=predict_request.tilt_deg,
        azimuth=predict_request.azimuth_deg,
        age_years=predict_request.install_age_years,
        pv_artifact=_store.pv,
        weather_artifact=_store.weather,
    )

    return PredictResponse(
        expected_annual_kwh=round(result.expected_annual_kwh, 1),
        monthly_kwh=[round(k, 1) for k in result.monthly_kwh],
        uncertainty=prospect_uncertainty_band(result.expected_annual_kwh),
        assumptions={
            'climatology': True,
            'elevation_m': elevation,
            'pv_model_version': _store.pv_version,
            'weather_model_version': _store.weather_version,
            'inverter_capacity_w': inverter_capacity_w,
        },
        caveats=[
            'This is a climatological (typical-year) estimate. Actual output will vary year-to-year.',
            'Age degradation: age_years=0 (new install) applies a fixed ~1.06× uplift (0.7%/year degradation); '
            'the model assumes monotone age decline, not non-physical site-by-site variation.',
            _VINTAGE_CAVEAT,
        ],
    )


@app.get(
    '/validate/sites',
    response_model=ValidateSitesResponse,
    responses={**_503, **_429},
)
@limiter.limit(validate_limit)
def validate_sites(request: Request, response: Response) -> ValidateSitesResponse:
    if _window_cache is None:
        raise HTTPException(status_code=503, detail='Validation window not loaded')
    window = _window_cache.current()
    if window is None:
        raise HTTPException(status_code=503, detail='Validation window not loaded')
    sites = []
    for sid in sorted(window.system_ids, key=lambda s: SITE_DISPLAY_ID.get(s, s)):
        display_id = SITE_DISPLAY_ID.get(sid, sid)
        try:
            pv_site = get_pv_site_by_system_id(sid)
            lat: float | None = float(pv_site.location.latitude)
            lng: float | None = float(pv_site.location.longitude)
        except KeyError:
            lat = None
            lng = None
        sites.append(
            SiteSummary(
                system_id=display_id,
                window_start=window.windows[sid]['time'].min().date(),
                window_end=window.windows[sid]['time'].max().date(),
                latitude=lat,
                longitude=lng,
            )
        )
    return ValidateSitesResponse(sites=sites)


@app.get(
    '/validate/{display_id}',
    response_model=ValidateSiteResponse,
    responses={**_503, **_404, **_429},
)
@limiter.limit(validate_limit)
def validate_site(
    request: Request, response: Response, display_id: int
) -> ValidateSiteResponse:
    if _store is None or _window_cache is None:
        raise HTTPException(status_code=503, detail='Service not ready')
    system_id = SITE_REAL_ID.get(display_id)
    if system_id is None:
        raise HTTPException(
            status_code=404, detail=f'Site {display_id} not found'
        )
    window = _window_cache.current()
    if window is None:
        raise HTTPException(status_code=503, detail='Validation window not loaded')
    site_df = window.for_site(system_id)
    if site_df is None:
        raise HTTPException(
            status_code=404, detail=f'Site {display_id} not in validation window'
        )
    try:
        pv_site = get_pv_site_by_system_id(system_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail=f'Site {display_id} not found in site registry'
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
        system_id=display_id,
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
