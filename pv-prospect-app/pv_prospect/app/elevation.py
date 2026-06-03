"""Elevation lookup via the Open-Meteo forecast API.

Uses the forecast API's top-level ``elevation`` field, which returns the
grid-cell DEM elevation — the same value OpenMeteo used when the weather
training data was extracted. This is load-bearing: the weather model was
trained with this grid-cell elevation, so using a point-DEM elevation
(e.g. from /v1/elevation) would silently bias temperature predictions.
"""

from __future__ import annotations

from functools import lru_cache

import httpx

_OPENMETEO_FORECAST_URL = 'https://api.open-meteo.com/v1/forecast'


@lru_cache(maxsize=4096)
def get_grid_cell_elevation(latitude: float, longitude: float) -> float:
    """Return the Open-Meteo grid-cell elevation for ``(latitude, longitude)``.

    Results are cached by rounded (2dp) coordinate to avoid redundant lookups
    for nearby points that map to the same grid cell.

    Raises ``RuntimeError`` if the API call fails.
    """
    lat = round(latitude, 2)
    lon = round(longitude, 2)
    try:
        response = httpx.get(
            _OPENMETEO_FORECAST_URL,
            params={
                'latitude': lat,
                'longitude': lon,
                'forecast_days': 0,
                'current': 'temperature_2m',
            },
            timeout=5.0,
        )
        response.raise_for_status()
        return float(response.json()['elevation'])
    except Exception as exc:
        raise RuntimeError(
            f'Failed to fetch elevation for ({lat}, {lon}) from Open-Meteo: {exc}'
        ) from exc
