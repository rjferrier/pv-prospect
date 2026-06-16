"""Batched, disk-cached grid-cell elevation from the Open-Meteo forecast API.

The weather model was trained on the forecast API's *grid-cell* DEM elevation
(see ``pv-prospect-app``'s elevation note), so the map fetches that same field —
in batches, since a grid has many cells — and caches it to disk so re-runs need
no network. Keyed by 2dp-rounded coordinate (the API snaps to its weather grid
anyway), matching the granularity of the app's per-request elevation cache.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import numpy as np

OPENMETEO_FORECAST_URL = 'https://api.open-meteo.com/v1/forecast'
ELEVATION_BATCH = 100


class ElevationProvider:
    """Resolves grid-cell elevations for many coordinates, caching to a JSON file."""

    def __init__(self, cache_path: Path) -> None:
        self._cache_path = cache_path
        self._cache: dict[str, float] = {}
        if cache_path.exists():
            self._cache = json.loads(cache_path.read_text())

    @staticmethod
    def _key(latitude: float, longitude: float) -> str:
        return f'{round(latitude, 2):.2f},{round(longitude, 2):.2f}'

    def elevations_for(self, coords: list[tuple[float, float]]) -> np.ndarray:
        """Return an elevation (m) per coordinate, fetching+caching any missing."""
        missing = sorted(
            {
                (round(lat, 2), round(lon, 2))
                for lat, lon in coords
                if self._key(lat, lon) not in self._cache
            }
        )
        for start in range(0, len(missing), ELEVATION_BATCH):
            batch = missing[start : start + ELEVATION_BATCH]
            self._fetch_batch(batch)
            print(
                f'  elevation: fetched '
                f'{min(start + ELEVATION_BATCH, len(missing))}/{len(missing)} cells',
                flush=True,
            )
        if missing:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(json.dumps(self._cache))
        return np.array([self._cache[self._key(lat, lon)] for lat, lon in coords])

    def _fetch_batch(self, batch: list[tuple[float, float]]) -> None:
        response = httpx.get(
            OPENMETEO_FORECAST_URL,
            params={
                'latitude': ','.join(f'{lat:.2f}' for lat, _ in batch),
                'longitude': ','.join(f'{lon:.2f}' for _, lon in batch),
                'forecast_days': 1,
                'current': 'temperature_2m',
            },
            timeout=30.0,
        )
        response.raise_for_status()
        payload = response.json()
        entries = payload if isinstance(payload, list) else [payload]
        if len(entries) != len(batch):
            raise RuntimeError(
                f'Open-Meteo returned {len(entries)} entries for {len(batch)} coords'
            )
        for (latitude, longitude), entry in zip(batch, entries, strict=False):
            self._cache[self._key(latitude, longitude)] = float(entry['elevation'])
