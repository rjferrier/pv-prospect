"""Load PV + weather artifacts from a promoted model store (local directory).

Reuses ``pv-prospect-model``'s artifact loaders directly, so the map does not
depend on ``pv-prospect-app``. Only the local-directory layout is supported
(``<root>/promoted/{pv,weather}``); fetch a ``gs://`` store locally first.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pv_prospect.model import load_artifact, load_weather_artifact
from pv_prospect.model.domain import ModelArtifact, WeatherModelArtifact


@dataclass
class ModelStore:
    pv: ModelArtifact
    weather: WeatherModelArtifact


def load_model_store(store_dir: Path) -> ModelStore:
    """Load the PV and weather artifacts under ``<store_dir>/promoted/``."""
    store_dir = Path(store_dir)
    pv = load_artifact(store_dir / 'promoted' / 'pv')
    weather = load_weather_artifact(store_dir / 'promoted' / 'weather')
    return ModelStore(pv=pv, weather=weather)
