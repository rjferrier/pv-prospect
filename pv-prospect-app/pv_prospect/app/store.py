"""Load the promoted-artifact store written by pv-prospect-model-trainer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pv_prospect.model.domain import ModelArtifact, WeatherModelArtifact
from pv_prospect.model.persistence import load_artifact, load_weather_artifact


@dataclass
class ModelStore:
    pv: ModelArtifact
    weather: WeatherModelArtifact
    current: dict  # type: ignore[type-arg]

    @property
    def pv_version(self) -> str:
        return str(self.current.get('pv', {}).get('model_version', 'unknown'))

    @property
    def weather_version(self) -> str:
        return str(self.current.get('weather', {}).get('model_version', 'unknown'))

    @property
    def pv_critical_metric(self) -> float:
        return float(self.pv.eval_report.test_power_space.r2)


def load_store(store_dir: Path) -> ModelStore:
    """Load PV and weather artifacts from the promoted store layout.

    Expected layout (written by ``pv-prospect-model-trainer bootstrap``):

        ``store_dir/``
            ``promoted/pv/``      ← 4-file PV artifact
            ``promoted/weather/`` ← 4-file weather artifact
            ``current.json``      ← metadata pointer
    """
    store_dir = Path(store_dir)
    pv = load_artifact(store_dir / 'promoted' / 'pv')
    weather = load_weather_artifact(store_dir / 'promoted' / 'weather')
    with open(store_dir / 'current.json') as f:
        current = json.load(f)
    return ModelStore(pv=pv, weather=weather, current=current)
