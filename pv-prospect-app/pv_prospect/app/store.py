"""Load the promoted-artifact store written by pv-prospect-model-trainer.

Supports two store locations:
- **Local directory** (dev): ``store_dir`` is a filesystem path.
- **GCS bucket** (prod): ``store_dir`` is a ``gs://<bucket>`` URI.  Files are
  downloaded to a temporary directory and loaded from there.

In both cases the expected layout is::

    <root>/
        current.json
        promoted/
            pv/      ← 4-file PV artifact
            weather/ ← 4-file weather artifact
"""

from __future__ import annotations

import json
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pv_prospect.model.domain import ModelArtifact, WeatherModelArtifact
from pv_prospect.model.persistence import load_artifact, load_weather_artifact

logger = logging.getLogger(__name__)

_ARTIFACT_FILES = (
    'model.pt',
    'feature_spec.json',
    'training_config.json',
    'eval_report.json',
)


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


def load_store(store_dir: str | Path) -> ModelStore:
    """Load PV and weather artifacts from a promoted store.

    ``store_dir`` may be a local filesystem path or a ``gs://<bucket>`` URI.
    GCS artifacts are downloaded to a temporary directory before loading.
    """
    store_dir_str = str(store_dir)
    if store_dir_str.startswith('gs://'):
        return _load_store_gcs(store_dir_str)
    return _load_store_local(Path(store_dir_str))


def _load_store_local(store_dir: Path) -> ModelStore:
    pv = load_artifact(store_dir / 'promoted' / 'pv')
    weather = load_weather_artifact(store_dir / 'promoted' / 'weather')
    with open(store_dir / 'current.json') as f:
        current = json.load(f)
    return ModelStore(pv=pv, weather=weather, current=current)


def _load_store_gcs(gcs_uri: str) -> ModelStore:
    """Download artifacts from GCS to a temp dir, then load locally."""
    from google.cloud import storage  # deferred: only needed for GCS loading

    bucket_name = gcs_uri.removeprefix('gs://')
    logger.info('Loading model store from gs://%s', bucket_name)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        current_blob = bucket.blob('current.json')
        current_bytes = current_blob.download_as_bytes()
        current = json.loads(current_bytes)

        for model_name in ('pv', 'weather'):
            dest_dir = tmp_path / 'promoted' / model_name
            dest_dir.mkdir(parents=True)
            for fname in _ARTIFACT_FILES:
                blob_name = f'promoted/{model_name}/{fname}'
                bucket.blob(blob_name).download_to_filename(str(dest_dir / fname))
                logger.debug('Downloaded %s', blob_name)

        pv = load_artifact(tmp_path / 'promoted' / 'pv')
        weather = load_weather_artifact(tmp_path / 'promoted' / 'weather')

    return ModelStore(pv=pv, weather=weather, current=current)
