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
import threading
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
from pv_prospect.etl.storage import (
    AnyStorageConfig,
    FileSystem,
    get_filesystem,
    parse_storage_config,
)
from pv_prospect.model import load_artifact, load_weather_artifact
from pv_prospect.model.domain import ModelArtifact, WeatherModelArtifact

logger = logging.getLogger(__name__)

_ARTIFACT_FILES = (
    'model.pt',
    'feature_spec.json',
    'training_config.json',
    'eval_report.json',
)

# Serving contract: artifact filenames and column names written by the producer.
# Deliberately duplicated here — the app must not depend on pv-prospect-data-transformation.
WINDOW_CSV = 'window.csv'
WINDOW_MANIFEST = 'manifest.json'
WINDOW_COLUMNS = (
    'system_id',
    'time',
    'temperature',
    'plane_of_array_irradiance',
    'power',
    'power_max',
)


def storage_config_for(location: str) -> AnyStorageConfig:
    """Resolve a location string to a storage config.

    ``location`` is either a ``gs://<bucket>[/<prefix>]`` URI or a local path.
    """
    if location.startswith('gs://'):
        bucket, _, prefix = location.removeprefix('gs://').partition('/')
        return parse_storage_config(
            {'backend': 'gcs', 'bucket_name': bucket, 'prefix': prefix}
        )
    return parse_storage_config({'backend': 'local', 'prefix': location})


def filesystem_for(location: str) -> FileSystem:
    return get_filesystem(storage_config_for(location))


@dataclass
class ValidationWindowStore:
    windows: dict[int, pd.DataFrame]
    manifest: dict[str, Any]

    @property
    def updated_at(self) -> str:
        return str(self.manifest['updated_at'])

    @property
    def system_ids(self) -> list[int]:
        return list(self.windows.keys())

    def for_site(self, system_id: int) -> pd.DataFrame | None:
        return self.windows.get(system_id)


def parse_window(csv_text: str) -> dict[int, pd.DataFrame]:
    df = pd.read_csv(StringIO(csv_text), parse_dates=['time'])
    return {
        int(system_id): frame.reset_index(drop=True)
        for system_id, frame in df.groupby('system_id')
    }


def load_validation_window(fs: FileSystem) -> ValidationWindowStore:
    manifest = json.loads(fs.read_text(WINDOW_MANIFEST))
    windows = parse_window(fs.read_text(WINDOW_CSV))
    return ValidationWindowStore(windows=windows, manifest=manifest)


class ValidationWindowCache:
    """Per-request freshness coordinator for the validation window artifact.

    ``load()`` is called at startup; ``current()`` is called per-request and
    reloads if the manifest ``updated_at`` has changed since the last load.
    Both are thread-safe: the lock is held only around the reference swap,
    never around I/O or parsing.
    """

    def __init__(self, fs: FileSystem) -> None:
        self._fs = fs
        self._lock = threading.Lock()
        self._current: ValidationWindowStore | None = None

    @property
    def snapshot(self) -> tuple[bool, str | None]:
        """Return (loaded, updated_at) from the last successful load.

        Does NOT trigger a freshness check or reload — safe to call on a
        status endpoint without incurring a GCS round-trip per request.
        """
        current = self._current
        if current is None:
            return False, None
        return True, current.updated_at

    def load(self) -> None:
        store = load_validation_window(self._fs)
        with self._lock:
            self._current = store

    def current(self) -> ValidationWindowStore | None:
        if self._current is None:
            try:
                self.load()
            except Exception:
                logger.warning('Validation window not available', exc_info=True)
            return self._current
        try:
            manifest = json.loads(self._fs.read_text(WINDOW_MANIFEST))
            if manifest.get('updated_at') != self._current.updated_at:
                try:
                    self.load()
                except Exception:
                    logger.warning(
                        'Validation window reload failed; using stale copy',
                        exc_info=True,
                    )
        except Exception:
            logger.warning(
                'Manifest freshness check failed; using current copy', exc_info=True
            )
        return self._current


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
