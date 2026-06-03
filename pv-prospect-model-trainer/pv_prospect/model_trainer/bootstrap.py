"""Bootstrap: clone instance repo at a data tag, pull prepared data, train models.

Writes a promoted-artifact store that the Prediction API can load directly:

    <output_dir>/
        promoted/
            pv/      ← 4-file PV artifact (model.pt, feature_spec.json, ...)
            weather/ ← 4-file weather artifact
        current.json ← metadata pointer read by the serving API
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pv_prospect.model.domain import ModelArtifact
from pv_prospect.model.persistence import save_artifact, save_weather_artifact
from pv_prospect.model.training.pv import train_pv
from pv_prospect.model.training.weather import train_weather
from pv_prospect.model_trainer.config import ModelTrainerConfig
from pv_prospect.versioning import clone_instance_repo, dvc_pull, setup_ssh

logger = logging.getLogger(__name__)


def bootstrap_models(
    data_version: str,
    output_dir: Path,
    config: ModelTrainerConfig,
    deploy_key: str,
) -> None:
    """Clone instance repo at ``data-v<data_version>``, pull prepared data, train both models.

    Writes the trained artifacts to the promoted-store layout under
    ``output_dir`` so the Prediction API can load them with no further setup.
    Fails closed if the deploy key is required but absent.

    Parameters
    ----------
    data_version
        ISO date string (e.g. ``'2026-05-31'``) identifying the ``data-v<date>``
        tag to clone.
    output_dir
        Local directory to write the promoted-artifact store into.
    config
        Trainer configuration (repo URL, remote names, paths).
    deploy_key
        SSH private key content for cloning the instance repo. Pass an empty
        string when the repo URL is a local path (no SSH needed).
    """
    tag = f'data-v{data_version}'
    logger.info('Bootstrapping models from %s', tag)

    with tempfile.TemporaryDirectory() as work_dir:
        env = setup_ssh(deploy_key, work_dir) if deploy_key else {}
        clone_dir = os.path.join(work_dir, 'instance')

        clone_instance_repo(
            config.instance_repo_url,
            branch=tag,
            clone_dir=clone_dir,
            env=env,
        )

        prepared_abs = os.path.join(clone_dir, config.prepared_data_dir)
        dvc_pull(clone_dir, config.feature_remote_name, targets=[prepared_abs])

        data_root = Path(clone_dir) / config.prepared_data_dir
        pv_sites_csv = Path(clone_dir) / config.pv_sites_csv_path

        logger.info('Training PV model')
        pv_artifact = train_pv(data_root=data_root, pv_sites_csv=pv_sites_csv)

        logger.info('Training weather model')
        weather_artifact = train_weather(data_root=data_root)

    output_dir = Path(output_dir)
    pv_dir = output_dir / 'promoted' / 'pv'
    weather_dir = output_dir / 'promoted' / 'weather'
    save_artifact(pv_artifact, pv_dir)
    save_weather_artifact(weather_artifact, weather_dir)

    _write_current_json(output_dir, data_version, pv_artifact)
    logger.info('Bootstrap complete. Store written to %s', output_dir)


def _write_current_json(
    output_dir: Path,
    data_version: str,
    pv_artifact: ModelArtifact,
) -> None:
    """Write the current.json metadata pointer consumed by the Prediction API."""
    promoted_at = datetime.now(tz=timezone.utc).isoformat()
    model_version = f'local-v{data_version}'

    current = {
        'pv': {
            'model_version': model_version,
            'promoted_at': promoted_at,
            'critical_metric': pv_artifact.eval_report.test_power_space.r2,
        },
        'weather': {
            'model_version': model_version,
            'promoted_at': promoted_at,
        },
    }
    with open(output_dir / 'current.json', 'w') as f:
        json.dump(current, f, indent=2)
    logger.info('current.json written')
