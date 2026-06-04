"""Promote: publish a trained artifact store to the model DVC remote and GCS.

Reads the ``provenance.json`` written by ``bootstrap``, then:

1. Clones the instance repo at ``config.instance_repo_branch`` (main).
2. Copies the 4-file artifact dirs into ``models/{pv,weather}/``.
3. ``dvc add`` each directory, injecting ``remote: model`` into the ``.dvc``
   files.
4. Writes ``models/provenance.json`` and ``models/current.json`` (committed
   files, not DVC-tracked).
5. Commits all staged files, tags ``model-v<date>``, pushes branch + tag.
6. ``dvc push`` the content-addressed objects to the ``model`` remote.
7. If ``config.model_bucket_name`` is set, also copies the serving artifacts
   to ``gs://<bucket>/promoted/{pv,weather}/`` and uploads ``current.json``
   to ``gs://<bucket>/current.json`` (the plain serving path read by the
   Prediction API).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pv_prospect.model_trainer.config import ModelTrainerConfig
from pv_prospect.versioning import (
    clone_instance_repo,
    dvc_add_files,
    dvc_push,
    git_commit_and_tag,
    git_push,
    set_commit_identity,
    setup_ssh,
)

logger = logging.getLogger(__name__)

_ARTIFACT_FILES = (
    'model.pt',
    'feature_spec.json',
    'training_config.json',
    'eval_report.json',
)
_COMMIT_IDENTITY_NAME = 'pv-prospect-model-trainer'
_COMMIT_IDENTITY_EMAIL = 'trainer@pv-prospect'


def promote_models(
    store_dir: Path,
    config: ModelTrainerConfig,
    deploy_key: str,
) -> str:
    """Publish a trained store to the model DVC remote.

    Reads provenance from ``store_dir/provenance.json``, commits the model
    artifacts to the instance repo under ``models/``, tags ``model-v<date>``,
    and pushes both the git tag and the DVC content.  If
    ``config.model_bucket_name`` is non-empty, also writes the plain serving
    artifacts to ``gs://<bucket>/promoted/...``.

    Parameters
    ----------
    store_dir
        Path to the promoted-artifact store written by ``bootstrap_models``.
    config
        Trainer configuration (repo URL, remote names, paths).
    deploy_key
        SSH private key content.  Pass an empty string when
        ``instance_repo_url`` is a local path.

    Returns
    -------
    str
        The model version tag created (e.g. ``model-v2026-06-04``).
    """
    store_dir = Path(store_dir)
    with open(store_dir / 'provenance.json') as f:
        provenance = json.load(f)

    promoted_at = datetime.now(tz=timezone.utc).isoformat()
    model_version = f'model-v{datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")}'
    tag = model_version
    logger.info('Promoting as %s', tag)

    with tempfile.TemporaryDirectory() as work_dir:
        env = setup_ssh(deploy_key, work_dir) if deploy_key else {}
        clone_dir = os.path.join(work_dir, 'instance')

        repo = clone_instance_repo(
            config.instance_repo_url,
            branch=config.instance_repo_branch,
            clone_dir=clone_dir,
            env=env,
        )
        set_commit_identity(repo, _COMMIT_IDENTITY_NAME, _COMMIT_IDENTITY_EMAIL)

        _copy_artifacts(store_dir, clone_dir, config.model_dir)

        dvc_file_paths = dvc_add_files(
            clone_dir,
            config.model_dir,
            ['pv', 'weather'],
            config.model_remote_name,
        )

        current = _build_current(provenance, model_version, promoted_at)
        promoted_provenance = {
            **provenance,
            'model_version': model_version,
            'promoted_at': promoted_at,
        }

        model_dir_abs = os.path.join(clone_dir, config.model_dir)
        provenance_rel = os.path.join(config.model_dir, 'provenance.json')
        current_rel = os.path.join(config.model_dir, 'current.json')

        with open(os.path.join(model_dir_abs, 'provenance.json'), 'w') as f:
            json.dump(promoted_provenance, f, indent=2)
        with open(os.path.join(model_dir_abs, 'current.json'), 'w') as f:
            json.dump(current, f, indent=2)

        dvc_push(clone_dir, config.model_remote_name, dvc_file_paths)

        git_commit_and_tag(
            repo,
            dvc_file_paths,
            tag,
            message=f'Promote models: {tag}',
            extra_paths=[provenance_rel, current_rel],
        )
        git_push(repo, env)

    if config.model_bucket_name:
        _upload_serving_artifacts(store_dir, config.model_bucket_name, current)

    logger.info('Promote complete: %s', tag)
    return tag


def _copy_artifacts(store_dir: Path, clone_dir: str, model_dir: str) -> None:
    for model_name in ('pv', 'weather'):
        src = store_dir / 'promoted' / model_name
        dst = os.path.join(clone_dir, model_dir, model_name)
        os.makedirs(dst, exist_ok=True)
        for fname in _ARTIFACT_FILES:
            shutil.copy2(str(src / fname), os.path.join(dst, fname))


def _build_current(
    provenance: dict,  # type: ignore[type-arg]
    model_version: str,
    promoted_at: str,
) -> dict:  # type: ignore[type-arg]
    return {
        'pv': {
            'model_version': model_version,
            'promoted_at': promoted_at,
            'critical_metric': provenance['pv_critical_metric'],
        },
        'weather': {
            'model_version': model_version,
            'promoted_at': promoted_at,
        },
    }


def _upload_serving_artifacts(
    store_dir: Path,
    bucket_name: str,
    current: dict,  # type: ignore[type-arg]
) -> None:
    """Copy serving artifacts to ``gs://<bucket>/promoted/...`` and ``current.json``."""
    from google.cloud import storage  # deferred: only needed for GCS promote

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    logger.info('Uploading serving artifacts to gs://%s', bucket_name)

    for model_name in ('pv', 'weather'):
        for fname in _ARTIFACT_FILES:
            src_path = store_dir / 'promoted' / model_name / fname
            blob_name = f'promoted/{model_name}/{fname}'
            bucket.blob(blob_name).upload_from_filename(str(src_path))
            logger.debug('Uploaded %s', blob_name)

    current_bytes = json.dumps(current, indent=2).encode()
    bucket.blob('current.json').upload_from_string(
        current_bytes, content_type='application/json'
    )
    logger.info('Serving artifacts uploaded to gs://%s', bucket_name)
