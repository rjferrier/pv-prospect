"""Scheduled-job trainer: bootstrap → gate → promote.

Composes the three phases into a single operation suitable for the Cloud Run
Job (and for local end-to-end testing).  Returns a ``TrainingOutcome`` in
both the pass and reject branches so the caller (``job.py``) can emit metrics
regardless of whether promotion occurred.
"""

from __future__ import annotations

import json
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pv_prospect.model_trainer.bootstrap import bootstrap_models
from pv_prospect.model_trainer.config import ModelTrainerConfig
from pv_prospect.model_trainer.gate import get_incumbent_metric, passes_promotion_gate
from pv_prospect.model_trainer.promote import promote_models

logger = logging.getLogger(__name__)


@dataclass
class TrainingOutcome:
    """Result of a training run — returned by ``run_trainer_job`` in both branches."""

    promoted: bool
    pv_r2: float
    pv_cf_r2: float
    weather_temp_rmse: float


def run_trainer_job(
    data_version: str,
    config: ModelTrainerConfig,
    deploy_key: str,
) -> TrainingOutcome:
    """Bootstrap, check promotion gate, and promote if the gate passes.

    Parameters
    ----------
    data_version
        ISO date string identifying the ``data-v<date>`` corpus tag to train
        from (e.g. ``'2026-06-04'``).
    config
        Trainer configuration.
    deploy_key
        SSH private key for cloning / pushing the instance repo.

    Returns
    -------
    TrainingOutcome
        Carries training metrics and whether the model was promoted.
        Always returned — callers emit metrics in both pass and reject branches.
    """
    with tempfile.TemporaryDirectory() as tmp:
        output_dir = Path(tmp)

        logger.info('Step 1/3 — bootstrap (data-v%s)', data_version)
        bootstrap_models(data_version, output_dir, config, deploy_key)

        with open(output_dir / 'provenance.json') as f:
            provenance = json.load(f)
        pv_r2 = float(provenance['pv_critical_metric'])
        pv_cf_r2 = float(provenance['pv_cf_r2'])
        weather_temp_rmse = float(provenance['weather_temp_rmse'])
        logger.info(
            'New model: PV R²=%.3f, PV CF R²=%.3f, weather temp RMSE=%.3f',
            pv_r2,
            pv_cf_r2,
            weather_temp_rmse,
        )

        logger.info('Step 2/3 — promotion gate')
        incumbent_metric = get_incumbent_metric(config.model_bucket_name or None)
        if not passes_promotion_gate(
            pv_r2, incumbent_metric, config.promotion_tolerance
        ):
            logger.warning(
                'Promotion gate REJECTED — new R²=%.3f, incumbent=%.3f, tolerance=%.3f. '
                'Existing model keeps serving.',
                pv_r2,
                incumbent_metric or 0.0,
                config.promotion_tolerance,
            )
            return TrainingOutcome(
                promoted=False,
                pv_r2=pv_r2,
                pv_cf_r2=pv_cf_r2,
                weather_temp_rmse=weather_temp_rmse,
            )

        logger.info(
            'Promotion gate PASSED — new R²=%.3f (incumbent=%s)',
            pv_r2,
            f'{incumbent_metric:.3f}'
            if incumbent_metric is not None
            else 'none (cold start)',
        )

        logger.info('Step 3/3 — promote')
        promote_models(output_dir, config, deploy_key)

    return TrainingOutcome(
        promoted=True,
        pv_r2=pv_r2,
        pv_cf_r2=pv_cf_r2,
        weather_temp_rmse=weather_temp_rmse,
    )
