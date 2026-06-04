"""Emit training outcome metrics to Cloud Monitoring.

Custom metric types:
    custom.googleapis.com/pv_prospect/pv_clamped_power_r2   — primary gate metric
    custom.googleapis.com/pv_prospect/pv_capacity_factor_r2 — secondary diagnostic
    custom.googleapis.com/pv_prospect/weather_temperature_rmse — secondary diagnostic
    custom.googleapis.com/pv_prospect/model_promoted        — 1.0 = promoted, 0.0 = rejected
"""

from __future__ import annotations

import logging
import time

from pv_prospect.model_trainer.trainer import TrainingOutcome

logger = logging.getLogger(__name__)

_METRIC_PREFIX = 'custom.googleapis.com/pv_prospect'
_RESOURCE_TYPE = 'global'


def emit_training_metrics(outcome: TrainingOutcome, project_id: str) -> None:
    """Write one time-series data point per metric to Cloud Monitoring.

    Silently logs and returns on failure so a monitoring outage never aborts
    the training job.
    """
    try:
        _emit(outcome, project_id)
    except Exception:
        logger.warning(
            'Failed to emit training metrics to Cloud Monitoring', exc_info=True
        )


def _emit(outcome: TrainingOutcome, project_id: str) -> None:
    from google.cloud import monitoring_v3  # deferred: only needed in production

    client = monitoring_v3.MetricServiceClient()
    project_name = f'projects/{project_id}'
    now_seconds = int(time.time())

    series_defs = [
        (f'{_METRIC_PREFIX}/pv_clamped_power_r2', outcome.pv_r2),
        (f'{_METRIC_PREFIX}/pv_capacity_factor_r2', outcome.pv_cf_r2),
        (f'{_METRIC_PREFIX}/weather_temperature_rmse', outcome.weather_temp_rmse),
        (f'{_METRIC_PREFIX}/model_promoted', 1.0 if outcome.promoted else 0.0),
    ]

    time_series = []
    for metric_type, value in series_defs:
        series = monitoring_v3.TimeSeries()
        series.metric.type = metric_type
        series.resource.type = _RESOURCE_TYPE
        series.resource.labels['project_id'] = project_id

        point = monitoring_v3.Point()
        point.value.double_value = value
        point.interval.end_time.seconds = now_seconds  # type: ignore[assignment]
        series.points = [point]
        time_series.append(series)

    client.create_time_series(name=project_name, time_series=time_series)
    logger.info(
        'Emitted metrics to Cloud Monitoring: promoted=%s pv_r2=%.3f weather_temp_rmse=%.3f',
        outcome.promoted,
        outcome.pv_r2,
        outcome.weather_temp_rmse,
    )
