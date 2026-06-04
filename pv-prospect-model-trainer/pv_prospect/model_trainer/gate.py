"""Promotion gate: decide whether a newly trained model should replace the incumbent."""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def passes_promotion_gate(
    new_metric: float,
    incumbent_metric: float | None,
    tolerance: float,
) -> bool:
    """Return True if the new model passes the promotion gate.

    A new model is promoted when:
    - There is no incumbent (cold start) — promote unconditionally, or
    - ``new_metric >= incumbent_metric - tolerance`` — metric is within tolerance.

    Parameters
    ----------
    new_metric
        PV clamped-power test R² from the freshly trained model.
    incumbent_metric
        Same metric from the currently serving model.  ``None`` signals cold
        start (no current ``models/current.json`` in the bucket).
    tolerance
        Maximum allowed degradation below the incumbent (e.g. 0.02 allows
        the new model to be up to 2 percentage points below the incumbent).
    """
    if incumbent_metric is None:
        logger.info(
            'Promotion gate: cold start — no incumbent, promoting unconditionally'
        )
        return True
    passes = new_metric >= incumbent_metric - tolerance
    return passes


def get_incumbent_metric(bucket_name: str | None) -> float | None:
    """Read the incumbent PV critical metric from the GCS serving ``current.json``.

    Returns ``None`` (cold start) when:
    - ``bucket_name`` is empty / ``None`` (local run, no GCS),
    - the bucket or blob does not exist yet,
    - reading fails for any reason (logs a warning, falls back to cold start).
    """
    if not bucket_name:
        return None
    try:
        from google.cloud import storage  # deferred: only needed in production

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob('current.json')
        if not blob.exists():
            logger.info(
                'No current.json in gs://%s — treating as cold start', bucket_name
            )
            return None
        data = json.loads(blob.download_as_bytes())
        metric = data.get('pv', {}).get('critical_metric')
        if metric is None:
            logger.warning(
                'current.json in gs://%s has no pv.critical_metric', bucket_name
            )
            return None
        logger.info('Incumbent metric from gs://%s: %.3f', bucket_name, float(metric))
        return float(metric)
    except Exception:
        logger.warning(
            'Could not read incumbent metric from gs://%s — treating as cold start',
            bucket_name,
            exc_info=True,
        )
        return None
