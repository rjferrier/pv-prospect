"""Leave-one-site-out (LOSO) cross-site evaluation for the PV model.

For each site, train on the other nine and score the held-out tenth — the
genuine prospect scenario (the held-out site contributes no training rows).
This runs on the Phase-1 bounded-prior architecture (no site embedding); its
primary purpose is to calibrate the prospect uncertainty band from the
out-of-sample per-site level spread. See ``plans/pv-age-feature.md`` §Phase 2.

Offline only: one full training per site (10 trainings for the standard corpus),
acceptable for a weekly job. It is diagnostic, never a promotion gate.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from pv_prospect.model.domain import LosoReport, LosoSiteMetrics, TrainingConfig
from pv_prospect.model.evaluation import (
    build_loso_report,
    clamped_power_pred,
    loso_site_metrics,
)
from pv_prospect.model.features import TARGET_COLUMN, build_pv_features
from pv_prospect.model.inference import _run_pv_forward
from pv_prospect.model.training.pv import train_pv

logger = logging.getLogger(__name__)


def loso_eval(
    data_root: Path,
    pv_sites_csv: Path,
    config: TrainingConfig | None = None,
    system_ids: list[int] | None = None,
    seed: int | None = None,
) -> LosoReport:
    """Run the leave-one-site-out eval and return a :class:`LosoReport`.

    For each site present in the corpus: train a fresh model on the other sites
    (reusing ``train_pv``'s ``system_ids`` inclusion list), predict the held-out
    site at its real ages, and score it. The held-out site's rows are sliced
    from a single all-sites feature build — featurisation is per-row independent,
    so this is identical to a per-site build but avoids re-reading the data.

    ``seed`` is set before each fold's training for reproducibility; pass the
    same value used elsewhere (the probes use ``1``). With ``r`` fixed, the
    degradation factor cancels in the level ratio, so the per-site ratio
    isolates the site's level rather than its age.
    """
    if config is None:
        config = TrainingConfig()

    all_df = build_pv_features(
        data_root=data_root,
        pv_sites_csv=pv_sites_csv,
        system_ids=system_ids,
        censoring_margin=config.censoring_margin,
    )
    sites = sorted(int(s) for s in all_df['system_id'].unique())
    if len(sites) < 2:
        raise ValueError(f'LOSO needs at least 2 sites, found {len(sites)}: {sites}')

    per_site: list[LosoSiteMetrics] = []
    pooled_power_true: list[np.ndarray] = []
    pooled_power_pred: list[np.ndarray] = []

    for i, held_out in enumerate(sites, start=1):
        train_ids = [s for s in sites if s != held_out]
        logger.info(
            'LOSO fold %d/%d — hold out site %d, train on %d sites',
            i,
            len(sites),
            held_out,
            len(train_ids),
        )
        if seed is not None:
            import torch  # local: keep torch out of import time for callers

            torch.manual_seed(seed)
            np.random.seed(seed)

        artifact = train_pv(
            data_root=data_root,
            pv_sites_csv=pv_sites_csv,
            config=config,
            system_ids=train_ids,
        )

        held_df = all_df[all_df['system_id'] == held_out].reset_index(drop=True)
        pred_cf = _run_pv_forward(
            artifact.model, artifact.scaler, artifact.feature_spec, held_df
        )

        site_metrics = loso_site_metrics(
            system_id=held_out,
            actual_cf=held_df[TARGET_COLUMN].to_numpy(),
            pred_cf=pred_cf,
            power_true=held_df['power'].to_numpy(),
            panel_capacity=held_df['panels_capacity'].to_numpy(),
            inverter_capacity=held_df['inverter_capacity'].to_numpy(),
        )
        per_site.append(site_metrics)
        pooled_power_true.append(held_df['power'].to_numpy())
        pooled_power_pred.append(
            clamped_power_pred(
                pred_cf,
                held_df['panels_capacity'].to_numpy(),
                held_df['inverter_capacity'].to_numpy(),
            )
        )

    return build_loso_report(
        per_site=tuple(per_site),
        pooled_power_true=np.concatenate(pooled_power_true),
        pooled_power_pred=np.concatenate(pooled_power_pred),
    )
