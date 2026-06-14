"""PV capacity-factor prediction network."""

from __future__ import annotations

import torch.nn as nn
from torch import Tensor


class CapacityFactorNet(nn.Module):
    """Predict capacity_factor as ``head(weather) * (1 - r * age)``.

    The weather head is a feed-forward MLP over the weather features only
    (day_of_year, temperature, plane_of_array_irradiance); architecture ported
    from the data-exploration notebook
    ``neural-network-cross-site-capacity-factor.ipynb`` (smoke run 2026-05-26,
    R²=0.69 overall on the test set). The head output is unbounded (no terminal
    sigmoid) because MSE loss pulls it into the correct scale; constraining the
    range would slow convergence without improving accuracy.

    Panel age is **not** a head input. It is routed into a fixed multiplicative
    degradation factor ``(1 - r * age)`` with ``r`` a non-fitted constant in the
    physical ~0.5-1 %/yr band (see ``domain.TrainingConfig.r_fixed`` and the
    ``pv-age-feature`` Phase 0 findings). This makes the age=0 prospect
    prediction defensible by construction instead of an unconstrained
    extrapolation of a freely-learned age feature.

    **Input contract.** ``forward`` takes a single tensor whose columns are the
    ``input_size`` scaled weather features followed by **one trailing column of
    raw (unscaled) age in years**. This single-tensor signature keeps the
    shared ``run_train_loop`` (used by both PV and weather) unchanged. Build the
    input via ``inference._pv_model_inputs`` so the age column is always present
    and last.
    """

    def __init__(self, input_size: int, r: float) -> None:
        super().__init__()
        self.input_size = input_size
        self.network = nn.Sequential(
            nn.Linear(input_size, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(32, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
        )
        # Fixed (non-fitted) degradation rate, stored as a plain float: sourced
        # solely from TrainingConfig at construction, never from the state dict
        # (so there is a single source of truth and no save/load drift). A
        # scalar float multiplies the head tensor on any device.
        self.r = float(r)

    def forward(self, x: Tensor) -> Tensor:
        if x.shape[1] != self.input_size + 1:
            raise ValueError(
                f'CapacityFactorNet expected {self.input_size + 1} columns '
                f'({self.input_size} weather features + 1 trailing age column), '
                f'got {x.shape[1]}'
            )
        weather_x = x[:, :-1]
        age = x[:, -1:]
        return self.network(weather_x) * (1.0 - self.r * age)
