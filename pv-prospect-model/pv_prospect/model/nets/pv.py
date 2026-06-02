"""PV capacity-factor prediction network."""

from __future__ import annotations

import torch.nn as nn
from torch import Tensor


class CapacityFactorNet(nn.Module):
    """Feed-forward net predicting capacity_factor in [0, ~0.3].

    Architecture ported from the data-exploration notebook
    ``neural-network-cross-site-capacity-factor.ipynb`` (smoke run 2026-05-26,
    R²=0.69 overall on the test set). The output is unbounded (no terminal
    sigmoid) because MSE loss pulls it into the correct scale; constraining
    the output range would slow convergence without improving accuracy.
    """

    def __init__(self, input_size: int) -> None:
        super().__init__()
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

    def forward(self, x: Tensor) -> Tensor:
        return self.network(x)
