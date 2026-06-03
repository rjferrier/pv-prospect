"""Weather prediction network: (latitude, longitude, elevation, day_of_year) → (temperature, DNI, DHI)."""

from __future__ import annotations

import torch.nn as nn
from torch import Tensor


class WeatherNet(nn.Module):
    """3-output weather climatology network.

    Architecture validated in the spatial-block evaluation notebook
    (2026-06-02, fold 0): four uniform 64-unit layers with dropout 0.1
    on each hidden layer. Outputs are unbounded; target scaling (fitted
    StandardScaler) handles the ≈15× scale difference between temperature
    (°C) and DNI (W/m²).
    """

    def __init__(self, input_size: int) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_size, 64),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(64, 3),
        )

    def forward(self, x: Tensor) -> Tensor:
        return self.net(x)
