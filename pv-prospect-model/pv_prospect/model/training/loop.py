"""Shared training loop with early stopping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import torch
import torch.nn as nn
from torch import Tensor
from torch.optim import Optimizer
from torch.utils.data import DataLoader


@dataclass
class TrainLoopResult:
    best_state: dict[str, Any]
    train_losses: list[float]
    val_losses: list[float]
    best_epoch: int


def run_train_loop(
    model: nn.Module,
    optimizer: Optimizer,
    train_loader: DataLoader,  # type: ignore[type-arg]
    val_X: Tensor,
    val_y: Tensor,
    num_epochs: int,
    patience: int,
    device: torch.device,
) -> TrainLoopResult:
    """Run the training loop with early stopping on validation MSE loss.

    Val data is passed in full (not as a DataLoader) because it is evaluated
    in one forward pass at the end of each epoch. Train data is batched and
    shuffled via ``train_loader``.

    Returns the result with the best model state loaded (lowest val loss).
    If no improvement is seen after ``patience`` consecutive epochs, training
    stops early.
    """
    criterion = nn.MSELoss()
    train_losses: list[float] = []
    val_losses: list[float] = []
    best_val_loss = float('inf')
    best_state: dict[str, Any] = {}
    best_epoch = 0
    epochs_since_improvement = 0

    for epoch in range(num_epochs):
        model.train()
        running = 0.0
        for batch_X, batch_y in train_loader:
            batch_X = batch_X.to(device)
            batch_y = batch_y.to(device)
            optimizer.zero_grad()
            loss = criterion(model(batch_X), batch_y)
            loss.backward()
            optimizer.step()
            running += loss.item()
        train_loss = running / len(train_loader)
        train_losses.append(train_loss)

        model.eval()
        with torch.no_grad():
            val_pred = model(val_X.to(device))
            val_loss = float(criterion(val_pred, val_y.to(device)).item())
        val_losses.append(val_loss)

        if val_loss < best_val_loss - 1e-7:
            best_val_loss = val_loss
            best_state = {
                k: v.detach().cpu().clone() for k, v in model.state_dict().items()
            }
            best_epoch = epoch
            epochs_since_improvement = 0
        else:
            epochs_since_improvement += 1

        if epochs_since_improvement >= patience:
            break

    return TrainLoopResult(
        best_state=best_state,
        train_losses=train_losses,
        val_losses=val_losses,
        best_epoch=best_epoch,
    )
