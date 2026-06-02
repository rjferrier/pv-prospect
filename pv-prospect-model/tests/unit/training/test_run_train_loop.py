"""Smoke-train test for run_train_loop."""

import torch
from pv_prospect.model.nets import CapacityFactorNet
from pv_prospect.model.training.loop import run_train_loop
from torch.utils.data import DataLoader, TensorDataset


def _make_loader(n: int = 20, n_features: int = 3) -> DataLoader:  # type: ignore[type-arg]
    X = torch.randn(n, n_features)
    y = torch.rand(n, 1) * 0.3
    return DataLoader(TensorDataset(X, y), batch_size=8, shuffle=True)


def test_run_train_loop_returns_result_with_best_state() -> None:
    """run_train_loop completes and returns a non-empty best_state."""
    model = CapacityFactorNet(3)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    train_loader = _make_loader()
    val_X = torch.randn(5, 3)
    val_y = torch.rand(5, 1) * 0.3
    device = torch.device('cpu')

    result = run_train_loop(
        model=model,
        optimizer=optimizer,
        train_loader=train_loader,
        val_X=val_X,
        val_y=val_y,
        num_epochs=2,
        patience=10,
        device=device,
    )

    assert len(result.best_state) > 0
    assert len(result.train_losses) == 2
    assert len(result.val_losses) == 2
    assert 0 <= result.best_epoch < 2


def test_run_train_loop_early_stops_when_val_loss_stagnates() -> None:
    """With patience=1, training stops after the second epoch of no improvement."""
    torch.manual_seed(0)
    model = CapacityFactorNet(3)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.0)
    train_loader = _make_loader()
    val_X = torch.randn(5, 3)
    val_y = torch.rand(5, 1) * 0.3
    device = torch.device('cpu')

    # lr=0 → weights frozen → val loss constant → no improvement → early stop at epoch 1
    result = run_train_loop(
        model=model,
        optimizer=optimizer,
        train_loader=train_loader,
        val_X=val_X,
        val_y=val_y,
        num_epochs=100,
        patience=1,
        device=device,
    )

    # Should stop at epoch 1 (epoch 0 sets the baseline, epoch 1 sees no improvement)
    assert len(result.train_losses) <= 3
