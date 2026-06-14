"""Tests for CapacityFactorNet: weather head × fixed degradation factor."""

from __future__ import annotations

import pytest
import torch
from pv_prospect.model.nets.pv import CapacityFactorNet


def _net(r: float = 0.007, input_size: int = 3) -> CapacityFactorNet:
    torch.manual_seed(0)
    net = CapacityFactorNet(input_size, r=r)
    net.eval()
    return net


def _inputs(weather: torch.Tensor, age: torch.Tensor) -> torch.Tensor:
    """Assemble the model input contract: weather columns + trailing age."""
    return torch.cat([weather, age], dim=1)


def test_age_zero_returns_bare_head_output() -> None:
    """At age=0 the degradation factor is 1, so forward == head(weather)."""
    net = _net()
    weather = torch.randn(5, 3)
    with torch.no_grad():
        head = net.network(weather)
        out = net(_inputs(weather, torch.zeros(5, 1)))
    assert torch.allclose(out, head, atol=1e-6)


def test_degradation_factor_is_applied_multiplicatively() -> None:
    """forward == head(weather) * (1 - r*age) for arbitrary age."""
    r = 0.008
    net = _net(r=r)
    weather = torch.randn(6, 3)
    age = torch.tensor([[0.0], [2.0], [5.0], [9.0], [13.0], [16.0]])
    with torch.no_grad():
        head = net.network(weather)
        out = net(_inputs(weather, age))
    assert torch.allclose(out, head * (1.0 - r * age), atol=1e-6)


def test_monotone_non_increasing_in_age_where_head_positive() -> None:
    """For rows with a positive head, CF does not increase with age (r > 0)."""
    net = _net()
    weather = torch.randn(32, 3)
    with torch.no_grad():
        head = net.network(weather)
        young = net(_inputs(weather, torch.zeros(32, 1)))
        old = net(_inputs(weather, torch.full((32, 1), 14.0)))
    positive = (head > 0).flatten()
    assert positive.any()
    assert torch.all(old[positive] <= young[positive])


def test_r_is_excluded_from_state_dict() -> None:
    """r is sourced from config at construction, not persisted in the state dict."""
    net = _net(r=0.006)
    assert net.r == pytest.approx(0.006)
    assert 'r' not in net.state_dict()


def test_forward_rejects_input_without_age_column() -> None:
    """A tensor with only the weather columns (no trailing age) errors clearly."""
    net = _net()
    with pytest.raises(ValueError, match='age'):
        net(torch.randn(4, 3))
