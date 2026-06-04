"""Tests for passes_promotion_gate."""

from __future__ import annotations

from pv_prospect.model_trainer.gate import passes_promotion_gate


def test_cold_start_promotes_unconditionally() -> None:
    assert (
        passes_promotion_gate(new_metric=0.50, incumbent_metric=None, tolerance=0.02)
        is True
    )


def test_passes_when_new_is_better() -> None:
    assert (
        passes_promotion_gate(new_metric=0.85, incumbent_metric=0.82, tolerance=0.02)
        is True
    )


def test_passes_when_new_is_equal() -> None:
    assert (
        passes_promotion_gate(new_metric=0.82, incumbent_metric=0.82, tolerance=0.02)
        is True
    )


def test_passes_exactly_at_tolerance_boundary() -> None:
    # 0.82 - 0.02 = 0.80; new=0.80 should pass
    assert (
        passes_promotion_gate(new_metric=0.80, incumbent_metric=0.82, tolerance=0.02)
        is True
    )


def test_rejects_when_degradation_exceeds_tolerance() -> None:
    # 0.82 - 0.02 = 0.80; new=0.79 should fail
    assert (
        passes_promotion_gate(new_metric=0.79, incumbent_metric=0.82, tolerance=0.02)
        is False
    )


def test_rejects_large_degradation() -> None:
    assert (
        passes_promotion_gate(new_metric=0.50, incumbent_metric=0.82, tolerance=0.02)
        is False
    )


def test_zero_tolerance_requires_no_degradation() -> None:
    assert (
        passes_promotion_gate(new_metric=0.82, incumbent_metric=0.82, tolerance=0.0)
        is True
    )
    assert (
        passes_promotion_gate(new_metric=0.819, incumbent_metric=0.82, tolerance=0.0)
        is False
    )
