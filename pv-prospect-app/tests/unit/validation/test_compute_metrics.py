import pandas as pd
from pv_prospect.app.validation import compute_metrics


def _series(*vals: float) -> pd.Series:
    return pd.Series(list(vals))


def test_clipped_rows_excluded_from_r2() -> None:
    # 3 rows: first two unclipped (perfect prediction), third clipped (bad pred)
    predicted = _series(1.0, 2.0, 99.0)
    actual = _series(1.0, 2.0, 1.0)
    clipped = _series(False, False, True)
    metrics = compute_metrics(predicted, actual, clipped, floor=0.0)
    assert metrics.power_space_r2 == 1.0


def test_clipped_rows_excluded_from_mape() -> None:
    predicted = _series(1.0, 2.0, 99.0)
    actual = _series(1.0, 2.0, 1.0)
    clipped = _series(False, False, True)
    metrics = compute_metrics(predicted, actual, clipped, floor=0.0)
    assert metrics.mape == 0.0


def test_below_floor_excluded_from_mape_only() -> None:
    # Row 0: actual=0.1 (below floor=0.5) — excluded from MAPE but not R²
    # Row 1: actual=2.0, perfect prediction
    predicted = _series(0.1, 2.0)
    actual = _series(0.1, 2.0)
    clipped = _series(False, False)
    metrics = compute_metrics(predicted, actual, clipped, floor=0.5)
    # R² uses both unclipped rows (perfect) → 1.0
    assert metrics.power_space_r2 == 1.0
    # MAPE uses only the above-floor row (perfect) → 0.0
    assert metrics.mape == 0.0


def test_r2_none_when_fewer_than_two_unclipped_rows() -> None:
    predicted = _series(1.0)
    actual = _series(1.0)
    clipped = _series(False)
    metrics = compute_metrics(predicted, actual, clipped, floor=0.0)
    assert metrics.power_space_r2 is None


def test_mape_none_when_all_actual_below_floor() -> None:
    predicted = _series(0.1, 0.2)
    actual = _series(0.1, 0.2)
    clipped = _series(False, False)
    metrics = compute_metrics(predicted, actual, clipped, floor=1.0)
    assert metrics.mape is None


def test_mape_none_when_all_rows_clipped() -> None:
    predicted = _series(1.0, 2.0)
    actual = _series(1.0, 2.0)
    clipped = _series(True, True)
    metrics = compute_metrics(predicted, actual, clipped, floor=0.0)
    assert metrics.mape is None
    assert metrics.power_space_r2 is None


def test_r2_scale_invariant() -> None:
    # R² should be the same whether inputs are in kWh or W (scale-invariant)
    predicted_kwh = _series(1.0, 2.0, 3.0)
    actual_kwh = _series(1.1, 1.9, 3.1)
    clipped = _series(False, False, False)
    metrics_kwh = compute_metrics(predicted_kwh, actual_kwh, clipped, floor=0.0)
    # Scale by 1000 (W instead of kWh)
    metrics_w = compute_metrics(
        predicted_kwh * 1000, actual_kwh * 1000, clipped, floor=0.0
    )
    assert (
        abs((metrics_kwh.power_space_r2 or 0) - (metrics_w.power_space_r2 or 0)) < 1e-9
    )
