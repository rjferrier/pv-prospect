import pandas as pd
from pv_prospect.model.features.pv import is_clipped


def _make_df(power_max: list[float], inverter_capacity: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            'power_max': power_max,
            'inverter_capacity': inverter_capacity,
        }
    )


def test_above_threshold_is_true() -> None:
    df = _make_df([6001], inverter_capacity=6000.0)
    result = is_clipped(df, margin=0.01)
    assert bool(result.iloc[0]) is True


def test_at_threshold_is_false() -> None:
    threshold = 6000.0 * (1 - 0.01)
    df = _make_df([threshold], inverter_capacity=6000.0)
    result = is_clipped(df, margin=0.01)
    assert bool(result.iloc[0]) is False


def test_well_below_threshold_is_false() -> None:
    df = _make_df([3000], inverter_capacity=6000.0)
    result = is_clipped(df)
    assert bool(result.iloc[0]) is False


def test_mixed_rows() -> None:
    df = _make_df([3000, 5940, 6000], inverter_capacity=6000.0)
    result = is_clipped(df, margin=0.01)
    # threshold = 5940; 3000 and 5940 not clipped, 6000 clipped
    assert list(result) == [False, False, True]


def test_returns_boolean_series() -> None:
    df = _make_df([5000], inverter_capacity=6000.0)
    result = is_clipped(df)
    assert result.dtype == bool
