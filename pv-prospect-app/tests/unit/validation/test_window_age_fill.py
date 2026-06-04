from datetime import date

from pv_prospect.app.validation import window_age_fill

from tests.unit.validation.helpers import make_window_df


def test_known_sites_return_global_median() -> None:
    # Site A: install 2015-01-01, rows in 2025 → age ≈ 10 years
    # Site B: install 2020-01-01, rows in 2025 → age ≈ 5 years
    # Median of all rows' ages depends on row count; with 1 row each → median of (10, 5) = 7.5
    windows = {
        1: make_window_df(system_id=1, n_days=1, start='2025-01-01'),
        2: make_window_df(system_id=2, n_days=1, start='2025-01-01'),
    }
    install_dates: dict[int, date | None] = {
        1: date(2015, 1, 1),
        2: date(2020, 1, 1),
    }
    result = window_age_fill(windows, install_dates)
    assert 4.0 < result < 12.0  # sanity: between the two ages


def test_all_unknown_returns_zero() -> None:
    windows = {1: make_window_df(system_id=1, n_days=3)}
    install_dates: dict[int, date | None] = {1: None}
    assert window_age_fill(windows, install_dates) == 0.0


def test_mixed_known_unknown_ignores_unknown_site() -> None:
    windows = {
        1: make_window_df(system_id=1, n_days=2, start='2025-06-01'),
        2: make_window_df(system_id=2, n_days=2, start='2025-06-01'),
    }
    install_dates: dict[int, date | None] = {
        1: date(2020, 1, 1),
        2: None,
    }
    result_mixed = window_age_fill(windows, install_dates)
    # Should equal the fill from site 1 alone (unknown site 2 excluded)
    windows_known_only = {1: windows[1]}
    install_dates_known_only: dict[int, date | None] = {1: date(2020, 1, 1)}
    result_known_only = window_age_fill(windows_known_only, install_dates_known_only)  # type: ignore[arg-type]
    assert abs(result_mixed - result_known_only) < 1e-9


def test_returns_float() -> None:
    windows = {1: make_window_df(system_id=1, n_days=2, start='2025-01-01')}
    install_dates: dict[int, date | None] = {1: date(2018, 1, 1)}
    result = window_age_fill(windows, install_dates)
    assert isinstance(result, float)
