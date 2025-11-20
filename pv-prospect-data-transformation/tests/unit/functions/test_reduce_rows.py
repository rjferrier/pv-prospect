"""Tests for reduce_rows function"""
import pandas as pd
import pytest
from pytz import timezone
from pv_prospect.data_transformation.dataframe_functions import reduce_rows

UTC = timezone('UTC')


@pytest.fixture
def sample_dataframe():
    """Create a sample dataframe with time, energy, and power columns."""
    data = {
        'time': pd.to_datetime([
            '2023-09-15 23:00:00',
            '2023-09-15 23:05:00',
            '2023-09-15 23:10:00',
            '2023-09-15 23:15:00',
            '2023-09-15 23:20:00',
            '2023-09-15 23:25:00',
            '2023-09-15 23:30:00',
            '2023-09-15 23:35:00',
        ]).tz_localize(UTC),
        'energy': [2.0, 11.0, 21.0, 31.0, 46.0, 62.0, 87.0, 99.0],
        'power': [28, 108, 120, 120, 180, float('nan'), 192, 201]
    }
    return pd.DataFrame(data)


@pytest.fixture
def reference_times():
    """Create reference times for aggregation."""
    return pd.Series(pd.to_datetime([
        '2023-09-15 23:00:00',
        '2023-09-15 23:15:00',
        '2023-09-15 23:30:00',
    ]).tz_localize(UTC))


def test_reduce_rows_time_weighted_average(sample_dataframe, reference_times):
    """Test that reduce_rows correctly performs time-weighted averaging."""
    result = reduce_rows(sample_dataframe, reference_times)

    # Should have 2 rows (first reference time is omitted due to no prior data)
    assert len(result) == 2

    # Check that result has the expected columns
    assert list(result.columns) == ['time', 'energy', 'power']


def test_reduce_rows_first_interval_calculation(sample_dataframe, reference_times):
    """Test the calculation for the first result interval (23:15:00)."""
    # Intervals: 23:00->23:05 (energy=11, power=108),
    #            23:05->23:10 (energy=21, power=120),
    #            23:10->23:15 (energy=31, power=120)
    # Weighted avg energy = (11*300 + 21*300 + 31*300) / 900 = 21.0
    # Weighted avg power = (108*300 + 120*300 + 120*300) / 900 = 116.0

    result = reduce_rows(sample_dataframe, reference_times)

    first_row = result.iloc[0]
    assert first_row['time'] == pd.Timestamp('2023-09-15 23:15:00', tz=UTC)
    assert first_row['energy'] == pytest.approx(21.0)
    assert first_row['power'] == pytest.approx(116.0)


def test_reduce_rows_second_interval_calculation(sample_dataframe, reference_times):
    """Test the calculation for the second result interval (23:30:00)."""
    # Intervals: 23:15->23:20 (energy=46, power=180),
    #            23:20->23:25 (energy=62, power=NaN),
    #            23:25->23:30 (energy=87, power=192)
    # Weighted avg energy = (46*300 + 62*300 + 87*300) / 900 = 65.0
    # Weighted avg power = NaN (because one value is NaN)

    result = reduce_rows(sample_dataframe, reference_times)

    second_row = result.iloc[1]
    assert second_row['time'] == pd.Timestamp('2023-09-15 23:30:00', tz=UTC)
    assert second_row['energy'] == pytest.approx(65.0)
    assert pd.isna(second_row['power'])


def test_reduce_rows_handles_nan_values(sample_dataframe, reference_times):
    """Test that NaN values in any interval result in NaN output."""
    result = reduce_rows(sample_dataframe, reference_times)

    # The second row should have NaN for power due to NaN at 23:25
    assert pd.isna(result.iloc[1]['power'])
    # But energy should still be calculated
    assert not pd.isna(result.iloc[1]['energy'])


def test_reduce_rows_omits_insufficient_data_rows(sample_dataframe, reference_times):
    """Test that rows without sufficient data are omitted."""
    result = reduce_rows(sample_dataframe, reference_times)

    # First reference time (23:00:00) should be omitted as it's at the start boundary
    assert result['time'].iloc[0] != pd.Timestamp('2023-09-15 23:00:00', tz=UTC)
    assert len(result) == 2  # Only 2 out of 3 reference times


def test_reduce_rows_empty_dataframe():
    """Test that empty dataframe returns empty result."""
    empty_df = pd.DataFrame(columns=['time', 'energy', 'power'])
    ref_times = pd.Series(pd.to_datetime(['2023-09-15 23:15:00']).tz_localize(UTC))

    result = reduce_rows(empty_df, ref_times)

    assert result.empty
    assert list(result.columns) == ['time', 'energy', 'power']


def test_reduce_rows_empty_reference_times(sample_dataframe):
    """Test that empty reference times returns empty result."""
    empty_ref_times = pd.Series([], dtype='datetime64[ns, UTC]')

    result = reduce_rows(sample_dataframe, empty_ref_times)

    assert result.empty


def test_reduce_rows_missing_time_column():
    """Test that missing time column raises ValueError."""
    df_no_time = pd.DataFrame({'energy': [1, 2, 3], 'power': [10, 20, 30]})
    ref_times = pd.Series(pd.to_datetime(['2023-09-15 23:15:00']).tz_localize(UTC))

    with pytest.raises(ValueError, match="DataFrame must have a 'time' column"):
        reduce_rows(df_no_time, ref_times)
