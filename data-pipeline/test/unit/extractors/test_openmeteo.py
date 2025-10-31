"""Unit tests for OpenMeteo extractor logic (module-style).

This file converts the class-based tests into module-level test functions and
lives under the `test.unit.extractors` package to match the source layout.
"""

from datetime import datetime

import pytest

from extractors.openmeteo import (
    APISelector, TimeResolution, APIHelper, Fields, Models
)
from domain.location import Location, Shading


def test_get_time_limit_params_forecast_hourly():
    """Test time limit params for forecast API with hourly resolution."""
    api_selector = APISelector.FORECAST
    start = datetime(2024, 1, 15, 10, 0)
    end = datetime(2024, 1, 15, 16, 0)

    params = api_selector.get_time_limit_params(TimeResolution.HOURLY, start, end)

    assert params == {
        'start_hour': '2024-01-15T10:00:00',
        'end_hour': '2024-01-15T16:00:00'
    }


def test_get_time_limit_params_forecast_quarterhourly():
    """Test time limit params for forecast API with 15-minute resolution."""
    api_selector = APISelector.FORECAST
    start = datetime(2024, 1, 15, 10, 15)
    end = datetime(2024, 1, 15, 16, 45)

    params = api_selector.get_time_limit_params(TimeResolution.QUARTERHOURLY, start, end)

    assert params == {
        'start_minutely_15': '2024-01-15T10:15:00',
        'end_minutely_15': '2024-01-15T16:45:00'
    }


def test_get_time_limit_params_historical():
    """Test time limit params for historical API."""
    api_selector = APISelector.HISTORICAL
    start = datetime(2024, 1, 15, 10, 0)
    end = datetime(2024, 1, 20, 16, 0)

    params = api_selector.get_time_limit_params(TimeResolution.HOURLY, start, end)

    assert params == {
        'start_date': '2024-01-15',
        'end_date': '2024-01-20'
    }


def test_get_time_limit_params_satellite():
    """Test time limit params for satellite API."""
    api_selector = APISelector.SATELLITE
    start = datetime(2024, 1, 1, 0, 0)
    end = datetime(2024, 1, 7, 23, 59)

    params = api_selector.get_time_limit_params(TimeResolution.HOURLY, start, end)

    assert params == {
        'start_date': '2024-01-01',
        'end_date': '2024-01-07'
    }


def test_get_url():
    """Test getting the base URL."""
    helper = APIHelper(
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.FORECAST,
        models=Models.BEST
    )

    assert helper.get_url() == "https://api.open-meteo.com/v1/forecast"


def test_get_query_params_basic():
    """Test building query parameters."""
    helper = APIHelper(
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.SOLAR_RADIATION,
        models=Models.BEST
    )

    location = Location(latitude=51.5074, longitude=-0.1278, shading=Shading.NONE)
    start = datetime(2024, 1, 15, 10, 0)
    end = datetime(2024, 1, 15, 16, 0)

    params = helper.get_query_params(location, start, end)

    assert params['latitude'] == '51.5074'
    assert params['longitude'] == '-0.1278'
    assert params['start_hour'] == '2024-01-15T10:00:00'
    assert params['end_hour'] == '2024-01-15T16:00:00'
    assert 'hourly' in params
    assert 'models' in params


def test_multi_date_property():
    """Test multi_date property for different API selectors."""
    forecast_helper = APIHelper(
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.FORECAST,
        models=Models.BEST
    )
    assert forecast_helper.multi_date is False

    historical_helper = APIHelper(
        api_selector=APISelector.HISTORICAL,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.FORECAST,
        models=Models.BEST
    )
    assert historical_helper.multi_date is True


def test_fields_comma_separated():
    """Test that fields are properly comma-separated."""
    result = Fields.SOLAR_RADIATION.comma_separated()

    assert 'direct_normal_irradiance' in result
    assert 'diffuse_radiation' in result
    assert ',' in result


def test_models_comma_separated_best():
    """Test single model comma separation."""
    result = Models.BEST.comma_separated()
    assert result == 'best_match'


def test_models_comma_separated_multiple():
    """Test multiple models comma separation."""
    result = Models.ALL_FORECAST.comma_separated()

    assert 'best_match' in result
    assert 'dmi_seamless' in result
    assert ',' in result

