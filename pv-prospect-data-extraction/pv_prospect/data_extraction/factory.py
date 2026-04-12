from functools import lru_cache
from typing import Callable

from pv_prospect.common.domain import Period
from pv_prospect.data_extraction import TimeSeriesDataExtractor
from pv_prospect.data_extraction.extractors import (
    OpenMeteoWeatherDataExtractor,
    PVOutputExtractor,
)
from pv_prospect.data_extraction.extractors.openmeteo import (
    DEFAULT_SPLIT_PERIOD as _OPENMETEO_DEFAULT_SPLIT_PERIOD,
)
from pv_prospect.data_extraction.extractors.openmeteo import (
    APISelector,
    Fields,
    Models,
    TimeResolution,
)
from pv_prospect.data_extraction.extractors.openmeteo import Mode as OMMode
from pv_prospect.data_extraction.extractors.pvoutput import (
    DEFAULT_SPLIT_PERIOD as _PVOUTPUT_DEFAULT_SPLIT_PERIOD,
)
from pv_prospect.data_sources import DataSource


@lru_cache(maxsize=None)
def get_extractor(data_source: DataSource) -> TimeSeriesDataExtractor:
    """Get an extractor instance for the given data source.

    Results are cached to avoid recreating extractors for the same source.

    Args:
        data_source: The DataSource enum value

    Returns:
        An extractor instance
    """
    factory = _EXTRACTOR_FACTORIES[data_source]
    return factory()


_MULTI_DATE_EXTRACTORS = {
    DataSource.OPENMETEO_HISTORICAL,
    DataSource.OPENMETEO_SATELLITE,
}


def supports_multi_date(data_source: DataSource) -> bool:
    return data_source in _MULTI_DATE_EXTRACTORS


_DEFAULT_SPLIT_PERIODS: dict[DataSource, Period | None] = {
    DataSource.PVOUTPUT: _PVOUTPUT_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_QUARTERHOURLY: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_HOURLY: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_SATELLITE: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_HISTORICAL: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_V0_QUARTERHOURLY: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
    DataSource.OPENMETEO_V0_HOURLY: _OPENMETEO_DEFAULT_SPLIT_PERIOD,
}


def default_split_period(data_source: DataSource) -> Period | None:
    return _DEFAULT_SPLIT_PERIODS[data_source]


_EXTRACTOR_FACTORIES: dict[DataSource, Callable[[], TimeSeriesDataExtractor]] = {
    DataSource.PVOUTPUT: lambda: PVOutputExtractor.from_env(),
    DataSource.OPENMETEO_QUARTERHOURLY: lambda: (
        OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.FORECAST,
            time_resolution=TimeResolution.QUARTERHOURLY,
            fields=Fields.FORECAST,
            models=Models.ALL_FORECAST,
        )
    ),
    DataSource.OPENMETEO_HOURLY: lambda: OpenMeteoWeatherDataExtractor.from_components(
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.FORECAST,
        models=Models.ALL_FORECAST,
    ),
    DataSource.OPENMETEO_SATELLITE: lambda: (
        OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.SATELLITE,
            time_resolution=TimeResolution.HOURLY,
            fields=Fields.SOLAR_RADIATION,
            models=Models.ALL_SATELLITE,
        )
    ),
    DataSource.OPENMETEO_HISTORICAL: lambda: (
        OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.HISTORICAL,
            time_resolution=TimeResolution.HOURLY,
            fields=Fields.FORECAST,
            models=Models.ALL_FORECAST,
        )
    ),
    DataSource.OPENMETEO_V0_QUARTERHOURLY: lambda: (
        OpenMeteoWeatherDataExtractor.from_mode(mode=OMMode.QUARTERHOURLY)
    ),
    DataSource.OPENMETEO_V0_HOURLY: lambda: OpenMeteoWeatherDataExtractor.from_mode(
        mode=OMMode.HOURLY
    ),
}
