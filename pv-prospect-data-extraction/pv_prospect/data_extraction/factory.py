from functools import lru_cache
from typing import Callable

from pv_prospect.common import Location, PVSite, get_location_by_pv_system_id
from pv_prospect.data_extraction import TimeSeriesDataExtractor
from pv_prospect.data_extraction.extractors import (
    OpenMeteoWeatherDataExtractor,
    PVOutputExtractor,
)
from pv_prospect.data_extraction.extractors.openmeteo import (
    APISelector,
    Fields,
    Models,
    TimeResolution,
)
from pv_prospect.data_extraction.extractors.openmeteo import Mode as OMMode
from pv_prospect.data_sources import SourceDescriptor


@lru_cache(maxsize=None)
def _location_getter(pv_site: PVSite) -> list[Location]:
    return [get_location_by_pv_system_id(pv_site.pvo_sys_id)]


@lru_cache(maxsize=None)
def get_extractor(source_descriptor: SourceDescriptor) -> TimeSeriesDataExtractor:
    """Get an extractor instance for the given source descriptor.

    Results are cached to avoid recreating extractors for the same source.

    Args:
        source_descriptor: The SourceDescriptor enum value

    Returns:
        An extractor instance
    """
    factory = _EXTRACTOR_FACTORIES[source_descriptor]
    return factory()


_MULTI_DATE_EXTRACTORS = {
    SourceDescriptor.OPENMETEO_HISTORICAL,
    SourceDescriptor.OPENMETEO_SATELLITE,
}


def supports_multi_date(source_descriptor: SourceDescriptor) -> bool:
    return source_descriptor in _MULTI_DATE_EXTRACTORS


_EXTRACTOR_FACTORIES: dict[SourceDescriptor, Callable[[], TimeSeriesDataExtractor]] = {
    SourceDescriptor.PVOUTPUT: lambda: PVOutputExtractor.from_env(),
    SourceDescriptor.OPENMETEO_QUARTERHOURLY: lambda: (
        OpenMeteoWeatherDataExtractor.from_components(
            location_getter=_location_getter,
            api_selector=APISelector.FORECAST,
            time_resolution=TimeResolution.QUARTERHOURLY,
            fields=Fields.FORECAST,
            models=Models.ALL_FORECAST,
        )
    ),
    SourceDescriptor.OPENMETEO_HOURLY: lambda: (
        OpenMeteoWeatherDataExtractor.from_components(
            location_getter=_location_getter,
            api_selector=APISelector.FORECAST,
            time_resolution=TimeResolution.HOURLY,
            fields=Fields.FORECAST,
            models=Models.ALL_FORECAST,
        )
    ),
    SourceDescriptor.OPENMETEO_SATELLITE: lambda: (
        OpenMeteoWeatherDataExtractor.from_components(
            location_getter=_location_getter,
            api_selector=APISelector.SATELLITE,
            time_resolution=TimeResolution.HOURLY,
            fields=Fields.SOLAR_RADIATION,
            models=Models.ALL_SATELLITE,
        )
    ),
    SourceDescriptor.OPENMETEO_HISTORICAL: lambda: (
        OpenMeteoWeatherDataExtractor.from_components(
            location_getter=_location_getter,
            api_selector=APISelector.HISTORICAL,
            time_resolution=TimeResolution.HOURLY,
            fields=Fields.FORECAST,
            models=Models.ALL_FORECAST,
        )
    ),
    SourceDescriptor.OPENMETEO_V0_QUARTERHOURLY: lambda: (
        OpenMeteoWeatherDataExtractor.from_mode(
            location_getter=_location_getter, mode=OMMode.QUARTERHOURLY
        )
    ),
    SourceDescriptor.OPENMETEO_V0_HOURLY: lambda: (
        OpenMeteoWeatherDataExtractor.from_mode(
            location_getter=_location_getter, mode=OMMode.HOURLY
        )
    ),
}
