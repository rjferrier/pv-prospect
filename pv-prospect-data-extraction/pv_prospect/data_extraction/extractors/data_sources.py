from enum import Enum
from functools import lru_cache

from pv_prospect.common import (
    InterpolationStrategy, Location, PVSite,
    get_openmeteo_bounding_box_by_pv_site_id,
)
from pv_prospect.data_extraction.extractors.openmeteo import (
    OpenMeteoWeatherDataExtractor, Mode as OMMode, APISelector, TimeResolution, Fields, Models
)
from pv_prospect.data_extraction.extractors.pvoutput import PVOutputExtractor

INTERPOLATION_STRATEGY = InterpolationStrategy.NEAREST


def _nearest_location_getter(pv_site: PVSite) -> list[Location]:
    bb = get_openmeteo_bounding_box_by_pv_site_id(pv_site.pvo_sys_id)
    return [bb.nearest_vertex_location(pv_site.location)]


def _bilinear_location_getter(pv_site: PVSite) -> list[Location]:
    bb = get_openmeteo_bounding_box_by_pv_site_id(pv_site.pvo_sys_id)
    return [v.location for v in bb.vertices]


_LOCATION_GETTERS = {
    InterpolationStrategy.NEAREST: _nearest_location_getter,
    InterpolationStrategy.BILINEAR: _bilinear_location_getter,
}

_location_getter = _LOCATION_GETTERS[INTERPOLATION_STRATEGY]


class SourceDescriptor(str, Enum):
    PVOUTPUT = 'pvoutput'
    OPENMETEO_QUARTERHOURLY = 'openmeteo/quarterhourly'
    OPENMETEO_HOURLY = 'openmeteo/hourly'
    OPENMETEO_SATELLITE = 'openmeteo/satellite'
    OPENMETEO_HISTORICAL = 'openmeteo/historical'
    OPENMETEO_V0_QUARTERHOURLY = 'openmeteo/v0/quarterhourly'
    OPENMETEO_V0_HOURLY = 'openmeteo/v0/hourly'

    def __str__(self) -> str:
        return self.value


@lru_cache(maxsize=None)
def get_extractor(source_descriptor: SourceDescriptor):
    """Get an extractor instance for the given source descriptor.

    Results are cached to avoid recreating extractors for the same source.

    Args:
        source_descriptor: The SourceDescriptor enum value

    Returns:
        An extractor instance
    """
    print(f"Using {INTERPOLATION_STRATEGY}")
    factory = _EXTRACTOR_FACTORIES[source_descriptor]
    return factory()


_MULTI_DATE_EXTRACTORS = {
    SourceDescriptor.OPENMETEO_HISTORICAL,
    SourceDescriptor.OPENMETEO_SATELLITE,
}


def supports_multi_date(source_descriptor: SourceDescriptor) -> bool:
    return source_descriptor in _MULTI_DATE_EXTRACTORS


_EXTRACTOR_FACTORIES = {
    SourceDescriptor.PVOUTPUT: lambda: PVOutputExtractor.from_env(),
    SourceDescriptor.OPENMETEO_QUARTERHOURLY: lambda: OpenMeteoWeatherDataExtractor.from_components(
        location_getter=_location_getter,
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution.QUARTERHOURLY,
        fields=Fields.FORECAST,
        models=Models.ALL_FORECAST,
    ),
    SourceDescriptor.OPENMETEO_HOURLY: lambda: OpenMeteoWeatherDataExtractor.from_components(
        location_getter=_location_getter,
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.FORECAST,
        models=Models.ALL_FORECAST,
    ),
    SourceDescriptor.OPENMETEO_SATELLITE: lambda: OpenMeteoWeatherDataExtractor.from_components(
        location_getter=_location_getter,
        api_selector=APISelector.SATELLITE,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.SOLAR_RADIATION,
        models=Models.ALL_SATELLITE,
    ),
    SourceDescriptor.OPENMETEO_HISTORICAL: lambda: OpenMeteoWeatherDataExtractor.from_components(
        location_getter=_location_getter,
        api_selector=APISelector.HISTORICAL,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.FORECAST,
        models=Models.ALL_FORECAST,
    ),
    SourceDescriptor.OPENMETEO_V0_QUARTERHOURLY: lambda: OpenMeteoWeatherDataExtractor.from_mode(
        location_getter=_location_getter,
        mode=OMMode.QUARTERHOURLY
    ),
    SourceDescriptor.OPENMETEO_V0_HOURLY: lambda: OpenMeteoWeatherDataExtractor.from_mode(
        location_getter=_location_getter,
        mode=OMMode.HOURLY
    ),
}

