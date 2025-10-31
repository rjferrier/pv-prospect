from enum import Enum
from functools import lru_cache

from .openmeteo import (
    OpenMeteoWeatherDataExtractor, Mode as OMMode, APISelector, TimeResolution, Fields, Models
)
from .pvoutput import PVOutputExtractor
from .visualcrossing import VCWeatherDataExtractor, Mode as VCMode


class SourceDescriptor(str, Enum):
    PVOUTPUT = 'pvoutput'
    OPENMETEO_QUARTERHOURLY = 'openmeteo/quarterhourly'
    OPENMETEO_HOURLY = 'openmeteo/hourly'
    OPENMETEO_SATELLITE = 'openmeteo/satellite'
    OPENMETEO_HISTORICAL = 'openmeteo/historical'
    OPENMETEO_V0_QUARTERHOURLY = 'openmeteo/v0/quarterhourly'
    OPENMETEO_V0_HOURLY = 'openmeteo/v0/hourly'
    VISUALCROSSING_QUARTERHOURLY = 'visualcrossing/quarterhourly'
    VISUALCROSSING_HOURLY = 'visualcrossing/hourly'

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
    factory = _EXTRACTOR_FACTORIES[source_descriptor]
    return factory()


_EXTRACTOR_FACTORIES = {
    SourceDescriptor.PVOUTPUT: lambda: PVOutputExtractor.from_env(),
    SourceDescriptor.OPENMETEO_QUARTERHOURLY: lambda: OpenMeteoWeatherDataExtractor.from_components(
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution.QUARTERHOURLY,
        fields=Fields.FORECAST,
        models=Models.ALL_FORECAST,
    ),
    SourceDescriptor.OPENMETEO_HOURLY: lambda: OpenMeteoWeatherDataExtractor.from_components(
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.FORECAST,
        models=Models.ALL_FORECAST,
    ),
    SourceDescriptor.OPENMETEO_SATELLITE: lambda: OpenMeteoWeatherDataExtractor.from_components(
        api_selector=APISelector.SATELLITE,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.SOLAR_RADIATION,
        models=Models.ALL_SATELLITE,
    ),
    SourceDescriptor.OPENMETEO_HISTORICAL: lambda: OpenMeteoWeatherDataExtractor.from_components(
        api_selector=APISelector.HISTORICAL,
        time_resolution=TimeResolution.HOURLY,
        fields=Fields.FORECAST,
        models=Models.ALL_FORECAST,
    ),
    SourceDescriptor.OPENMETEO_V0_QUARTERHOURLY: lambda: OpenMeteoWeatherDataExtractor.from_mode(OMMode.QUARTERHOURLY),
    SourceDescriptor.OPENMETEO_V0_HOURLY: lambda: OpenMeteoWeatherDataExtractor.from_mode(OMMode.HOURLY),
    SourceDescriptor.VISUALCROSSING_QUARTERHOURLY: lambda: VCWeatherDataExtractor.from_env(mode=VCMode.QUARTERHOURLY),
    SourceDescriptor.VISUALCROSSING_HOURLY: lambda: VCWeatherDataExtractor.from_env(mode=VCMode.HOURLY),
}
