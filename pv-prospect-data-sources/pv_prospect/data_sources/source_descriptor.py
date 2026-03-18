from enum import Enum


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
