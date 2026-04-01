from enum import Enum


class DataSourceType(str, Enum):
    PV = 'pv'
    WEATHER = 'weather'

    def __str__(self) -> str:
        return self.value


class DataSource(str, Enum):
    PVOUTPUT = 'pvoutput'
    OPENMETEO_QUARTERHOURLY = 'openmeteo/quarterhourly'
    OPENMETEO_HOURLY = 'openmeteo/hourly'
    OPENMETEO_SATELLITE = 'openmeteo/satellite'
    OPENMETEO_HISTORICAL = 'openmeteo/historical'
    OPENMETEO_V0_QUARTERHOURLY = 'openmeteo/v0/quarterhourly'
    OPENMETEO_V0_HOURLY = 'openmeteo/v0/hourly'

    @property
    def type(self) -> DataSourceType:
        return (
            DataSourceType.PV if self == DataSource.PVOUTPUT else DataSourceType.WEATHER
        )

    def __str__(self) -> str:
        return self.value
