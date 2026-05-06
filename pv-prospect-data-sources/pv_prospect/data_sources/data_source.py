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
    OPENMETEO_HISTORICAL = 'openmeteo/historical'

    @property
    def type(self) -> DataSourceType:
        return (
            DataSourceType.PV if self == DataSource.PVOUTPUT else DataSourceType.WEATHER
        )

    def __str__(self) -> str:
        return self.value
