import json
from dataclasses import dataclass
from datetime import datetime, date, time
from enum import Enum
from typing import Callable

import requests
from typing_extensions import deprecated

from src.domain.location import Location
from src.domain.pv_site import PVSite
from src.extractors.base import ExtractionResult
from src.util.retry import retry_on_429


MIN_TIME = time(4, 0)
MAX_TIME = time(22, 0)

CONSTANT_QUERY_PARAMS = {
    'timezone': 'Europe/London',
}


@dataclass(frozen=True)
class TimeResolutionData:
    om_descriptor: str


class TimeResolution(Enum):
    HOURLY = TimeResolutionData(om_descriptor='hourly')
    QUARTERHOURLY = TimeResolutionData(om_descriptor='minutely_15')

    @property
    def om_descriptor(self) -> str:
        return self.value.om_descriptor


@dataclass(frozen=True)
class APISelectorData:
    base_url: str
    time_limit_suffix_supplier: Callable[[TimeResolution], str]
    time_stringifier: Callable[[datetime], str]
    multi_date: bool = False
    other_parameters: dict[str, str] = None


class APISelector(Enum):
    FORECAST = APISelectorData(
        base_url="https://api.open-meteo.com/v1/forecast",
        time_limit_suffix_supplier=lambda time_res: (
            'hour' if time_res is TimeResolution.HOURLY else time_res.om_descriptor
        ),
        time_stringifier=lambda dt: dt.isoformat(),
    )
    HISTORICAL = APISelectorData(
        base_url="https://historical-forecast-api.open-meteo.com/v1/forecast",
        time_limit_suffix_supplier=lambda time_res: 'date',
        time_stringifier=lambda dt: dt.strftime("%Y-%m-%d"),
        multi_date=True,
    )
    SATELLITE = APISelectorData(
        base_url="https://satellite-api.open-meteo.com/v1/archive",
        time_limit_suffix_supplier=lambda time_res: 'date',
        time_stringifier=lambda dt: dt.strftime("%Y-%m-%d"),
    )

    @property
    def base_url(self) -> str:
        return self.value.base_url

    def get_time_limit_params(
            self, time_resolution: TimeResolution, start_datetime: datetime, end_datetime: datetime
    ) -> dict[str, str]:
        suffix = self.value.time_limit_suffix_supplier(time_resolution)
        start = self.value.time_stringifier(start_datetime)
        end = self.value.time_stringifier(end_datetime)
        return {
            f'start_{suffix}': start,
            f'end_{suffix}': end,
        }

    @property
    def other_params(self) -> dict[str, str]:
        return self.value.other_parameters if self.value.other_parameters else {}


class Fields(Enum):
    FORECAST = (
        'temperature_2m', 'relative_humidity_2m', 'pressure_msl', 'cloud_cover',
        'wind_speed_80m', 'wind_direction_80m', 'wind_speed_180m', 'wind_direction_180m',
        'direct_normal_irradiance', 'diffuse_radiation',
        'visibility', 'weather_code',
    )
    FORECAST_DEPRECATED = (
        'temperature_2m', 'visibility',
        'cloud_cover', 'cloud_cover_low', 'cloud_cover_mid', 'cloud_cover_high',
        'direct_radiation', 'direct_normal_irradiance', 'diffuse_radiation',
        'diffuse_radiation_instant', 'direct_normal_irradiance_instant', 'direct_radiation_instant',
    )
    SOLAR_RADIATION_DEPRECATED = (
        'direct_radiation', 'direct_normal_irradiance', 'diffuse_radiation',
        'diffuse_radiation_instant', 'direct_normal_irradiance_instant', 'direct_radiation_instant',
    )

    def comma_separated(self) -> str:
        return ','.join(self.value)


class Models(Enum):
    BEST = ('best_match',)
    ALL_FORECAST = (
        'best_match', 'dmi_seamless', 'gem_seamless', 'gfs_seamless', 'icon_seamless',
        'jma_seamless', 'kma_seamless', 'knmi_seamless', 'meteofrance_seamless', 'meteoswiss_icon_seamless',
        'metno_seamless', 'ukmo_seamless',
    )
    ALL_SATELLITE = (
        'best_match', 'dmi_seamless', 'gem_seamless', 'gfs_seamless', 'icon_seamless',
        'jma_seamless', 'kma_seamless', 'knmi_seamless', 'meteofrance_seamless',
        'meteoswiss_icon_seamless', 'metno_seamless', 'ukmo_seamless',
        'era5_seamless', 'satellite_radiation_seamless',
    )

    def comma_separated(self) -> str:
        return ','.join(self.value)


@dataclass(frozen=True)
class APIHelper:
    api_selector: APISelector
    time_resolution: TimeResolution
    fields: Fields
    models: Models

    def get_url(self) -> str:
        return self.api_selector.base_url

    def get_query_params(
            self, location: Location, start_datetime: datetime, end_datetime: datetime
    ) -> dict[str, str]:
        return {
            **self._get_location_params(location),
            **self.api_selector.get_time_limit_params(self.time_resolution, start_datetime, end_datetime),
            **self._get_fields_param(),
            **self._get_models_param(),
            **self.api_selector.other_params,
            **CONSTANT_QUERY_PARAMS,
        }

    @property
    def multi_date(self) -> bool:
        return self.api_selector.value.multi_date

    @staticmethod
    def _get_location_params(location: Location) -> dict[str, str]:
        return {
            'latitude': str(location.latitude),
            'longitude': str(location.longitude),
        }

    def _get_fields_param(self) -> dict[str, str]:
        return {self.time_resolution.om_descriptor: self.fields.comma_separated()}

    def _get_models_param(self) -> dict[str, str]:
        return {'models': self.models.comma_separated()}


@deprecated("Mode enum is deprecated")
class Mode(Enum):
    QUARTERHOURLY = 'quarterhourly'
    HOURLY = 'hourly'


API_HELPERS_BY_MODE = {
    Mode.QUARTERHOURLY: APIHelper(
        api_selector=APISelector.FORECAST,
        time_resolution=TimeResolution['QUARTERHOURLY'],
        models=Models.BEST,
        fields=Fields.FORECAST_DEPRECATED
    ),
    Mode.HOURLY: APIHelper(
        api_selector=APISelector.SATELLITE,
        time_resolution=TimeResolution['HOURLY'],
        models=Models.ALL_SATELLITE,
        fields=Fields.SOLAR_RADIATION_DEPRECATED
    )
}


class OpenMeteoWeatherDataExtractor:
    def __init__(self, api_helper: APIHelper) -> None:
        self.api_helper = api_helper

    @classmethod
    def from_mode(cls, mode: Mode) -> 'OpenMeteoWeatherDataExtractor':
        api_helper = API_HELPERS_BY_MODE[mode]
        return cls(api_helper=api_helper)

    @classmethod
    def from_components(
            cls,
            api_selector: APISelector,
            time_resolution: TimeResolution,
            models: Models,
            fields: Fields,
    ) -> 'OpenMeteoWeatherDataExtractor':
        return cls(APIHelper(
            api_selector=api_selector,
            time_resolution=time_resolution,
            models=models,
            fields=fields,
        ))

    @property
    def multi_date(self):
        return self.api_helper.api_selector.value.multi_date

    @retry_on_429
    def extract(self, pv_site: PVSite, date_: date, end_date: date = None) -> ExtractionResult:
        if not pv_site:
            raise ValueError("PVSite must be provided")

        start_datetime = datetime.combine(date_, MIN_TIME)

        # For multi-date extractors, use end_date if provided; otherwise use same day
        end_datetime = datetime.combine(end_date if end_date else date_, MAX_TIME)

        url = self.api_helper.get_url()
        params = self.api_helper.get_query_params(pv_site.location, start_datetime, end_datetime)
        response = requests.get(url=url, params=params)
        response.raise_for_status()

        # Parse JSON response and convert to CSV rows
        data = json.loads(response.text)

        # Extract the data section based on time resolution
        om_descriptor = self.api_helper.time_resolution.om_descriptor
        try:
            time_data = data[om_descriptor]
        except KeyError as e:
            raise ValueError(
                f"Expected time resolution '{om_descriptor}' not found in response:\n"
                f"{response.text}"
            ) from e

        metadata = dict(data)
        del metadata[om_descriptor]

        # Get the headers (field names) and corresponding arrays
        headers = list(time_data.keys())
        arrays = [time_data[header] for header in headers]

        # Ensure all arrays have the same length
        if not arrays or not arrays[0]:
            return ExtractionResult(data=[], metadata=metadata)

        num_rows = len(arrays[0])

        # Build CSV rows: header row + data rows
        rows = [headers]
        for i in range(num_rows):
            row = [str(array[i]) if array[i] is not None else '' for array in arrays]
            rows.append(row)

        return ExtractionResult(data=rows, metadata=metadata)
