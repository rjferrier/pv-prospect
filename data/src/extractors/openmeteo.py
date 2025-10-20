import json
from dataclasses import dataclass
from datetime import datetime, date, time
from enum import Enum
from typing import Callable

import requests

from src.domain.location import Location
from src.domain.pv_site import PVSite
from src.util.retry import retry_on_429

MIN_TIME = time(4, 0)
MAX_TIME = time(22, 0)

CONSTANT_QUERY_PARAMS = {
    'timezone': 'Europe/London',
}

BASIC_FIELDS = ['temperature_2m', 'visibility']
CLOUD_COVER_FIELDS = ['cloud_cover', 'cloud_cover_low', 'cloud_cover_mid', 'cloud_cover_high']
SOLAR_RADIATION_FIELDS = [
    'direct_radiation', 'direct_normal_irradiance', 'diffuse_radiation',
    'diffuse_radiation_instant', 'direct_normal_irradiance_instant', 'direct_radiation_instant',
]


class Mode(Enum):
    QUARTERHOURLY = 'quarterhourly'
    HOURLY = 'hourly'


@dataclass(frozen=True)
class APIHelper:
    time_resolution: str
    base_url: str
    models: str
    time_delimiter_suffix: str
    date_stringifier: Callable[[datetime], str]
    fields: list[str]

    def get_query_params(
            self, location: Location, start_datetime: datetime, end_datetime: datetime
    ) -> dict[str, str]:
        return {
            'latitude': location.latitude,
            'longitude': location.longitude,
            f'start_{self.time_delimiter_suffix}': self.date_stringifier(start_datetime),
            f'end_{self.time_delimiter_suffix}': self.date_stringifier(end_datetime),
            self.time_resolution: ','.join(self.fields),
            'models': self.models,
            **CONSTANT_QUERY_PARAMS,
        }

    def get_fields_param(self) -> dict[str, str]:
        return {self.time_resolution: ','.join(self.fields)}


API_HELPERS_BY_MODE = {
    Mode.QUARTERHOURLY: APIHelper(
        time_resolution='minutely_15',
        base_url="https://api.open-meteo.com/v1/forecast",
        models='best_match',
        time_delimiter_suffix='minutely_15',
        date_stringifier=lambda dt: dt.isoformat(),
        fields = BASIC_FIELDS + CLOUD_COVER_FIELDS + SOLAR_RADIATION_FIELDS
    ),
    Mode.HOURLY: APIHelper(
        time_resolution='hourly',
        base_url="https://satellite-api.open-meteo.com/v1/archive",
        models='best_match,satellite_radiation_seamless,ukmo_seamless,meteoswiss_icon_seamless,knmi_seamless,'
               'kma_seamless,jma_seamless,gfs_seamless,gem_seamless,icon_seamless,meteofrance_seamless,'
               'dmi_seamless,metno_seamless,era5_seamless',
        time_delimiter_suffix='date',
        date_stringifier=lambda dt: dt.strftime("%Y-%m-%d"),
        fields=SOLAR_RADIATION_FIELDS
    )
}


class OpenMeteoWeatherDataExtractor:
    def __init__(self, mode: Mode) -> None:
        self.mode = mode

    @retry_on_429
    def extract(self, pv_site: PVSite, date_: date) -> list[list[str]]:
        if not pv_site:
            raise ValueError("PVSite must be provided")

        start_datetime = datetime.combine(date_, MIN_TIME)
        end_datetime = datetime.combine(date_, MAX_TIME)

        api_helper = API_HELPERS_BY_MODE[self.mode]

        response = requests.get(
            url=api_helper.base_url,
            params=api_helper.get_query_params(pv_site.location, start_datetime, end_datetime)
        )
        response.raise_for_status()

        # Parse JSON response and convert to CSV rows
        data = json.loads(response.text)

        # Extract the data section based on time resolution
        time_data = data.get(api_helper.time_resolution, {})
        if not time_data:
            return []

        # Get the headers (field names) and corresponding arrays
        headers = list(time_data.keys())
        arrays = [time_data[header] for header in headers]

        # Ensure all arrays have the same length
        if not arrays or not arrays[0]:
            return []

        num_rows = len(arrays[0])

        # Build CSV rows: header row + data rows
        rows = [headers]
        for i in range(num_rows):
            row = [str(array[i]) if array[i] is not None else '' for array in arrays]
            rows.append(row)

        return rows
