import csv
from dataclasses import dataclass
from datetime import datetime, date, time
from enum import Enum
from io import StringIO

import requests

from src.domain.pv_site import PVSite
from src.util.env_mapper import map_from_env, VarMapping

BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
MIN_TIME = time(4, 0)
MAX_TIME = time(22, 0)


class Mode(Enum):
    QUARTERHOURLY = 'quarterhourly'
    HOURLY = 'hourly'


CONSTANT_QUERY_PARAMS = {
    'unitGroup': 'metric',
    'contentType': 'csv',
    'elements': 'datetime,temp,humidity,cloudcover,visibility,'
                'solarradiation,windspeed,winddir,precip,precipremote,'
                'preciptype,pressure',
}


@dataclass(frozen=True)
class APIHelper:
    time_unit: str
    minute_interval: int | None = None

    def get_query_params(self, api_key: str) -> dict[str, str]:
        query_params = {
            'key': api_key,
            'include': self.time_unit,
        }
        if self.minute_interval:
            query_params['minuteInterval'] = str(self.minute_interval)

        return {
            **CONSTANT_QUERY_PARAMS,
            **query_params,
        }

    def get_fields_param(self) -> dict[str, str]:
        return {self.time_unit: ','.join(self.fields)}


API_HELPERS_BY_MODE = {
    Mode.QUARTERHOURLY: APIHelper(time_unit='minutes', minute_interval=15),
    Mode.HOURLY: APIHelper(time_unit='hours')
}


class VCWeatherDataExtractor:
    def __init__(self, api_key: str, mode: Mode) -> None:
        self.api_key = api_key
        self.mode = mode

    @classmethod
    def from_env(cls, mode: Mode) -> 'VCWeatherDataExtractor':
        return map_from_env(VCWeatherDataExtractor, {
            'api_key': VarMapping('VC_API_KEY', str)
        }, mode=mode)

    def extract(self, pv_site: PVSite, date_: date) -> list[list[str]]:
        if not pv_site:
            raise ValueError("PVSite must be provided")

        start_datetime = datetime.combine(date_, MIN_TIME)
        end_datetime = datetime.combine(date_, MAX_TIME)

        url = '/'.join((
            BASE_URL,
            pv_site.location.get_coordinates(),
            start_datetime.strftime('%Y-%m-%d'),
            end_datetime.strftime('%Y-%m-%d')
        ))
        query_params = API_HELPERS_BY_MODE[self.mode].get_query_params(self.api_key)

        response = requests.get(url, params=query_params)
        response.raise_for_status()

        # Parse CSV text into list of lists
        csv_reader = csv.reader(StringIO(response.text))
        return list(csv_reader)
