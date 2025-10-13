import csv
from datetime import datetime, date, time
from io import StringIO

import requests

from src.domain.pv_site import get_pv_site_by_system_id
from src.util.env_mapper import map_from_env, VarMapping

# ReturnedField = namedtuple('ReturnedField', ['name', 'format', 'unit'])

BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
MIN_TIME = time(4, 0)
MAX_TIME = time(22, 0)

CONSTANT_QUERY_PARAMS = {
    'unitGroup': 'metric',
    'include': 'minutes',
    'minuteInterval': '15',
    'contentType': 'csv',
    'elements': 'datetime,temp,humidity,cloudcover,visibility,solarradiation,windspeed,winddir,precip,precipremote,'
                'preciptype,pressure,source,stations',
}

# RETURNED_FIELDS = [
#     ReturnedField(name='Date', format='YYYYMMDD', unit=''),
#     ReturnedField(name='Time', format='HH:MM', unit=''),
#     ReturnedField(name='Energy Generation', format='int', unit='Wh'),
#     ReturnedField(name='Energy Efficiency', format='float', unit='Wh/kW'),
#     ReturnedField(name='Instantaneous Power', format='int', unit='W'),
#     ReturnedField(name='Average Power', format='int', unit='W'),
#     ReturnedField(name='Normalised Output', format='float', unit=''),
#     ReturnedField(name='Energy Consumption', format='int', unit='Wh'),
#     ReturnedField(name='Power Consumption', format='int', unit='W'),
#     ReturnedField(name='Temperature', format='float', unit='W'),
#     ReturnedField(name='Voltage', format='float', unit='V'),
# ]


class VCWeatherDataExtractor:
    def __init__(self, api_key) -> None:
        self.api_key = api_key

    @classmethod
    def from_env(cls) -> 'VCWeatherDataExtractor':
        return map_from_env(
            VCWeatherDataExtractor,
            api_key=VarMapping('VC_API_KEY', str),
        )

    def extract(self, system_id: int, date_: date) -> list[list[str]]:
        site = get_pv_site_by_system_id(system_id)
        if site is None:
            raise ValueError(f"No PV site found with system ID {system_id}")

        start_datetime = datetime.combine(date_, MIN_TIME)
        end_datetime = datetime.combine(date_, MAX_TIME)

        url = '/'.join((
            BASE_URL,
            str(site.location),
            start_datetime.strftime('%Y-%m-%dT%H:%M:%S'),
            end_datetime.strftime('%Y-%m-%dT%H:%M:%S')
        ))

        query_params = {
            'key': self.api_key,
            **CONSTANT_QUERY_PARAMS
        }

        response = requests.get(url, params=query_params)
        response.raise_for_status()

        # Parse CSV text into list of lists
        csv_reader = csv.reader(StringIO(response.text))
        return list(csv_reader)
