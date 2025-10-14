import json
from datetime import date, time

import requests

from src.domain.pv_site import get_pv_site_by_system_id
from src.util.env_mapper import map_from_env, VarMapping

BASE_URL = "https://api.weatherapi.com/v1/history.json"
MIN_TIME = time(4, 0)
MAX_TIME = time(22, 0)


CONSTANT_QUERY_PARAMS = {
    'aqi': 'no',
}


class WeatherAPIWeatherDataExtractor:
    def __init__(self, api_key) -> None:
        self.api_key = api_key

    @classmethod
    def from_env(cls) -> 'WeatherAPIWeatherDataExtractor':
        return map_from_env(
            WeatherAPIWeatherDataExtractor,
            api_key=VarMapping('WAPI_API_KEY', str),
        )

    def extract(self, system_id: int, date_: date) -> list[list[str]]:
        site = get_pv_site_by_system_id(system_id)
        if site is None:
            raise ValueError(f"No PV site found with system ID {system_id}")

        query_params = {
            'key': self.api_key,
            'q': str(site.location),
            'dt': date_.strftime('%Y-%m-%d'),
            **CONSTANT_QUERY_PARAMS
        }

        response = requests.get(BASE_URL, params=query_params)
        response.raise_for_status()

        # Parse JSON response and convert to CSV rows
        data = json.loads(response.text)

        # Extract the hourly data from forecast.forecastday[0].hour
        forecast_data = data.get('forecast', {})
        forecastday = forecast_data.get('forecastday', [])

        if not forecastday:
            return []

        hourly_data = forecastday[0].get('hour', [])

        if not hourly_data:
            return []

        # Flatten nested dictionaries (like 'condition') into the row
        def flatten_dict(d, parent_key=''):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}_{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key).items())
                else:
                    items.append((new_key, v))
            return dict(items)

        # Flatten all hourly records
        flattened_hours = [flatten_dict(hour) for hour in hourly_data]

        # Get headers from the first record
        headers = list(flattened_hours[0].keys())

        # Build CSV rows: header row + data rows
        rows = [headers]
        for hour in flattened_hours:
            row = [str(hour.get(col, '')) for col in headers]
            rows.append(row)

        return rows
