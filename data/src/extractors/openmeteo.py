import json
from datetime import datetime, date, time

import requests

from src.domain.pv_site import get_pv_site_by_system_id
from src.util.env_mapper import map_from_env

BASE_URL = "https://api.open-meteo.com/v1/forecast"
MIN_TIME = time(4, 0)
MAX_TIME = time(22, 0)


CONSTANT_QUERY_PARAMS = {
    'models': 'best_match',
    'timezone': 'Europe/London',
    'minutely_15': 'temperature_2m,'
                   'visibility,'
                   'cloud_cover,'
                   'cloud_cover_low,'
                   'cloud_cover_mid,'
                   'cloud_cover_high,'
                   'shortwave_radiation,'
                   'diffuse_radiation,'
                   'direct_radiation,'
                   'direct_normal_irradiance,'
                   'terrestrial_radiation,'
                   'shortwave_radiation_instant,'
                   'diffuse_radiation_instant,'
                   'direct_radiation_instant,'
                   'direct_normal_irradiance_instant,'
                   'terrestrial_radiation_instant',
}


class OpenMeteoWeatherDataExtractor:
    def __init__(self) -> None:
        pass

    @classmethod
    def from_env(cls) -> 'OpenMeteoWeatherDataExtractor':
        return map_from_env(
            OpenMeteoWeatherDataExtractor,
        )

    def extract(self, system_id: int, date_: date) -> list[list[str]]:
        site = get_pv_site_by_system_id(system_id)
        if site is None:
            raise ValueError(f"No PV site found with system ID {system_id}")

        start_datetime = datetime.combine(date_, MIN_TIME)
        end_datetime = datetime.combine(date_, MAX_TIME)

        query_params = {
            'latitude': site.location.latitude,
            'longitude': site.location.longitude,
            'start_minutely_15': start_datetime.isoformat(),
            'end_minutely_15': end_datetime.isoformat(),
            **CONSTANT_QUERY_PARAMS
        }

        response = requests.get(BASE_URL, params=query_params)
        response.raise_for_status()

        # Parse JSON response and convert to CSV rows
        data = json.loads(response.text)

        # Extract the minutely_15 data
        minutely_data = data.get('minutely_15', {})
        if not minutely_data:
            return []

        # Get the headers (field names) and corresponding arrays
        headers = list(minutely_data.keys())
        arrays = [minutely_data[header] for header in headers]

        # Ensure all arrays have the same length
        if not arrays or not arrays[0]:
            return []

        num_rows = len(arrays[0])

        # Build CSV rows: header row + data rows
        rows = [headers]
        for i in range(num_rows):
            row = [str(array[i]) for array in arrays]
            rows.append(row)

        return rows
