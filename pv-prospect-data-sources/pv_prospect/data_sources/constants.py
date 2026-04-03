from enum import StrEnum

PV_SITES_CSV_FILE = 'pv_sites.csv'
LOCATION_MAPPING_CSV_FILE = 'location_mapping.csv'
SUPPORTING_RESOURCES = [PV_SITES_CSV_FILE, LOCATION_MAPPING_CSV_FILE]


class WeatherDataSubfolder(StrEnum):
    WEATHER_GRID = 'weather-grid'
    PV_SITES = 'pv-sites'


__all__ = [
    'PV_SITES_CSV_FILE',
    'LOCATION_MAPPING_CSV_FILE',
    'SUPPORTING_RESOURCES',
    'WeatherDataSubfolder',
]
