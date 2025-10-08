from collections import namedtuple
from datetime import date

import requests

from src.util.env_mapper import map_from_env, VarMapping

ReturnedField = namedtuple('ReturnedField', ['name', 'format', 'unit'])

URL = "https://pvoutput.org/service/r2/getstatus.jsp"
API_KEY_HEADER_NAME = "X-Pvoutput-Apikey"
OWN_SYSTEM_ID_HEADER_NAME = "X-Pvoutput-SystemId"

CONSTANT_QUERY_PARAMS = {
    'h': 1,         # Get full status (see FIELDS)
    'limit': 288,   # Get all statuses for the day
    'asc': 1,       # Ascending order
}

RETURNED_FIELDS = [
    ReturnedField(name='Date', format='YYYYMMDD', unit=''),
    ReturnedField(name='Time', format='HH:MM', unit=''),
    ReturnedField(name='Energy Generation', format='int', unit='Wh'),
    ReturnedField(name='Energy Efficiency', format='float', unit='Wh/kW'),
    ReturnedField(name='Instantaneous Power', format='int', unit='W'),
    ReturnedField(name='Average Power', format='int', unit='W'),
    ReturnedField(name='Normalised Output', format='float', unit=''),
    ReturnedField(name='Energy Consumption', format='int', unit='Wh'),
    ReturnedField(name='Power Consumption', format='int', unit='W'),
    ReturnedField(name='Temperature', format='float', unit='W'),
    ReturnedField(name='Voltage', format='float', unit='V'),
]


class PVOutputExtractor:
    def __init__(self, api_key: str, system_id: str) -> None:
        self.api_key = api_key
        self.system_id = system_id

    @classmethod
    def from_env(cls) -> 'PVOutputExtractor':
        return map_from_env(
            PVOutputExtractor,
            api_key=VarMapping('PVO_API_KEY', str),
            system_id=VarMapping('PVO_SYSTEM_ID', str)
        )

    def extract(self, system_id: int, date_: date) -> list[list[str]]:
        if not system_id:
            raise ValueError("System ID must be provided and non-zero")

        headers = {
            API_KEY_HEADER_NAME: self.api_key,
            OWN_SYSTEM_ID_HEADER_NAME: self.system_id
        }

        params = {
            'sid1': system_id,              # Target system ID
            'd': date_.strftime('%Y%m%d'),  # Date in YYYYMMDD format
            **CONSTANT_QUERY_PARAMS
        }

        response = requests.get(URL, headers=headers, params=params)
        response.raise_for_status()
        return _to_clean_entries(response.text)


def _to_clean_entries(text: str) -> list[list[str]]:
    lines = text.split(';')
    return [
        [_delete_if_nan(entry) for entry in line.split(',')]
        for line in lines
    ]


def _delete_if_nan(text: str) -> str:
    if text == 'NaN':
        return ''
    return text

def _remove_nans(rows: list[list[str]]) -> list[list[str]]:
    return [[field if field != '-1.#IND' else '' for field in row] for row in rows]