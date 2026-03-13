import csv
from decimal import Decimal
from typing import TextIO

from .domain import Location

_location_by_pv_system_id: dict[int, Location] = {}


def build_location_mapping_repo(csv_stream: TextIO) -> None:
    if len(_location_by_pv_system_id) != 0:
        return

    csv_stream.seek(0)

    reader = csv.DictReader(csv_stream)
    for row in reader:
        pv_system_id, location = _parse_row(row)
        _location_by_pv_system_id[pv_system_id] = location


def get_location_by_pv_system_id(pv_system_id: int) -> Location:
    return _location_by_pv_system_id[pv_system_id]


def _parse_row(row: dict[str, str]) -> tuple[int, Location]:
    pv_system_id = int(row['pv_system_id'])
    location = _parse_coordinate(row['openmeteo_coords'])
    return pv_system_id, location


def _parse_coordinate(coord_str: str) -> Location:
    """
    Parse a coordinate string in format '<LATITUDE>_<LONGITUDE>' where
    decimal points are stripped and coordinates are given to 4 decimal places.

    Example: '516004_-41776' -> Location(51.6004, -4.1776)
    """
    lat_str, lon_str = coord_str.split('_')
    return Location(
        latitude=_str_to_coordinate(lat_str), longitude=_str_to_coordinate(lon_str)
    )


def _str_to_coordinate(coord_str: str) -> Decimal:
    """Convert a coordinate string to Decimal by inserting decimal point."""
    is_negative = coord_str.startswith('-')
    if is_negative:
        coord_str = coord_str[1:]

    if len(coord_str) <= 4:
        result = Decimal('0.' + coord_str.zfill(4))
    else:
        result = Decimal(coord_str[:-4] + '.' + coord_str[-4:])

    return -result if is_negative else result
