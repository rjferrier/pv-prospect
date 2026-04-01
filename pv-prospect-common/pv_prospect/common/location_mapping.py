import csv
from typing import TextIO

from .domain import Location

_location_by_pv_system_id: dict[int, Location] = {}


def build_location_mapping_repo(csv_stream: TextIO) -> None:
    if len(_location_by_pv_system_id) != 0:
        return

    csv_stream.seek(0)

    reader = csv.DictReader(csv_stream)
    for row in reader:
        pv_system_id = int(row['pv_system_id'])
        _location_by_pv_system_id[pv_system_id] = Location.from_coordinate_string(
            row['openmeteo_coords'], filename_friendly=True
        )


def get_location_by_pv_system_id(pv_system_id: int) -> Location:
    return _location_by_pv_system_id[pv_system_id]
