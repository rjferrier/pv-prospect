import csv
from decimal import Decimal
from io import TextIOWrapper

from .domain.bounding_box import BoundingBox
from .domain.location import Location

om_bounding_boxes_by_pv_site_id = {}


def build_openmeteo_bounding_box_repo(csv_stream: TextIOWrapper) -> None:
    if len(om_bounding_boxes_by_pv_site_id) != 0:
        return

    # Read directly from the text stream
    csv_stream.seek(0)

    reader = csv.DictReader(csv_stream)
    for row in reader:
        pv_site_id, bb = _parse_row(row)
        om_bounding_boxes_by_pv_site_id[pv_site_id] = bb


def get_openmeteo_bounding_box_by_pv_site_id(pv_site_id: int) -> BoundingBox:
    return om_bounding_boxes_by_pv_site_id[pv_site_id]


def _parse_row(row) -> tuple[int, BoundingBox]:
    pv_site_id = int(row['pv_site_id'])

    sw = _parse_coordinate(row['sw'])
    se = _parse_coordinate(row['se'])
    nw = _parse_coordinate(row['nw'])
    ne = _parse_coordinate(row['ne'])

    bb = BoundingBox.from_locations(sw=sw, se=se, nw=nw, ne=ne)
    return pv_site_id, bb


def _parse_coordinate(coord_str: str) -> Location:
    """
    Parse a coordinate string in format '<LATITUDE>_<LONGITUDE>' where
    decimal points are stripped and coordinates are given to 4 decimal places.

    Example: '516004_-41776' -> Location(51.6004, -4.1776)
    """
    lat_str, lon_str = coord_str.split('_')

    # Insert decimal point at position 4 from the end (before last 4 digits)
    # Handle potential negative sign
    lat = _str_to_coordinate(lat_str)
    lon = _str_to_coordinate(lon_str)

    return Location(latitude=lat, longitude=lon)


def _str_to_coordinate(coord_str: str) -> Decimal:
    """Convert a coordinate string to Decimal by inserting decimal point."""
    # Handle negative sign
    is_negative = coord_str.startswith('-')
    if is_negative:
        coord_str = coord_str[1:]

    # Insert decimal point 4 positions from the end
    if len(coord_str) <= 4:
        # All digits are after decimal point
        result = Decimal('0.' + coord_str.zfill(4))
    else:
        # Split into integer and fractional parts
        result = Decimal(coord_str[:-4] + '.' + coord_str[-4:])

    return -result if is_negative else result
