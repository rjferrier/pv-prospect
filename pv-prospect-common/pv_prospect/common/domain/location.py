from dataclasses import dataclass
from decimal import Decimal
from typing import Callable


def _parse_filename_friendly_coordinate(s: str) -> Decimal:
    """Parse a 4-decimal-place integer string (e.g. '504900' or '-35400') to Decimal."""
    negative = s.startswith('-')
    digits = s[1:] if negative else s
    result = (
        Decimal(digits[:-4] + '.' + digits[-4:])
        if len(digits) > 4
        else Decimal('0.' + digits.zfill(4))
    )
    return -result if negative else result


def _get_coordinate_parser(filename_friendly: bool) -> Callable[[str], Decimal]:
    return _parse_filename_friendly_coordinate if filename_friendly else Decimal


def _stringify_filename_friendly_coordinate(d: Decimal) -> str:
    return f'{d:.4f}'.replace('.', '')


def _get_coordinate_stringifier(filename_friendly: bool) -> Callable[[Decimal], str]:
    return _stringify_filename_friendly_coordinate if filename_friendly else str


def _get_separator(filename_friendly: bool) -> str:
    return '_' if filename_friendly else ','


@dataclass(frozen=True)
class Location:
    latitude: Decimal
    longitude: Decimal

    @classmethod
    def from_coordinates(
        cls, latitude: str | Decimal, longitude: str | Decimal
    ) -> 'Location':
        """Create a Location from string or Decimal values, rounding to 4dp."""

        if isinstance(latitude, str):
            latitude = Decimal(latitude)
        if isinstance(longitude, str):
            longitude = Decimal(longitude)
        return cls(
            latitude=round(latitude, 4),
            longitude=round(longitude, 4),
        )

    @classmethod
    def from_coordinate_string(
        cls, coordinates: str, filename_friendly: bool = False
    ) -> 'Location':
        sep = _get_separator(filename_friendly)
        parse = _get_coordinate_parser(filename_friendly)
        lat_str, lon_str = coordinates.split(sep, 1)
        return cls.from_coordinates(parse(lat_str), parse(lon_str))

    def to_coordinate_string(self, filename_friendly: bool = False) -> str:
        sep = _get_separator(filename_friendly)
        stringify = _get_coordinate_stringifier(filename_friendly)
        return stringify(self.latitude) + sep + stringify(self.longitude)
