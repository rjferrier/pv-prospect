from dataclasses import dataclass
from decimal import Decimal


def _parse_coordinate(s: str) -> Decimal:
    """Parse a 4-decimal-place integer string (e.g. '504900' or '-35400') to Decimal."""
    negative = s.startswith('-')
    digits = s[1:] if negative else s
    result = (
        Decimal(digits[:-4] + '.' + digits[-4:])
        if len(digits) > 4
        else Decimal('0.' + digits.zfill(4))
    )
    return -result if negative else result


@dataclass(frozen=True)
class OpenMeteoTimeSeriesDescriptor:
    """Identifies a weather grid point by its stringified lat_lon coordinate."""

    location_id: str
    latitude: Decimal
    longitude: Decimal

    def __str__(self) -> str:
        return self.location_id

    @classmethod
    def from_coordinates(
        cls, latitude: Decimal, longitude: Decimal
    ) -> 'OpenMeteoTimeSeriesDescriptor':
        lat = round(latitude, 4)
        lon = round(longitude, 4)
        lat_str = f'{lat:.4f}'.replace('.', '')
        lon_str = f'{lon:.4f}'.replace('.', '')
        return cls(f'{lat_str}_{lon_str}', lat, lon)

    @classmethod
    def from_str(cls, location_id: str) -> 'OpenMeteoTimeSeriesDescriptor':
        """Parse a location_id string such as '504900_-35400' back to coordinates."""
        lat_str, lon_str = location_id.split('_', 1)
        return cls.from_coordinates(
            _parse_coordinate(lat_str), _parse_coordinate(lon_str)
        )
