from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from pv_prospect.common.domain import Location


@dataclass(frozen=True)
class OpenMeteoTimeSeriesDescriptor:
    """Identifies a weather grid point via its Location."""

    location: Location

    def __str__(self) -> str:
        return self.location.to_coordinate_string(filename_friendly=True)

    @classmethod
    def from_str(cls, coords: str) -> OpenMeteoTimeSeriesDescriptor:
        return cls(Location.from_coordinate_string(coords, filename_friendly=True))

    @classmethod
    def from_coordinates(
        cls, latitude: Decimal, longitude: Decimal
    ) -> OpenMeteoTimeSeriesDescriptor:
        return cls(Location.from_coordinates(latitude, longitude))
