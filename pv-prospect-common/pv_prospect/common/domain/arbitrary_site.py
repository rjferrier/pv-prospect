import math
from dataclasses import dataclass

from .location import Location


@dataclass(frozen=True)
class ArbitrarySite:
    location: Location

    def __str__(self) -> str:
        return f'ArbitrarySite(id={self.id})'

    @property
    def id(self) -> str:
        return self.location.to_coordinate_string(filename_friendly=True)

    @property
    def bin(self) -> str:
        lat = math.floor(self.location.latitude)
        lon = math.floor(self.location.longitude)
        return f'{lat}_{lon}'

    @classmethod
    def from_id(cls, site_id: str) -> 'ArbitrarySite':
        return cls(Location.from_coordinate_string(site_id, filename_friendly=True))
