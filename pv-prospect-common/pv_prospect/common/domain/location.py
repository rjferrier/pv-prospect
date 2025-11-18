from dataclasses import dataclass
from enum import Enum


class Shading(Enum):
    NONE = 0
    LOW = 1
    MEDIUM = 2


@dataclass(frozen=True)
class Location:
    latitude: float
    longitude: float
    shading: Shading

    def get_coordinates(self):
        return f"{self.latitude},{self.longitude}"

