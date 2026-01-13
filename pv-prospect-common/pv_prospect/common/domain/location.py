from dataclasses import dataclass
from decimal import Decimal
import math


@dataclass(frozen=True)
class Location:
    latitude: Decimal
    longitude: Decimal

    @classmethod
    def from_floats(cls, latitude: float, longitude: float) -> 'Location':
        """
        Create a Location from float values, rounding to 4 decimal places.

        Args:
            latitude: Latitude as a float
            longitude: Longitude as a float

        Returns:
            Location instance with Decimal coordinates rounded to 4 decimal places
        """
        return cls(
            latitude=Decimal(str(round(latitude, 4))),
            longitude=Decimal(str(round(longitude, 4)))
        )

    def get_coordinates(self) -> str:
        return f"{self.latitude},{self.longitude}"

    def euclidean_distance(self, other: 'Location') -> float:
        """
        Calculate the Euclidean distance between this location and another location.

        Args:
            other: Another Location instance

        Returns:
            The Euclidean distance as a float
        """
        lat_diff = float(self.latitude - other.latitude)
        lon_diff = float(self.longitude - other.longitude)
        return math.sqrt(lat_diff ** 2 + lon_diff ** 2)

