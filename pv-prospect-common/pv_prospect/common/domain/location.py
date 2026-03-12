from dataclasses import dataclass
from decimal import Decimal


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
            longitude=Decimal(str(round(longitude, 4))),
        )

    def get_coordinates(self) -> str:
        return f'{self.latitude},{self.longitude}'
