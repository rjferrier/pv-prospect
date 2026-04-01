from dataclasses import dataclass

from .location import Location


@dataclass(frozen=True)
class GridPoint:
    location: Location

    @property
    def id(self) -> str:
        return self.location.to_coordinate_string(filename_friendly=True)

    def __str__(self) -> str:
        return f'GridPoint(id={self.id})'

    @classmethod
    def from_id(cls, grid_point_id: str) -> 'GridPoint':
        return cls(
            Location.from_coordinate_string(grid_point_id, filename_friendly=True)
        )
