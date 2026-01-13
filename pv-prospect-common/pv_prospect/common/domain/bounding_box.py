from dataclasses import dataclass
from enum import Enum
from typing import Type

from .location import Location

class VertexLabel(Enum):
    SW = 1
    SE = 2
    NW = 3
    NE = 4


@dataclass(frozen=True)
class Vertex:
    location: Location
    label: VertexLabel


@dataclass(frozen=True)
class BoundingBox:
    vertices: tuple[Vertex, Vertex, Vertex, Vertex]

    @classmethod
    def from_locations(
            cls: Type['BoundingBox'], *,
            sw: Location,
            se: Location,
            nw: Location,
            ne: Location
    ) -> 'BoundingBox':
        return cls((
            Vertex(sw, VertexLabel.SW),
            Vertex(se, VertexLabel.SE),
            Vertex(nw, VertexLabel.NW),
            Vertex(ne, VertexLabel.NE),
        ))

    def get_vertices_dict(self) -> dict[Vertex, Location]:
        """
        Get vertices as a dictionary mapping corner label strings to Location objects.

        Returns:
            Dictionary with keys 'sw', 'se', 'nw', 'ne' mapping to Location objects
        """
        return {
            vertex.label: vertex.location
            for vertex in self.vertices
        }

