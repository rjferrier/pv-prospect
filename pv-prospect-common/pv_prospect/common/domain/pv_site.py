from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional, List

from numpy import deg2rad
from spherical_coordinates import az_zd_to_cx_cy_cz

from .location import Location


class Shading(Enum):
    NONE = 0
    LOW = 1
    MEDIUM = 2


@dataclass
class PanelGeometry:
    azimuth: int
    tilt: int
    area_fraction: float

    @property
    def azimuth_radians(self) -> float:
        return deg2rad(self.azimuth)

    @property
    def tilt_radians(self) -> float:
        return deg2rad(self.tilt)

    @property
    def v_norm(self) -> tuple[float, float, float]:
        cx_cy_cz = az_zd_to_cx_cy_cz(self.azimuth_radians, self.tilt_radians)
        return cx_cy_cz[0], cx_cy_cz[1], -cx_cy_cz[2]


@dataclass
class System:
    brand: str
    capacity: int


@dataclass(frozen=True)
class PVSite:
    pvo_sys_id: int
    name: str
    location: Location
    shading: Shading
    panel_system: System
    panel_geometries: List[PanelGeometry]
    inverter_system: System
    installation_date: Optional[date] = None

    def __str__(self) -> str:
        return f"{self.name} (system_id={self.pvo_sys_id})"

