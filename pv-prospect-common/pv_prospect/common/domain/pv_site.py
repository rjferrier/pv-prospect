from dataclasses import dataclass
from datetime import date
from typing import Optional, List

from .location import Location


@dataclass
class PanelGeometry:
    azimuth: int
    tilt: int
    area_fraction: float


@dataclass
class System:
    brand: str
    capacity: int


@dataclass(frozen=True)
class PVSite:
    pvo_sys_id: int
    name: str
    location: Location
    panel_system: System
    panel_geometries: List[PanelGeometry]
    inverter_system: System
    installation_date: Optional[date] = None

    def __str__(self) -> str:
        return f"{self.name} (system_id={self.pvo_sys_id})"

