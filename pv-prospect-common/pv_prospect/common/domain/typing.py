from typing import TypeAlias

from . import Entity
from .grid_point import GridPoint
from .pv_site import PVSite

AnyEntity: TypeAlias = Entity | GridPoint | PVSite
