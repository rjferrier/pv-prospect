from typing import TypeAlias

from . import Site
from .arbitrary_site import ArbitrarySite
from .pv_site import PVSite

AnySite: TypeAlias = Site | ArbitrarySite | PVSite
