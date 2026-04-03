"""Resolve a weather Location from a PV system ID or location string."""

from typing import Callable

from pv_prospect.common.domain import AnyEntity, GridPoint, Location, PVSite

from .constants import WeatherDataSubfolder
from .data_source import DataSourceType


def resolve_grid_point(
    get_location_by_pv_system_id: Callable[[int], Location],
    pv_system_id: int | None = None,
    location_str: str | None = None,
) -> GridPoint:
    """Resolve a grid point. Exactly one argument must be provided."""
    if pv_system_id is not None and location_str is not None:
        raise ValueError(
            'Ambiguous input: both pv_system_id and location_str are set. '
            'Provide exactly one.'
        )
    if location_str is not None:
        return GridPoint.from_id(location_str)
    if pv_system_id is not None:
        location = get_location_by_pv_system_id(pv_system_id)
        return GridPoint(location)
    raise ValueError('Either pv_system_id or location_str must be provided.')


def resolve_subfolder(data_source_type: DataSourceType, entity: AnyEntity) -> str:
    if data_source_type == DataSourceType.WEATHER:
        if isinstance(entity, PVSite):
            return f'{WeatherDataSubfolder.PV_SITES}/{entity.id}'
        elif isinstance(entity, GridPoint):
            return f'{WeatherDataSubfolder.WEATHER_GRID}/{entity.bin}'

    return entity.id
