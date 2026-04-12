"""Resolve sites from pipeline parameters."""

from typing import Callable

from pv_prospect.common.domain import AnySite, ArbitrarySite, Location, PVSite

from .constants import WeatherDataSubfolder
from .data_source import DataSourceType


def resolve_pv_system_ids(
    get_all_pv_system_ids: Callable[[], list[int]],
    pv_system_ids: list[int] | str | None,
) -> list[int]:
    if pv_system_ids == 'all':
        return get_all_pv_system_ids()
    if isinstance(pv_system_ids, str):
        return [int(x.strip()) for x in pv_system_ids.split(',') if x.strip()]
    if isinstance(pv_system_ids, list):
        return pv_system_ids
    return []


def resolve_location_strings(
    resolve_sample_file: Callable[[int], list[str]] | None = None,
    location_strings: list[str] | str | None = None,
    sample_file_index: int | None = None,
) -> list[str]:
    if sample_file_index is not None:
        if resolve_sample_file is None:
            raise ValueError('resolve_sample_file must be provided')
        return resolve_sample_file(sample_file_index)

    if isinstance(location_strings, str):
        parts = [x.strip() for x in location_strings.split(',') if x.strip()]
        if len(parts) % 2 != 0:
            raise ValueError(
                f'Expected pairs of lat,lon values but got {len(parts)} values.'
            )
        return [f'{parts[i]},{parts[i + 1]}' for i in range(0, len(parts), 2)]
    if isinstance(location_strings, list):
        return location_strings
    return []


def resolve_site(
    data_source_type: DataSourceType,
    get_pv_site_by_system_id: Callable[[int], PVSite],
    pv_system_id: int | None = None,
    location_str: str | None = None,
) -> AnySite:
    if pv_system_id is not None and location_str is not None:
        raise ValueError('Cannot provide both pv_system_id and location_str')

    if pv_system_id is not None:
        return get_pv_site_by_system_id(pv_system_id)

    if location_str is not None:
        if data_source_type == DataSourceType.PV:
            raise ValueError('Cannot create ArbitrarySite for PV data source.')
        return ArbitrarySite(Location.from_coordinate_string(location_str))

    raise ValueError('Either pv_system_id or location_str must be provided')


def resolve_subfolder(data_source_type: DataSourceType, site: AnySite) -> str:
    if data_source_type == DataSourceType.WEATHER:
        if isinstance(site, PVSite):
            return f'{WeatherDataSubfolder.PV_SITES}/{site.id}'
        elif isinstance(site, ArbitrarySite):
            return f'{WeatherDataSubfolder.WEATHER_GRID}/{site.bin}'

    return site.id
