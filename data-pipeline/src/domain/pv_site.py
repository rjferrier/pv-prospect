import csv
from dataclasses import dataclass
from datetime import date
from typing import Optional, List

import resources
from importlib_resources import files

from domain.location import Location


@dataclass
class PanelGeometry:
    azimuth: int
    tilt: int
    area_fraction: float


@dataclass
class System:
    brand: str
    capacity: int


@dataclass
class PVSite:
    pvo_sys_id: int
    name: str
    location: Location
    panel_system: System
    panel_geometries: List[PanelGeometry]
    inverter_system: System
    installation_date: Optional[date] = None


def _create_panel_geometry_from_row(row: dict, index: int) -> Optional[PanelGeometry]:
    """
    Create a PanelGeometry from CSV row data for a given index (1 or 2).

    Args:
        row (dict): CSV row data
        index (int): Panel geometry index (1 or 2)

    Returns:
        Optional[PanelGeometry]: PanelGeometry with defaults for missing fields, None if all fields are missing
    """
    azimuth_entry = row.get(f'panel_azimuth_{index}')
    tilt_entry = row.get(f'panel_elevation_{index}')
    area_fraction_entry = row.get(f'panel_area_fraction_{index}')

    # Return None only if all geometry data is missing for this index
    if not (azimuth_entry or tilt_entry or area_fraction_entry):
        return None

    return PanelGeometry(
        azimuth=int(azimuth_entry) if azimuth_entry else 180,
        tilt=int(tilt_entry) if tilt_entry else 0,
        area_fraction=float(area_fraction_entry) if area_fraction_entry else 1.0
    )


def _create_pv_site_from_csv_row(row: dict) -> PVSite:
    """
    Create a PVSite object from a CSV row dictionary.

    Args:
        row (dict): Dictionary containing CSV row data

    Returns:
        PVSite: The constructed PVSite object
    """
    # Convert string values to appropriate types
    location = Location(
        latitude=float(row['latitude']),
        longitude=float(row['longitude']),
        shading=row['shading']
    )

    # Create panel and inverter systems
    panel_system = System(
        brand=row['panel_brand'],
        capacity=int(row['panels_capacity'])
    )

    inverter_system = System(
        brand=row['inverter_brand'],
        capacity=int(row['inverter_capacity'])
    )

    # Create panel geometries
    panel_geometries = [
        geometry for i in (1, 2)
        if (geometry := _create_panel_geometry_from_row(row, i)) is not None
    ]

    if not panel_geometries:
        raise ValueError("At least one panel geometry must be defined in the CSV row")

    installation_date_str = row.get('installation_date')
    installation_date = date.fromisoformat(installation_date_str) if installation_date_str else None

    return PVSite(
        pvo_sys_id=int(row['pvoutput_system_id']),
        name=row['name'],
        location=location,
        panel_system=panel_system,
        panel_geometries=panel_geometries,
        inverter_system=inverter_system,
        installation_date=installation_date
    )


def get_all_system_ids() -> List[int]:
    """
    Retrieve a list of all PV site system IDs from the pv_sites.csv file.

    Returns:
        List[int]: List of all PVO system IDs
    """
    # noinspection PyTypeChecker
    source = files(resources).joinpath("pv_sites.csv")
    system_ids = []

    with source.open('r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            system_ids.append(int(row['pvoutput_system_id']))

    return system_ids


class PVSiteRepository:
    """Repository for managing PVSite objects loaded from pv_sites.csv."""

    def __init__(self, sites_by_id: dict[int, PVSite]):
        """
        Initialize the repository with a dictionary of PV sites.

        Args:
            sites_by_id: Dictionary mapping system IDs to PVSite objects
        """
        self._sites_by_id = sites_by_id

    @classmethod
    def from_csv(cls) -> 'PVSiteRepository':
        """
        Factory method to create a repository by loading all PV sites from the CSV file.

        Returns:
            PVSiteRepository: A new repository instance with all sites loaded
        """
        # noinspection PyTypeChecker
        source = files(resources).joinpath("pv_sites.csv")
        sites_by_id = {}

        with source.open('r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                pv_site = _create_pv_site_from_csv_row(row)
                sites_by_id[pv_site.pvo_sys_id] = pv_site

        return cls(sites_by_id)

    def get_by_system_ids(self, system_ids: List[int]) -> List[PVSite]:
        """
        Get PVSite objects for the given system IDs.

        Args:
            system_ids (List[int]): List of PVO system IDs to retrieve

        Returns:
            List[PVSite]: List of PVSite objects corresponding to the system IDs,
                         in the same order as requested

        Raises:
            ValueError: If any system ID is not found in the repository
        """
        missing_ids = [sid for sid in system_ids if sid not in self._sites_by_id]
        if missing_ids:
            raise ValueError(f"System ID(s) not found in pv_sites.csv: {sorted(missing_ids)}")

        return [self._sites_by_id[sid] for sid in system_ids]

    def get_all(self) -> List[PVSite]:
        """
        Get all PVSite objects, ordered by system ID.

        Returns:
            List[PVSite]: List of all PVSite objects, sorted by system ID
        """
        return [self._sites_by_id[sid] for sid in sorted(self._sites_by_id.keys())]
