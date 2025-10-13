import csv
from dataclasses import dataclass
from typing import Optional, List
from importlib_resources import files

from src.domain.location import Location
import resources


@dataclass
class PanelGeometry:
    azimuth: int
    tilt: int
    area_fraction: float


@dataclass
class System:
    brand: str
    rated_output: int


@dataclass
class PVSite:
    name: str
    location: Location
    pvoutput_system_id: int
    panel_system: System
    panel_geometries: List[PanelGeometry]
    inverter_system: System


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
        longitude=float(row['longitude'])
    )

    # Create panel and inverter systems
    panel_system = System(
        brand=row['panel_brand'],
        rated_output=int(row['panel_rating'])
    )

    inverter_system = System(
        brand=row['inverter_brand'],
        rated_output=int(row['inverter_rating'])
    )

    # Create panel geometries
    panel_geometries = [
        geometry for i in (1, 2)
        if (geometry := _create_panel_geometry_from_row(row, i)) is not None
    ]

    if not panel_geometries:
        raise ValueError("At least one panel geometry must be defined in the CSV row")

    return PVSite(
        name=row['name'],
        location=location,
        pvoutput_system_id=int(row['pvoutput_system_id']),
        panel_system=panel_system,
        panel_geometries=panel_geometries,
        inverter_system=inverter_system
    )


def get_pv_site_by_system_id(system_id: int) -> Optional[PVSite]:
    """
    Look up a PV site by system ID from the pv_sites.csv file.

    Args:
        system_id (int): The PVO system ID to search for

    Returns:
        Optional[PVSite]: The PVSite object if found, None otherwise
    """
    source = files(resources).joinpath("pv_sites.csv")

    with source.open('r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if int(row['pvoutput_system_id']) == system_id:
                return _create_pv_site_from_csv_row(row)

    return None
