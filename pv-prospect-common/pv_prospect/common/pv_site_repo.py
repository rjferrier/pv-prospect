import csv
from datetime import date
from io import TextIOWrapper
from typing import Optional

from .domain import PVSite, Location, System, PanelGeometry


pv_sites_by_system_id = {}


def build_pv_site_repo(csv_stream: TextIOWrapper) -> None:
    """
    Build the PV site repository by loading PV site data from a CSV text stream.
    Or simply return if it is already built.

    This function reads the CSV data from the text stream, parses it, and populates
    the module-level pv_sites_by_system_id dictionary with PVSite objects.

    Args:
        csv_stream: text stream representing a CSV file.

    Raises:
        FileNotFoundError: If pv_sites.csv is not found in storage.
        ValueError: If CSV data is malformed or missing required fields.
    """
    if len(pv_sites_by_system_id) != 0:
        return

    # Read directly from the text stream
    csv_stream.seek(0)

    reader = csv.DictReader(csv_stream)
    for row in reader:
        pv_site = _create_pv_site_from_csv_row(row)
        pv_sites_by_system_id[pv_site.pvo_sys_id] = pv_site


def get_all_pv_system_ids() -> list[int]:
    """
    Return a sorted list of all PV system IDs available in the pv_sites resource CSV.

    Returns:
        list[int]: Sorted list of PV system IDs (integers).
    """
    return sorted(pv_sites_by_system_id.keys())


def get_pv_site_by_system_id(system_id: int) -> PVSite:
    """
    Look up and return the PVSite object for a given system ID.

    Args:
        system_id: The PV system identifier to look up.

    Returns:
        PVSite: The PVSite instance matching the given system_id.

    Raises:
        KeyError: If the system_id is not present in the pv_sites CSV.
    """
    return pv_sites_by_system_id[system_id]


def create_pv_sites_by_system_id(pv_site_csv_stream: TextIOWrapper) -> dict[int, PVSite]:
    pv_site_csv_stream.seek(0)

    reader = csv.DictReader(pv_site_csv_stream)
    pv_sites = (_create_pv_site_from_csv_row(row) for row in reader)
    return {pv_site.pvo_sys_id: pv_site for pv_site in pv_sites}


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
