from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from decimal import Decimal
from os import listdir
from pathlib import Path
import time

import pandas as pd
from pv_prospect.common.pv_site_repo import build_pv_site_repo, get_all_pv_system_ids, get_pv_site_by_system_id
from pv_prospect.common.openmeteo_bounding_box_repo import build_openmeteo_bounding_box_repo, get_openmeteo_bounding_box_by_pv_site_id
from pv_prospect.common.domain.location import Location
from pv_prospect.common.domain.bounding_box import Vertex, VertexLabel
from pytz import timezone

from pv_prospect.data_transformation.helpers.csv_loader import read_and_combine_csv_rows
from pv_prospect.data_transformation.helpers.data_operations import (
    reduce_rows,
    NON_INTERPOLABLE_COLUMN_PATTERN,
    CellData,
    VertexData
)
from pv_prospect.data_transformation.helpers.file_metadata import (
    RawDataFileMetadata,
    OpenMeteoFileMetadata,
    DataSource,
    date_to_str
)

# Configuration
UKTZ = timezone('Europe/London')
UTC = timezone('UTC')


SOURCE_DIR = Path('data-0')
TARGET_DIR = Path('data-1')
TIMESERIES_FOLDER = 'timeseries'

SOURCE_TIMESERIES_DIR = SOURCE_DIR / TIMESERIES_FOLDER
TARGET_TIMESERIES_DIR = TARGET_DIR / TIMESERIES_FOLDER

PV_SITES_CSV_PATH = SOURCE_DIR / 'pv_sites.csv'
BOUNDING_BOXES_CSV_PATH = SOURCE_DIR / 'openmeteo_bounding_boxes.csv'
OPENMETEO_HISTORICAL_DIR = SOURCE_TIMESERIES_DIR / 'openmeteo/historical'
PVOUTPUT_DIR = SOURCE_TIMESERIES_DIR / 'pvoutput'

ALLOW_OVERWRITES = True


# ============================================================================
# Main preprocessing orchestration
# ============================================================================

def main() -> int:
    """Main entry point for preprocessing historical data."""
    print("Starting preprocessing of historical data...")

    # Load PV sites and bounding boxes repositories
    print("Loading PV sites repository...")
    with open(PV_SITES_CSV_PATH, 'r') as pv_sites_file:
        build_pv_site_repo(pv_sites_file)
    print("PV sites repository loaded successfully")

    print("Loading bounding boxes repository...")
    with open(BOUNDING_BOXES_CSV_PATH, 'r') as bb_file:
        build_openmeteo_bounding_box_repo(bb_file)
    print("Bounding boxes repository loaded successfully")

    # Parse all OpenMeteo files and create lookup
    om_filenames = _list_files_sorted(OPENMETEO_HISTORICAL_DIR)
    print(f"Found {len(om_filenames)} OpenMeteo historical files")

    om_file_lookup = _build_openmeteo_file_lookup(om_filenames)
    print(f"Built lookup for {len(om_file_lookup)} unique (lat, lon, date) combinations")

    # Get available PVOutput files
    present_pvo_filenames = _list_files_as_set(PVOUTPUT_DIR)
    print(f"Found {len(present_pvo_filenames)} PVOutput files")

    # Get existing target files
    try:
        filenames_present_in_target_folder = _list_files_as_set(TARGET_TIMESERIES_DIR)
        print(f"Found {len(filenames_present_in_target_folder)} existing files in target directory")
    except FileNotFoundError:
        filenames_present_in_target_folder = set()
        print("Target directory not found - will create new files")

    # Prepare tasks for parallel processing
    tasks = []
    pv_site_ids = get_all_pv_system_ids()
    print(f"\nProcessing {len(pv_site_ids)} PV sites...")

    for pv_site_id in pv_site_ids:
        try:
            pv_site = get_pv_site_by_system_id(pv_site_id)
            bounding_box = get_openmeteo_bounding_box_by_pv_site_id(pv_site_id)
        except KeyError:
            print(f"Skipping site {pv_site_id}: missing site or bounding box data")
            continue

        # Find all dates that have complete bounding box data
        vertex_locations_dict = bounding_box.get_vertices_dict()

        # Get all dates available for any vertex
        available_dates = set()
        for vertex_loc in vertex_locations_dict.values():
            vertex_key_prefix = (
                vertex_loc.latitude,
                vertex_loc.longitude
            )
            for key in om_file_lookup.keys():
                if key[0] == vertex_key_prefix[0] and key[1] == vertex_key_prefix[1]:
                    available_dates.add(key[2])

        # For each date, check if all 4 vertices are available
        for start_date in sorted(available_dates):
            vertex_files = {}
            all_vertices_present = True

            for vertex_label, vertex_loc in vertex_locations_dict.items():
                lookup_key = (
                    vertex_loc.latitude,
                    vertex_loc.longitude,
                    start_date
                )

                if lookup_key in om_file_lookup:
                    vertex_files[vertex_label] = om_file_lookup[lookup_key]
                else:
                    all_vertices_present = False
                    break

            if not all_vertices_present:
                continue

            # Calculate date range (7 days for historical data)
            end_date = start_date + pd.Timedelta(days=7)

            # Check if corresponding PVOutput files exist
            pvo_filenames = _get_desired_pvoutput_filenames_for_site(
                pv_site_id, start_date, end_date
            )
            pvo_filenames_available = sorted(pvo_filenames.intersection(present_pvo_filenames))

            if len(pvo_filenames_available) == 0:
                continue

            # Check if target file already exists
            target_filename = _build_target_filename(pv_site_id, start_date, end_date)
            if not ALLOW_OVERWRITES and target_filename in filenames_present_in_target_folder:
                continue

            # Add task
            tasks.append((pv_site_id, pv_site, vertex_locations_dict, vertex_files, pvo_filenames_available, start_date, end_date))

    print(f"\nPrepared {len(tasks)} processing tasks")

    # Process sites in parallel
    start_time = time.perf_counter()
    completed = 0
    failed = 0

    with ProcessPoolExecutor() as executor:
        # TODO get rid
        # results = [
        #     _process_site(pv_site_id, pv_site, vertex_locations_dict, vertex_files, pvo_filenames, start_date, end_date)
        #     for pv_site_id, pv_site, vertex_locations_dict, vertex_files, pvo_filenames, start_date, end_date in tasks
        # ]

        futures = {
            executor.submit(_process_site, pv_site_id, pv_site, vertex_locations_dict, vertex_files, pvo_filenames, start_date, end_date):
            (pv_site_id, start_date)
            for pv_site_id, pv_site, vertex_locations_dict, vertex_files, pvo_filenames, start_date, end_date in tasks
        }

        for future in as_completed(futures):
            pv_site_id, start_date = futures[future]
            try:
                result = future.result()
                completed += 1
                print(f"[{completed}/{len(tasks)}] Completed: {result}")
            except Exception as e:
                failed += 1
                print(f"[{completed + failed}/{len(tasks)}] Failed: site {pv_site_id}, date {start_date} - Error: {str(e)}")

    end_time = time.perf_counter()
    total_time = end_time - start_time

    print(f"\nProcessing complete!")
    print(f"Successfully processed: {completed}")
    print(f"Failed: {failed}")
    print(f"Total: {len(tasks)}")
    print(f"Total time taken: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    if completed > 0:
        print(f"Average time per task: {total_time/completed:.2f} seconds")

    return 0


def _process_site(
    pv_site_id: int,
    pv_site,
    vertex_locations_dict: dict[VertexLabel, Location],
    vertex_files: dict[VertexLabel, str],
    pvo_filenames: list[str],
    start_date: date,
    end_date: date
) -> str:
    """Process a single PV site with bounding box interpolation - designed to be called in parallel."""

    # Load all four vertices as VertexData objects
    vertex_data_list = [
        _to_vertex_data(Vertex(location=vertex_locations_dict[vertex_label], label=vertex_label), filename)
        for vertex_label, filename in vertex_files.items()
    ]

    # Create CellData from labeled vertices
    cell_data = CellData.from_vertices(vertex_data_list)

    # Load PVOutput data
    pvo_df = read_and_combine_csv_rows(PVOUTPUT_DIR, pvo_filenames)

    # Preprocess with bilinear interpolation
    dataframe = preprocess(cell_data, pvo_df, pv_site)

    # Save result
    target_filename = _build_target_filename(pv_site_id, start_date, end_date)
    dataframe.to_csv(TARGET_TIMESERIES_DIR / target_filename, index=False, encoding='utf-8')

    return target_filename


def _to_vertex_data(vertex: Vertex, filename: str) -> VertexData:
    df = read_and_combine_csv_rows(OPENMETEO_HISTORICAL_DIR, filename)
    # Convert time column to datetime
    df['time'] = pd.to_datetime(df['time'])
    return VertexData(vertex=vertex, dataframe=df)


# ============================================================================
# Core preprocessing logic
# ============================================================================

def preprocess(
    cell_data: CellData,
    pvoutput: pd.DataFrame,
    pv_site
) -> pd.DataFrame:
    """
    Preprocess and join OpenMeteo weather data with PVOutput data using bilinear interpolation.

    Args:
        cell_data: CellData object containing all four bounding box vertices
        pvoutput: DataFrame with PV output data
        pv_site: PVSite object containing location information

    Returns:
        DataFrame with interpolated weather data joined with PV output data
    """
    # Note: No need to convert time columns here - they should already be in the correct format
    # or the bilinear_interpolate method will handle them

    # Get a sample dataframe to identify columns
    sample_df = cell_data.sw.dataframe
    all_columns = [col for col in sample_df.columns if col != 'time']

    # Identify columns to interpolate (exclude weather_code columns)
    interpolable_columns = [
        col for col in all_columns
        if not NON_INTERPOLABLE_COLUMN_PATTERN.search(col)
    ]

    # Perform bilinear interpolation for continuous variables
    target_location = Location(
        latitude=pv_site.location.latitude,
        longitude=pv_site.location.longitude
    )

    interpolated_df = cell_data.bilinear_interpolate(
        target_location,
        interpolable_columns
    )

    # Get non-interpolable columns from nearest neighbor
    non_interpolables_df = cell_data.select_nearest_non_interpolables(
        target_location,
        NON_INTERPOLABLE_COLUMN_PATTERN
    )

    # Merge interpolated data with non-interpolables
    if len(non_interpolables_df.columns) > 1:  # More than just 'time' column
        openmeteo = interpolated_df.merge(non_interpolables_df, on='time', how='inner')
    else:
        openmeteo = interpolated_df

    # Join with PVOutput data
    _clean_pvoutput_times(pvoutput)
    pvo_reduced = reduce_rows(
        pvoutput[['time', 'power']], openmeteo['time']
    )
    joined = openmeteo.join(pvo_reduced.set_index('time'), on='time', how='inner')

    return joined


def _clean_pvoutput_times(df: pd.DataFrame) -> pd.DataFrame:
    """Convert PVOutput date/time columns to UTC timestamps."""
    df['time'] = (
        pd.to_datetime(df['date'].astype(str) + 'T' + df['time'])
        .apply(_undo_uk_daylight_savings)
    )
    df.drop('date', axis=1, inplace=True)
    return df


def _undo_uk_daylight_savings(dt_local):
    """Convert UK local time to UTC, accounting for daylight savings."""
    return UKTZ.localize(dt_local).astimezone(UTC).replace(tzinfo=None)



# ============================================================================
# File management helpers
# ============================================================================

def _list_files_sorted(directory: Path) -> list[str]:
    """List all files in directory, sorted."""
    result = _list_files(directory)
    result.sort()
    return result


def _list_files_as_set(directory: Path) -> set[str]:
    """List all files in directory as a set."""
    return set(_list_files(directory))


def _list_files(directory: Path) -> list[str]:
    """List all files in directory."""
    result = listdir(str(directory))
    result.sort()
    return result


def _build_openmeteo_file_lookup(om_filenames: list[str]) -> dict[tuple[Decimal, Decimal, date], str]:
    """
    Build lookup dictionary for OpenMeteo files by (latitude, longitude, date).

    Args:
        om_filenames: List of OpenMeteo filenames

    Returns:
        Dictionary mapping (lat, lon, date) to filename
    """
    lookup = {}
    for filename in om_filenames:
        metadata = OpenMeteoFileMetadata.from_filename(filename)
        if metadata:
            key = (
                metadata.latitude,
                metadata.longitude,
                metadata.from_date
            )
            lookup[key] = filename
    return lookup


def _get_desired_pvoutput_filenames_for_site(
    pv_site_id: int,
    start_date: date,
    end_date: date
) -> set[str]:
    """
    Get set of desired PVOutput filenames for a given PV site and date range.

    Args:
        pv_site_id: PV site ID
        start_date: Start date
        end_date: End date (exclusive)

    Returns:
        Set of filenames
    """
    filenames = set()
    current_date = start_date
    while current_date < end_date:
        metadata = RawDataFileMetadata(
            data_source=DataSource.PV_OUTPUT,
            pv_site_id=pv_site_id,
            date_=current_date
        )
        filenames.add(metadata.get_file_name())
        current_date += pd.Timedelta(days=1)
    return filenames


def _build_target_filename(pv_site_id: int, from_date: date, to_date: date) -> str:
    """Build target filename from site ID and date range."""
    stem = '_'.join((str(pv_site_id), date_to_str(from_date), date_to_str(to_date)))
    return stem + '.csv'




if __name__ == '__main__':
    exit(main())

