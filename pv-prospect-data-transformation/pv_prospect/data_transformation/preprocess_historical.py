from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from os import listdir
from pathlib import Path
import time

from pv_prospect.common.pv_site_repo import build_pv_site_repo, get_pv_site_by_system_id

from pv_prospect.data_transformation.csv_loader import read_and_combine_csv_rows
from pv_prospect.data_transformation.helpers import RawDataFileMetadata, DataSource, date_to_str
from pv_prospect.data_transformation.preprocessing import preprocess

GIT_REPO = "git@github.com:rjferrier/pv-prospect.git"

SOURCE_DIR = Path('data-0')
TARGET_DIR = Path('data-1')
TIMESERIES_FOLDER = 'timeseries'

SOURCE_TIMESERIES_DIR = SOURCE_DIR / TIMESERIES_FOLDER
TARGET_TIMESERIES_DIR = TARGET_DIR / TIMESERIES_FOLDER

OPENMETEO_HISTORICAL_DIR = SOURCE_TIMESERIES_DIR / 'openmeteo/historical'
PVOUTPUT_DIR = SOURCE_TIMESERIES_DIR / 'pvoutput'

ALLOW_OVERWRITES = True


def main() -> int:
    print("Starting preprocessing of historical data...")

    om_filenames = _list_files_sorted(OPENMETEO_HISTORICAL_DIR)
    print(f"Found {len(om_filenames)} OpenMeteo historical files")

    present_pvo_filenames = _list_files_as_set(PVOUTPUT_DIR)
    print(f"Found {len(present_pvo_filenames)} PVOutput files")

    try:
        filenames_present_in_target_folder = _list_files_as_set(TARGET_TIMESERIES_DIR)
        print(f"Found {len(filenames_present_in_target_folder)} existing files in target directory")
    except FileNotFoundError:
        filenames_present_in_target_folder = set()
        print("Target directory not found - will create new files")

    print("Loading PV sites repository...")
    with open(SOURCE_DIR / 'pv_sites.csv', 'r') as om_filename:
        build_pv_site_repo(om_filename)
    print("PV sites repository loaded successfully")

    # Prepare tasks for parallel processing
    tasks = []
    for om_filename in om_filenames:
        om_metadata = RawDataFileMetadata.from_filename(om_filename)

        pvo_filenames = sorted(_get_desired_pvoutput_filenames(om_metadata).intersection(present_pvo_filenames))
        if len(pvo_filenames) == 0:
            # no corresponding pvoutput data
            print(f"Skipping {om_filename}: no corresponding PVOutput data")
            continue

        target_filename = _build_target_filename(
            om_metadata.pv_site_id,
            om_metadata.from_date,
            om_metadata.to_date
        )
        if not ALLOW_OVERWRITES and target_filename in filenames_present_in_target_folder:
            print(f"Skipping {target_filename}: target file already exists")
            continue

        tasks.append((om_filename, pvo_filenames, om_metadata))

    print(f"\nProcessing {len(tasks)} files in parallel...")

    # Process files in parallel
    start_time = time.perf_counter()
    completed = 0
    failed = 0
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(_process_file, om_filename, pvo_filenames, om_metadata): om_filename
            for om_filename, pvo_filenames, om_metadata in tasks
        }

        for future in as_completed(futures):
            om_filename = futures[future]
            try:
                result = future.result()
                completed += 1
                print(f"[{completed}/{len(tasks)}] Completed: {result}")
            except Exception as e:
                failed += 1
                print(f"[{completed + failed}/{len(tasks)}] Failed: {om_filename} - Error: {str(e)}")

    end_time = time.perf_counter()
    total_time = end_time - start_time

    print(f"\nProcessing complete!")
    print(f"Successfully processed: {completed}")
    print(f"Failed: {failed}")
    print(f"Total: {len(tasks)}")
    print(f"Total time taken: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    if completed > 0:
        print(f"Average time per file: {total_time/completed:.2f} seconds")

    return 0


def _process_file(om_filename: str, pvo_filenames: list[str], om_metadata: RawDataFileMetadata) -> str:
    """Process a single file - designed to be called in parallel."""
    om_df = read_and_combine_csv_rows(OPENMETEO_HISTORICAL_DIR, om_filename)
    pvo_df = read_and_combine_csv_rows(PVOUTPUT_DIR, pvo_filenames)
    pv_site = get_pv_site_by_system_id(om_metadata.pv_site_id)

    dataframe = preprocess(om_df, pvo_df, pv_site)

    target_filename = _build_target_filename(
        om_metadata.pv_site_id,
        om_metadata.from_date,
        om_metadata.to_date
    )
    dataframe.to_csv(TARGET_TIMESERIES_DIR / target_filename, index=False, encoding='utf-8')

    return target_filename


def _list_files_sorted(directory: Path) -> list[str]:
    result = _list_files(directory)
    result.sort()
    return result


def _list_files_as_set(directory: Path) -> set[str]:
    return set(_list_files(directory))


def _list_files(directory: Path) -> list[str]:
    result = listdir(str(directory))
    result.sort()
    return result


def _build_target_filename(pv_site_id: int, from_date: date, to_date: date) -> Path:
    stem = '_'.join((str(pv_site_id), date_to_str(from_date), date_to_str(to_date)))
    return stem + '.csv'


def _get_desired_pvoutput_filenames(om_metadata: RawDataFileMetadata | None) -> set[Path]:
    metadata_objs = (
        om_metadata.replace(data_source=DataSource.PV_OUTPUT, date_=date_)
        for date_ in om_metadata.get_date_range()
    )
    return set(metadata.get_file_name() for metadata in metadata_objs)


def _get_corresponding_pvoutput_file_path(om_metadata: RawDataFileMetadata | None, date_: date) -> Path:
    return PVOUTPUT_DIR / om_metadata.replace(data_source=DataSource.PV_OUTPUT, date_=date_).get_file_name()


if __name__ == '__main__':
    exit(main())
