import argparse
from contextlib import contextmanager
from datetime import date
from glob import glob
from os import listdir
from pathlib import Path

from mypy.build import build
from pyspark.sql import SparkSession
from pv_prospect.common.pv_site_repo import build_pv_site_repo, get_pv_site_by_system_id

from pv_prospect.data_transformation.preprocessing import preprocess
from pv_prospect.data_transformation.csv_transformer import CsvTransformer
from pv_prospect.data_transformation.helpers import RawDataFileMetadata, DataSource, date_to_str

GIT_REPO = "git@github.com:rjferrier/pv-prospect.git"

SOURCE_DIR = Path('data-0')
TARGET_DIR = Path('data-1')
TIMESERIES_FOLDER = 'timeseries'

OPENMETEO_HISTORICAL_DIR = SOURCE_DIR / TIMESERIES_FOLDER / 'openmeteo/historical'
PVOUTPUT_DIR = SOURCE_DIR / TIMESERIES_FOLDER / 'pvoutput'

ALLOW_OVERWRITES = True


def main() -> int:
    om_filenames = _list_files_sorted(OPENMETEO_HISTORICAL_DIR)
    present_pvo_filename_set = _list_files_as_set(PVOUTPUT_DIR)
    try:
        present_target_filename_set = _list_files_as_set(TARGET_DIR / TIMESERIES_FOLDER)
    except FileNotFoundError:
        present_target_filename_set = set()

    with open(SOURCE_DIR / 'pv_sites.csv', 'r') as om_filename:
        build_pv_site_repo(om_filename)

    with _get_spark_session() as spark:
        csv_transformer = CsvTransformer(spark)

        for om_filename in om_filenames:
            om_metadata = RawDataFileMetadata.from_filename(om_filename)

            pvo_filenames = _get_corresponding_pvoutput_filenames(om_metadata)
            if count_matches(pvo_filenames, present_pvo_filename_set) == 0:
                # no corresponding pvoutput data
                continue

            target_path = _build_target_path(
                om_metadata.pv_site_id,
                om_metadata.from_date,
                om_metadata.to_date
            )
            if not ALLOW_OVERWRITES and target_path in present_target_filename_set:
                continue

            om_df = csv_transformer.read_and_combine_csv_rows(OPENMETEO_HISTORICAL_DIR, om_filename)
            pvo_df = csv_transformer.read_and_combine_csv_rows(PVOUTPUT_DIR, pvo_filenames)
            pv_site = get_pv_site_by_system_id(om_metadata.pv_site_id)

            dataframe = preprocess(om_df, pvo_df, pv_site)

            dataframe.coalesce(1).write.csv(target_path, header=True, mode='overwrite', encoding='utf-8')

    return 0


def count_matches(items: list, reference_set: set) -> int:
    return len(set(items).intersection(reference_set))


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


@contextmanager
def _get_spark_session():
    """
    Create and manage a Spark session as a context manager.

    Configures Spark with optimizations for CSV processing and ensures proper cleanup
    by stopping the session when the context exits.

    Yields:
        SparkSession: A configured Spark session for CSV processing.
    """
    spark = SparkSession.builder \
        .appName("PreprocessHistorical") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    yield spark

    spark.stop()


def _build_target_path(pv_site_id: int, from_date: date, to_date: date) -> Path:
    stem = Path('_'.join((str(pv_site_id), date_to_str(from_date), date_to_str(to_date))))
    return TARGET_DIR / stem.with_suffix('.csv')


def _get_corresponding_pvoutput_filenames(om_metadata: RawDataFileMetadata | None) -> list[Path]:
    metadata_objs = (
        om_metadata.replace(data_source=DataSource.PV_OUTPUT, date_=date_)
        for date_ in om_metadata.get_date_range()
    )
    return sorted(metadata.get_file_name() for metadata in metadata_objs)


def _get_corresponding_pvoutput_file_path(om_metadata: RawDataFileMetadata | None, date_: date) -> Path:
    return PVOUTPUT_DIR / om_metadata.replace(data_source=DataSource.PV_OUTPUT, date_=date_).get_file_name()


if __name__ == '__main__':
    exit(main())
