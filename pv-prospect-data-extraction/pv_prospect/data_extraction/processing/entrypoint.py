"""Cloud Run Job entrypoint.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding core function.

Environment variables
---------------------
JOB_TYPE
    ``preprocess`` or ``extract_and_load``

For **preprocess**:
    SOURCE_DESCRIPTOR — e.g. ``openmeteo/quarterhourly``

For **extract_and_load**:
    SOURCE_DESCRIPTOR — e.g. ``openmeteo/quarterhourly``
    PV_SYSTEM_ID      — integer system id
    START_DATE        — ISO date ``YYYY-MM-DD``
    END_DATE          — ISO date ``YYYY-MM-DD``
    OVERWRITE         — ``true`` or ``false`` (default ``false``)
    DRY_RUN           — ``true`` or ``false`` (default ``false``)
    BY_WEEK           — ``true`` or ``false`` (chunking hint)
"""

import os
import sys
from datetime import date

from pv_prospect.common import (
    DateRange,
    Period,
    build_location_mapping_repo,
    build_pv_site_repo,
    get_pv_site_by_system_id,
)
from pv_prospect.common.config_parser import get_config
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.extractors import (
    SourceDescriptor,
    get_extractor,
    supports_multi_date,
)
from pv_prospect.data_extraction.processing import core
from pv_prospect.etl.extract.resolve import dvc
from pv_prospect.etl.factory import get_extractor as get_storage_extractor
from pv_prospect.etl.factory import get_loader as get_storage_loader


def _env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() in ('true', '1', 'yes')


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')
    config = get_config(DataExtractionConfig)

    # Cloud Run always uses GCS — resolve storage backends once.
    staging_extractor = get_storage_extractor(config.staged_raw_data_storage)
    staging_loader = get_storage_loader(config.staged_raw_data_storage)
    versioned_extractor = get_storage_extractor(config.versioned_resources_storage)
    dvc_prefix = config.versioned_resources_storage.tracking.prefix

    if job_type == 'preprocess':
        source_descriptor = SourceDescriptor(os.environ['SOURCE_DESCRIPTOR'])

        print(f'[entrypoint] preprocess: {source_descriptor}')
        core.preprocess(
            dvc.resolve_path,
            versioned_extractor,
            staging_loader,
            dvc_prefix,
            source_descriptor,
        )

    elif job_type == 'extract_and_load':
        source_descriptor = SourceDescriptor(os.environ['SOURCE_DESCRIPTOR'])
        pv_system_id = int(os.environ['PV_SYSTEM_ID'])
        start_date = date.fromisoformat(os.environ['START_DATE'])
        end_date = date.fromisoformat(os.environ['END_DATE'])
        overwrite = _env_bool('OVERWRITE')
        dry_run = _env_bool('DRY_RUN')
        by_week = _env_bool('BY_WEEK')

        complete_date_range = DateRange(start_date, end_date)
        print(
            f'[entrypoint] extract_and_load: {source_descriptor}, '
            f'site={pv_system_id}, {complete_date_range}, by_week={by_week}'
        )

        # Initialise in-memory repos once
        build_pv_site_repo(staging_extractor.read_file(core.PV_SITES_CSV_FILE))
        build_location_mapping_repo(
            staging_extractor.read_file(core.LOCATION_MAPPING_CSV_FILE)
        )

        split_period = Period.WEEK if by_week else Period.DAY
        sub_date_ranges = complete_date_range.split_by(split_period)

        if by_week and not supports_multi_date(source_descriptor):
            # Fall back to days if the source doesn't support multi-date extraction
            final_ranges = []
            for dr in sub_date_ranges:
                final_ranges.extend(dr.split_by(Period.DAY))
        else:
            final_ranges = sub_date_ranges

        for dr in final_ranges:
            result = core.extract_and_load(
                get_pv_site_by_system_id,
                get_extractor,
                source_descriptor,
                staging_extractor,
                staging_loader,
                pv_system_id,
                dr,
                overwrite,
                dry_run,
            )
            print(f'[entrypoint] {dr}: {result.type.value}')

    else:
        print(f'[entrypoint] ERROR: unknown JOB_TYPE={job_type!r}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
