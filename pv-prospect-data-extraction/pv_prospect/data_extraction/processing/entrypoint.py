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

from pv_prospect.common import DateRange, Period
from pv_prospect.data_extraction.extractors import SourceDescriptor, supports_multi_date
from pv_prospect.data_extraction.processing import core


def _env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() in ('true', '1', 'yes')


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')

    if job_type == 'preprocess':
        source_descriptor = SourceDescriptor(os.environ['SOURCE_DESCRIPTOR'])

        print(f"[entrypoint] preprocess: {source_descriptor}")
        core.preprocess(
            source_descriptor=source_descriptor,
            local_dir=None,          # Cloud Run always writes to GCS
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
        print(f"[entrypoint] extract_and_load: {source_descriptor}, "
              f"site={pv_system_id}, {complete_date_range}, by_week={by_week}")

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
                source_descriptor=source_descriptor,
                pv_system_id=pv_system_id,
                date_range=dr,
                local_dir=None,
                overwrite=overwrite,
                dry_run=dry_run,
            )
            print(f"[entrypoint] {dr}: {result.type.value}")

    else:
        print(f"[entrypoint] ERROR: unknown JOB_TYPE={job_type!r}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
