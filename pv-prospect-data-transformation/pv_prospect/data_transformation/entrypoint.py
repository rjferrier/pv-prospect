"""Cloud Run Job entrypoint for Data Transformation.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding clean or process function.

Environment variables
---------------------
TRANSFORM_STEP
    ``clean_weather``, ``clean_pvoutput``, ``process_weather``, or ``process_pv``
RAW_DATA_BUCKET
    GCS bucket name containing staged raw CSV data
MODEL_DATA_BUCKET
    GCS bucket name for writing output Parquet data
DATE
    ISO date ``YYYY-MM-DD`` to process
PV_SYSTEM_ID
    (Optional) integer system id, required for pv steps
"""

import io
import os
import sys
from io import StringIO

import pandas as pd

from pv_prospect.common import (
    build_location_mapping_repo,
    build_pv_site_repo,
    get_pv_site_by_system_id,
)
from pv_prospect.data_transformation.clean_pvoutput import clean_pvoutput
from pv_prospect.data_transformation.clean_weather import clean_weather
from pv_prospect.data_transformation.gcs_io import GcsIoHelper
from pv_prospect.data_transformation.process_pv import process_pv
from pv_prospect.data_transformation.process_weather import process_weather


def _load_resources(io_helper: GcsIoHelper, raw_bucket: str) -> None:
    """Load the PV site and bounding box repos from the raw data bucket."""
    # The extraction pipeline provisions these into the staging prefix.
    # In the raw bucket, they should be at the root or under a prefix.
    # Assuming they are provisioned into the staging prefix by extraction:
    pv_sites_csv = io_helper._raw_bucket.blob('staging/pv_sites.csv')
    if pv_sites_csv.exists():
        data = pv_sites_csv.download_as_string().decode('utf-8')
        build_pv_site_repo(StringIO(data))

    location_mapping_csv = io_helper._raw_bucket.blob('staging/location_mapping.csv')
    if location_mapping_csv.exists():
        data = location_mapping_csv.download_as_string().decode('utf-8')
        build_location_mapping_repo(StringIO(data))


def main() -> None:
    step = os.environ.get('TRANSFORM_STEP', '')
    raw_bucket = os.environ['RAW_DATA_BUCKET']
    model_bucket = os.environ['MODEL_DATA_BUCKET']
    target_date = os.environ['DATE']

    # Strip hyphens for filename dates (YYYYMMDD)
    date_str = target_date.replace('-', '')

    io_helper = GcsIoHelper(raw_bucket_name=raw_bucket, model_bucket_name=model_bucket)

    print(f'[entrypoint] Starting {step} for date {target_date}')

    if step == 'clean_weather':
        # Weather files are named: openmeteo-quarterhourly_{lat}_{lon}_{date}.csv
        # We need to find all weather files for this date and clean them.
        # This is a bit tricky: we need the bounding boxes to know the lat/lon.
        _load_resources(io_helper, raw_bucket)

        # In a real scenario, we'd list files or iterate over known bounding boxes.
        # Let's iterate over all blobs with the date in the name in the openmeteo folder.
        prefix = 'staging/timeseries/openmeteo/quarterhourly/'
        blobs = io_helper._raw_bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            if date_str in blob.name and blob.name.endswith('.csv'):
                print(f'[clean_weather] Processing {blob.name}')
                df = io_helper.read_csv_from_raw(blob.name)
                if df is not None and not df.empty:
                    cleaned_df = clean_weather(df)

                    # Write back to raw bucket under cleaned/
                    # e.g., cleaned/timeseries/openmeteo/quarterhourly/...
                    out_path = blob.name.replace('staging/', 'cleaned/').replace(
                        '.csv', '.parquet'
                    )
                    io_helper.write_parquet_to_raw(cleaned_df, out_path)

    elif step == 'clean_pvoutput':
        pv_system_id = os.environ['PV_SYSTEM_ID']
        # e.g. staging/timeseries/pvoutput/{sys_id}/pvoutput_{sys_id}_{date}.csv
        in_path = f'staging/timeseries/pvoutput/{pv_system_id}/pvoutput_{pv_system_id}_{date_str}.csv'

        print(f'[clean_pvoutput] Processing {in_path}')
        df = io_helper.read_csv_from_raw(in_path)
        if df is not None and not df.empty:
            cleaned_df = clean_pvoutput(df)
            out_path = in_path.replace('staging/', 'cleaned/').replace(
                '.csv', '.parquet'
            )
            io_helper.write_parquet_to_raw(cleaned_df, out_path)

    elif step == 'process_weather':
        # Process all cleaned weather files
        prefix = 'cleaned/timeseries/openmeteo/quarterhourly/'
        blobs = io_helper._raw_bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            if date_str in blob.name and blob.name.endswith('.parquet'):
                print(f'[process_weather] Processing {blob.name}')

                data = blob.download_as_bytes()
                with io.BytesIO(data) as buf:
                    cleaned_df = pd.read_parquet(buf)

                processed_df = process_weather(cleaned_df)

                out_path = blob.name.replace(
                    'cleaned/timeseries/openmeteo/quarterhourly/', ''
                )
                io_helper.write_parquet_to_model(processed_df, out_path)

    elif step == 'process_pv':
        pv_sys_id = int(os.environ['PV_SYSTEM_ID'])
        _load_resources(io_helper, raw_bucket)
        pv_site = get_pv_site_by_system_id(pv_sys_id)

        # 1. Load cleaned PV data
        in_pv_path = f'cleaned/timeseries/pvoutput/{pv_sys_id}/pvoutput_{pv_sys_id}_{date_str}.parquet'
        pv_blob = io_helper._raw_bucket.blob(in_pv_path)
        if not pv_blob.exists():
            print(f'[process_pv] Cleaned PV data not found: {in_pv_path}')
            return

        with io.BytesIO(pv_blob.download_as_bytes()) as buf:
            cleaned_pv_df = pd.read_parquet(buf)

        # 2. Find corresponding cleaned weather data
        # TODO: use get_location_by_pv_system_id to match weather files precisely
        prefix = 'cleaned/timeseries/openmeteo/quarterhourly/'
        weather_blobs = list(io_helper._raw_bucket.list_blobs(prefix=prefix))
        weather_blob = None
        for b in weather_blobs:
            # Simple heuristic: find the file with the date
            if date_str in b.name:
                # Need a more robust match for lat/lon in a real implementation
                weather_blob = b
                break

        if not weather_blob:
            print(f'[process_pv] Cleaned weather data not found for date {date_str}')
            return

        with io.BytesIO(weather_blob.download_as_bytes()) as buf:
            cleaned_weather_df = pd.read_parquet(buf)

        # 3. Process
        print(f'[process_pv] Joining weather={weather_blob.name} with pv={in_pv_path}')
        processed_df = process_pv(
            weather_df=cleaned_weather_df, pvoutput_df=cleaned_pv_df, pv_site=pv_site
        )

        out_path = f'{pv_sys_id}/processed_pv_{pv_sys_id}_{date_str}.parquet'
        io_helper.write_parquet_to_model(processed_df, out_path)

    else:
        print(f'[entrypoint] ERROR: unknown TRANSFORM_STEP={step}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
