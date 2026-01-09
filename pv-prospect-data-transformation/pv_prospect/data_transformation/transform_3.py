from pathlib import Path
import re

import pandas as pd
from pv_prospect.common.pv_site_repo import build_pv_site_repo, get_all_pv_system_ids


PV_SITES_PATH = Path('data-0/pv_sites.csv')

TIMESERIES_FOLDER = 'timeseries'
SOURCE_TIMESERIES_DIR = Path('data-2') / TIMESERIES_FOLDER
TARGET_TIMESERIES_DIR = Path('data-3') / TIMESERIES_FOLDER


def concatenate_timeseries_data() -> None:
    """
    Concatenate processed micro-batches from data-2/timeseries into single CSV files per site.

    For each PV site:
    - Collect all processed micro-batches for that site from data-2/timeseries
    - Concatenate them in chronological order
    - Write to a single CSV file in data-3/timeseries with format {pv_site_id}.csv
    """
    # Load PV sites repository to get all site IDs
    print("Loading PV sites repository...")
    with open(PV_SITES_PATH, 'r') as pv_sites_file:
        build_pv_site_repo(pv_sites_file)
    print("PV sites repository loaded successfully")

    # Create target directory if it doesn't exist
    TARGET_TIMESERIES_DIR.mkdir(parents=True, exist_ok=True)

    # Get all processed micro-batch files
    print(f"Scanning {SOURCE_TIMESERIES_DIR} for processed micro-batches...")
    micro_batches = list(SOURCE_TIMESERIES_DIR.glob('*.csv'))
    print(f"Found {len(micro_batches)} processed micro-batch files")

    # Group micro-batches by PV site ID
    site_batches = _group_batches_by_site(micro_batches)
    print(f"Concatenating data for {len(site_batches)} PV sites...")

    # Process each site
    for site_id in sorted(site_batches.keys()):
        print(f"\nConcatenating site {site_id}...")

        # Load and concatenate all micro-batches for this site
        batch_files = sorted(site_batches[site_id])
        print(f"  Loading {len(batch_files)} micro-batches...")

        dfs = []
        for batch_file in batch_files:
            df = pd.read_csv(batch_file)
            df['time'] = pd.to_datetime(df['time'])
            dfs.append(df)

        # Concatenate all batches
        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)

            # Sort by time to ensure chronological order
            final_df = final_df.sort_values('time').reset_index(drop=True)

            # Write to output
            output_file = TARGET_TIMESERIES_DIR / f"{site_id}.csv"
            final_df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"  Written {len(final_df)} records to {output_file}")

    print("\nConcatenation complete!")


def _group_batches_by_site(batch_files: list[Path]) -> dict[int, list[Path]]:
    """Group micro-batch files by PV site ID."""
    # Micro-batch filename format: {pv_site_id}_{start_date}_{end_date}.csv
    pattern = re.compile(r'^(\d+)_\d{8}_\d{8}\.csv$')

    site_batches = {}
    for batch_file in batch_files:
        match = pattern.match(batch_file.name)
        if match:
            site_id = int(match.group(1))
            if site_id not in site_batches:
                site_batches[site_id] = []
            site_batches[site_id].append(batch_file)

    return site_batches


def main() -> int:
    """Main entry point for the concatenation script."""
    concatenate_timeseries_data()
    return 0


if __name__ == '__main__':
    exit(main())

