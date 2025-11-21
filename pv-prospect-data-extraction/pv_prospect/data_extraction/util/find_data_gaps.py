#!/usr/bin/env python3
"""
Script to identify gaps in the dataset.

Expected intervals:
- openmeteo/historical: 7 days (weekly)
- pvoutput, visualcrossing/*: 1 day (daily)
"""

import re
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

from pv_prospect.data_extraction.loaders import get_storage_client
from pv_prospect.data_extraction.loaders.factory import StorageClient


def parse_date_from_filename(filename: str) -> datetime | None:
    """Extract date from filename in format: prefix_id_YYYYMMDD.csv"""
    match = re.search(r'_(\d{8})\.csv$', filename)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            return None
    return None


def get_expected_interval(dataset_path: str) -> int:
    """
    Return expected interval in days based on dataset path.
    - openmeteo/historical: 7 days
    - everything else: 1 day
    """
    if 'openmeteo' in dataset_path and 'historical' in dataset_path:
        return 7
    return 1


def find_gaps(dates: List[datetime], interval_days: int) -> List[Tuple[datetime, datetime, int]]:
    """
    Find gaps in a list of dates.
    Returns list of (gap_start, gap_end, missing_count) tuples.
    """
    if not dates or len(dates) < 2:
        return []
    
    dates_sorted = sorted(dates)
    gaps = []
    
    for i in range(len(dates_sorted) - 1):
        current = dates_sorted[i]
        next_date = dates_sorted[i + 1]
        expected_next = current + timedelta(days=interval_days)
        
        # Calculate the actual gap
        actual_gap = (next_date - current).days
        
        if actual_gap > interval_days:
            # Calculate how many files are missing
            missing_count = (actual_gap // interval_days) - 1
            if missing_count > 0:
                gap_start = current + timedelta(days=interval_days)
                gap_end = next_date - timedelta(days=interval_days)
                gaps.append((gap_start, gap_end, missing_count))
    
    return gaps


def analyze_dataset(storage_client: StorageClient) -> Dict[str, Dict[str, List[Tuple[datetime, datetime, int]]]]:
    """
    Analyze the entire dataset and find gaps.
    Returns nested dict: {dataset_path: {entity_id: [gaps]}}
    """
    results = defaultdict(lambda: defaultdict(list))
    
    # Define dataset paths to check
    dataset_paths = [
        'openmeteo/historical',
        'openmeteo/quarterhourly',
        'pvoutput',
        'visualcrossing/hourly',
        'visualcrossing/quarterhourly',
    ]
    
    for dataset_path in dataset_paths:
        # Use polymorphic list_files method - works for both local and GDrive
        files = storage_client.list_files(folder_path=dataset_path, pattern='*.csv')

        if not files:
            continue
        
        interval_days = get_expected_interval(dataset_path)
        
        # Group files by entity ID
        entity_files = defaultdict(list)
        
        for file_info in files:
            filename = file_info['name']

            date = parse_date_from_filename(filename)
            if date is None:
                continue
            
            # Extract entity ID from filename (format: prefix_id_YYYYMMDD.csv)
            match = re.match(r'^[^_]+_(\d+)_\d{8}\.csv$', filename)
            if match:
                entity_id = match.group(1)
                entity_files[entity_id].append(date)
        
        # Find gaps for each entity
        for entity_id, dates in sorted(entity_files.items()):
            gaps = find_gaps(dates, interval_days)
            if gaps:
                results[dataset_path][entity_id] = gaps
    
    return results


def aggregate_gaps(entities_gaps: Dict[str, List[Tuple[datetime, datetime, int]]], interval_days: int) -> List[Tuple[datetime, datetime, int, set]]:
    """
    Aggregate gaps across multiple entities, merging overlapping date ranges.

    Args:
        entities_gaps: Dict mapping entity_id to list of (gap_start, gap_end, missing_count)
        interval_days: The expected interval between files (for proper merging)

    Returns:
        List of (gap_start, gap_end, total_missing, entity_ids) tuples, sorted by date
        where entity_ids is a set of entity IDs associated with the gap
    """
    # Collect all gap segments with their entity info
    all_segments = []
    for entity_id, gaps in entities_gaps.items():
        for gap_start, gap_end, missing_count in gaps:
            all_segments.append((gap_start, gap_end, missing_count, entity_id))

    if not all_segments:
        return []

    # Sort by start date
    all_segments.sort(key=lambda x: x[0])

    # Merge overlapping or adjacent gaps
    merged = []
    current_start = all_segments[0][0]
    current_end = all_segments[0][1]
    current_missing = all_segments[0][2]
    entities = {all_segments[0][3]}

    for i in range(1, len(all_segments)):
        gap_start, gap_end, missing_count, entity_id = all_segments[i]

        # Check if this gap is adjacent or overlapping with current gap
        # Adjacent means: gap_start is at most interval_days after current_end
        if gap_start <= current_end + timedelta(days=interval_days):
            # Merge: extend the end date and add missing files
            current_end = max(current_end, gap_end)
            current_missing += missing_count
            entities.add(entity_id)
        else:
            # No overlap: save current and start new
            merged.append((current_start, current_end, current_missing, entities))
            current_start = gap_start
            current_end = gap_end
            current_missing = missing_count
            entities = {entity_id}

    # Don't forget the last one
    merged.append((current_start, current_end, current_missing, entities))

    return merged


def format_gap_report(results: Dict[str, Dict[str, List[Tuple[datetime, datetime, int]]]]) -> str:
    """Format the gap analysis results into a readable report with aggregated gaps."""
    lines = []
    lines.append("=" * 80)
    lines.append("DATA GAP ANALYSIS REPORT")
    lines.append("=" * 80)
    lines.append("")
    
    total_gaps = sum(len(entities) for entities in results.values())
    
    if total_gaps == 0:
        lines.append("✓ No gaps found in the dataset!")
        lines.append("")
        return "\n".join(lines)
    
    for dataset_path in sorted(results.keys()):
        entities = results[dataset_path]
        interval_days = get_expected_interval(dataset_path)
        interval_str = f"{interval_days} day{'s' if interval_days > 1 else ''}"
        
        lines.append(f"\n{dataset_path.upper()}")
        lines.append(f"Expected interval: {interval_str}")
        lines.append("-" * 80)
        
        # Aggregate gaps across all entities
        aggregated_gaps = aggregate_gaps(entities, interval_days)

        lines.append(f"\n  Total gaps: {len(aggregated_gaps)}")

        for gap_start, gap_end, missing_count, entity_ids in aggregated_gaps:
            entity_count = len(entity_ids)
            entity_str = "entity" if entity_count == 1 else "entities"
            file_str = "file" if missing_count == 1 else "files"

            # For the display, add interval to end date to show full range (exclusive end)
            display_end = gap_end + timedelta(days=interval_days)

            lines.append(f"    • {gap_start.strftime('%Y-%m-%d')} to {display_end.strftime('%Y-%m-%d')} "
                       f"({missing_count} missing {file_str} across {entity_count} {entity_str})")

        lines.append("")
    
    lines.append("=" * 80)
    lines.append(f"SUMMARY: Found gaps in {total_gaps} entity/dataset combination(s)")
    lines.append("=" * 80)
    
    return "\n".join(lines)


def get_source_name(dataset_path: str) -> str:
    """
    Map dataset path to task_producer.py source name.

    Returns:
        Source name for task_producer.py command
    """
    if 'pvoutput' in dataset_path:
        return 'pv'
    elif 'openmeteo/historical' in dataset_path:
        return 'weather-om-historical'
    elif 'openmeteo/quarterhourly' in dataset_path:
        return 'weather-om-15'
    elif 'openmeteo/hourly' in dataset_path:
        return 'weather-om-60'
    elif 'visualcrossing/quarterhourly' in dataset_path:
        return 'weather-vc-15'
    elif 'visualcrossing/hourly' in dataset_path:
        return 'weather-vc-60'
    else:
        return 'unknown'


def generate_docker_commands(results: Dict[str, Dict[str, List[Tuple[datetime, datetime, int]]]], local_dir: str | None) -> str:
    """
    Generate Docker Compose commands to rectify the identified gaps.

    Commands are in the format:
    docker compose run --rm taskproducer <source> <entities> -d <start> -e <end> [-w] [-l <local-dir>]

    Where:
    - <source> is the data source (pv, weather-om-15, weather-om-historical, etc.)
    - <entities> is a comma-separated list of entity IDs
    - <start> is the inclusive start date (YYYY-MM-DD)
    - <end> is the exclusive end date (YYYY-MM-DD)
    - -w flag is only included for weekly-spaced data
    - -l <local-dir> is included if local_dir is specified
    """
    lines = []
    lines.append("=" * 80)
    lines.append("COMMANDS TO RECTIFY DATA GAPS")
    lines.append("=" * 80)
    lines.append("")

    total_commands = 0

    for dataset_path in sorted(results.keys()):
        entities = results[dataset_path]
        interval_days = get_expected_interval(dataset_path)
        source_name = get_source_name(dataset_path)

        lines.append(f"\n# {dataset_path.upper()}")
        lines.append(f"# Source: {source_name}")
        lines.append("-" * 80)

        # Aggregate gaps across all entities for command generation
        aggregated_gaps = aggregate_gaps(entities, interval_days)

        for gap_start, gap_end, missing_count, entity_ids in aggregated_gaps:
            # Calculate exclusive end date (add interval to gap_end)
            exclusive_end = gap_end + timedelta(days=interval_days)

            # Format entity IDs as comma-separated list
            entity_list = ','.join(sorted(entity_ids, key=int))

            # Format the command with entity IDs
            command = (f"docker compose run --rm taskproducer {source_name} "
                      f"{entity_list} -d {gap_start.strftime('%Y-%m-%d')} "
                      f"-e {exclusive_end.strftime('%Y-%m-%d')}")

            # Add -w flag only for weekly-spaced data
            if interval_days == 7:
                command += " -w"

            # Add -l flag if local directory is specified
            if local_dir:
                command += f" -l {local_dir}"

            lines.append(command)
            total_commands += 1

        lines.append("")

    lines.append("=" * 80)
    lines.append(f"Total commands: {total_commands}")
    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Analyze dataset gaps and generate reports or commands.")
    parser.add_argument('--generate-commands', action='store_true',
                        help="Generate Docker Compose commands to rectify gaps")
    parser.add_argument('-l', '--local-dir', type=str, default=None,
                        help='Analyze files in a local directory instead of Google Drive. Specify the directory path.')

    args = parser.parse_args()

    # Get the storage client
    storage_client = get_storage_client(args.local_dir)

    if args.local_dir:
        print(f"Analyzing local dataset at: {args.local_dir}")
    else:
        print("Analyzing dataset on Google Drive")

    print("This may take a moment...\n")
    
    # Analyze the dataset
    results = analyze_dataset(storage_client)

    if args.generate_commands:
        # Generate and print Docker commands
        commands = generate_docker_commands(results, args.local_dir)
        print(commands)
    else:
        # Generate and print report
        report = format_gap_report(results)
        print(report)

        # Optionally save to file
        script_dir = Path(__file__).parent
        output_file = script_dir / "data_gaps_report.txt"
        with open(output_file, 'w') as f:
            f.write(report)
        print(f"\nReport saved to: {output_file}")

    return 0


if __name__ == "__main__":
    exit(main())
