from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Tuple, Optional


@dataclass
class ProcessingStats:
    """Track statistics for data processing operations."""
    processed: int = 0
    skipped_existing: int = 0
    skipped_dry_run: int = 0
    failed: int = 0
    failures: List[Tuple[str, date, int, str, str]] = field(default_factory=list)
    failure_log_path: Optional[str] = field(default=None, init=False)

    def record_success(self):
        """Record a successful processing operation."""
        self.processed += 1

    def record_skip_existing(self):
        """Record a skipped operation due to existing file."""
        self.skipped_existing += 1

    def record_skip_dry_run(self):
        """Record a skipped operation due to dry-run mode."""
        self.skipped_dry_run += 1

    def record_failure(self, source: str, date_: date, system_id: int, site_name: str, error: str):
        """Record a failed operation with details and write to log immediately."""
        # Create log file on first failure
        if self.failed == 0:
            self.failure_log_path = f"failure_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            with open(self.failure_log_path, 'w') as f:
                f.write(f"Failure Log - Created {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")

        self.failed += 1
        self.failures.append((source, date_, system_id, site_name, error))

        # Write to log file immediately
        with open(self.failure_log_path, 'a') as f:
            f.write(f"Failure #{self.failed} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: {source}\n")
            f.write(f"Date: {date_}\n")
            f.write(f"System ID: {system_id}\n")
            f.write(f"Site: {site_name}\n")
            f.write(f"Error: {error}\n")
            f.write("-" * 80 + "\n\n")

    def total(self) -> int:
        """Calculate total number of items processed."""
        return self.processed + self.skipped_existing + self.skipped_dry_run + self.failed

    def print_summary(self, dry_run: bool = False):
        """Print a summary of the processing results to the console."""
        print("\n" + "=" * 80)
        print("PROCESSING SUMMARY")
        print("=" * 80)

        total = self.total()

        print(f"Total items processed: {total}")
        print(f"  ✓ Successfully processed: {self.processed}")
        print(f"  ⊗ Skipped (already exists): {self.skipped_existing}")
        if dry_run:
            print(f"  ⊗ Skipped (dry run): {self.skipped_dry_run}")
        print(f"  ✗ Failed: {self.failed}")

        if self.failures:
            print(f"\nFailure details have been written to: {self.failure_log_path}")

        print("=" * 80)
