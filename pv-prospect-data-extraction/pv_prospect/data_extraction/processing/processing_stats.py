from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, overload, Union

from pv_prospect.data_extraction.processing.value_objects import Task, Result, ResultType, FailureDetails

from pv_prospect.common.pv_site_repo import get_pv_site_by_system_id


@dataclass
class ProcessingStats:
    """Track statistics for data processing operations."""
    processed: int = 0
    skipped_existing: int = 0
    skipped_dry_run: int = 0
    failed: int = 0
    failures: List[tuple[Task, FailureDetails]] = field(default_factory=list)
    failure_log_path: Optional[str] = field(default=None, init=False)

    @overload
    def record(self, result: Result) -> None: ...

    @overload
    def record(self, results: List[Result]) -> None: ...

    def record(self, result_or_results: Union[Result, List[Result]]):
        """Record the result(s) of task execution. Accepts a single Result or a list of Results.

        This is implemented as a dispatcher: when a list is provided, each Result is
        recorded individually by delegating back to this method for the single-case.
        """
        # If a list was provided, iterate and record each one.
        if isinstance(result_or_results, list):
            for res in result_or_results:
                # Delegate to the single-result path
                self.record(res)
            return

        # Single Result path
        result: Result = result_or_results
        if result.type == ResultType.SUCCESS:
            self.record_success()
        elif result.type == ResultType.FAILURE:
            self.record_failure(result.task, result.failure_details)
        elif result.type == ResultType.SKIPPED_EXISTING:
            self.record_skip_existing()
        elif result.type == ResultType.SKIPPED_DRY_RUN:
            self.record_skip_dry_run()

    def record_success(self):
        """Record a successful processing operation."""
        self.processed += 1

    def record_skip_existing(self):
        """Record a skipped operation due to existing file."""
        self.skipped_existing += 1

    def record_skip_dry_run(self):
        """Record a skipped operation due to dry-run mode."""
        self.skipped_dry_run += 1

    def record_failure(self, task: Task, failure_details: FailureDetails):
        """Record a failed operation with details and write to log immediately."""
        # Create log file on first failure
        if self.failed == 0:
            self.failure_log_path = f"failure_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            with open(self.failure_log_path, 'w') as f:
                f.write(f"Failure Log - Created {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")

        self.failed += 1
        self.failures.append((task, failure_details))

        # Try to resolve PV site information for nicer logging; fall back to raw id
        pv_system_id = getattr(task, 'pv_system_id', None)
        site_name = 'unknown'
        site_pvo_id = pv_system_id
        try:
            if pv_system_id is not None:
                pv_site = get_pv_site_by_system_id(int(pv_system_id))
                site_name = pv_site.name
                site_pvo_id = pv_site.pvo_sys_id
        except Exception:
            # keep fallback values
            pass

        # Write to log file immediately
        with open(self.failure_log_path, 'a') as f:
            f.write(f"Failure #{self.failed} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: {task.source_descriptor}\n")
            f.write(f"Date: {task.date_range.start}\n")
            f.write(f"System ID: {site_pvo_id}\n")
            f.write(f"Site: {site_name}\n")
            f.write(f"Error: {failure_details.error}\n")
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
