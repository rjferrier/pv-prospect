import random
from dataclasses import dataclass
from typing import Callable

from celery.result import ResultSet, AsyncResult

from pv_prospect.data_extraction.config import EtlConfig
from pv_prospect.common import DateRange
from pv_prospect.data_extraction.extractors import SourceDescriptor
from .tasks import create_folders, extract_and_load


@dataclass
class AsyncResultsWrapper:
    _join_timeout: int
    _async_results: list[AsyncResult]
    _callback: Callable[[list], None]

    def wait_for_completion(self):
        try:
            results = ResultSet(self._async_results).join(propagate=True, timeout=self._join_timeout)
        except Exception as e:
            # If join times out or another error occurs, collect whatever completed results are available.
            print(f"Warning: timeout or error while waiting for task results (waited {self._join_timeout}s): {e}")
            results = [ar.get(propagate=True) for ar in self._async_results if ar.ready()]

        self._callback(results)


class TaskQueuer:
    def __init__(self, task_spacing: float, task_jitter: float, join_timeout: float) -> None:
        self.task_spacing = task_spacing
        self.task_jitter = task_jitter
        self.join_timeout = join_timeout

    @classmethod
    def from_config(cls, config: EtlConfig) -> 'TaskQueuer':
        return cls(
            task_spacing=config.task_spacing,
            task_jitter=config.task_jitter,
            join_timeout=config.join_timeout,
        )

    def create_folders(
            self, source_descriptors: list[SourceDescriptor], local_dir: str | None, include_metadata: bool
    ) -> AsyncResultsWrapper:
        print(f"Creating folders for:", ', '.join(source_descriptors))
        results_async = [
            create_folders.apply_async(args=(sd, local_dir, include_metadata), countdown=self._calculate_delay(i))
            for i, sd in enumerate(source_descriptors)
        ]
        return self._wrap_async_results(results_async, _create_folders_callback)

    def extract_and_load(
            self,
            source_descriptor: SourceDescriptor,
            pv_system_id: str,
            date_range: DateRange,
            local_dir: str | None,
            write_metadata: bool,
            overwrite: bool,
            dry_run: bool,
            counter: int
    ):
        extract_and_load.apply_async(
            args=(
                source_descriptor,
                pv_system_id,
                date_range,
                local_dir,
                write_metadata,
                overwrite,
                dry_run,
            ),
            countdown=self._calculate_delay(counter)
        )

    def _calculate_delay(self, counter: int) -> float:
        # Calculate delay with spacing and random jitter to avoid hammering APIs
        base_delay = counter * self.task_spacing
        jitter = random.uniform(0, self.task_jitter)
        delay = base_delay + jitter
        return delay

    def _wrap_async_results(
            self, async_results: list[AsyncResult], callback: Callable[[list], None]
    ) -> AsyncResultsWrapper:
        return AsyncResultsWrapper(self.join_timeout, async_results, callback)


def _create_folders_callback(results: list[list[str]]):
    folder_ids = [el for sublist in results for el in sublist if el]
    if not folder_ids:
        print(f"No new folders to create.")
        return

    print(f"Created folders:", folder_ids)
