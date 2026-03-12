import random
from dataclasses import dataclass
from typing import Callable

from celery.result import AsyncResult, ResultSet

from pv_prospect.common import DateRange
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.extractors import SourceDescriptor
from pv_prospect.data_extraction.processing.tasks import extract_and_load, preprocess


@dataclass
class AsyncResultsWrapper:
    _join_timeout: float
    _async_results: list[AsyncResult]
    _callback: Callable[[list], None]

    def wait_for_completion(self) -> None:
        try:
            results = ResultSet(self._async_results).join(
                propagate=True, timeout=self._join_timeout
            )
        except Exception as e:
            # If join times out or another error occurs, collect whatever completed results are available.
            print(
                f'Warning: timeout or error while waiting for task results (waited {self._join_timeout}s): {e}'
            )
            results = [
                ar.get(propagate=True) for ar in self._async_results if ar.ready()
            ]

        self._callback(results)


class TaskQueuer:
    def __init__(
        self, task_spacing: float, task_jitter: float, join_timeout: float
    ) -> None:
        self.task_spacing = task_spacing
        self.task_jitter = task_jitter
        self.join_timeout = join_timeout

    @classmethod
    def from_config(cls, config: DataExtractionConfig) -> 'TaskQueuer':
        return cls(
            task_spacing=config.task_queue.task_spacing,
            task_jitter=config.task_queue.task_jitter,
            join_timeout=config.task_queue.join_timeout,
        )

    def preprocess(
        self, source_descriptors: list[SourceDescriptor], local_dir: str | None
    ) -> AsyncResultsWrapper:
        print('Preprocessing for:', ', '.join(source_descriptors))
        results_async = [
            preprocess.apply_async(
                args=(sd, local_dir), countdown=self._calculate_delay(i)
            )
            for i, sd in enumerate(source_descriptors)
        ]
        return self._wrap_async_results(results_async, _preprocess_callback)

    def extract_and_load(
        self,
        source_descriptor: SourceDescriptor,
        pv_system_id: int,
        date_range: DateRange,
        local_dir: str | None,
        overwrite: bool,
        dry_run: bool,
        counter: int,
    ) -> None:
        extract_and_load.apply_async(
            args=(
                source_descriptor,
                pv_system_id,
                date_range,
                local_dir,
                overwrite,
                dry_run,
            ),
            countdown=self._calculate_delay(counter),
        )

    def _calculate_delay(self, counter: int) -> float:
        # Calculate delay with spacing and random jitter to avoid hammering APIs
        base_delay = counter * self.task_spacing
        jitter = random.uniform(0, self.task_jitter)  # nosec B311
        delay = base_delay + jitter
        return delay

    def _wrap_async_results(
        self, async_results: list[AsyncResult], callback: Callable[[list], None]
    ) -> AsyncResultsWrapper:
        return AsyncResultsWrapper(self.join_timeout, async_results, callback)


def _preprocess_callback(results: list[list[str]]) -> None:
    folder_ids = [el for sublist in results for el in sublist if el]
    if not folder_ids:
        print('No new folders to create.')
        return

    print('Created folders:', folder_ids)
