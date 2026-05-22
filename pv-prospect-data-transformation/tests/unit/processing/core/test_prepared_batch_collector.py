"""Tests for PreparedBatchCollector."""

import threading

import pandas as pd
from pv_prospect.data_transformation.processing import PreparedBatchCollector

_WINDOW = ('2026-01-01', '2026-01-15')


def _frame(value: int) -> pd.DataFrame:
    return pd.DataFrame({'power': [value]})


def test_weather_groups_keys_frames_by_sample_and_window() -> None:
    collector = PreparedBatchCollector()
    collector.add_weather(7, *_WINDOW, _frame(1))
    collector.add_weather(7, *_WINDOW, _frame(2))

    groups = collector.weather_groups()

    assert [f['power'].iloc[0] for f in groups[(7, *_WINDOW)]] == [1, 2]


def test_weather_groups_separates_distinct_sample_files() -> None:
    collector = PreparedBatchCollector()
    collector.add_weather(7, *_WINDOW, _frame(1))
    collector.add_weather(8, *_WINDOW, _frame(2))

    groups = collector.weather_groups()

    assert set(groups) == {(7, *_WINDOW), (8, *_WINDOW)}


def test_pv_groups_keys_frames_by_window() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(101, *_WINDOW, _frame(1))
    collector.add_pv(101, *_WINDOW, _frame(3))

    groups = collector.pv_groups(101)

    assert [f['power'].iloc[0] for f in groups[_WINDOW]] == [1, 3]


def test_pv_groups_returns_only_the_requested_system() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(101, *_WINDOW, _frame(1))
    collector.add_pv(202, *_WINDOW, _frame(2))

    assert [f['power'].iloc[0] for f in collector.pv_groups(101)[_WINDOW]] == [1]
    assert [f['power'].iloc[0] for f in collector.pv_groups(202)[_WINDOW]] == [2]


def test_weather_groups_empty_by_default() -> None:
    assert PreparedBatchCollector().weather_groups() == {}


def test_pv_groups_empty_for_unknown_system() -> None:
    assert PreparedBatchCollector().pv_groups(999) == {}


def test_weather_groups_returns_a_copy() -> None:
    collector = PreparedBatchCollector()
    collector.add_weather(7, *_WINDOW, _frame(1))

    collector.weather_groups()[(7, *_WINDOW)].clear()

    assert len(collector.weather_groups()[(7, *_WINDOW)]) == 1


def test_concurrent_add_weather_keeps_every_frame() -> None:
    collector = PreparedBatchCollector()

    def add(index: int) -> None:
        collector.add_weather(0, *_WINDOW, _frame(index))

    threads = [threading.Thread(target=add, args=(i,)) for i in range(200)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(collector.weather_groups()[(0, *_WINDOW)]) == 200


def test_concurrent_add_pv_keeps_every_frame() -> None:
    collector = PreparedBatchCollector()

    def add(index: int) -> None:
        collector.add_pv(index % 5, *_WINDOW, _frame(index))

    threads = [threading.Thread(target=add, args=(i,)) for i in range(200)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    total = sum(
        len(frames)
        for system_id in range(5)
        for frames in collector.pv_groups(system_id).values()
    )
    assert total == 200
