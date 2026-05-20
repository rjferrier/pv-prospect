"""Tests for PreparedBatchCollector."""

import threading

import pandas as pd
from pv_prospect.data_transformation.processing import PreparedBatchCollector


def _frame(value: int) -> pd.DataFrame:
    return pd.DataFrame({'power': [value]})


def test_weather_frames_returns_added_frames() -> None:
    collector = PreparedBatchCollector()
    collector.add_weather(_frame(1))
    collector.add_weather(_frame(2))

    frames = collector.weather_frames()

    assert [f['power'].iloc[0] for f in frames] == [1, 2]


def test_pv_frames_are_keyed_by_system() -> None:
    collector = PreparedBatchCollector()
    collector.add_pv(101, _frame(1))
    collector.add_pv(202, _frame(2))
    collector.add_pv(101, _frame(3))

    assert [f['power'].iloc[0] for f in collector.pv_frames(101)] == [1, 3]
    assert [f['power'].iloc[0] for f in collector.pv_frames(202)] == [2]


def test_weather_frames_empty_by_default() -> None:
    assert PreparedBatchCollector().weather_frames() == []


def test_pv_frames_empty_for_unknown_system() -> None:
    assert PreparedBatchCollector().pv_frames(999) == []


def test_weather_frames_returns_a_copy() -> None:
    collector = PreparedBatchCollector()
    collector.add_weather(_frame(1))

    collector.weather_frames().clear()

    assert len(collector.weather_frames()) == 1


def test_concurrent_add_weather_keeps_every_frame() -> None:
    collector = PreparedBatchCollector()

    def add(index: int) -> None:
        collector.add_weather(_frame(index))

    threads = [threading.Thread(target=add, args=(i,)) for i in range(200)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(collector.weather_frames()) == 200


def test_concurrent_add_pv_keeps_every_frame() -> None:
    collector = PreparedBatchCollector()

    def add(index: int) -> None:
        collector.add_pv(index % 5, _frame(index))

    threads = [threading.Thread(target=add, args=(i,)) for i in range(200)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    total = sum(len(collector.pv_frames(system_id)) for system_id in range(5))
    assert total == 200
