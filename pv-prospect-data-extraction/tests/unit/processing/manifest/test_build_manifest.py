"""Tests for build_manifest."""

from datetime import date, timedelta

from pv_prospect.data_extraction.processing.manifest import (
    BACKFILL_WINDOW_DAYS,
    BackfillCursor,
    Batch,
    build_manifest,
    initial_cursor,
)

_TODAY = date(2026, 4, 3)
_NUM_SAMPLE_FILES = 32
_EPOCH = date(1970, 1, 1)


def _epoch_days(d: date) -> int:
    return (d - _EPOCH).days


def test_step2_uses_todays_modulus() -> None:
    cursor = initial_cursor(_TODAY)
    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)

    expected_index = _epoch_days(_TODAY) % _NUM_SAMPLE_FILES
    assert manifest.step2_batch.sample_file_index == expected_index


def test_step2_covers_trailing_14_days() -> None:
    cursor = initial_cursor(_TODAY)
    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert manifest.step2_batch.end_date == _TODAY
    assert manifest.step2_batch.start_date == _TODAY - timedelta(
        days=BACKFILL_WINDOW_DAYS
    )


def test_step3_produces_requested_batch_count() -> None:
    cursor = initial_cursor(_TODAY)
    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor, step3_batch_count=5)

    assert len(manifest.step3_batches) == 5


def test_step3_default_batch_count_is_8() -> None:
    cursor = initial_cursor(_TODAY)
    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert len(manifest.step3_batches) == 8


def test_step3_batches_march_backwards_in_14_day_windows() -> None:
    cursor = initial_cursor(_TODAY)
    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)

    for i, batch in enumerate(manifest.step3_batches):
        assert batch.end_date - batch.start_date == timedelta(days=BACKFILL_WINDOW_DAYS)
        if i > 0:
            assert batch.end_date == manifest.step3_batches[i - 1].start_date


def test_step3_first_batch_starts_where_cursor_points() -> None:
    cursor = initial_cursor(_TODAY)
    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert manifest.step3_batches[0].end_date == cursor.next_end_date


def test_step3_each_batch_uses_different_sample_file() -> None:
    cursor = initial_cursor(_TODAY)
    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)

    indices = [b.sample_file_index for b in manifest.step3_batches]
    assert len(set(indices)) == len(indices)


def test_step3_sample_indices_differ_from_step2() -> None:
    cursor = initial_cursor(_TODAY)
    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)

    step2_index = manifest.step2_batch.sample_file_index
    step3_indices = {b.sample_file_index for b in manifest.step3_batches}
    assert step2_index not in step3_indices


def test_updated_cursor_continues_from_last_batch() -> None:
    cursor = initial_cursor(_TODAY)
    _, updated_cursor = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)

    manifest, _ = build_manifest(_TODAY, _NUM_SAMPLE_FILES, cursor)
    last_batch = manifest.step3_batches[-1]
    assert updated_cursor.next_end_date == last_batch.start_date


def test_consecutive_days_produce_contiguous_step3_batches() -> None:
    day1 = _TODAY
    cursor = initial_cursor(day1)
    _, cursor = build_manifest(day1, _NUM_SAMPLE_FILES, cursor)

    day2 = day1 + timedelta(days=1)
    manifest2, _ = build_manifest(day2, _NUM_SAMPLE_FILES, cursor)

    # Day 2's first Step 3 batch should start right where day 1 left off
    manifest1, _ = build_manifest(day1, _NUM_SAMPLE_FILES, initial_cursor(day1))
    last_day1 = manifest1.step3_batches[-1]
    first_day2 = manifest2.step3_batches[0]
    assert first_day2.end_date == last_day1.start_date


def test_sample_file_indices_wrap_around() -> None:
    num_files = 4
    cursor = BackfillCursor(
        next_end_date=_TODAY - timedelta(days=BACKFILL_WINDOW_DAYS),
        next_sample_offset=1,
    )
    manifest, _ = build_manifest(_TODAY, num_files, cursor, step3_batch_count=6)

    indices = [b.sample_file_index for b in manifest.step3_batches]
    # With 4 files and 6 batches, indices must wrap around
    assert all(0 <= i < num_files for i in indices)


def test_initial_cursor_starts_after_step2_window() -> None:
    cursor = initial_cursor(_TODAY)

    assert cursor.next_end_date == _TODAY - timedelta(days=BACKFILL_WINDOW_DAYS)
    assert cursor.next_sample_offset == 1


def test_batch_sample_file_name() -> None:
    batch = Batch(sample_file_index=7, start_date=_TODAY, end_date=_TODAY)
    assert batch.sample_file_name == 'sample_007.csv'
