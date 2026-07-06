"""Unit tests for the AppendRows 20 MB request splitter (chunk_proto_rows).

The Storage Write API caps each AppendRows request at 20 MB; the worker splits
every queued batch into chunks under MAX_REQUEST_BYTES (19.5 decimal MB) before
dispatch. These tests exercise the packer with a tiny artificial limit so they
stay fast — the logic is size-relative.

Run: poetry run pytest tests/test_chunk_proto_rows.py -v
"""
import pytest
from google.cloud.bigquery_storage_v1 import types

from target_bigquery.storage_write import (
    MAX_REQUEST_BYTES,
    PER_ROW_OVERHEAD,
    RecordTooLargeError,
    chunk_proto_rows,
)

LIMIT = 1000  # artificial budget for tests


def make_rows(*sizes: int) -> types.ProtoRows:
    rows = types.ProtoRows()
    for i, n in enumerate(sizes):
        rows.serialized_rows.append(bytes([65 + i % 26]) * n)
    return rows


def sizes_of(chunk: types.ProtoRows):
    return [len(r) for r in chunk.serialized_rows]


def test_empty_batch_yields_nothing():
    assert list(chunk_proto_rows(make_rows(), LIMIT)) == []


def test_single_row_single_chunk():
    chunks = list(chunk_proto_rows(make_rows(100), LIMIT))
    assert len(chunks) == 1
    assert sizes_of(chunks[0]) == [100]


def test_all_fit_one_chunk_order_preserved():
    rows = make_rows(100, 200, 300)
    chunks = list(chunk_proto_rows(rows, LIMIT))
    assert len(chunks) == 1
    assert chunks[0].serialized_rows[:] == rows.serialized_rows[:]


def test_split_preserves_every_row_and_order():
    sizes = [300, 300, 300, 300, 300, 300, 300]  # 308 w/ overhead → 3 per chunk
    rows = make_rows(*sizes)
    chunks = list(chunk_proto_rows(rows, LIMIT))
    assert len(chunks) == 3
    assert [len(c.serialized_rows) for c in chunks] == [3, 3, 1]
    flattened = [r for c in chunks for r in c.serialized_rows]
    assert flattened == list(rows.serialized_rows)


def test_no_chunk_exceeds_limit():
    sizes = [450, 450, 450, 100, 900, 50, 50, 50, 800]
    chunks = list(chunk_proto_rows(make_rows(*sizes), LIMIT))
    for c in chunks:
        assert sum(len(r) + PER_ROW_OVERHEAD for r in c.serialized_rows) <= LIMIT


def test_exact_boundary_fits():
    # two rows that together land exactly on the limit stay in one chunk
    half = LIMIT // 2 - PER_ROW_OVERHEAD
    chunks = list(chunk_proto_rows(make_rows(half, half), LIMIT))
    assert len(chunks) == 1


def test_one_byte_over_boundary_splits():
    half = LIMIT // 2 - PER_ROW_OVERHEAD
    chunks = list(chunk_proto_rows(make_rows(half, half + 1), LIMIT))
    assert len(chunks) == 2


def test_oversized_single_row_raises():
    with pytest.raises(RecordTooLargeError):
        list(chunk_proto_rows(make_rows(LIMIT + 1), LIMIT))


def test_oversized_row_reports_size_in_message():
    with pytest.raises(RecordTooLargeError, match=str(LIMIT + 1 + PER_ROW_OVERHEAD)):
        list(chunk_proto_rows(make_rows(LIMIT + 1), LIMIT))


def test_default_limit_is_under_20mb_both_readings():
    assert MAX_REQUEST_BYTES <= 20_000_000       # decimal MB reading
    assert MAX_REQUEST_BYTES <= 20 * 1024 * 1024  # MiB reading
