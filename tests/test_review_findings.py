"""Failing-first tests for the 2026-07-14 review findings on storage_write.

Each test encodes the CORRECT behavior and therefore FAILS on the current code,
proving the reported bug is real. They must pass after the fixes land.

  1. P1 — a partially dispatched split batch is requeued whole, re-sending
     chunks whose original futures may still succeed (duplicate rows).
  2. P1 — client/stream creation happens outside the protected block, so a
     transient create failure kills the worker and silently drops the job.
  3. P2 — the oversized-row Load Job fallback resolves its schema WITHOUT
     column-name transforms, mismatching the transformed record keys.
"""
from queue import Empty
from types import SimpleNamespace

import pytest
from google.cloud.bigquery_storage_v1 import types

import target_bigquery.storage_write as sw
from target_bigquery.storage_write import (
    APPEND_RESULT_TIMEOUT,
    BigQueryStorageWriteDenormalizedSink,
    Job,
    StorageWriteBatchWorker,
)

PARENT = "projects/p/datasets/d/tables/t"
DEFAULT_PATH = PARENT + "/streams/_default"


class FakeFuture:
    def result(self, timeout=None):
        assert timeout == APPEND_RESULT_TIMEOUT
        return None


class FakePipe:
    def __init__(self):
        self.sent = []

    def send(self, msg):
        self.sent.append(msg)


class FakeStream:
    def __init__(self):
        self._closed = False

    def close(self):
        self._closed = True


class FakeQueue:
    """queue.Queue stand-in; put() front-inserts so a requeued job is retried
    before the run loop drains (mirrors a busy queue where retries interleave)."""

    def __init__(self, items):
        self.items = list(items)

    def get(self, timeout=None):
        if not self.items:
            raise Empty
        return self.items.pop(0)

    def put(self, item):
        self.items.insert(0, item)

    def task_done(self):
        pass


def make_rows(*payloads):
    rows = types.ProtoRows()
    for p in payloads:
        rows.serialized_rows.append(p)
    return rows


def make_worker(queue, cache):
    worker = StorageWriteBatchWorker.__new__(StorageWriteBatchWorker)
    worker.ext_id = "test-worker"
    worker.logger = sw.logger
    worker.queue = queue
    worker.cache = cache
    # run() sets offsets[parent] = 0 whenever it creates a cache entry; a
    # prepopulated cache must carry the same invariant
    worker.offsets = {parent: 0 for parent in cache}
    worker.awaiting = []
    worker.max_errors_before_recycle = 5
    worker.max_request_bytes = 70  # rows below are 60 B (+8 overhead) -> 1 row per chunk
    worker.credentials = None
    worker.error_notifier = FakePipe()
    worker.job_notifier = FakePipe()
    worker.log_notifier = FakePipe()
    return worker


# --------------------------------------------------------------------------- #
# Finding 1 — partial dispatch must not duplicate already-sent chunks
# --------------------------------------------------------------------------- #

class FlakyDispatch:
    """Succeeds for chunk A, raises on the SECOND call (chunk B, first try),
    then succeeds for everything after — a stream hiccup mid-batch."""

    def __init__(self):
        self.dispatched = []  # first byte of each successfully dispatched chunk
        self.calls = 0

    def __call__(self, request):
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("stream closed mid-batch")
        key = bytes(request.proto_rows.rows.serialized_rows[0])[:1]
        self.dispatched.append(key)
        return FakeFuture()


def test_partially_dispatched_job_does_not_resend_completed_chunks():
    dispatch = FlakyDispatch()
    job = Job(parent=PARENT,
              template=SimpleNamespace(write_stream=DEFAULT_PATH),
              stream_notifier=FakePipe(),
              data=make_rows(b"A" * 60, b"B" * 60, b"C" * 60))  # 3 chunks
    queue = FakeQueue([job])
    worker = make_worker(queue, {PARENT: (DEFAULT_PATH, FakeStream(), dispatch)})

    worker.run()

    # every chunk lands exactly once — chunk A must NOT be re-sent by the retry
    assert dispatch.dispatched.count(b"A") == 1, (
        f"chunk A dispatched {dispatch.dispatched.count(b'A')}x -> duplicate rows"
    )
    assert sorted(dispatch.dispatched) == [b"A", b"B", b"C"]
    assert worker.error_notifier.sent == []
    # exactly one completion ping for the one enqueued job
    assert worker.job_notifier.sent == [True]


# --------------------------------------------------------------------------- #
# Finding 2 — stream-creation failure must not kill the worker silently
# --------------------------------------------------------------------------- #

def test_stream_creation_failure_reports_instead_of_killing_worker(monkeypatch):
    monkeypatch.setattr(sw, "fresh_storage_client", lambda creds: "client")
    job = Job(parent=PARENT,
              template=SimpleNamespace(write_stream=DEFAULT_PATH),
              stream_notifier=FakePipe(),
              data=make_rows(b"A" * 60))
    queue = FakeQueue([job])
    worker = make_worker(queue, {})  # cache miss -> stream creation path

    def failing_components(client, job):
        raise RuntimeError("create_write_stream: 503 unavailable")

    worker.get_stream_components = failing_components

    worker.run()  # must NOT raise (currently the exception escapes = dead worker)

    # the job was retried and, once attempts were exhausted, reported loudly —
    # never silently dropped
    assert len(worker.error_notifier.sent) == 1
    assert "503" in str(worker.error_notifier.sent[0][0])


# --------------------------------------------------------------------------- #
# Finding 3 — fallback Load Job schema must use transformed column names
# --------------------------------------------------------------------------- #

class FakeTable:
    def get_resolved_schema(self, apply_transforms: bool = False):
        return ["transformed_schema"] if apply_transforms else ["raw_schema"]


def test_fallback_job_config_applies_column_transforms():
    sink = BigQueryStorageWriteDenormalizedSink.__new__(
        BigQueryStorageWriteDenormalizedSink
    )
    sink.table = FakeTable()
    # denormalized sinks apply transforms (core.py: strategy is not FIXED);
    # the buffered NDJSON rows carry transformed keys, so the load schema must too
    assert sink.apply_transforms is True
    cfg = sink.fallback_job_config
    assert cfg["schema"] == ["transformed_schema"], (
        "fallback Load Job uses the untransformed schema while its rows carry "
        "transformed keys -> columns silently dropped (ignore_unknown_values)"
    )
