"""Failing-first tests for the only-oversized-row shutdown failure (PR #9,
reported 2026-07-15, reproduced live in the singer worker pod).

Scenario: a batch whose only row takes the Load Job fallback leaves an EMPTY
ProtoRows job. The sink enqueued it anyway, the worker opened an AppendRows
stream for it that never sent, and closing that virgin stream raises
StreamClosedError on google-cloud-bigquery-storage >= 2.3x — which flowed into
error_notifier and aborted EVERY sink's MERGE (healthy streams included).

Three layers, one test each — all must fail on the pre-fix code:
  1. the sink must not enqueue an empty batch at all
  2. the worker must not open a stream for an empty job (defense in depth)
  3. close_cached_streams must treat StreamClosedError as cosmetic
"""
from types import SimpleNamespace

from google.cloud.bigquery_storage_v1 import exceptions as bqstorage_exceptions
from google.cloud.bigquery_storage_v1 import types

import target_bigquery.storage_write as sw
from target_bigquery.storage_write import (
    BigQueryStorageWriteSink,
    Job,
    StorageWriteBatchWorker,
)
from test_review_findings import FakePipe, FakeQueue, FakeStream, fake_client, make_worker

PARENT = "projects/p/datasets/d/tables/t"
DEFAULT_PATH = PARENT + "/streams/_default"


# --------------------------------------------------------------------------- #
# 1. sink: an empty batch (all rows took the fallback) must not become a Job
# --------------------------------------------------------------------------- #

class RecordingQueue:
    def __init__(self):
        self.items = []

    def qsize(self):
        return 0

    def put(self, item):
        self.items.append(item)


def test_sink_does_not_enqueue_empty_batch():
    sink = BigQueryStorageWriteSink.__new__(BigQueryStorageWriteSink)
    sink.proto_rows = types.ProtoRows()          # empty: only-oversized batch
    sink.global_queue = RecordingQueue()
    sink.MAX_JOBS_QUEUED = 1
    sink.parent = PARENT
    sink.template = SimpleNamespace()
    sink.stream_notifier = FakePipe()
    calls = {"enqueued": 0, "flushed": 0}
    sink.increment_jobs_enqueued = lambda: calls.__setitem__("enqueued", calls["enqueued"] + 1)
    sink._flush_fallback_buffer = lambda: calls.__setitem__("flushed", calls["flushed"] + 1)

    sink.process_batch({})

    assert sink.global_queue.items == [], "empty batch must not be enqueued"
    assert calls["enqueued"] == 0, "job counter must not increment for an empty batch"
    assert calls["flushed"] == 1, "fallback rows must still be uploaded"


# --------------------------------------------------------------------------- #
# 2. worker: an empty job must not open a stream (defense in depth)
# --------------------------------------------------------------------------- #

def test_worker_does_not_open_stream_for_empty_job(monkeypatch):
    opened = []
    monkeypatch.setattr(sw, "fresh_storage_client", lambda creds: "client")
    job = Job(parent=PARENT,
              template=SimpleNamespace(write_stream=DEFAULT_PATH),
              stream_notifier=FakePipe(),
              data=types.ProtoRows())            # zero rows
    worker = make_worker(FakeQueue([job]), {})   # empty cache: open path armed
    worker.get_stream_components = lambda client, j: (
        opened.append(j.parent) or (DEFAULT_PATH, FakeStream(), lambda r: None, client)
    )

    worker.run()

    assert opened == [], "no stream may be opened for a job with nothing to send"
    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]    # enqueue count still balanced


# --------------------------------------------------------------------------- #
# 3. cleanup: StreamClosedError on close is cosmetic, real errors still report
# --------------------------------------------------------------------------- #

class ClosedStream:
    _closed = False

    def close(self):
        raise bqstorage_exceptions.StreamClosedError(
            "Cannot close again when the connection is already closed."
        )


class BrokenStream:
    _closed = False

    def close(self):
        raise ValueError("genuinely broken")


def test_close_cached_streams_tolerates_already_closed():
    worker = StorageWriteBatchWorker.__new__(StorageWriteBatchWorker)
    worker.ext_id = "test-worker"
    worker.logger = sw.logger
    worker.error_notifier = FakePipe()
    worker.cache = {
        "a": ("path-a", ClosedStream(), None, fake_client()),   # cosmetic — must be swallowed
        "b": ("path-b", BrokenStream(), None, fake_client()),   # real — must be reported
    }

    worker.close_cached_streams()

    reported = [type(e).__name__ for e, _ in worker.error_notifier.sent]
    assert "StreamClosedError" not in reported, (
        "an already-closed stream is not a load failure and must not abort merges"
    )
    assert reported == ["ValueError"], "genuine close failures must still be reported"
