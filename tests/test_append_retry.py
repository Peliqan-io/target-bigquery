"""Unit tests for StorageWriteBatchWorker.wait() — append deadline, stream
recycle, and bounded re-send of failed AppendRows requests.

Covers the two failure modes seen on real 1M-row wide-table runs:
  * a transient 400 row_errors response killing the sync (now re-sent), and
  * a wedged bidi stream never resolving its futures (now bounded by
    APPEND_RESULT_TIMEOUT, stream closed, request re-sent on a fresh channel).
"""
import concurrent.futures
from types import SimpleNamespace

import target_bigquery.storage_write as sw
from target_bigquery.storage_write import (
    APPEND_RESULT_TIMEOUT,
    MAX_APPEND_ATTEMPTS,
    StorageWriteBatchWorker,
)

PARENT = "projects/p/datasets/d/tables/t"
DEFAULT_PATH = PARENT + "/streams/_default"


class FakeFuture:
    def __init__(self, exc=None):
        self.exc = exc

    def result(self, timeout=None):
        assert timeout == APPEND_RESULT_TIMEOUT
        if self.exc:
            raise self.exc


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


class FakeDispatch:
    """Dispatcher returning queued futures, recording every re-sent request."""

    def __init__(self, futures):
        self.futures = list(futures)
        self.requests = []

    def __call__(self, request):
        self.requests.append(request)
        return self.futures.pop(0)


def make_job(stream_path=DEFAULT_PATH):
    # chunks_pending mirrors the real Job: run() sets it to the number of
    # dispatched chunks before wait() ever sees the job; these tests dispatch
    # one request per job.
    return SimpleNamespace(parent=PARENT,
                           template=SimpleNamespace(write_stream=stream_path),
                           chunks_pending=1)


def make_worker(first_future, job, dispatch, stream=None):
    worker = StorageWriteBatchWorker.__new__(StorageWriteBatchWorker)
    worker.ext_id = "test-worker"
    worker.logger = sw.logger
    worker.error_notifier = FakePipe()
    worker.job_notifier = FakePipe()
    worker.cache = {PARENT: ("stream-path", stream or FakeStream(), dispatch)}
    worker.awaiting = [(first_future, job, "request-payload", 1)]
    return worker


def test_success_notifies_job_once():
    dispatch = FakeDispatch([])
    worker = make_worker(FakeFuture(), make_job(), dispatch)
    worker.wait(drain=True)
    assert dispatch.requests == []
    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]


def test_transient_error_resent_on_healthy_stream():
    stream = FakeStream()
    dispatch = FakeDispatch([FakeFuture()])  # retry succeeds
    boom = ValueError("400 row_errors")
    worker = make_worker(FakeFuture(boom), make_job(), dispatch, stream)
    worker.wait(drain=True)
    assert dispatch.requests == ["request-payload"]  # same request re-sent
    assert stream._closed is False  # healthy stream is NOT recycled
    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]


def test_timeout_closes_stream_and_resends_on_fresh_one(monkeypatch):
    old_stream = FakeStream()
    old_dispatch = FakeDispatch([])
    new_dispatch = FakeDispatch([FakeFuture()])  # resend succeeds
    worker = make_worker(
        FakeFuture(concurrent.futures.TimeoutError("no response")),
        make_job(), old_dispatch, old_stream,
    )
    monkeypatch.setattr(sw, "fresh_storage_client", lambda creds: "client")
    worker.credentials = None
    worker.get_stream_components = (
        lambda client, job: ("new-path", FakeStream(), new_dispatch)
    )
    worker.wait(drain=True)
    assert old_stream._closed is True  # wedged stream declared dead
    assert old_dispatch.requests == []
    assert new_dispatch.requests == ["request-payload"]  # re-sent on fresh stream
    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]


def test_persistent_failure_reported_after_max_attempts():
    boom = ValueError("permanent 400")
    dispatch = FakeDispatch([FakeFuture(boom)] * (MAX_APPEND_ATTEMPTS - 1))
    worker = make_worker(FakeFuture(boom), make_job(), dispatch)
    worker.wait(drain=True)
    assert len(dispatch.requests) == MAX_APPEND_ATTEMPTS - 1  # initial + resends
    assert len(worker.error_notifier.sent) == 1
    assert worker.error_notifier.sent[0][0] is boom
    assert worker.job_notifier.sent == [True]


def test_application_stream_fails_fast_no_resend():
    boom = ValueError("append failed")
    dispatch = FakeDispatch([])
    job = make_job(stream_path=PARENT + "/streams/app-stream-1")
    worker = make_worker(FakeFuture(boom), job, dispatch)
    worker.wait(drain=True)
    assert dispatch.requests == []  # offsets/commit semantics: no blind resend
    assert len(worker.error_notifier.sent) == 1
    assert worker.error_notifier.sent[0][0] is boom
    assert worker.job_notifier.sent == [True]


def test_reopen_failure_reports_original_error(monkeypatch):
    boom = concurrent.futures.TimeoutError("no response")
    worker = make_worker(FakeFuture(boom), make_job(), FakeDispatch([]))
    monkeypatch.setattr(sw, "fresh_storage_client", lambda creds: "client")
    worker.credentials = None

    def broken_reopen(client, job):
        raise RuntimeError("channel down")

    worker.get_stream_components = broken_reopen
    worker.wait(drain=True)
    assert len(worker.error_notifier.sent) == 1
    assert worker.error_notifier.sent[0][0] is boom
    assert worker.job_notifier.sent == [True]
