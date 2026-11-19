"""The dispatcher each stream factory returns must give up after 5 tries.

`retry(fn, wait=..., stop=...)` -- the function passed positionally together with
the settings -- is taken by tenacity as the bare `@retry` decorator and the
keywords are dropped: retry on any exception, forever, with no wait. A rejected
stream open (e.g. proto STRING vs table INT64) then re-opened every ~0.5 s,
leaked a gRPC thread per attempt and never failed the sync (1,700 threads after
16 min on the local stack).
"""
from types import SimpleNamespace

import pytest

import target_bigquery.storage_write as sw

PARENT = "projects/p/datasets/d/tables/t"


class RunawayRetry(BaseException):
    """Not an Exception, so tenacity's default retry predicate lets it through:
    turns a regression into a failing test instead of a hung one."""


class RejectingStream:
    """Stands in for BoundedOpenAppendRowsStream: every send fails."""

    def __init__(self, client, template):
        self.calls = 0

    def send(self, request):
        self.calls += 1
        if self.calls > 20:
            raise RunawayRetry(f"send retried {self.calls} times")
        raise RuntimeError("There was a problem opening the stream")


class Pipe:
    def send(self, msg):
        pass


def job():
    return SimpleNamespace(parent=PARENT, template=SimpleNamespace(write_stream=None), stream_notifier=Pipe())


client = SimpleNamespace(create_write_stream=lambda parent, write_stream: SimpleNamespace(name=PARENT + "/streams/s1"))


@pytest.mark.parametrize("factory", [sw.get_default_stream, sw.get_application_stream])
def test_dispatch_gives_up_after_five_tries(monkeypatch, factory):
    monkeypatch.setattr(sw, "BoundedOpenAppendRowsStream", RejectingStream)
    _, stream, dispatch, _ = factory(client, job())
    sleeps = []
    dispatch.retry.sleep = sleeps.append

    with pytest.raises(RuntimeError, match="problem opening the stream"):
        dispatch("request")

    assert stream.calls == 5
    assert sleeps == [2, 2, 2, 2]


# --------------------------------------------------------------------------- #
# A job re-enqueued behind the shutdown sentinel must not be dropped silently.  #
# With the retry bounded, a failed send reaches run()'s generic handler, which  #
# re-enqueues the job (attempts <= 3). At a barrier the sentinel is already     #
# queued, so the job lands behind it; the worker used to exit on the sentinel   #
# and the target wrote STATE past rows that were never written.                #
# --------------------------------------------------------------------------- #
from queue import Queue

from google.cloud.bigquery_storage_v1 import types


class RecordingPipe:
    def __init__(self):
        self.sent = []

    def send(self, msg):
        self.sent.append(msg)


class StreamStub:
    _closed = False

    def close(self):
        self._closed = True


def run_worker(monkeypatch, dispatch, *items, stream=None):
    monkeypatch.setattr(sw, "fresh_storage_client", lambda credentials: SimpleNamespace(
        transport=SimpleNamespace(close=lambda: None)))
    worker = sw.StorageWriteBatchWorker.__new__(sw.StorageWriteBatchWorker)
    worker.ext_id, worker.logger, worker.credentials = "test-worker", sw.logger, None
    worker.error_notifier, worker.job_notifier, worker.log_notifier = RecordingPipe(), RecordingPipe(), RecordingPipe()
    worker.cache, worker.awaiting, worker.offsets = {}, [], {}
    worker.max_errors_before_recycle = 5
    worker._assert_target_table_exists = lambda parent: None
    stream = stream or StreamStub()
    worker.get_stream_components = lambda client, job: (PARENT + "/streams/_default", stream, dispatch, client)
    worker.queue = Queue()
    for item in items:
        worker.queue.put(item)
    worker.run()
    return worker


def make_job():
    return sw.Job(PARENT, types.AppendRowsRequest(), RecordingPipe(), types.ProtoRows(serialized_rows=[b"row"]))


def test_job_requeued_behind_shutdown_sentinel_is_reported(monkeypatch):
    def reject(request):
        raise RuntimeError("There was a problem opening the stream")

    worker = run_worker(monkeypatch, reject, make_job(), None)

    assert len(worker.error_notifier.sent) == 1
    exc, formatted = worker.error_notifier.sent[0]
    # the send's own error, not a generic "job left behind" message
    assert str(exc) == "There was a problem opening the stream"
    assert "There was a problem opening the stream" in formatted
    assert worker.queue.empty()


def test_clean_shutdown_reports_nothing_and_keeps_other_sentinels(monkeypatch):
    class Done:
        def result(self, timeout=None):
            return None

    worker = run_worker(monkeypatch, lambda request: Done(), make_job(), None, None)

    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]
    assert list(worker.queue.queue) == [None]  # the second worker's sentinel is left for it


def test_reports_the_servers_reason_not_the_clients_generic_open_error(monkeypatch):
    """The client raises "There was a problem opening the stream" and only hands the
    server's reason to the stream's _on_rpc_done; that reason is what gets reported."""
    from google.api_core.exceptions import InvalidArgument, Unknown

    server = InvalidArgument("The proto field mismatched with BigQuery field at Proto.n, "
                             "the proto field type string, BigQuery field type INTEGER")
    stream = StreamStub()

    def reject(request):
        stream.rpc_error = server
        raise Unknown("There was a problem opening the stream. Try turning on DEBUG level logs to see the error.")

    worker = run_worker(monkeypatch, reject, make_job(), None, stream=stream)

    exc, formatted = worker.error_notifier.sent[0]
    assert exc is server
    assert "BigQuery field type INTEGER" in formatted
