"""A failed write must be retried or reported -- never dropped while STATE moves on.

* The AppendRows send retry is bounded. `retry(fn, wait=..., stop=...)` is
  tenacity's bare `@retry`: it dropped the settings and retried forever with no
  wait, so a rejected stream open (e.g. proto STRING vs table INT64) never failed
  the sync and leaked a gRPC thread per attempt.
* A failed job is retried by the worker that ran it. Re-enqueued on the shared
  queue at a barrier, it landed behind the shutdown sentinel, the worker exited
  without retrying it and the target wrote STATE past rows that were never loaded.
* The three NamedTuple-Job workers (batch load, GCS stage, streaming insert) did
  `job.attempt += 1`, which raised AttributeError inside the error handler: the
  worker died, nothing was reported, and the run ended COMPLETED.
"""
from queue import Queue
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import BadRequest, InvalidArgument, Unknown
from google.cloud.bigquery_storage_v1 import types

import target_bigquery.batch_job as bj
import target_bigquery.storage_write as sw
from test_review_findings import DEFAULT_PATH, PARENT, FakePipe, FakeStream, fake_client, make_rows, make_worker

OPEN_ERROR = "There was a problem opening the stream"


class Done:
    def result(self, timeout=None):
        return None


def failing(times, exc, then=None):
    """A callable that raises `exc` on its first `times` calls, then returns `then()`."""
    calls = []

    def call(*args, **kwargs):
        calls.append(args)
        if len(calls) <= times:
            raise exc
        return then() if then else None

    call.calls = calls
    return call


def storage_worker(*items, dispatch=None, components=None):
    """A Storage Write worker over a real queue holding `items`, opening its stream
    through `components` (default: a stream that dispatches through `dispatch`)."""
    queue = Queue()
    for item in items:
        queue.put(item)
    worker = make_worker(queue, {})
    worker._assert_target_table_exists = lambda parent: None
    worker.get_stream_components = components or (
        lambda client, job: (DEFAULT_PATH, FakeStream(), dispatch, client))
    return worker


def storage_job():
    return sw.Job(PARENT, SimpleNamespace(write_stream=DEFAULT_PATH), FakePipe(), make_rows(b"A" * 60))


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(sw, "fresh_storage_client", lambda credentials: fake_client())


# --------------------------------------------------------------------------- #
# bounded send retry                                                           #
# --------------------------------------------------------------------------- #

class RunawayRetry(BaseException):
    """Not an Exception, so tenacity's default predicate lets it through:
    a regression to the unbounded retry fails the test instead of hanging it."""


class RejectingStream:
    def __init__(self, client, template):
        self.calls = 0

    def send(self, request):
        self.calls += 1
        if self.calls > 20:
            raise RunawayRetry(f"send retried {self.calls} times")
        raise RuntimeError(OPEN_ERROR)


@pytest.mark.parametrize("factory", [sw.get_default_stream, sw.get_application_stream])
def test_dispatch_gives_up_after_five_tries(monkeypatch, factory):
    monkeypatch.setattr(sw, "BoundedOpenAppendRowsStream", RejectingStream)
    client = SimpleNamespace(create_write_stream=lambda parent, write_stream: SimpleNamespace(name=PARENT + "/streams/s1"))
    job = SimpleNamespace(parent=PARENT, template=SimpleNamespace(write_stream=None), stream_notifier=FakePipe())
    _, stream, dispatch, _ = factory(client, job)
    sleeps = []
    monkeypatch.setattr(dispatch.retry, "sleep", sleeps.append)

    with pytest.raises(RuntimeError, match=OPEN_ERROR):
        dispatch("request")

    assert stream.calls == 5
    assert sleeps == [2, 2, 2, 2]


# --------------------------------------------------------------------------- #
# Storage Write: retried even when the shutdown sentinel is already queued     #
# --------------------------------------------------------------------------- #

def test_failed_send_is_retried_although_the_sentinel_is_queued():
    dispatch = failing(2, RuntimeError(OPEN_ERROR), then=Done)
    worker = storage_worker(storage_job(), None, None, dispatch=dispatch)

    worker.run()

    assert len(dispatch.calls) == 3
    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]
    assert list(worker.queue.queue) == [None]  # the other worker's sentinel is untouched


def test_failed_stream_setup_is_retried_although_the_sentinel_is_queued():
    setup = failing(2, RuntimeError("create_write_stream: 503"),
                    then=lambda: (DEFAULT_PATH, FakeStream(), lambda request: Done(), fake_client()))
    worker = storage_worker(storage_job(), None, components=lambda client, job: setup())

    worker.run()

    assert len(setup.calls) == 3
    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]


def test_persistent_failure_reports_the_servers_reason_not_the_clients_generic_error():
    """The client raises a generic Unknown and hands the server's reason only to the
    stream's _on_rpc_done; that reason is what gets reported, once, after 3 attempts."""
    server = InvalidArgument("The proto field mismatched with BigQuery field at Proto.n, "
                             "the proto field type string, BigQuery field type INTEGER")
    stream = FakeStream()

    def reject(request):
        stream.rpc_error = server
        raise Unknown(OPEN_ERROR + ". Try turning on DEBUG level logs to see the error.")

    worker = storage_worker(storage_job(), None,
                            components=lambda client, job: (DEFAULT_PATH, stream, reject, client))

    worker.run()

    assert [exc for exc, _ in worker.error_notifier.sent] == [server]
    assert "BigQuery field type INTEGER" in worker.error_notifier.sent[0][1]


def test_stream_recycle_keeps_the_worker_and_its_retry():
    """The 5th error recycles the streams in place; raising there used to kill the
    worker together with the retry it held."""
    dispatch = failing(1, RuntimeError(OPEN_ERROR), then=Done)
    worker = storage_worker(storage_job(), None, dispatch=dispatch)
    worker.max_errors_before_recycle = 1

    worker.run()

    assert worker.max_errors_before_recycle == 5
    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]


# --------------------------------------------------------------------------- #
# batch load (NamedTuple Job)                                                  #
# --------------------------------------------------------------------------- #

def batch_worker(monkeypatch, load):
    monkeypatch.setattr(bj, "bigquery_client_factory",
                        lambda credentials: SimpleNamespace(load_table_from_file=load))
    queue = Queue()
    queue.put(bj.Job(table="p.d.t", data=b"{}", config={}))
    queue.put(None)
    worker = bj.BatchJobWorker.__new__(bj.BatchJobWorker)
    worker.ext_id, worker.queue, worker.credentials = "test-worker", queue, None
    worker.error_notifier, worker.job_notifier, worker.log_notifier = FakePipe(), FakePipe(), FakePipe()
    return worker


def test_batch_load_failure_is_retried_although_the_sentinel_is_queued(monkeypatch):
    load = failing(2, BadRequest("transient"), then=lambda: SimpleNamespace(result=lambda: None))
    worker = batch_worker(monkeypatch, load)

    worker.run()

    assert len(load.calls) == 3
    assert worker.error_notifier.sent == []
    assert worker.job_notifier.sent == [True]


def test_batch_load_persistent_failure_reports_the_load_error(monkeypatch):
    error = BadRequest("Provided Schema does not match Table. Field n has changed type")
    worker = batch_worker(monkeypatch, failing(99, error))

    with pytest.raises(BadRequest):  # not AttributeError from `job.attempt += 1`
        worker.run()

    assert [exc for exc, _ in worker.error_notifier.sent] == [error]
