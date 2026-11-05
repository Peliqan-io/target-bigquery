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
