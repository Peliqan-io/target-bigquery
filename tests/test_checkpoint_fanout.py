"""Verify that checkpointed sinks are removed from _sinks_active so they
are not revisited on later drains (PQ-3547 fan-out fix)."""

from unittest.mock import MagicMock, patch, call

from target_bigquery.target import TargetBigQuery


def make_target():
    """Minimal TargetBigQuery with enough state for drain_all.

    Uses __new__ to skip __init__ (which needs CLI args / config), then
    sets only the attributes drain_all actually touches."""
    t = TargetBigQuery.__new__(TargetBigQuery)
    t._latest_state = {"bookmarks": {}}
    t._sinks_active = {}
    t._records_since_checkpoint = 0
    t._state_pending = False
    t._delete_buffer = {}
    t.max_parallelism = 1
    t._config = {"fail_fast": True}
    return t


def make_sink(stream_name, checkpoint_eligible=True):
    sink = MagicMock()
    sink.stream_name = stream_name
    if checkpoint_eligible:
        sink.merge_target = MagicMock()
        sink.overwrite_target = None
    else:
        sink.merge_target = None
        sink.overwrite_target = None
    return sink


def _drain_patches(t):
    """Context-manager stack that mocks out everything drain_all calls
    except the checkpoint loop itself."""
    from contextlib import ExitStack
    stack = ExitStack()
    stack.enter_context(patch.object(t, '_method_supports_checkpoint', return_value=True))
    stack.enter_context(patch.object(t, '_drain_all'))
    stack.enter_context(patch.object(t, '_shutdown_workers'))
    stack.enter_context(patch.object(t, '_raise_pending_worker_error'))
    stack.enter_context(patch.object(t, '_write_state_message'))
    stack.enter_context(patch.object(t, '_reset_max_record_age'))
    return stack


class TestSequentialFanout:
    """Tap-peliqan model: streams arrive one at a time, never revisited."""

    def test_checkpointed_sinks_removed_after_drain(self):
        """After draining 3 sequential streams, each checkpoint'd sink
        must be removed from _sinks_active."""
        t = make_target()

        with _drain_patches(t):
            for name in ["stream_a", "stream_b", "stream_c"]:
                sink = make_sink(name)
                t._sinks_active[name] = sink
                t._latest_state = {"bookmarks": {name: {"key": 1}}}
                t.drain_all(is_endofpipe=False)

                sink.checkpoint.assert_called_once()
                assert name not in t._sinks_active

            assert t._sinks_active == {}

    def test_no_fanout_on_50_streams(self):
        """With 50 sequential streams, total checkpoint calls must be 50,
        not 50*51/2 = 1275."""
        t = make_target()
        total_checkpoints = 0

        with _drain_patches(t):
            for i in range(50):
                name = f"stream_{i}"
                sink = make_sink(name)
                t._sinks_active[name] = sink
                t._latest_state = {"bookmarks": {name: {"key": i}}}
                t.drain_all(is_endofpipe=False)
                total_checkpoints += sink.checkpoint.call_count

        assert total_checkpoints == 50


class TestInterleavingStream:
    """Non-sequential tap: a stream reappears after being checkpoint'd."""

    def test_returning_stream_creates_new_sink(self):
        """Stream A checkpoint'd and removed. Stream A records arrive
        again -> new sink is created via get_sink -> works correctly."""
        t = make_target()

        with _drain_patches(t):
            sink_a1 = make_sink("stream_a")
            t._sinks_active["stream_a"] = sink_a1
            t._latest_state = {"bookmarks": {"stream_a": {"key": 1}}}
            t.drain_all(is_endofpipe=False)
            assert "stream_a" not in t._sinks_active
            sink_a1.checkpoint.assert_called_once()

            sink_b = make_sink("stream_b")
            t._sinks_active["stream_b"] = sink_b
            t._latest_state = {"bookmarks": {"stream_b": {"key": 1}}}
            t.drain_all(is_endofpipe=False)
            assert "stream_b" not in t._sinks_active

            # Stream A comes back — simulate what get_sink/add_sink does
            sink_a2 = make_sink("stream_a")
            t._sinks_active["stream_a"] = sink_a2
            t._latest_state = {"bookmarks": {"stream_a": {"key": 2}}}
            t.drain_all(is_endofpipe=False)

            sink_a1.checkpoint.assert_called_once()
            sink_a2.checkpoint.assert_called_once()
            assert sink_a1 is not sink_a2
