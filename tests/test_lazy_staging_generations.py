"""Lazy staging generation regression tests (PQ-3547).

All tests feed REAL Singer message lines through the singer-sdk dispatch
(_process_lines -> _process_schema/record/state_message -> drain_all), so
they exercise _assert_sink_exists and the actual checkpoint path. No test
touches _sinks_active by hand -- that was the flaw of the deleted
test_checkpoint_fanout.py harness, which validated a contract production
dispatch does not have.

The critical regression: tap-peliqan emits SCHEMA once per endpoint and
STATE per category. The reverted sink-pop fix crashed with
RecordsWithoutSchemaException on the first RECORD after a checkpoint,
because the popped sink could never be recreated (no second SCHEMA).

BigQuery externals are mocked at the narrowest possible boundary:
client factory, physical CREATE TABLE, the MERGE statement, and the
worker pool. Everything in between (SDK dispatch, sink bookkeeping,
generation open/close) is real.
"""
import io
import json
from unittest.mock import MagicMock

import pytest

import target_bigquery.core as core
from target_bigquery.core import BaseBigQuerySink, BigQueryTable
from target_bigquery.target import TargetBigQuery


CONFIG = {
    "project": "test-project",
    "dataset": "test_dataset",
    "method": "batch_job",
    "denormalized": True,
    "upsert": True,
    "checkpoint_row_threshold": 0,  # every category boundary checkpoints
}


def schema_line(stream):
    return json.dumps({
        "type": "SCHEMA",
        "stream": stream,
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": ["string", "null"]},
            },
        },
        "key_properties": ["id"],
    })


def record_line(stream, rec):
    return json.dumps({"type": "RECORD", "stream": stream, "record": rec})


def state_line(value):
    return json.dumps({"type": "STATE", "value": value})


class Recorder:
    """Collects staging CREATEs and MERGEs with the table names involved."""

    def __init__(self):
        self.created = []   # table names passed to BigQueryTable.create_table
        self.merged = []    # (stream_name, staging_table_name) per MERGE
        self.merge_error = None

    def is_staging(self, name):
        return "__" in name


@pytest.fixture
def recorder(monkeypatch):
    rec = Recorder()

    monkeypatch.setattr(core, "bigquery_client_factory", lambda creds: MagicMock())
    monkeypatch.setattr(core.time, "sleep", lambda s: None)

    def fake_create_table(self, client, apply_transforms=False, **kwargs):
        rec.created.append(self.name)

    monkeypatch.setattr(BigQueryTable, "create_table", fake_create_table)

    def fake_merge(self):
        if rec.merge_error is not None:
            raise rec.merge_error
        rec.merged.append((self.stream_name, self.table.name))

    monkeypatch.setattr(BaseBigQuerySink, "_merge_staging_into_target", fake_merge)
    # Denormalized.update_schema reads live table schemas from BigQuery.
    monkeypatch.setattr(
        "target_bigquery.batch_job.BigQueryBatchJobDenormalizedSink.update_schema",
        lambda self: None,
    )
    # No real worker threads; jobs stay on the in-process queue.
    monkeypatch.setattr(TargetBigQuery, "resize_worker_pool", lambda self: None)
    return rec


def run_target(lines, endofpipe=True):
    target = TargetBigQuery(config=CONFIG)
    target._process_lines(io.StringIO("\n".join(lines) + "\n"))
    if endofpipe:
        target.drain_all(is_endofpipe=True)
    return target


# --------------------------------------------------------------------------- #
# The crash regression: multiple categories, SCHEMA sent only once
# --------------------------------------------------------------------------- #
def test_multi_category_stream_survives_checkpoint(recorder):
    """SCHEMA once, then two category cycles (RECORDs + STATE each), then EOF.

    The reverted pop fix raised RecordsWithoutSchemaException on the first
    RECORD of category 2. This must sync cleanly: same sink throughout, one
    staging generation per category, nothing left open after EOF.
    """
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1, "name": "jan-a"}),
        record_line("deals", {"id": 2, "name": "jan-b"}),
        state_line({"bookmarks": {"deals": {"d": "2024-01"}}}),
        # category 2: NO second SCHEMA, records arrive for the same stream
        record_line("deals", {"id": 3, "name": "feb-a"}),
        state_line({"bookmarks": {"deals": {"d": "2024-02"}}}),
    ]
    target = TargetBigQuery(config=CONFIG)
    target._process_lines(io.StringIO("\n".join(lines) + "\n"))

    # the sink survived both checkpoints
    assert "deals" in target._sinks_active
    sink = target._sinks_active["deals"]

    target.drain_all(is_endofpipe=True)

    # one staging generation per category, each merged exactly once
    staging = [n for n in recorder.created if recorder.is_staging(n)]
    assert len(staging) == 2
    assert [t for _, t in recorder.merged] == staging
    # closed at EOF: no orphan generation, sink finalized
    assert sink._staging_open is False
    assert sink.merge_target is None


def test_same_sink_object_across_categories(recorder):
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1}),
        state_line({"v": 1}),
    ]
    target = TargetBigQuery(config=CONFIG)
    target._process_lines(io.StringIO("\n".join(lines) + "\n"))
    sink_before = target._sinks_active["deals"]

    target._process_lines(io.StringIO(
        record_line("deals", {"id": 2}) + "\n" + state_line({"v": 2}) + "\n"
    ))
    assert target._sinks_active["deals"] is sink_before


# --------------------------------------------------------------------------- #
# Fan-out: closed sinks cost nothing on later drains
# --------------------------------------------------------------------------- #
def test_sequential_streams_one_merge_per_generation(recorder):
    """3 sequential streams. Counted across ALL sinks (the deleted harness
    only counted the newest sink), each stream must MERGE exactly once --
    finished streams are never re-merged by later drains."""
    lines = []
    for stream in ("s_a", "s_b", "s_c"):
        lines.append(schema_line(stream))
        lines.append(record_line(stream, {"id": 1}))
        lines.append(state_line({"bookmarks": {stream: {"v": 1}}}))
    target = run_target(lines)

    per_stream = {}
    for stream, _ in recorder.merged:
        per_stream[stream] = per_stream.get(stream, 0) + 1
    assert per_stream == {"s_a": 1, "s_b": 1, "s_c": 1}
    # all sinks stayed registered to the end
    assert set(target._sinks_active) == {"s_a", "s_b", "s_c"}


def test_empty_category_no_table_no_merge(recorder):
    """A STATE with no records since the last checkpoint must not create a
    staging table nor run a MERGE."""
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1}),
        state_line({"v": 1}),
        state_line({"v": 2}),  # empty category boundary
    ]
    run_target(lines)

    staging = [n for n in recorder.created if recorder.is_staging(n)]
    assert len(staging) == 1
    assert len(recorder.merged) == 1


def test_eof_with_closed_generation_is_noop(recorder):
    """After the last category's checkpoint the generation is closed; EOF
    clean_up must not CREATE or MERGE anything (no orphan staging table)."""
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1}),
        state_line({"v": 1}),
    ]
    target = TargetBigQuery(config=CONFIG)
    target._process_lines(io.StringIO("\n".join(lines) + "\n"))

    creates_before = len(recorder.created)
    merges_before = len(recorder.merged)
    target.drain_all(is_endofpipe=True)

    assert len(recorder.created) == creates_before
    assert len(recorder.merged) == merges_before


def test_small_stream_merges_at_eof_only(recorder):
    """A stream that never hits a STATE boundary keeps its generation open
    and MERGEs exactly once, at end-of-pipe."""
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1}),
        record_line("deals", {"id": 2}),
    ]
    run_target(lines)

    staging = [n for n in recorder.created if recorder.is_staging(n)]
    assert len(staging) == 1
    assert len(recorder.merged) == 1


# --------------------------------------------------------------------------- #
# Failure paths
# --------------------------------------------------------------------------- #
def test_merge_failure_keeps_generation_open_and_emits_no_state(recorder, capsys):
    """A failed MERGE must propagate, leave the generation open (data still
    staged for a retry) and emit no STATE for that boundary."""
    recorder.merge_error = RuntimeError("merge boom")
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1}),
        state_line({"bookmarks": {"deals": {"v": 1}}}),
    ]
    target = TargetBigQuery(config=CONFIG)
    with pytest.raises(RuntimeError, match="merge boom"):
        target._process_lines(io.StringIO("\n".join(lines) + "\n"))

    sink = target._sinks_active["deals"]
    assert sink._staging_open is True
    out = capsys.readouterr().out
    assert '"bookmarks"' not in out


def test_state_emitted_after_successful_checkpoint(recorder, capsys):
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1}),
        state_line({"bookmarks": {"deals": {"v": 1}}}),
    ]
    run_target(lines)
    out = capsys.readouterr().out
    assert '"bookmarks"' in out
