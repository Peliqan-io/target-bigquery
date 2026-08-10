"""ACTIVATE_VERSION full-table replacement (PQ-3877).

Exercises the versioned-stream detection, staging isolation, atomic swap,
empty-response truncate, and multi-stream coexistence — all with mocked
BigQuery I/O, following the same pattern as test_lazy_staging_generations.
"""
import io
import json
from unittest.mock import MagicMock, call

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
    "dedupe_before_upsert": True,
    "batch_size": 100,
}


def schema_line(stream):
    return json.dumps({
        "type": "SCHEMA",
        "stream": stream,
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "value": {"type": ["string", "null"]},
            },
        },
        "key_properties": ["id"],
    })


def record_line(stream, rec, version=None):
    msg = {"type": "RECORD", "stream": stream, "record": rec}
    if version is not None:
        msg["version"] = version
    return json.dumps(msg)


def state_line(stream):
    return json.dumps({"type": "STATE", "value": {"bookmarks": {stream: {}}}})


def activate_line(stream, version):
    return json.dumps({"type": "ACTIVATE_VERSION", "stream": stream, "version": version})


class Recorder:
    def __init__(self):
        self.created = []
        self.merged = []
        self.replaced = []  # (source_name, dest_name)
        self.deleted = []
        self.truncated = []

    def is_staging(self, name):
        return "__" in name


@pytest.fixture
def recorder(monkeypatch):
    rec = Recorder()
    mock_client = MagicMock()

    monkeypatch.setattr(core, "bigquery_client_factory", lambda creds: mock_client)
    monkeypatch.setattr(core.time, "sleep", lambda s: None)

    def fake_create_table(self, client, apply_transforms=False, **kwargs):
        rec.created.append(self.name)

    monkeypatch.setattr(BigQueryTable, "create_table", fake_create_table)

    def fake_merge(self):
        rec.merged.append((self.stream_name, self.table.name))

    monkeypatch.setattr(BaseBigQuerySink, "_merge_staging_into_target", fake_merge)

    def fake_replace(self, source, dest):
        rec.replaced.append((source.name, dest.name))

    monkeypatch.setattr(BaseBigQuerySink, "_replace_table_from", fake_replace)

    def fake_delete(ref, not_found_ok=False):
        rec.deleted.append(str(ref))

    mock_client.delete_table = fake_delete

    def fake_query(sql):
        result = MagicMock()
        if "TRUNCATE" in sql:
            rec.truncated.append(sql)
        return result

    mock_client.query = fake_query

    monkeypatch.setattr(
        "target_bigquery.batch_job.BigQueryBatchJobDenormalizedSink.update_schema",
        lambda self: None,
    )
    monkeypatch.setattr(TargetBigQuery, "resize_worker_pool", lambda self: None)
    return rec


def run_target(lines):
    target = TargetBigQuery(config=CONFIG)
    target._process_lines(io.StringIO("\n".join(lines) + "\n"))
    target.drain_all(is_endofpipe=True)
    return target


# --------------------------------------------------------------------------- #
# Detection: versioned records route into staging
# --------------------------------------------------------------------------- #
def test_versioned_stream_creates_staging(recorder):
    """A versioned stream must create a staging table and redirect writes there."""
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1, "value": "a"}, version=100),
        state_line("deals"),
        activate_line("deals", 100),
    ]
    target = run_target(lines)
    sink = target._sinks_active["deals"]

    staging_tables = [n for n in recorder.created if recorder.is_staging(n)]
    assert len(staging_tables) == 1, f"expected 1 staging table, got {recorder.created}"
    assert sink.activate_version_target is None, "should be cleared after clean_up"


def test_unversioned_stream_no_staging(recorder):
    """An unversioned stream must NOT create staging or trigger the swap path."""
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1, "value": "a"}),
        state_line("deals"),
    ]
    run_target(lines)

    assert recorder.replaced == [], "unversioned stream should not replace"
    assert recorder.truncated == [], "unversioned stream should not truncate"


# --------------------------------------------------------------------------- #
# Swap: ACTIVATE_VERSION replaces the table
# --------------------------------------------------------------------------- #
def test_activate_version_swaps_staging_onto_live(recorder):
    """With ACTIVATE_VERSION, clean_up must replace live from staging."""
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1, "value": "a"}, version=100),
        state_line("deals"),
        activate_line("deals", 100),
    ]
    run_target(lines)

    assert len(recorder.replaced) == 1
    source, dest = recorder.replaced[0]
    assert recorder.is_staging(source), f"source should be staging, got {source}"
    assert dest == "deals", f"dest should be live table, got {dest}"
    assert any(recorder.is_staging(d) for d in recorder.deleted), "staging should be dropped"


def test_no_activate_version_discards_staging(recorder):
    """Without ACTIVATE_VERSION, staging should be dropped and live left intact."""
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1, "value": "a"}, version=100),
        state_line("deals"),
        # no activate_line
    ]
    run_target(lines)

    assert recorder.replaced == [], "should NOT replace without ACTIVATE_VERSION"
    assert any(recorder.is_staging(d) for d in recorder.deleted), "staging should still be dropped"


# --------------------------------------------------------------------------- #
# Empty response: ACTIVATE_VERSION with zero records truncates
# --------------------------------------------------------------------------- #
def test_empty_response_with_activate_version_truncates(recorder):
    """ACTIVATE_VERSION with zero records must TRUNCATE the live table."""
    lines = [
        schema_line("deals"),
        # no records
        state_line("deals"),
        activate_line("deals", 200),
    ]
    run_target(lines)

    assert len(recorder.truncated) == 1
    assert "deals" in recorder.truncated[0]
    assert recorder.replaced == [], "no staging to swap"


def test_empty_response_without_activate_version_keeps_table(recorder):
    """No records AND no ACTIVATE_VERSION must leave the table untouched."""
    lines = [
        schema_line("deals"),
        state_line("deals"),
    ]
    run_target(lines)

    assert recorder.truncated == [], "should NOT truncate"
    assert recorder.replaced == [], "should NOT replace"


# --------------------------------------------------------------------------- #
# Multi-stream: versioned + unversioned coexist
# --------------------------------------------------------------------------- #
def test_versioned_and_unversioned_streams_coexist(recorder):
    """A versioned stream must not disturb an unversioned sibling."""
    lines = [
        schema_line("full_table_stream"),
        record_line("full_table_stream", {"id": 1, "value": "a"}, version=100),
        state_line("full_table_stream"),
        activate_line("full_table_stream", 100),
        schema_line("incremental_stream"),
        record_line("incremental_stream", {"id": 1, "value": "x"}),
        state_line("incremental_stream"),
    ]
    run_target(lines)

    assert len(recorder.replaced) == 1
    _, dest = recorder.replaced[0]
    assert dest == "full_table_stream"

    assert any(
        stream == "incremental_stream" for stream, _ in recorder.merged
    ), "incremental stream should still MERGE normally"


# --------------------------------------------------------------------------- #
# Overwrite path (B1-B3 fix): uses _replace_table_from
# --------------------------------------------------------------------------- #
def test_overwrite_path_uses_replace(recorder):
    """The overwrite path must use _replace_table_from, not DROP+CTAS."""
    overwrite_config = {**CONFIG, "upsert": False, "overwrite": True}
    target = TargetBigQuery(config=overwrite_config)
    target._process_lines(io.StringIO("\n".join([
        schema_line("overwrite_stream"),
        record_line("overwrite_stream", {"id": 1, "value": "a"}),
        state_line("overwrite_stream"),
    ]) + "\n"))
    target.drain_all(is_endofpipe=True)

    assert len(recorder.replaced) == 1
    source, dest = recorder.replaced[0]
    assert recorder.is_staging(source)
    assert dest == "overwrite_stream"

    sink = target._sinks_active["overwrite_stream"]
    assert sink.table is not None, "B3: self.table must not be None after overwrite"
    assert sink.table.name == "overwrite_stream"
