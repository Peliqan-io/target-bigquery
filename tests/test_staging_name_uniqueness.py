"""Two concurrent syncs of one connection must not share a staging table.

Production root cause (2026-08-11, runs 7000866 / 7000876 / 7000892 / 7000900 /
7000906): the platform started two runs of the SAME connection in the same
second on two celery workers. Both target processes replay the same tap in
lockstep, so `table_name + int(time.time()) + _staging_seq` produced an
IDENTICAL staging table name. `BigQueryTable.create_table` uses
`exists_ok=True`, so the second process silently adopted the first one's table
instead of erroring; both appended rows to it; whichever MERGEd first dropped
it, and the other's MERGE died with

    Not found: Table ...__1786490673_2 was not found in location EU

Verified in S3: the surviving sibling run's log contains the exact staging
table name the failed run died on.
"""
import io
import json
from unittest.mock import MagicMock

import pytest

import target_bigquery.core as core
from target_bigquery.core import BigQueryTable
from target_bigquery.target import TargetBigQuery


CONFIG = {
    "project": "test-project",
    "dataset": "test_dataset",
    "method": "batch_job",
    "denormalized": True,
    "upsert": True,
    "checkpoint_row_threshold": 0,
}


def lines_for(stream="deals"):
    return [
        json.dumps({
            "type": "SCHEMA", "stream": stream,
            "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
            "key_properties": ["id"],
        }),
        json.dumps({"type": "RECORD", "stream": stream, "record": {"id": 1}}),
        json.dumps({"type": "STATE", "value": {"v": 1}}),
    ]


@pytest.fixture
def created(monkeypatch):
    """Records every staging table name, with time frozen to one second.

    Freezing time is the point: it reproduces two lockstep processes opening a
    generation inside the same epoch second.
    """
    names = []
    monkeypatch.setattr(core, "bigquery_client_factory", lambda creds: MagicMock())
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    monkeypatch.setattr(core.time, "time", lambda: 1786490673.0)

    def fake_create_table(self, client, apply_transforms=False, **kw):
        names.append(self.name)

    monkeypatch.setattr(BigQueryTable, "create_table", fake_create_table)
    monkeypatch.setattr(
        "target_bigquery.batch_job.BigQueryBatchJobDenormalizedSink.update_schema",
        lambda self: None,
    )
    monkeypatch.setattr(TargetBigQuery, "resize_worker_pool", lambda self: None)
    monkeypatch.setattr(
        core.BaseBigQuerySink, "_merge_staging_into_target", lambda self: None
    )
    return names


def test_two_concurrent_syncs_get_distinct_staging_tables(created):
    """Same connection, same stream, same epoch second, same generation index."""
    for _ in range(2):  # two independent "processes"
        target = TargetBigQuery(config=CONFIG)
        target._process_lines(io.StringIO("\n".join(lines_for()) + "\n"))

    staging = [n for n in created if "__" in n]
    assert len(staging) == 2, staging
    assert staging[0] != staging[1], (
        f"both syncs derived the same staging table {staging[0]!r}; one of them "
        "will DROP it and the other's MERGE will fail"
    )


def test_generations_within_one_sink_stay_distinct(created):
    """The same-second guard still holds for successive generations."""
    lines = lines_for() + [
        json.dumps({"type": "RECORD", "stream": "deals", "record": {"id": 2}}),
        json.dumps({"type": "STATE", "value": {"v": 2}}),
    ]
    target = TargetBigQuery(config=CONFIG)
    target._process_lines(io.StringIO("\n".join(lines) + "\n"))

    staging = [n for n in created if "__" in n]
    assert len(staging) == 2
    assert staging[0] != staging[1]


def test_staging_name_keeps_table_and_generation_readable(created):
    """The nonce must not obscure which table/generation a staging table is."""
    target = TargetBigQuery(config=CONFIG)
    target._process_lines(io.StringIO("\n".join(lines_for()) + "\n"))

    name = next(n for n in created if "__" in n)
    assert name.startswith("deals__1786490673_1_"), name
