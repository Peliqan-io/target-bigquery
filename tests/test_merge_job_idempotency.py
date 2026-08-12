"""The checkpoint MERGE must be safe to re-run as a whole job (PQ-3547 follow-up).

`client.query()` defaults to `job_retry=DEFAULT_JOB_RETRY`. When a query job
fails with a retriable reason (rateLimitExceeded / backendError / internalError
/ jobRateLimitExceeded) `QueryJob.result()` does not resume the old job -- it
submits the SAME SQL as a brand new job (`job = retry_do_query()` in
google/cloud/bigquery/job/query.py).

So every statement we bundle into the checkpoint script may execute twice. A
trailing `DROP TABLE IF EXISTS <staging>` makes the script non-idempotent: if
attempt #1 reaches the DROP and is then reported as failed for a retriable
reason, attempt #2 re-runs the script and dies with

    Not found: Table <staging> was not found in location EU

which is fatal to the whole sync (drain_all is deliberately fail-fast, so no
bookmark is emitted). Observed in production runs 7000892, 7000900 and 16769.

The staging drop therefore has to happen outside the retried unit. What is left
in the job -- an optional `CREATE OR REPLACE TEMP TABLE` plus a MERGE keyed on
key_properties -- is naturally idempotent.
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


@pytest.fixture
def client(monkeypatch):
    """A mock BigQuery client, with the real _merge_staging_into_target intact."""
    mock = MagicMock()
    monkeypatch.setattr(core, "bigquery_client_factory", lambda creds: mock)
    monkeypatch.setattr(core.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        BigQueryTable, "create_table", lambda self, client, apply_transforms=False, **kw: None
    )
    monkeypatch.setattr(
        "target_bigquery.batch_job.BigQueryBatchJobDenormalizedSink.update_schema",
        lambda self: None,
    )
    monkeypatch.setattr(TargetBigQuery, "resize_worker_pool", lambda self: None)
    return mock


def checkpoint_once(client):
    """Drive one real checkpoint and return (sql_statements, sink)."""
    lines = [
        schema_line("deals"),
        record_line("deals", {"id": 1, "name": "a"}),
        state_line({"bookmarks": {"deals": {"v": 1}}}),
    ]
    target = TargetBigQuery(config=CONFIG)
    target._process_lines(io.StringIO("\n".join(lines) + "\n"))
    sqls = [c.args[0] for c in client.query.call_args_list if c.args]
    return sqls, target._sinks_active["deals"]


def test_merge_job_carries_no_drop_statement(client):
    """The retried unit must not contain the DROP -- that is the bug."""
    sqls, _ = checkpoint_once(client)

    assert sqls, "the checkpoint never issued a query"
    merge_sql = next(s for s in sqls if "MERGE" in s.upper())
    assert "DROP TABLE" not in merge_sql.upper(), (
        "DROP TABLE is bundled with the MERGE; a job_retry re-run of this "
        "script fails with 'Table ... was not found'"
    )


def test_staging_table_dropped_out_of_band(client):
    """The generation is still reclaimed -- via an idempotent delete_table."""
    _, sink = checkpoint_once(client)

    assert client.delete_table.called, "staging generation was never dropped"
    assert client.delete_table.call_args.kwargs.get("not_found_ok") is True, (
        "delete_table must tolerate an already-absent table"
    )
    assert sink._staging_open is False


def test_merge_runs_before_the_drop(client):
    """Order matters: dropping first would discard the staged batch."""
    calls = []
    client.query.side_effect = lambda sql, *a, **k: calls.append("query") or MagicMock()
    client.delete_table.side_effect = lambda *a, **k: calls.append("delete")

    checkpoint_once(client)

    assert calls.index("query") < calls.index("delete")


def test_drop_failure_does_not_fail_the_checkpoint(client):
    """A leaked staging table is harmless (it has a 1-day expiry) and must not
    turn a completed MERGE into a failed sync -- the data is already merged and
    the bookmark is safe to advance."""
    from google.api_core.exceptions import BadRequest

    client.delete_table.side_effect = BadRequest("transient")

    _, sink = checkpoint_once(client)

    assert sink._staging_open is False
