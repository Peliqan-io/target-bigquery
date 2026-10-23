"""Staging tables carry the pipeline run id as a BigQuery label (PQ-3846).

The Peliqan backend sets PELIQAN_PIPELINE_RUN_ID on this process. Every pqtemp__
staging table is labelled with it so the backend can later drop exactly the temp
tables belonging to runs that have finished, instead of guessing from table names.

The label rides in the table options rather than the description, because the
description already carries the Singer schema dump (default_table_options).

These tests drive the REAL BigQueryTable.create_table against a mock client. That
is deliberate: as_table is @cache'd on its kwargs, so routing a dict of labels
through it raises "unhashable type: 'dict'" and takes down every staging table
creation. Stubbing create_table out would hide exactly that failure.
"""
import datetime
from unittest.mock import MagicMock

import pytest

from target_bigquery.core import (
    PIPELINE_RUN_ID_ENV_VAR,
    PQ_RUN_ID_LABEL,
    TEMP_TABLE_MARKER,
    BaseBigQuerySink,
    BigQueryTable,
    IngestionStrategy,
)

JSONSCHEMA = {"type": "object", "properties": {"id": {"type": "integer"}}}
# bigquery.Table normalises the expiry to UTC on assignment.
EXPIRES = datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)


class ConcreteSink(BaseBigQuerySink):
    """BaseBigQuerySink is abstract; _new_staging_table lives on it regardless."""

    def process_batch(self, context):
        raise NotImplementedError

    @staticmethod
    def worker_cls_factory(worker_executor_cls, config):
        raise NotImplementedError


def make_table(name):
    return BigQueryTable(
        name=name,
        dataset="test_dataset",
        project="test-project",
        jsonschema=JSONSCHEMA,
        ingestion_strategy=IngestionStrategy.FIXED,
    )


def make_client():
    """A client whose get_table misses, so create_table takes the creation path."""
    from google.api_core.exceptions import NotFound

    client = MagicMock()
    client.get_table.side_effect = NotFound("no such table")
    client.get_dataset.return_value.location = "EU"
    return client


def create(table, client, **table_options):
    table.create_table(
        client,
        False,
        **{
            "table": table_options,
            "dataset": {"location": "EU"},
        },
    )
    return client.create_table.call_args.args[0]


@pytest.fixture(autouse=True)
def no_consistency_sleep(monkeypatch):
    monkeypatch.setattr("target_bigquery.core.time.sleep", lambda seconds: None)


def test_labels_reach_the_created_table(monkeypatch):
    """The end-to-end guard: a dict of labels survives the cached as_table."""
    monkeypatch.setenv(PIPELINE_RUN_ID_ENV_VAR, "4242")

    created = create(
        make_table(f"{TEMP_TABLE_MARKER}orders__1"),
        make_client(),
        expires=EXPIRES,
        labels={PQ_RUN_ID_LABEL: "4242"},
    )

    assert created.labels[PQ_RUN_ID_LABEL] == "4242"
    # The rest of the options still applied through as_table.
    assert created.expires == EXPIRES


def test_create_table_without_labels_is_unchanged():
    """No labels passed: the table is built exactly as it was before."""
    created = create(
        make_table("orders"),
        make_client(),
        expires=EXPIRES,
    )

    assert not created.labels
    assert created.expires == EXPIRES


def staging_options(monkeypatch, run_id):
    """Run _new_staging_table and return the table options it built."""
    captured = {}

    def fake_create_table(self, client, apply_transforms=False, **kwargs):
        captured.update(kwargs["table"])
        captured["name"] = self.name

    monkeypatch.setattr(BigQueryTable, "create_table", fake_create_table)

    if run_id is None:
        monkeypatch.delenv(PIPELINE_RUN_ID_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(PIPELINE_RUN_ID_ENV_VAR, run_id)

    sink = ConcreteSink.__new__(ConcreteSink)
    sink._staging_seq = 0
    sink.stream_name = "orders"          # table_name derives from this
    sink.client = MagicMock()
    sink._config = {"project": "test-project", "dataset": "test_dataset"}
    sink._staging_opts = {
        "dataset": "test_dataset",
        "project": "test-project",
        "jsonschema": JSONSCHEMA,
        "ingestion_strategy": IngestionStrategy.FIXED,
    }
    sink._new_staging_table()

    return captured


def test_staging_table_is_stamped_with_the_run_id(monkeypatch):
    options = staging_options(monkeypatch, "777")

    assert options["name"].startswith(TEMP_TABLE_MARKER)
    assert options["labels"] == {PQ_RUN_ID_LABEL: "777"}
    # The 1-day expiry safety net is untouched.
    assert "expires" in options


def test_staging_table_is_unlabelled_without_the_env_var(monkeypatch):
    """The target is also run outside Peliqan; an absent id must not break it."""
    options = staging_options(monkeypatch, None)

    assert "labels" not in options
    assert "expires" in options


@pytest.mark.parametrize("run_id", ["", "not-a-number", "12 34", "RUN/1"])
def test_a_non_numeric_run_id_is_skipped(monkeypatch, run_id):
    """BigQuery label values must match [a-z0-9_-]{0,63}.

    A junk value is dropped rather than sent: a rejected label would fail table
    creation and take the whole load down over a cleanup hint.
    """
    options = staging_options(monkeypatch, run_id)

    assert "labels" not in options
