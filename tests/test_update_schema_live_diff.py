"""`Denormalized.update_schema` must diff against the LIVE BigQuery table.

Production symptom (PQ-3118): a tap starts sending a new field mid-sync, the
column is never added to the target, and the next MERGE dies with

    400 Query error: Name CostcenterCode not found inside target

Root cause: the diff base came from `BigQueryTable.as_table()`, which builds a
Table client-side out of the very jsonschema being compared -- so "current" and
"expected" were the same object graph, the diff was always empty, and no ALTER
was ever issued. The MERGE, built from that same expected schema, then named a
column BigQuery did not have.

These tests pin the contract that actually matters: the schema we diff against
is fetched from BigQuery, an ALTER fires only when the live table is missing a
column, and a live column absent from the tap's schema is never dropped.
"""
from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from target_bigquery.core import BigQueryTable, Denormalized, IngestionStrategy


def make_table(*names):
    """A BigQueryTable whose jsonschema declares exactly `names` (plus ID)."""
    props = {n: {"type": ["null", "string"]} for n in names}
    return BigQueryTable(
        name="orders",
        dataset="ds",
        project="proj",
        jsonschema={"type": "object", "properties": props},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )


class Sink:
    """Minimal stand-in exposing only what update_schema touches."""

    apply_transforms = True
    update_schema = Denormalized.update_schema

    def __init__(self, table, live_columns):
        self.table = table
        self.client = MagicMock()
        self.client.get_table.return_value = bigquery.Table(
            table.as_ref(),
            schema=[bigquery.SchemaField(c, "STRING") for c in live_columns],
        )


def test_adds_column_the_live_table_is_missing():
    sink = Sink(make_table("colA", "colB", "CostcenterCode"), ["colA", "colB"])

    sink.update_schema()

    sink.client.get_table.assert_called_once()
    sink.client.update_table.assert_called_once()
    table, fields = sink.client.update_table.call_args[0][:2]
    assert fields == ["schema"]
    assert [f.name for f in table.schema] == ["colA", "colB", "CostcenterCode"]


def test_no_alter_when_live_table_already_matches():
    sink = Sink(make_table("colA", "colB"), ["colA", "colB"])

    sink.update_schema()

    sink.client.update_table.assert_not_called()


def test_never_drops_a_live_column_the_tap_stopped_sending():
    """The field mask replaces the schema wholesale, so the live column set must
    survive. Diffing against a client-side schema would silently drop `Legacy`."""
    sink = Sink(make_table("colA", "New"), ["colA", "Legacy"])

    sink.update_schema()

    table = sink.client.update_table.call_args[0][0]
    assert [f.name for f in table.schema] == ["colA", "Legacy", "New"]


def test_diff_base_is_fetched_not_built_locally():
    """The regression guard: as_table() must not be the source of `current`.

    Its schema disagrees with the live table, so if update_schema ever reverts
    to it the assertion below fails even though the ALTER still 'looks' right.
    """
    table = make_table("colA", "CostcenterCode")
    sink = Sink(table, ["colA"])

    sink.update_schema()

    fetched_ref = sink.client.get_table.call_args[0][0]
    assert fetched_ref == table.as_ref()
    assert [f.name for f in table.as_table().schema] == ["colA", "CostcenterCode"]
