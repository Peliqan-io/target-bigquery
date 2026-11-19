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
from google.api_core.exceptions import NotFound
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


def test_case_only_difference_is_not_re_added():
    """BigQuery enforces column uniqueness case-insensitively but stores the case
    it was given, so a live `created` already IS the tap's `Created`. Appending it
    makes update_table fail with

        400 PATCH ...: Field Created already exists in schema

    which kills the whole run. Reads and MERGEs are unaffected either way, since
    BigQuery SQL identifiers are case-insensitive.
    """
    sink = Sink(make_table("Created", "Currency"), ["created", "currency"])

    sink.update_schema()

    sink.client.update_table.assert_not_called()


def test_genuinely_new_column_still_added_alongside_case_only_matches():
    """Case-insensitive matching must not swallow real schema drift."""
    sink = Sink(make_table("Created", "NewCol"), ["created"])

    sink.update_schema()

    table = sink.client.update_table.call_args[0][0]
    assert [f.name for f in table.schema] == ["created", "NewCol"]


# --------------------------------------------------------------------------- #
# Nested RECORD drift                                                          #
#                                                                              #
# Exact Online added ProjectRebillItem to PurchaseEntryLines (an array of      #
# objects). The top-level-only diff saw "PurchaseEntryLines" by name and       #
# skipped it, so staging got a 53-field STRUCT, the live table kept 52, and   #
# the MERGE died with 400 assigning one to the other.                         #
# --------------------------------------------------------------------------- #
from target_bigquery.core import BaseBigQuerySink

STR = {"type": ["null", "string"]}


def lines_prop(*subfields):
    """An array-of-objects property, i.e. a REPEATED RECORD column."""
    return {
        "type": ["null", "array"],
        "items": {"type": ["null", "object"], "properties": {n: STR for n in subfields}},
    }


def nested_table(*subfields):
    return BigQueryTable(
        name="purchaseentry",
        dataset="ds",
        project="proj",
        jsonschema={"type": "object", "properties": {"ID": STR, "Lines": lines_prop(*subfields)}},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )


def lines_field(*subfields, mode="REPEATED"):
    return bigquery.SchemaField(
        "Lines", "RECORD", mode, fields=[bigquery.SchemaField(n, "STRING") for n in subfields]
    )


class NestedSink(Sink):
    def __init__(self, table, live_schema):
        self.table = table
        self.client = MagicMock()
        self.client.get_table.return_value = bigquery.Table(table.as_ref(), schema=live_schema)


def live_lines(sink):
    table = sink.client.update_table.call_args[0][0]
    return {f.name: f for f in table.schema}["Lines"]


def test_adds_subfield_missing_from_live_record():
    sink = NestedSink(
        nested_table("Amount", "Item", "ProjectRebillItem"),
        [bigquery.SchemaField("ID", "STRING"), lines_field("Amount", "Item")],
    )

    sink.update_schema()

    sink.client.update_table.assert_called_once()
    lines = live_lines(sink)
    assert lines.mode == "REPEATED"
    assert [f.name for f in lines.fields] == ["Amount", "Item", "ProjectRebillItem"]


def test_new_subfield_goes_last_even_if_tap_puts_it_mid_record():
    """Existing subfields must keep their positions; see the MERGE tests below."""
    sink = NestedSink(
        nested_table("Amount", "ProjectRebillItem", "Item"),
        [bigquery.SchemaField("ID", "STRING"), lines_field("Amount", "Item")],
    )

    sink.update_schema()

    assert [f.name for f in live_lines(sink).fields] == ["Amount", "Item", "ProjectRebillItem"]


def test_keeps_live_subfield_the_tap_stopped_sending():
    sink = NestedSink(
        nested_table("Amount", "New"),
        [bigquery.SchemaField("ID", "STRING"), lines_field("Amount", "Legacy")],
    )

    sink.update_schema()

    assert [f.name for f in live_lines(sink).fields] == ["Amount", "Legacy", "New"]


def test_no_alter_when_nested_record_matches_case_insensitively():
    sink = NestedSink(
        nested_table("Amount", "Item"),
        [bigquery.SchemaField("id", "STRING"), lines_field("amount", "item")],
    )

    sink.update_schema()

    sink.client.update_table.assert_not_called()
    assert [f.name for f in sink._live_target_schema] == ["id", "Lines"]


def test_adds_subfield_two_levels_deep():
    table = BigQueryTable(
        name="t",
        dataset="ds",
        project="proj",
        jsonschema={
            "type": "object",
            "properties": {
                "Outer": {
                    "type": ["null", "object"],
                    "properties": {"Lines": lines_prop("a", "b")},
                }
            },
        },
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )
    live = [bigquery.SchemaField("Outer", "RECORD", fields=[lines_field("a")])]
    sink = NestedSink(table, live)

    sink.update_schema()

    outer = sink.client.update_table.call_args[0][0].schema[0]
    assert [f.name for f in outer.fields[0].fields] == ["a", "b"]


def test_live_schema_after_update_is_kept_for_merge():
    sink = NestedSink(
        nested_table("Amount", "New"),
        [bigquery.SchemaField("ID", "STRING"), lines_field("Amount")],
    )

    sink.update_schema()

    lines = {f.name: f for f in sink._live_target_schema}["Lines"]
    assert [f.name for f in lines.fields] == ["Amount", "New"]


class StagingSink:
    """Just what _new_staging_table touches."""

    apply_transforms = True
    table_name = "purchaseentry"
    config = {"location": "EU"}
    _new_staging_table = BaseBigQuerySink._new_staging_table

    def __init__(self, tap_subfields, live_schema, merge=True):
        self.client = MagicMock()
        self.client.get_dataset.return_value = MagicMock(location="EU")
        self.client.get_table.side_effect = NotFound("staging")
        self._staging_seq = 0
        tap = nested_table(*tap_subfields)
        self._staging_opts = {
            "project": tap.project,
            "dataset": tap.dataset,
            "jsonschema": tap.jsonschema,
            "ingestion_strategy": IngestionStrategy.DENORMALIZED,
        }
        self.merge_target = tap if merge else None
        self._live_target_schema = live_schema

    def staging_schema(self):
        self._new_staging_table()
        return list(self.client.create_table.call_args[0][0].schema)


@pytest.fixture(autouse=False)
def no_sleep(monkeypatch):
    monkeypatch.setattr("target_bigquery.core.time.sleep", lambda s: None)


def test_merge_staging_takes_live_layout_not_tap_order(no_sleep):
    """Reproduced on real BigQuery: staging built from the tap's order
    [Amount, ProjectRebillItem, Item] MERGEd into live [Amount, Item, Gone]
    stored ProjectRebillItem's value in Item and Item's in Gone, because STRUCT
    assignment is positional. Staging must carry the live layout instead."""
    live = [
        bigquery.SchemaField("ID", "STRING"),
        lines_field("Amount", "Item", "Gone", "ProjectRebillItem"),
        bigquery.SchemaField("Legacy", "STRING"),
    ]
    schema = StagingSink(("Amount", "ProjectRebillItem", "Item"), live).staging_schema()

    assert schema == live


def test_merge_staging_layout_follows_update_schema(no_sleep):
    sink = NestedSink(
        nested_table("Amount", "ProjectRebillItem", "Item"),
        [bigquery.SchemaField("ID", "STRING"), lines_field("Amount", "Item")],
    )
    sink.update_schema()
    staging = StagingSink(("Amount", "ProjectRebillItem", "Item"), sink._live_target_schema)

    lines = {f.name: f for f in staging.staging_schema()}["Lines"]

    assert [f.name for f in lines.fields] == ["Amount", "Item", "ProjectRebillItem"]


def test_non_merge_staging_keeps_tap_schema(no_sleep):
    """Overwrite / activate_version staging replaces the live table wholesale
    (WRITE_TRUNCATE), so it must not resurrect live columns the tap dropped."""
    live = [bigquery.SchemaField("ID", "STRING"), lines_field("Amount", "Item", "Gone")]
    schema = StagingSink(("Amount", "Item"), live, merge=False).staging_schema()

    assert [f.name for f in schema] == ["ID", "Lines"]
    assert [f.name for f in schema[1].fields] == ["Amount", "Item"]
