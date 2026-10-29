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
from target_bigquery.core import BaseBigQuerySink, _aligned_record_sql

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


def test_aligned_record_sql_is_noop_when_layouts_match():
    f = lines_field("a", "b")
    assert _aligned_record_sql(f, f, "source.`Lines`") == "source.`Lines`"


def test_aligned_record_sql_rebuilds_repeated_record_by_name():
    sql = _aligned_record_sql(lines_field("a", "c", "b"), lines_field("a", "b", "c"), "source.`Lines`")
    assert sql == (
        "ARRAY(SELECT AS STRUCT _e0.`a` AS `a`, _e0.`c` AS `c`, _e0.`b` AS `b`"
        " FROM UNNEST(source.`Lines`) AS _e0 WITH OFFSET AS _o0 ORDER BY _o0)"
    )


def test_aligned_record_sql_fills_subfield_source_lacks_with_typed_null():
    target = bigquery.SchemaField(
        "R", "RECORD", fields=[bigquery.SchemaField("a", "STRING"), bigquery.SchemaField("n", "INTEGER")]
    )
    source = bigquery.SchemaField("R", "RECORD", fields=[bigquery.SchemaField("a", "STRING")])
    assert _aligned_record_sql(target, source, "source.`R`") == (
        "IF(source.`R` IS NULL, NULL, STRUCT(source.`R`.`a` AS `a`, CAST(NULL AS INT64) AS `n`))"
    )


class MergeSink:
    """Just what _merge_staging_into_target touches."""

    apply_transforms = True
    key_properties = ["ID"]
    _merge = BaseBigQuerySink._merge_staging_into_target

    def __init__(self, staging_subfields, live_schema):
        self.client = MagicMock()
        self.merge_target = nested_table(*staging_subfields)
        self.table = BigQueryTable(
            name="purchaseentry__staging",
            dataset="ds",
            project="proj",
            jsonschema=self.merge_target.jsonschema,
            ingestion_strategy=IngestionStrategy.DENORMALIZED,
        )
        self._live_target_schema = live_schema

    def _is_dedupe_before_upsert_candidate(self):
        return False

    def merge_sql(self):
        self._merge()
        return self.client.query.call_args[0][0]


def test_merge_rebuilds_record_whose_new_subfield_is_mid_struct_in_staging():
    """Staging: Amount, ProjectRebillItem, Item. Live after ALTER: Amount, Item,
    ProjectRebillItem. A plain `= source.Lines` would put Item's value into
    ProjectRebillItem and vice versa (all STRINGs, so no error to warn us)."""
    live = [bigquery.SchemaField("ID", "STRING"), lines_field("Amount", "Item", "ProjectRebillItem")]
    sql = MergeSink(("Amount", "ProjectRebillItem", "Item"), live).merge_sql()

    rebuilt = (
        "ARRAY(SELECT AS STRUCT _e0.`Amount` AS `Amount`, _e0.`Item` AS `Item`,"
        " _e0.`ProjectRebillItem` AS `ProjectRebillItem` FROM UNNEST(source.`Lines`) AS _e0"
        " WITH OFFSET AS _o0 ORDER BY _o0)"
    )
    assert f"target.`Lines` = {rebuilt}" in sql
    assert f"VALUES (source.`ID`, {rebuilt})" in sql


def test_merge_sql_unchanged_when_record_layouts_match():
    live = [bigquery.SchemaField("ID", "STRING"), lines_field("Amount", "Item")]
    sql = MergeSink(("Amount", "Item"), live).merge_sql()

    assert "target.`Lines` = source.`Lines`" in sql
    assert "VALUES (source.`ID`, source.`Lines`)" in sql
