"""Offline validation of the self-contained proto descriptor used by storage_write.

The Storage Write API receives a single DescriptorProto (ProtoSchema) and must be
able to resolve every message type referenced by its fields WITHOUT any other
context. We simulate exactly that here: rebuild each generated descriptor inside a
brand-new DescriptorPool. If any nested RECORD message definition is missing or a
type_name reference is dangling, pool.Add() raises — the offline equivalent of the
server's "There was a problem opening the stream" (issue #71 -> https://github.com/z3z1ma/target-bigquery/issues/71).

Run: poetry run pytest tests/test_self_contained_descriptor.py -v
"""
import itertools

import pytest
from google.protobuf import descriptor_pb2, descriptor_pool

from target_bigquery.core import SchemaTranslator, SchemaResolverVersion
from target_bigquery.proto_gen import proto_schema_factory_v2
from target_bigquery.storage_write import _self_contained_descriptor

_counter = itertools.count()


def _descriptor_for(schema: dict):
    """jsonschema -> BQ schema -> proto message -> self-contained DescriptorProto."""
    translator = SchemaTranslator(
        schema=schema, transforms={}, resolver_version=SchemaResolverVersion.V2
    )
    message_cls = proto_schema_factory_v2(translator.translated_schema)
    return _self_contained_descriptor(message_cls.DESCRIPTOR)


def _assert_resolvable(proto: descriptor_pb2.DescriptorProto):
    """Rebuild the descriptor in an empty pool — raises if not self-contained."""
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = f"test_{next(_counter)}.proto"
    fdp.package = "storage_write_check"
    fdp.message_type.add().MergeFrom(proto)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)  # KeyError/TypeError here == descriptor NOT self-contained
    return pool.FindMessageTypeByName(f"storage_write_check.{proto.name}")


def _props(**kwargs):
    return {"type": "object", "properties": kwargs}


STRING = {"type": ["string", "null"]}
INTEGER = {"type": ["integer", "null"]}


def test_simple_record():
    schema = _props(id=INTEGER, address=_props(city=STRING, zip=STRING))
    _assert_resolvable(_descriptor_for(schema))


def test_repeated_record():
    schema = _props(
        id=INTEGER,
        line_items={"type": ["array", "null"], "items": _props(sku=STRING, qty=INTEGER)},
    )
    _assert_resolvable(_descriptor_for(schema))


def test_deep_nesting_three_levels():
    schema = _props(
        id=INTEGER,
        l1=_props(a=STRING, l2=_props(b=STRING, l3=_props(c=STRING))),
    )
    desc = _assert_resolvable(_descriptor_for(schema))
    # walk down the chain to prove each level actually resolved
    l1 = desc.fields_by_name["l1"].message_type
    l2 = l1.fields_by_name["l2"].message_type
    l3 = l2.fields_by_name["l3"].message_type
    assert "c" in l3.fields_by_name


def test_sibling_records_with_identical_shape():
    """billing.address and shipping.address — same sub-message shape, must not collide."""
    address = _props(city=STRING, zip=STRING)
    schema = _props(
        id=INTEGER,
        billing=_props(address=address, vat=STRING),
        shipping=_props(address=address, note=STRING),
    )
    desc = _assert_resolvable(_descriptor_for(schema))
    b = desc.fields_by_name["billing"].message_type
    s = desc.fields_by_name["shipping"].message_type
    assert "city" in b.fields_by_name["address"].message_type.fields_by_name
    assert "city" in s.fields_by_name["address"].message_type.fields_by_name


def test_repeated_record_containing_record():
    schema = _props(
        id=INTEGER,
        orders={
            "type": ["array", "null"],
            "items": _props(
                ref=STRING,
                customer=_props(name=STRING, address=_props(city=STRING)),
            ),
        },
    )
    _assert_resolvable(_descriptor_for(schema))


def test_json_column_inside_record():
    """A free-form object (JSON column) nested in a RECORD stays a plain string field."""
    schema = _props(
        id=INTEGER,
        payload=_props(kind=STRING, extra={"type": ["object", "null"]}),
    )
    desc = _assert_resolvable(_descriptor_for(schema))
    payload = desc.fields_by_name["payload"].message_type
    extra = payload.fields_by_name["extra"]
    assert extra.message_type is None  # JSON -> STRING, no nested message


def test_wide_schema_many_records():
    """Many distinct RECORD columns in one message (the 'larger dataset' shape)."""
    schema = _props(
        id=INTEGER,
        **{f"rec_{i}": _props(**{f"f_{i}_{j}": STRING for j in range(5)}) for i in range(20)},
    )
    desc = _assert_resolvable(_descriptor_for(schema))
    assert desc.fields_by_name["rec_19"].message_type is not None


def test_old_descriptor_copy_is_not_self_contained():
    """Control: the pre-fix approach (plain CopyToProto) must FAIL this check,
    proving the check itself detects the issue #71 condition."""
    schema = _props(id=INTEGER, address=_props(city=STRING))
    translator = SchemaTranslator(
        schema=schema, transforms={}, resolver_version=SchemaResolverVersion.V2
    )
    message_cls = proto_schema_factory_v2(translator.translated_schema)
    plain = descriptor_pb2.DescriptorProto()
    message_cls.DESCRIPTOR.CopyToProto(plain)  # old generate_template behaviour
    with pytest.raises(Exception):
        _assert_resolvable(plain)
