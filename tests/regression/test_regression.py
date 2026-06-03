"""
target-bigquery regression tests — Python 3.9 → 3.11 migration.

Compares BigQuery schema translations, type mappings, and unit test results
between baseline (3.9) and current (3.11).

Run:
    python -m pytest test_regression.py -v
"""
import json
import sys
from pathlib import Path

import pytest

REGRESSION_DIR = Path(__file__).parent
BASELINE_DIR = REGRESSION_DIR / "baseline"
CURRENT_DIR = REGRESSION_DIR / "current"


def load(directory, filename):
    path = directory / filename
    if not path.exists():
        pytest.skip(f"File not found: {path}. Run capture.py first.")
    return json.loads(path.read_text())


class TestMeta:
    def test_python_version_is_311(self):
        meta = load(CURRENT_DIR, "meta.json")
        parts = meta["python_version"].split(".")
        assert (int(parts[0]), int(parts[1])) >= (3, 11)

    def test_baseline_is_39(self):
        meta = load(BASELINE_DIR, "meta.json")
        assert meta["python_version"].startswith("3.9")

    def test_same_message_count(self):
        b = load(BASELINE_DIR, "meta.json")
        c = load(CURRENT_DIR, "meta.json")
        assert b["message_count"] == c["message_count"]


class TestSchemaTranslations:
    def test_same_streams(self):
        b = load(BASELINE_DIR, "schema_translations.json")
        c = load(CURRENT_DIR, "schema_translations.json")
        assert sorted(b.keys()) == sorted(c.keys())

    def test_bq_fields_match(self):
        """BigQuery schema fields must be identical for every stream."""
        b = load(BASELINE_DIR, "schema_translations.json")
        c = load(CURRENT_DIR, "schema_translations.json")
        mismatches = []
        for stream in b:
            bf = b[stream]["bq_fields"]
            cf = c.get(stream, {}).get("bq_fields", [])
            if bf != cf:
                mismatches.append(stream)
        assert not mismatches, (
            f"BQ schema differs for streams: {mismatches}"
        )

    def test_name_transforms_match(self):
        """Column name transformations must be identical."""
        b = load(BASELINE_DIR, "schema_translations.json")
        c = load(CURRENT_DIR, "schema_translations.json")
        mismatches = []
        for stream in b:
            bt = b[stream]["name_transforms"]
            ct = c.get(stream, {}).get("name_transforms", {})
            if bt != ct:
                mismatches.append(stream)
        assert not mismatches, (
            f"Name transforms differ for streams: {mismatches}"
        )

    def test_key_properties_match(self):
        b = load(BASELINE_DIR, "schema_translations.json")
        c = load(CURRENT_DIR, "schema_translations.json")
        for stream in b:
            assert b[stream]["key_properties"] == c.get(stream, {}).get("key_properties", []), (
                f"Key properties differ for {stream}"
            )


class TestTypeMappings:
    def test_all_mappings_match(self):
        """Every JSON schema type → BigQuery type mapping must be identical."""
        b = load(BASELINE_DIR, "type_mappings.json")
        c = load(CURRENT_DIR, "type_mappings.json")
        assert b == c, "Type mappings differ between 3.9 and 3.11"

    def test_no_errors(self):
        """No type mapping should produce an error on 3.11."""
        c = load(CURRENT_DIR, "type_mappings.json")
        errors = {k: v for k, v in c.items() if isinstance(v, str) and v.startswith("ERROR")}
        assert not errors, f"Type mapping errors on 3.11: {errors}"


class TestUnitTests:
    def test_same_pass_count(self):
        """Same number of tests should pass on both versions."""
        b = load(BASELINE_DIR, "unit_test_results.json")
        c = load(CURRENT_DIR, "unit_test_results.json")
        assert b["passed"] == c["passed"], (
            f"Pass count differs: 3.9={b['passed']}, 3.11={c['passed']}"
        )

    def test_same_fail_count(self):
        """Same number of tests should fail on both (no new regressions)."""
        b = load(BASELINE_DIR, "unit_test_results.json")
        c = load(CURRENT_DIR, "unit_test_results.json")
        assert c["failed"] <= b["failed"], (
            f"New failures on 3.11: 3.9 had {b['failed']} failures, 3.11 has {c['failed']}"
        )

    def test_same_exit_code(self):
        """Both runs should have the same exit code."""
        b = load(BASELINE_DIR, "unit_test_results.json")
        c = load(CURRENT_DIR, "unit_test_results.json")
        assert b["exit_code"] == c["exit_code"], (
            f"Exit code changed: 3.9={b['exit_code']}, 3.11={c['exit_code']}"
        )
