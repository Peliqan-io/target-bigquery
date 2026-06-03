#!/usr/bin/env python3
"""
Capture target-bigquery schema translations and type mappings for regression comparison.

target-bigquery uses the BigQuery Python client (not raw SQL), so we capture:
  1. JSON schema → BigQuery schema translations (via SchemaTranslator)
  2. Column type mappings (via bigquery_type)
  3. Column name transformations (via transform_column_name)
  4. Existing unit test results (pytest exit code + summary)

Usage:
    python capture.py --output baseline   # on 3.9 (master)
    python capture.py --output current    # on 3.11 (migration branch)
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path


def capture_schema_translations(messages_path):
    """Parse SCHEMA messages and translate each to BigQuery schema."""
    from target_bigquery.core import SchemaTranslator, bigquery_type, transform_column_name

    schemas = {}
    with open(messages_path) as f:
        for line in f:
            msg = json.loads(line.strip())
            if msg.get("type") != "SCHEMA":
                continue
            stream = msg["stream"]
            json_schema = msg["schema"]
            key_properties = msg.get("key_properties", [])

            # Translate JSON schema → BigQuery schema fields
            translator = SchemaTranslator(json_schema, transforms={})
            bq_schema = translator.translated_schema

            fields = []
            for field in bq_schema:
                fields.append({
                    "name": field.name,
                    "field_type": field.field_type,
                    "mode": field.mode,
                    "description": field.description or "",
                })

            # Also capture column name transforms
            name_transforms = {}
            for prop_name in json_schema.get("properties", {}):
                name_transforms[prop_name] = {
                    "snake_case": transform_column_name(prop_name, snake_case=True),
                    "lower": transform_column_name(prop_name, lower=True),
                    "raw": transform_column_name(prop_name),
                }

            schemas[stream] = {
                "bq_fields": sorted(fields, key=lambda f: f["name"]),
                "name_transforms": name_transforms,
                "key_properties": key_properties,
            }

    return schemas


def capture_type_mappings():
    """Exercise bigquery_type() with a comprehensive set of JSON schema types."""
    from target_bigquery.core import bigquery_type

    test_cases = [
        # (property_type, property_format, description)
        (["null", "string"], None, "nullable string"),
        (["string"], None, "string"),
        (["null", "integer"], None, "nullable integer"),
        (["integer"], None, "integer"),
        (["null", "number"], None, "nullable number"),
        (["number"], None, "number"),
        (["null", "boolean"], None, "nullable boolean"),
        (["boolean"], None, "boolean"),
        (["null", "object"], None, "nullable object"),
        (["object"], None, "object"),
        (["null", "array"], None, "nullable array"),
        (["array"], None, "array"),
        (["string"], "date", "date"),
        (["string"], "date-time", "datetime"),
        (["string"], "time", "time"),
        (["null", "string"], "date", "nullable date"),
        (["null", "string"], "date-time", "nullable datetime"),
        (["null", "string"], "time", "nullable time"),
        (["string"], "singer.decimal", "singer decimal"),
        (["null", "string"], "singer.decimal", "nullable singer decimal"),
        (["integer", "string"], None, "integer+string multitype"),
        (["number", "string"], None, "number+string multitype"),
        (["boolean", "string"], None, "boolean+string multitype"),
    ]

    results = {}
    for prop_type, prop_format, desc in test_cases:
        key = f"{desc} ({prop_type}, {prop_format})"
        try:
            results[key] = bigquery_type(prop_type, prop_format)
        except Exception as e:
            results[key] = f"ERROR: {e}"

    return results


def run_existing_tests():
    """Run the existing unit tests and capture the result."""
    result = subprocess.run(
        [sys.executable, "-m", "pytest",
         "target_bigquery/tests/test_utils.py", "-v", "--tb=short"],
        capture_output=True, text=True, timeout=120
    )
    return {
        "exit_code": result.returncode,
        "stdout_tail": result.stdout[-2000:] if result.stdout else "",
        "stderr_tail": result.stderr[-500:] if result.stderr else "",
        "passed": result.stdout.count(" PASSED") if result.stdout else 0,
        "failed": result.stdout.count(" FAILED") if result.stdout else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Capture target-bigquery regression output")
    parser.add_argument("--output", required=True, choices=["baseline", "current"])
    parser.add_argument("--messages", default=None)
    args = parser.parse_args()

    regression_dir = Path(__file__).parent
    output_dir = regression_dir / args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    messages_path = args.messages or str(regression_dir / "fixtures" / "messages.jsonl")
    if not Path(messages_path).exists():
        print(f"ERROR: Messages file not found: {messages_path}")
        sys.exit(1)

    print(f"Python version: {sys.version}")
    print(f"Output dir:     {output_dir}")
    print(f"Messages:       {messages_path}")

    # 1. Schema translations
    print("\nCapturing schema translations...")
    schemas = capture_schema_translations(messages_path)
    (output_dir / "schema_translations.json").write_text(
        json.dumps(schemas, indent=2, sort_keys=True)
    )
    print(f"  Streams: {list(schemas.keys())}")

    # 2. Type mappings
    print("Capturing type mappings...")
    type_mappings = capture_type_mappings()
    (output_dir / "type_mappings.json").write_text(
        json.dumps(type_mappings, indent=2, sort_keys=True)
    )
    print(f"  Type cases: {len(type_mappings)}")

    # 3. Existing unit tests
    print("Running existing unit tests...")
    test_results = run_existing_tests()
    (output_dir / "unit_test_results.json").write_text(
        json.dumps(test_results, indent=2)
    )
    print(f"  Passed: {test_results['passed']}, Failed: {test_results['failed']}, Exit: {test_results['exit_code']}")

    # 4. Meta
    meta = {
        "python_version": sys.version.split()[0],
        "message_count": sum(1 for _ in open(messages_path)),
        "stream_count": len(schemas),
        "type_mapping_count": len(type_mappings),
        "unit_tests_passed": test_results["passed"],
        "unit_tests_failed": test_results["failed"],
    }
    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    print(f"\nCapture complete. Files written to: {output_dir}/")


if __name__ == "__main__":
    main()
