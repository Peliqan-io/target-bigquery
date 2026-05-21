# target-bigquery — Python 3.9 → 3.11 Regression Tests

Before/after parity test for the Python 3.9 → 3.11 migration.

## Strategy

target-bigquery uses the BigQuery Python client (not raw SQL), so we compare:
1. **Schema translations** — JSON schema → BigQuery schema fields (via `SchemaTranslator`)
2. **Type mappings** — JSON schema types → BigQuery column types (via `bigquery_type`)
3. **Column name transforms** — snake_case, lower, raw transformations
4. **Existing unit tests** — must pass on both Python versions with same pass count

## Usage

```bash
cd target-bigquery

# Capture baseline (master + Python 3.9)
git checkout master
docker run --rm -v "$(pwd)":/target -w /target python:3.9-slim \
  bash -c 'apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1 && pip install -e ".[dev]" -q 2>/dev/null && python tests/regression/capture.py --output baseline'

# Capture current (migration branch + Python 3.11)
git checkout python-311-migration
docker run --rm -v "$(pwd)":/target -w /target python:3.11-slim \
  bash -c 'apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1 && pip install -e ".[dev]" -q 2>/dev/null && python tests/regression/capture.py --output current'

# Compare
python3 -m pytest tests/regression/test_regression.py -v
```
