"""Shared configuration for target-bigquery regression tests."""
import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "regression: target-bigquery 3.9→3.11 migration regression tests")
