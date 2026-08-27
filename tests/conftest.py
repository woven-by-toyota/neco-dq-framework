# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Pytest configuration for neco_dq tests.

This module handles:
- Custom marker registration
- Conditional test skipping for Spark-dependent tests
- Dynamic fixture loading from fixtures/ directory

Fixtures are organized in separate modules:
- fixtures/spark.py: SparkSession management
- fixtures/dataframes.py: DataFrame fixtures for testing
"""

import pytest

# ---------------------------------------------------------------------------
# Dynamic fixture loading from fixtures directory
# Following the api_tests standard pattern
# IMPORTANT: pytest_plugins must be defined before any imports that reference
# the fixture modules, otherwise pytest cannot rewrite assertions
# ---------------------------------------------------------------------------

pytest_plugins = [
    "tests.fixtures.spark",
    "tests.fixtures.dataframes",
    "tests.fixtures.bronze",
    "tests.fixtures.silver",
    "tests.fixtures.gold",
]


# ---------------------------------------------------------------------------
# Pytest hooks
# ---------------------------------------------------------------------------


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "spark: marks tests as requiring local Spark (skip if unavailable)")


def pytest_collection_modifyitems(config, items):  # noqa: ARG001
    """Skip spark-marked tests if local Spark is not available."""
    # Import here to avoid circular imports (after pytest_plugins loads the module)
    from tests.fixtures.spark import is_spark_available

    if is_spark_available():
        return

    skip_spark = pytest.mark.skip(reason="Local Spark not available (databricks-connect installed)")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip_spark)
