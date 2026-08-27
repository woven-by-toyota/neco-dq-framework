# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Spark session fixtures for dq_framework tests.

This module provides SparkSession management with conditional execution
based on whether local pyspark or databricks-connect is installed.
"""

import pytest

# Check if local Spark is available (not databricks-connect)
_SPARK_AVAILABLE = False
_spark_session = None

try:
    from pyspark.sql import SparkSession

    # Try to create a local session - will fail with databricks-connect
    test_spark = (
        SparkSession.builder.master("local[1]")
        .appName("spark_check")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    test_spark.stop()
    _SPARK_AVAILABLE = True
except (ImportError, OSError, RuntimeError):
    # ImportError: pyspark not installed
    # OSError: Spark native libraries not available
    # RuntimeError: databricks-connect installed (can't create local session)
    _SPARK_AVAILABLE = False


def is_spark_available() -> bool:
    """Check if local Spark is available for testing.

    Note: This function is used by conftest.py to conditionally skip tests.
    """
    return _SPARK_AVAILABLE


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing.

    This fixture is only available when running with pyspark (not databricks-connect).
    Tests using this fixture should be marked with @pytest.mark.spark.

    Returns:
        SparkSession: A local Spark session configured for testing.
    """
    global _spark_session

    if not _SPARK_AVAILABLE:
        pytest.skip("Local Spark not available")

    if _spark_session is None:
        from pyspark.sql import SparkSession

        _spark_session = (
            SparkSession.builder.master("local[2]")
            .appName("neco_dq_tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )

    return _spark_session


@pytest.fixture(scope="session", autouse=True)
def cleanup_spark():
    """Stop SparkSession after all tests complete."""
    yield
    global _spark_session
    if _spark_session is not None:
        _spark_session.stop()
        _spark_session = None
