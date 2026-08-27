# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Gold suite specific fixtures for dq_framework tests.

This module provides fixtures specific to Gold layer tests:
- Dimension fixtures (completeness, coverage)
- Metrics fixtures (volume, trend)
- Reconciliation fixtures (gold vs silver comparison)

For common fixtures shared across suites, see fixtures/dataframes.py.
"""

import pytest

from neco_dq.test_suites.gold.gold_dq_test_suite import SparkGoldDQTestSuite


# =============================================================================
# Suite Instance
# =============================================================================


@pytest.fixture(scope="module")
def gold_suite(spark):
    """Create Gold suite instance with minimal config."""
    config = {"gold": {"tables": [], "default_checks": {}}}
    return SparkGoldDQTestSuite(spark, config)


# =============================================================================
# Dimension Fixtures - Completeness tests
# =============================================================================


@pytest.fixture(scope="module")
def df_dimensions(spark):
    """DataFrame with dimension values for completeness tests.

    Schema: category STRING, value INT
    Rows: 3 rows with categories A, B, C
    """
    data = [("A", 100), ("B", 200), ("C", 300)]
    return spark.createDataFrame(data, ["category", "value"])


@pytest.fixture(scope="module")
def df_dimensions_incomplete(spark):
    """DataFrame with missing dimension values.

    Schema: category STRING, value INT
    Rows: 2 rows, missing category C
    """
    data = [("A", 100), ("B", 200)]
    return spark.createDataFrame(data, ["category", "value"])


# =============================================================================
# Metrics Fixtures - Volume/Trend tests
# =============================================================================


@pytest.fixture(scope="module")
def df_metrics(spark):
    """DataFrame with metrics for volume/trend tests.

    Schema: group_id STRING, amount INT
    Rows: 4 rows, grouped (G1: 100+200=300, G2: 150+250=400)
    Total: 700
    """
    data = [
        ("G1", 100),
        ("G1", 200),
        ("G2", 150),
        ("G2", 250),
    ]
    return spark.createDataFrame(data, ["group_id", "amount"])


@pytest.fixture(scope="module")
def df_metrics_low_volume(spark):
    """DataFrame with low volume metrics.

    Schema: group_id STRING, amount INT
    Total: 50 (below typical thresholds)
    """
    data = [
        ("G1", 20),
        ("G1", 30),
    ]
    return spark.createDataFrame(data, ["group_id", "amount"])


# =============================================================================
# Reconciliation Fixtures - Gold vs Silver comparison
# =============================================================================


@pytest.fixture(scope="module")
def df_gold_recon(spark):
    """Gold DataFrame for reconciliation tests.

    Schema: id INT, amount INT
    Total: 600
    """
    data = [(1, 100), (2, 200), (3, 300)]
    return spark.createDataFrame(data, ["id", "amount"])


@pytest.fixture(scope="module")
def df_silver_recon(spark):
    """Silver DataFrame for reconciliation tests (matching gold).

    Schema: id INT, amount INT
    Total: 600 (matches gold)
    """
    data = [(1, 100), (2, 200), (3, 300)]
    return spark.createDataFrame(data, ["id", "amount"])


@pytest.fixture(scope="module")
def df_silver_recon_mismatch(spark):
    """Silver DataFrame for reconciliation tests (mismatched).

    Schema: id INT, amount INT
    Total: 2997 (doesn't match gold)
    """
    data = [(1, 999), (2, 999), (3, 999)]
    return spark.createDataFrame(data, ["id", "amount"])


# =============================================================================
# Cross Gold Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_gold_left(spark):
    """Left Gold DataFrame for cross-gold tests.

    Schema: id INT, amount INT
    Total: 300
    """
    data = [(1, 100), (2, 200)]
    return spark.createDataFrame(data, ["id", "amount"])


@pytest.fixture(scope="module")
def df_gold_right(spark):
    """Right Gold DataFrame for cross-gold tests.

    Schema: id INT, total INT
    Total: 300 (matches left)
    """
    data = [(1, 100), (2, 200)]
    return spark.createDataFrame(data, ["id", "total"])


@pytest.fixture(scope="module")
def df_gold_right_mismatch(spark):
    """Right Gold DataFrame with mismatched values.

    Schema: id INT, total INT
    Total: 1099 (doesn't match left)
    """
    data = [(1, 100), (2, 999)]
    return spark.createDataFrame(data, ["id", "total"])


# =============================================================================
# Coverage Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_coverage_gold(spark):
    """Gold DataFrame for coverage tests.

    Schema: group_id STRING, amount INT
    Groups: G1 (150), G2 (200)
    """
    data = [("G1", 100), ("G1", 50), ("G2", 200)]
    return spark.createDataFrame(data, ["group_id", "amount"])


@pytest.fixture(scope="module")
def df_coverage_silver(spark):
    """Silver DataFrame for coverage tests (matching gold).

    Schema: group_id STRING, total INT
    Groups: G1 (150), G2 (200) - matches gold
    """
    data = [("G1", 150), ("G2", 200)]
    return spark.createDataFrame(data, ["group_id", "total"])
