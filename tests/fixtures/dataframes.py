# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Common DataFrame fixtures for dq_framework tests.

This module provides reusable test DataFrames that are shared across multiple test modules.
Fixtures are organized by data pattern rather than by test suite.

Organization:
- Common fixtures: Generic data patterns used across suites
- CSV fixtures: Data loaded from CSV files (for simple tabular data)
- Inline fixtures: Data created in Python (for JSON arrays, complex structures)

For suite-specific fixtures, see:
- fixtures/bronze.py: Binary, schema, partition fixtures
- fixtures/silver.py: Transition, temporal, flow fixtures
- fixtures/gold.py: Dimension, metrics, reconciliation fixtures
"""

from pathlib import Path

import pytest

# Path to CSV fixtures
CSV_DIR = Path(__file__).parent / "csv"


# =============================================================================
# Common Fixtures - Generic data patterns
# =============================================================================


@pytest.fixture(scope="session")
def df_empty(spark):
    """Empty DataFrame for edge case tests.

    Schema: id INT, code STRING
    Rows: 0
    """
    return spark.createDataFrame([], "id INT, code STRING")


@pytest.fixture(scope="session")
def df_simple(spark):
    """Simple DataFrame with 3 rows for basic tests.

    Schema: id INT, code STRING
    Rows: 3 unique rows
    """
    data = [(1, "A"), (2, "B"), (3, "C")]
    return spark.createDataFrame(data, ["id", "code"])


@pytest.fixture(scope="session")
def df_unique(spark):
    """DataFrame with unique key-value pairs.

    Schema: id INT, code STRING, value INT
    Rows: 3 unique rows
    Use for: idempotency tests, uniqueness tests
    """
    data = [(1, "A", 100), (2, "B", 200), (3, "C", 300)]
    return spark.createDataFrame(data, ["id", "code", "value"])


@pytest.fixture(scope="session")
def df_duplicates(spark):
    """DataFrame with duplicate rows.

    Schema: id INT, code STRING, value INT
    Rows: 4 rows, (1, A, 100) appears twice
    Use for: duplicate detection, idempotency failure tests
    """
    data = [(1, "A", 100), (1, "A", 100), (2, "B", 200), (3, "C", 300)]
    return spark.createDataFrame(data, ["id", "code", "value"])


@pytest.fixture(scope="session")
def df_with_nulls(spark):
    """DataFrame with null values in various columns.

    Schema: id INT, name STRING, status STRING
    Rows: 3 rows, name is null in row 2, status is null in row 3
    Use for: mandatory fields tests, null handling tests
    """
    data = [(1, "A", "active"), (2, None, "active"), (3, "C", None)]
    return spark.createDataFrame(data, ["id", "name", "status"])


# =============================================================================
# CSV Fixtures - Load test data from CSV files
# =============================================================================


@pytest.fixture(scope="session")
def df_basic(spark):
    """Load basic.csv - comprehensive data for count/aggregate tests.

    Schema:
        - id: int
        - name: string
        - category: string (A, B)
        - value: int
        - status: string (active, inactive)
    """
    return spark.read.csv(str(CSV_DIR / "basic.csv"), header=True, inferSchema=True)


@pytest.fixture(scope="session")
def df_nulls(spark):
    """Load nulls.csv - data with null values for completion_ratio tests.

    Schema:
        - id: int
        - name: string (some null)
        - email: string (some null)
        - score: int (some null)
        - notes: string (some null)
    """
    return spark.read.csv(str(CSV_DIR / "nulls.csv"), header=True, inferSchema=True, nullValue="", emptyValue=None)


# =============================================================================
# Inline Fixtures - JSON array data (created in Python)
# =============================================================================


@pytest.fixture(scope="session")
def df_arrays(spark):
    """Create DataFrame with JSON array columns for array metric tests.

    Note: Created inline because CSV loading doesn't preserve JSON array parsing correctly.

    Schema:
        - id: int
        - category_id: string
        - items: string (JSON array with text, value fields)
    """
    data = [
        (1, "S1", '[{"text":"Item A","value":"10"},{"text":"Item B","value":"20"}]'),
        (
            2,
            "S1",
            '[{"text":"Item C","value":"30"},{"text":"Item D","value":"40"},{"text":"Item E","value":"50"}]',
        ),
        (3, "S2", '[{"text":"Item F","value":"15"}]'),
        (4, "S2", '[{"text":"Item G","value":"25"},{"text":"Item H","value":"35"}]'),
    ]
    return spark.createDataFrame(data, ["id", "category_id", "items"])


@pytest.fixture(scope="session")
def df_rankings(spark):
    """Create DataFrame with ranking data for Kendall W / Borda Count tests.

    Note: Created inline because CSV loading doesn't preserve JSON array parsing correctly.

    Schema:
        - id: int
        - question_id: string
        - ranking_data: string (JSON array with item_code, mean_rank, item_appearances_n)
    """
    data = [
        (
            1,
            "Q1",
            '[{"item_code":"A","mean_rank":1.5,"item_appearances_n":10},'
            '{"item_code":"B","mean_rank":2.5,"item_appearances_n":10},'
            '{"item_code":"C","mean_rank":3.0,"item_appearances_n":10}]',
        ),
        (
            2,
            "Q2",
            '[{"item_code":"X","mean_rank":2.0,"item_appearances_n":5},'
            '{"item_code":"Y","mean_rank":2.0,"item_appearances_n":5},'
            '{"item_code":"Z","mean_rank":2.0,"item_appearances_n":5}]',
        ),
    ]
    return spark.createDataFrame(data, ["id", "question_id", "ranking_data"])


# =============================================================================
# Expected Resolver Test Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def df_expected_arrays(spark):
    """Create DataFrame with arrays for expected resolver tests.

    Note: Created inline because CSV loading doesn't preserve JSON array parsing correctly.

    Schema:
        - id: int
        - group_id: string
        - items: string (JSON array with item_code, value fields)
    """
    data = [
        (1, "G1", '[{"item_code":"A","value":"10"},{"item_code":"B","value":"20"}]'),
        (2, "G1", '[{"item_code":"C","value":"30"}]'),
        (3, "G2", '[{"item_code":"D","value":"40"},{"item_code":"E","value":"50"}]'),
    ]
    return spark.createDataFrame(data, ["id", "group_id", "items"])


@pytest.fixture(scope="session")
def df_expected_ordered(spark):
    """Create DataFrame with ordered arrays for expected resolver tests.

    Note: Created inline because CSV loading doesn't preserve JSON array parsing correctly.

    Schema:
        - id: int
        - group_id: string
        - rankings: string (JSON array with rank, score fields)
    """
    data = [
        (1, "G1", '[{"rank":"1","score":"100"},{"rank":"2","score":"80"},{"rank":"3","score":"60"}]'),
        (2, "G2", '[{"rank":"1","score":"90"},{"rank":"2","score":"70"}]'),
    ]
    return spark.createDataFrame(data, ["id", "group_id", "rankings"])
