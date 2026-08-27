# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Silver suite specific fixtures for dq_framework tests.

This module provides fixtures specific to Silver layer tests:
- Status transition fixtures (state machine validation)
- Temporal fixtures (timestamp consistency)
- Cardinality fixtures (group cardinality checks)
- Flow fixtures (flow completeness checks)
- Totals fixtures (totals consistency checks)

For common fixtures shared across suites, see fixtures/dataframes.py.
"""

from datetime import UTC, datetime, timedelta

import pytest

from neco_dq.test_suites.silver.silver_dq_test_suite import SparkSilverDQTestSuite


# =============================================================================
# Suite Instance
# =============================================================================


@pytest.fixture(scope="module")
def silver_suite(spark):
    """Create Silver suite instance with minimal config."""
    config = {"silver": {"tables": [], "default_checks": {}}}
    return SparkSilverDQTestSuite(spark, config)


# =============================================================================
# Status Transition Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_transitions(spark):
    """DataFrame with valid status transitions for status_transition check.

    Schema: entity_id INT, status STRING, ts TIMESTAMP
    Rows: Status history with valid forward progression

    Entity 1: pending -> active -> completed
    Entity 2: pending -> active
    """
    now = datetime.now(UTC)
    data = [
        (1, "pending", now - timedelta(hours=5)),
        (1, "active", now - timedelta(hours=3)),
        (1, "completed", now - timedelta(hours=1)),
        (2, "pending", now - timedelta(hours=4)),
        (2, "active", now - timedelta(hours=2)),
    ]
    return spark.createDataFrame(data, ["entity_id", "status", "ts"])


@pytest.fixture(scope="module")
def df_invalid_transitions(spark):
    """DataFrame with invalid status transitions for status_transition check.

    Schema: entity_id INT, status STRING, ts TIMESTAMP
    Rows: Status history with invalid backward progression

    Entity 1: pending -> completed -> active (invalid: completed before active)
    """
    now = datetime.now(UTC)
    data = [
        (1, "pending", now - timedelta(hours=5)),
        (1, "completed", now - timedelta(hours=3)),  # Invalid: skipped active
        (1, "active", now - timedelta(hours=1)),  # Invalid: went back from completed
    ]
    return spark.createDataFrame(data, ["entity_id", "status", "ts"])


@pytest.fixture(scope="module")
def df_transitions_unknown(spark):
    """DataFrame with unknown statuses for status_transition check.

    Schema: entity_id INT, status STRING, ts TIMESTAMP
    Rows: Status history containing statuses not in the allowed flow

    Entity 1: pending -> UNKNOWN_STATUS -> active (unknown status in the middle)
    """
    now = datetime.now(UTC)
    data = [
        (1, "pending", now - timedelta(hours=5)),
        (1, "UNKNOWN_STATUS", now - timedelta(hours=3)),  # Unknown status
        (1, "active", now - timedelta(hours=1)),
    ]
    return spark.createDataFrame(data, ["entity_id", "status", "ts"])


# =============================================================================
# Temporal Fixtures - Timestamp consistency tests
# =============================================================================


@pytest.fixture(scope="module")
def df_temporal(spark):
    """DataFrame with valid temporal relationships.

    Schema: id INT, start_time TIMESTAMP, end_time TIMESTAMP
    Rows: 3 rows where start_time <= end_time
    """
    now = datetime.now(UTC)
    data = [
        (1, now - timedelta(hours=3), now - timedelta(hours=1)),
        (2, now - timedelta(hours=5), now - timedelta(hours=2)),
        (3, now - timedelta(hours=4), now - timedelta(hours=4)),  # Equal times
    ]
    return spark.createDataFrame(data, ["id", "start_time", "end_time"])


@pytest.fixture(scope="module")
def df_temporal_invalid(spark):
    """DataFrame with invalid temporal relationships.

    Schema: id INT, start_time TIMESTAMP, end_time TIMESTAMP
    Rows: 3 rows, one has end_time before start_time
    """
    now = datetime.now(UTC)
    data = [
        (1, now - timedelta(hours=3), now - timedelta(hours=1)),
        (2, now - timedelta(hours=1), now - timedelta(hours=5)),  # Invalid: end < start
        (3, now - timedelta(hours=4), now - timedelta(hours=2)),
    ]
    return spark.createDataFrame(data, ["id", "start_time", "end_time"])


# =============================================================================
# Cardinality Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_cardinality(spark):
    """DataFrame for cardinality tests.

    Schema: group_id STRING, item_id INT
    Rows: 6 rows with varying cardinality per group
        - G1: 3 items
        - G2: 2 items
        - G3: 1 item
    """
    data = [
        ("G1", 1),
        ("G1", 2),
        ("G1", 3),
        ("G2", 1),
        ("G2", 2),
        ("G3", 1),
    ]
    return spark.createDataFrame(data, ["group_id", "item_id"])


# =============================================================================
# Totals Consistency Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_totals(spark):
    """DataFrame for totals consistency tests.

    Schema: group_id STRING, amount INT, total INT
    Rows: Data where detail amounts should sum to total
    """
    data = [
        ("G1", 100, 300),  # G1 total should be 300
        ("G1", 100, 300),
        ("G1", 100, 300),
        ("G2", 50, 100),  # G2 total should be 100
        ("G2", 50, 100),
    ]
    return spark.createDataFrame(data, ["group_id", "amount", "total"])


@pytest.fixture(scope="module")
def df_totals_mismatch(spark):
    """DataFrame with mismatched totals.

    Schema: group_id STRING, amount INT, total INT
    Rows: Data where detail amounts don't sum to total
    """
    data = [
        ("G1", 100, 999),  # Total should be 300, not 999
        ("G1", 100, 999),
        ("G1", 100, 999),
    ]
    return spark.createDataFrame(data, ["group_id", "amount", "total"])


# =============================================================================
# Flow Completeness Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_flow(spark):
    """DataFrame for flow completeness tests.

    Schema: stage STRING, count INT
    Rows: Data representing flow through pipeline stages
    """
    data = [
        ("input", 100),
        ("processed", 95),
        ("output", 90),
    ]
    return spark.createDataFrame(data, ["stage", "count"])


@pytest.fixture(scope="module")
def df_flow_incomplete(spark):
    """DataFrame with incomplete flow (high drop rate).

    Schema: stage STRING, count INT
    Rows: Data where output is much less than input
    """
    data = [
        ("input", 100),
        ("processed", 50),  # 50% loss
        ("output", 20),  # 80% total loss
    ]
    return spark.createDataFrame(data, ["stage", "count"])


# =============================================================================
# Cross-table Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def df_cross_left(spark):
    """Left DataFrame for cross-table equality tests.

    Schema: id INT, value INT
    """
    data = [(1, 100), (2, 200), (3, 300)]
    return spark.createDataFrame(data, ["id", "value"])


@pytest.fixture(scope="module")
def df_cross_right(spark):
    """Right DataFrame for cross-table equality tests.

    Schema: id INT, total INT
    """
    data = [(1, 100), (2, 200), (3, 300)]
    return spark.createDataFrame(data, ["id", "total"])


@pytest.fixture(scope="module")
def df_cross_right_mismatch(spark):
    """Right DataFrame with mismatched values for cross-table tests.

    Schema: id INT, total INT
    """
    data = [(1, 100), (2, 999), (3, 300)]  # id=2 is different
    return spark.createDataFrame(data, ["id", "total"])
