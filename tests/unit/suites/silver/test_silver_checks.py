# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Silver DQ Test Suite checks.

Tests verify that Silver-layer checks work correctly with proper configurations.
Each test uses fixtures from tests/fixtures/silver.py and tests/fixtures/dataframes.py.

Note: Checks requiring actual Delta tables (referential_integrity, totals_consistency,
cross_table_equality, temporal_consistency, flow_completeness) have placeholder tests
marked as skip - they require Unity Catalog and will be run as integration tests.
"""

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

# Mark all tests in this module as requiring Spark
pytestmark = pytest.mark.spark


# =============================================================================
# Suite Identity Tests
# =============================================================================


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.SUITE_IDENTITY)
class TestSilverSuiteIdentity:
    """Tests for Silver suite identity properties."""

    @allure.title("Suite name is silver")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_name(self, silver_suite):
        """Suite name should be 'silver'."""
        with allure.step("Verify: Suite name is silver"):
            assert silver_suite._suite_name == "silver"

    @allure.title("Suite config key is silver")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_config_key(self, silver_suite):
        """Suite config key should be 'silver'."""
        with allure.step("Verify: Suite config key is silver"):
            assert silver_suite._suite_config_key == "silver"


# =============================================================================
# Idempotency Tests
# =============================================================================


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.IDEMPOTENCY)
class TestSilverIdempotency:
    """Tests for idempotency check."""

    @allure.title("Idempotency passes with unique records")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_idempotency_pass(self, silver_suite, df_unique):
        """Idempotency should pass when all key combinations are unique."""
        with allure.step("Setup test data"):
            cfg = {"key_columns": "id,code"}

        with allure.step("Execute idempotency check"):
            passed, details, column = silver_suite._check_idempotency(df_unique, cfg)

        with allure.step("Verify: Idempotency should pass with unique records"):
            assert passed is True
            assert "All" in details and "keys unique" in details
            assert column == "id,code"

    @allure.title("Idempotency fails with duplicate records")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_idempotency_fail(self, silver_suite, df_duplicates):
        """Idempotency should fail when duplicate key combinations exist."""
        with allure.step("Setup test data"):
            cfg = {"key_columns": "id,code"}

        with allure.step("Execute idempotency check"):
            passed, details, column = silver_suite._check_idempotency(df_duplicates, cfg)

        with allure.step("Verify: Idempotency should fail with duplicate records"):
            assert passed is False
            assert "Found 1 duplicate keys" in details
            assert column == "id,code"

    @allure.title("Idempotency fails when key_column config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_idempotency_missing_config(self, silver_suite, df_unique):
        """Idempotency should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {}

        with allure.step("Execute idempotency check"):
            passed, details, _column = silver_suite._check_idempotency(df_unique, cfg)

        with allure.step("Verify: Idempotency should fail when config is missing"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Idempotency passes with key_column as list")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_idempotency_key_column_list(self, silver_suite, df_unique):
        """Idempotency should pass when key_column is provided as a list."""
        with allure.step("Setup test data"):
            cfg = {"key_columns": ["id", "code"]}

        with allure.step("Execute idempotency check"):
            passed, details, column = silver_suite._check_idempotency(df_unique, cfg)

        with allure.step("Verify: Idempotency should pass with key_column as list"):
            assert passed is True
            assert "All" in details and "keys unique" in details
            assert column == "id,code"

    @allure.title("Idempotency sets debug fields on failure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_idempotency_debug_fields_on_failure(self, silver_suite, df_duplicates):
        """Idempotency should set debug fields (failure_stats, investigation_query, execution) on failure."""
        with allure.step("Setup test data"):
            cfg = {"key_columns": "id,code"}

        with allure.step("Execute idempotency check"):
            passed, _details, _column = silver_suite._check_idempotency(df_duplicates, cfg)

        with allure.step("Verify: debug fields should be set on failure"):
            assert passed is False
            assert silver_suite._last_failure_stats is not None
            assert silver_suite._last_failure_stats["failure_type"] == "duplicate_key"
            assert silver_suite._last_failure_stats["affected_columns"] == ["id", "code"]
            assert silver_suite._last_investigation_query is not None
            assert "duplicate" in silver_suite._last_investigation_query["description"].lower()
            assert silver_suite._last_execution is not None
            assert "duration_ms" in silver_suite._last_execution


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.STATUS_TRANSITION)
class TestSilverStatusTransition:
    """Tests for status_transition check."""

    @allure.title("Status transition passes with valid flow")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_status_transition_pass(self, silver_suite, df_transitions):
        """Status transition should pass when all transitions follow allowed flow."""
        with allure.step("Setup test data"):
            cfg = {
                "key_columns": "entity_id",
                "status_column": "status",
                "timestamp_column": "ts",
                "allowed_flow": ["pending", "active", "completed"],
            }

        with allure.step("Execute status transition check"):
            passed, details, column = silver_suite._check_status_transition(df_transitions, cfg)

        with allure.step("Verify: Status transition should pass with valid flow"):
            assert passed is True
            assert "All" in details and "transitions valid" in details
            assert column == "status"

    @allure.title("Status transition fails with invalid flow")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_status_transition_fail(self, silver_suite, df_invalid_transitions):
        """Status transition should fail when transitions violate allowed flow."""
        with allure.step("Setup test data"):
            cfg = {
                "key_columns": "entity_id",
                "status_column": "status",
                "timestamp_column": "ts",
                "allowed_flow": ["pending", "active", "completed"],
            }

        with allure.step("Execute status transition check"):
            passed, _details, column = silver_suite._check_status_transition(df_invalid_transitions, cfg)

        with allure.step("Verify: Status transition should fail with invalid flow"):
            assert passed is False
            assert column == "status"

    @allure.title("Status transition fails when config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_status_transition_missing_config(self, silver_suite, df_transitions):
        """Status transition should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {"key_columns": "entity_id"}  # Missing status_column and timestamp_column

        with allure.step("Execute status transition check"):
            passed, details, _column = silver_suite._check_status_transition(df_transitions, cfg)

        with allure.step("Verify: Status transition should fail when config is missing"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Status transition detects unknown statuses via debug fields")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_status_transition_unknown_statuses(self, silver_suite, df_transitions_unknown):
        """Status transition should detect unknown statuses and report via failure_stats."""
        with allure.step("Setup test data"):
            cfg = {
                "key_columns": "entity_id",
                "status_column": "status",
                "timestamp_column": "ts",
                "allowed_flow": ["pending", "active", "completed"],
            }

        with allure.step("Execute status transition check"):
            passed, details, column = silver_suite._check_status_transition(df_transitions_unknown, cfg)

        with allure.step("Verify: Status transition detects unknown statuses"):
            assert passed is False
            assert "invalid transitions" in details
            assert column == "status"
            # Unknown statuses are in failure_stats diagnostic
            assert silver_suite._last_failure_stats is not None
            assert "UNKNOWN_STATUS" in silver_suite._last_failure_stats["diagnostic"]["unknown_statuses"]


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.CARDINALITY)
class TestSilverCardinality:
    """Tests for cardinality check."""

    @allure.title("Cardinality passes when all groups meet minimum")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cardinality_pass(self, silver_suite, df_cardinality):
        """Cardinality should pass when all groups have at least minimum rows."""
        with allure.step("Setup test data"):
            cfg = {"group_by": "group_id", "min_count": 1}

        with allure.step("Execute cardinality check"):
            passed, details, column = silver_suite._check_cardinality(df_cardinality, cfg)

        with allure.step("Verify: Cardinality should pass when minimum is met"):
            assert passed is True
            assert "All" in details and "groups meet minimum" in details
            assert column == "group_id"

    @allure.title("Cardinality fails when groups below minimum")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_cardinality_fail(self, silver_suite, df_cardinality):
        """Cardinality should fail when some groups have fewer than minimum rows."""
        with allure.step("Setup test data"):
            # G3 has only 1 item, so min=2 should fail
            cfg = {"group_by": "group_id", "min_count": 2}

        with allure.step("Execute cardinality check"):
            passed, details, column = silver_suite._check_cardinality(df_cardinality, cfg)

        with allure.step("Verify: Cardinality should fail when groups below minimum"):
            assert passed is False
            assert "1 groups below minimum" in details
            assert column == "group_id"

    @allure.title("Cardinality fails when config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_cardinality_missing_config(self, silver_suite, df_cardinality):
        """Cardinality should fail when group_by config is missing."""
        with allure.step("Setup test data"):
            cfg = {}

        with allure.step("Execute cardinality check"):
            passed, details, column = silver_suite._check_cardinality(df_cardinality, cfg)

        with allure.step("Verify: Cardinality should fail when config is missing"):
            assert passed is False
            assert "Missing config" in details
            assert column is None

    @allure.title("Cardinality supports group_by as list")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cardinality_group_by_list(self, silver_suite, spark):
        """Cardinality should support group_by as a list of columns."""
        with allure.step("Setup test data"):
            # Create DataFrame with multiple group columns
            data = [
                ("A", "X", 1),
                ("A", "X", 2),
                ("A", "Y", 1),
                ("B", "X", 1),
            ]
            df = spark.createDataFrame(data, ["group1", "group2", "item_id"])
            cfg = {"group_by": ["group1", "group2"], "min_count": 1}

        with allure.step("Execute cardinality check"):
            passed, details, column = silver_suite._check_cardinality(df, cfg)

        with allure.step("Verify: Cardinality supports group_by as list"):
            assert passed is True
            assert "All" in details and "groups meet minimum" in details
            assert column == "group1,group2"


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.ROW_TEMPORAL_CONSISTENCY)
class TestSilverRowTemporalConsistency:
    """Tests for row_temporal_consistency check."""

    @allure.title("Row temporal consistency passes with valid timestamps")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_row_temporal_pass(self, silver_suite, df_temporal):
        """Row temporal consistency should pass when left >= right timestamps."""
        with allure.step("Setup test data"):
            # Note: left_timestamp >= right_timestamp means end_time >= start_time
            cfg = {"left_timestamp_column": "end_time", "right_timestamp_column": "start_time"}

        with allure.step("Execute row temporal consistency check"):
            passed, details, column = silver_suite._check_row_temporal_consistency(df_temporal, cfg)

        with allure.step("Verify: Row temporal consistency should pass with valid timestamps"):
            assert passed is True
            assert "All" in details and "row timestamps ordered" in details
            assert column == "end_time"

    @allure.title("Row temporal consistency fails with invalid timestamps")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_row_temporal_fail(self, silver_suite, df_temporal_invalid):
        """Row temporal consistency should fail when left < right timestamps."""
        with allure.step("Setup test data"):
            cfg = {"left_timestamp_column": "end_time", "right_timestamp_column": "start_time"}

        with allure.step("Execute row temporal consistency check"):
            passed, details, column = silver_suite._check_row_temporal_consistency(df_temporal_invalid, cfg)

        with allure.step("Verify: Row temporal consistency should fail with invalid timestamps"):
            assert passed is False
            assert "1 timestamp inconsistencies" in details
            assert column == "end_time"

    @allure.title("Row temporal consistency fails when config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_row_temporal_missing_config(self, silver_suite, df_temporal):
        """Row temporal consistency should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {"left_timestamp_column": "end_time"}  # Missing right_timestamp_column

        with allure.step("Execute row temporal consistency check"):
            passed, details, _column = silver_suite._check_row_temporal_consistency(df_temporal, cfg)

        with allure.step("Verify: Row temporal consistency should fail when config is missing"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Row temporal consistency sets debug fields on failure")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_row_temporal_debug_fields_on_failure(self, silver_suite, df_temporal_invalid):
        """Row temporal consistency should set debug fields on failure."""
        with allure.step("Setup test data"):
            cfg = {"left_timestamp_column": "end_time", "right_timestamp_column": "start_time"}

        with allure.step("Execute row temporal consistency check"):
            passed, _details, _column = silver_suite._check_row_temporal_consistency(df_temporal_invalid, cfg)

        with allure.step("Verify: debug fields should be set on failure"):
            assert passed is False
            assert silver_suite._last_failure_stats is not None
            assert silver_suite._last_failure_stats["failure_type"] == "row_temporal_violation"
            assert silver_suite._last_execution is not None
            assert "duration_ms" in silver_suite._last_execution


# =============================================================================
# Integration Test Placeholders (require Unity Catalog / Databricks)
# =============================================================================


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.REFERENTIAL_INTEGRITY)
class TestSilverReferentialIntegrity:
    """Tests for referential_integrity check.

    Note: These tests require spark.table() with catalog.schema.table format.
    They are placeholders for integration tests on Databricks.
    """

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Referential integrity passes when all FKs exist")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_referential_integrity_pass(self, silver_suite):
        """Referential integrity should pass when all foreign keys exist in target table."""
        # Integration test: requires spark.table() with catalog.schema.table
        pass

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Referential integrity fails with orphan FKs")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_referential_integrity_fail(self, silver_suite):
        """Referential integrity should fail when orphan foreign keys exist."""
        # Integration test: requires spark.table() with catalog.schema.table
        pass


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.TOTALS_CONSISTENCY)
class TestSilverTotalsConsistency:
    """Tests for totals_consistency check.

    Note: These tests require spark.table() with catalog.schema.table format.
    They are placeholders for integration tests on Databricks.
    """

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Totals consistency passes when header matches detail sum")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_totals_consistency_pass(self, silver_suite):
        """Totals should pass when header amount equals sum of detail rows."""
        # Integration test: requires spark.table() for detail table
        pass

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Totals consistency fails when amounts mismatch")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_totals_consistency_fail(self, silver_suite):
        """Totals should fail when header amount differs from sum of detail rows."""
        # Integration test: requires spark.table() for detail table
        pass


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.CROSS_TABLE_EQUALITY)
class TestSilverCrossTableEquality:
    """Tests for cross_table_equality check.

    Note: These tests require spark.table() with catalog.schema.table format.
    They are placeholders for integration tests on Databricks.
    """

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Cross table equality passes when values match")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cross_table_equality_pass(self, silver_suite):
        """Cross table equality should pass when column values match after join."""
        # Integration test: requires spark.table() for both tables
        pass

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Cross table equality fails when values mismatch")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_cross_table_equality_fail(self, silver_suite):
        """Cross table equality should fail when column values differ."""
        # Integration test: requires spark.table() for both tables
        pass


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.TEMPORAL_CONSISTENCY)
class TestSilverTemporalConsistency:
    """Tests for temporal_consistency check (cross-table).

    Note: These tests require spark.table() with catalog.schema.table format.
    They are placeholders for integration tests on Databricks.
    """

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Temporal consistency passes with valid timestamp order")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_temporal_consistency_pass(self, silver_suite):
        """Temporal consistency should pass when left timestamp <= right timestamp."""
        # Integration test: requires spark.table() for both tables
        pass

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Temporal consistency fails with invalid timestamp order")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_temporal_consistency_fail(self, silver_suite):
        """Temporal consistency should fail when left timestamp > right timestamp."""
        # Integration test: requires spark.table() for both tables
        pass


@allure.feature(AllureFeature.SILVER_SUITE)
@allure.story(AllureStory.FLOW_COMPLETENESS)
class TestSilverFlowCompleteness:
    """Tests for flow_completeness check.

    Note: These tests require spark.table() with catalog.schema.table format.
    They are placeholders for integration tests on Databricks.
    """

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Flow completeness passes when all source records have target")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_flow_completeness_pass(self, silver_suite):
        """Flow completeness should pass when all source records exist in target."""
        # Integration test: requires spark.table() for source table
        pass

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Flow completeness fails when source records missing in target")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_flow_completeness_fail(self, silver_suite):
        """Flow completeness should fail when source records are missing from target."""
        # Integration test: requires spark.table() for source table
        pass
