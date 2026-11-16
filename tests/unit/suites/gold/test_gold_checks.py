# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Gold DQ Test Suite checks.

Tests verify that Gold-layer checks work correctly with proper configurations.
Each test uses fixtures from tests/fixtures/gold.py and tests/fixtures/dataframes.py.

Note: Checks requiring actual Delta tables (reconciliation_expr, referential_integrity,
coverage, cross_gold, trend) are tested in integration tests.
"""

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

# Mark all tests in this module as requiring Spark
pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.SUITE_IDENTITY)
class TestGoldSuiteIdentity:
    """Tests for suite identity properties."""

    @allure.title("Suite name returns gold")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_name(self, gold_suite):
        """Suite name should return 'gold'."""
        with allure.step("Verify: suite name is gold"):
            assert gold_suite._suite_name == "gold"

    @allure.title("Default severity is critical")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_severity_default(self, gold_suite):
        """Gold layer should default to critical severity."""
        with allure.step("Setup test data"):
            cfg = {}

        with allure.step("Verify: default severity is critical"):
            assert gold_suite._severity(cfg) == "critical"

    @allure.title("Severity can be overridden")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_severity_override(self, gold_suite):
        """Severity can be overridden via config."""
        with allure.step("Setup test data"):
            cfg = {"severity": "warning"}

        with allure.step("Verify: severity is overridden"):
            assert gold_suite._severity(cfg) == "warning"


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.UNIQUENESS)
class TestGoldUniqueness:
    """Tests for uniqueness check."""

    @allure.title("Uniqueness passes with unique records")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_uniqueness_pass(self, gold_suite, df_unique):
        """Uniqueness should pass when all column combinations are unique."""
        with allure.step("Setup test data"):
            cfg = {"columns": ["id", "code"]}

        with allure.step("Execute uniqueness check"):
            passed, details, column = gold_suite._check_uniqueness(df_unique, cfg)

        with allure.step("Verify: Uniqueness should pass with unique records"):
            assert passed is True
            assert "All 3 rows unique" in details
            assert column == "id,code"

    @allure.title("Uniqueness fails with duplicate records")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_uniqueness_fail(self, gold_suite, df_duplicates):
        """Uniqueness should fail when duplicate column combinations exist."""
        with allure.step("Setup test data"):
            cfg = {"columns": ["id", "code"]}

        with allure.step("Execute uniqueness check"):
            passed, details, column = gold_suite._check_uniqueness(df_duplicates, cfg)

        with allure.step("Verify: Uniqueness should fail with duplicate records"):
            assert passed is False
            assert "duplicate rows" in details
            assert column == "id,code"

    @allure.title("Uniqueness fails when columns config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_uniqueness_missing_config(self, gold_suite, df_unique):
        """Uniqueness should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {}

        with allure.step("Execute uniqueness check"):
            passed, details, _column = gold_suite._check_uniqueness(df_unique, cfg)

        with allure.step("Verify: Uniqueness should fail when config is missing"):
            assert passed is False
            assert "Missing config" in details


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.VOLUME)
class TestGoldVolume:
    """Tests for volume check."""

    @allure.title("Volume passes within min/max bounds")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_volume_pass(self, gold_suite, df_metrics):
        """Volume should pass when metric is within bounds."""
        with allure.step("Setup test data"):
            # df_metrics total is 700
            cfg = {"metric": "SUM(amount)", "min_value": 500, "max_value": 1000}
            table_ctx = {"catalog": "test", "schema": "test", "table": "test"}

        with allure.step("Execute volume check"):
            passed, details, _column = gold_suite._check_volume(df_metrics, cfg, table_ctx)

        with allure.step("Verify: Volume should pass within bounds"):
            assert passed is True
            assert "Volume OK: 700" in details

    @allure.title("Volume fails below minimum")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_volume_fail_below_min(self, gold_suite, df_metrics_low_volume):
        """Volume should fail when metric is below minimum."""
        with allure.step("Setup test data"):
            # df_metrics_low_volume total is 50
            cfg = {"metric": "SUM(amount)", "min_value": 100}
            table_ctx = {"catalog": "test", "schema": "test", "table": "test"}

        with allure.step("Execute volume check"):
            passed, details, _column = gold_suite._check_volume(df_metrics_low_volume, cfg, table_ctx)

        with allure.step("Verify: Volume should fail below minimum"):
            assert passed is False
            assert "Volume 50" in details
            assert "below minimum" in details

    @allure.title("Volume fails above maximum")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_volume_fail_above_max(self, gold_suite, df_metrics):
        """Volume should fail when metric exceeds maximum."""
        with allure.step("Setup test data"):
            # df_metrics total is 700
            cfg = {"metric": "SUM(amount)", "max_value": 500}
            table_ctx = {"catalog": "test", "schema": "test", "table": "test"}

        with allure.step("Execute volume check"):
            passed, details, _column = gold_suite._check_volume(df_metrics, cfg, table_ctx)

        with allure.step("Verify: Volume should fail above maximum"):
            assert passed is False
            assert "Volume 700" in details
            assert "above maximum" in details

    @allure.title("Volume with grouped aggregation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_volume_grouped(self, gold_suite, df_metrics):
        """Volume should work with group_by aggregation."""
        with allure.step("Setup test data"):
            # df_metrics: G1=300, G2=400, total=700
            cfg = {"metric": "SUM(amount)", "group_by": ["group_id"], "min_value": 500}
            table_ctx = {"catalog": "test", "schema": "test", "table": "test"}

        with allure.step("Execute volume check"):
            passed, details, _column = gold_suite._check_volume(df_metrics, cfg, table_ctx)

        with allure.step("Verify: Volume with group_by should pass"):
            assert passed is True
            assert "Volume OK: 700" in details


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.COMPLETENESS)
class TestGoldCompleteness:
    """Tests for completeness check."""

    @allure.title("Completeness passes when all values present")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_completeness_pass(self, gold_suite, df_dimensions):
        """Completeness should pass when all expected values are present."""
        with allure.step("Setup test data"):
            cfg = {"column": "category", "expected_values": ["A", "B", "C"]}

        with allure.step("Execute completeness check"):
            passed, details, column = gold_suite._check_completeness(df_dimensions, cfg)

        with allure.step("Verify: Completeness should pass with all values"):
            assert passed is True
            assert "All 3 expected values present" in details
            assert column == "category"

    @allure.title("Completeness fails with missing values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_completeness_fail_missing(self, gold_suite, df_dimensions_incomplete):
        """Completeness should fail when expected values are missing."""
        with allure.step("Setup test data"):
            cfg = {"column": "category", "expected_values": ["A", "B", "C"]}

        with allure.step("Execute completeness check"):
            passed, details, column = gold_suite._check_completeness(df_dimensions_incomplete, cfg)

        with allure.step("Verify: Completeness should fail with missing values"):
            assert passed is False
            assert "C" in details  # Missing value
            assert column == "category"

    @allure.title("Completeness allows extra values by default")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_completeness_allows_extra(self, gold_suite, df_dimensions):
        """Completeness should allow extra values when allow_extra=True (default)."""
        with allure.step("Setup test data"):
            # df_dimensions has A, B, C but we only expect A, B
            cfg = {"column": "category", "expected_values": ["A", "B"]}

        with allure.step("Execute completeness check"):
            passed, _details, column = gold_suite._check_completeness(df_dimensions, cfg)

        with allure.step("Verify: Completeness should pass with extra values allowed"):
            assert passed is True
            assert column == "category"

    @allure.title("Completeness fails with extra values when not allowed")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_completeness_fail_extra(self, gold_suite, df_dimensions):
        """Completeness should fail when extra values exist and allow_extra=False."""
        with allure.step("Setup test data"):
            cfg = {"column": "category", "expected_values": ["A", "B"], "allow_extra": False}

        with allure.step("Execute completeness check"):
            passed, details, column = gold_suite._check_completeness(df_dimensions, cfg)

        with allure.step("Verify: Completeness should fail with extra values"):
            assert passed is False
            assert "C" in details  # Extra value
            assert column == "category"

    @allure.title("Completeness fails when config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_completeness_missing_config(self, gold_suite, df_dimensions):
        """Completeness should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {"column": "category"}  # Missing expected_values

        with allure.step("Execute completeness check"):
            passed, details, _column = gold_suite._check_completeness(df_dimensions, cfg)

        with allure.step("Verify: Completeness should fail when config is missing"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Completeness fails when column not found")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_completeness_column_not_found(self, gold_suite, df_dimensions):
        """Completeness should fail when column doesn't exist."""
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent_column", "expected_values": ["A", "B"]}

        with allure.step("Execute completeness check"):
            passed, details, column = gold_suite._check_completeness(df_dimensions, cfg)

        with allure.step("Verify: Completeness should fail when column not found"):
            assert passed is False
            assert "not found" in details
            assert column == "nonexistent_column"


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.HELPER_METHODS)
class TestExtractMetricFromDetails:
    """Tests for _extract_metric_from_details helper method."""

    @allure.title("Extract metric value from details string")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_extract_metric_value(self, gold_suite):
        """Should extract metric= value from details string."""
        with allure.step("Setup test data"):
            details = "metric=123;status=ok"

        with allure.step("Execute extraction"):
            result = gold_suite._extract_metric_from_details(details)

        with allure.step("Verify: metric value should be extracted"):
            assert result == 123.0

    @allure.title("Extract value from details string")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_extract_value(self, gold_suite):
        """Should extract value= from details string."""
        with allure.step("Setup test data"):
            details = "value=456.78;extra=info"

        with allure.step("Execute extraction"):
            result = gold_suite._extract_metric_from_details(details)

        with allure.step("Verify: value should be extracted"):
            assert result == 456.78

    @allure.title("Return None when no metric in details")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_extract_no_metric(self, gold_suite):
        """Should return None when no numeric value found."""
        with allure.step("Setup test data"):
            details = "status=ok;result=passed"

        with allure.step("Execute extraction"):
            result = gold_suite._extract_metric_from_details(details)

        with allure.step("Verify: should return None"):
            assert result is None

    @allure.title("Return None on invalid metric value")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_extract_invalid_value(self, gold_suite):
        """Should return None when metric value is not a valid number."""
        with allure.step("Setup test data"):
            details = "metric=invalid;value=not_a_number"

        with allure.step("Execute extraction"):
            result = gold_suite._extract_metric_from_details(details)

        with allure.step("Verify: should return None"):
            assert result is None

    @allure.title("Extract current value from new trend format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_extract_current_value_new_format(self, gold_suite):
        """Should extract current= value from new trend format."""
        with allure.step("Setup test data"):
            details = "Trend OK (stable): current=123.5, baseline=100 (deviation 5.0%)"

        with allure.step("Execute extraction"):
            result = gold_suite._extract_metric_from_details(details)

        with allure.step("Verify: current value should be extracted"):
            assert result == 123.5

    @allure.title("Extract volume value from new format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_extract_volume_new_format(self, gold_suite):
        """Should extract value from Volume OK: X format."""
        with allure.step("Setup test data"):
            details = "Volume OK: 500 (within 10% of baseline 480)"

        with allure.step("Execute extraction"):
            result = gold_suite._extract_metric_from_details(details)

        with allure.step("Verify: volume value should be extracted"):
            assert result == 500.0


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.RECONCILIATION_EXPR)
class TestGoldReconciliationExpr:
    """Tests for reconciliation_expr check using temp views."""

    @allure.title("Global reconciliation passes when values match")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_reconciliation_global_pass(self, gold_suite, df_gold_recon, df_silver_recon):
        """Global reconciliation should pass when gold and silver match."""
        with allure.step("Setup test data"):
            df_silver_recon.createOrReplaceTempView("test_silver_recon")
            cfg = {
                "silver": {"table": "test_silver_recon"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold_recon, cfg)

        with allure.step("Verify: reconciliation should pass"):
            assert passed is True
            assert "Reconciliation OK" in details

    @allure.title("Global reconciliation fails when values mismatch")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_reconciliation_global_fail(self, gold_suite, df_gold_recon, df_silver_recon_mismatch):
        """Global reconciliation should fail when values exceed tolerance."""
        with allure.step("Setup test data"):
            df_silver_recon_mismatch.createOrReplaceTempView("test_silver_mismatch")
            cfg = {
                "silver": {"table": "test_silver_mismatch"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold_recon, cfg)

        with allure.step("Verify: reconciliation should fail"):
            assert passed is False
            assert "exceeds" in details

    @allure.title("Grouped reconciliation passes")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_reconciliation_grouped_pass(self, spark, gold_suite):
        """Grouped reconciliation should pass when all groups match."""
        with allure.step("Setup test data"):
            gold_data = [("G1", 100), ("G2", 200)]
            silver_data = [("G1", 100), ("G2", 200)]
            df_gold = spark.createDataFrame(gold_data, ["group_id", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["group_id", "amount"])
            df_silver.createOrReplaceTempView("test_silver_grouped")
            cfg = {
                "silver": {"table": "test_silver_grouped"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "group_by": ["group_id"],
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold, cfg)

        with allure.step("Verify: grouped reconciliation should pass"):
            assert passed is True
            assert "groups reconcile" in details

    @allure.title("Reconciliation with filter")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_reconciliation_with_filter(self, spark, gold_suite):
        """Reconciliation should apply filters before comparison."""
        with allure.step("Setup test data"):
            gold_data = [(1, "active", 100), (2, "active", 200), (3, "inactive", 999)]
            silver_data = [(1, "active", 100), (2, "active", 200), (3, "inactive", 999)]
            df_gold = spark.createDataFrame(gold_data, ["id", "status", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["id", "status", "amount"])
            df_silver.createOrReplaceTempView("test_silver_filtered")
            cfg = {
                "silver": {"table": "test_silver_filtered"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "gold_filter": "status = 'active'",
                "silver_filter": "status = 'active'",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold, cfg)

        with allure.step("Verify: filtered reconciliation should pass"):
            assert passed is True
            assert "Reconciliation OK" in details

    @allure.title("Reconciliation fails when config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_reconciliation_missing_config(self, gold_suite, df_gold_recon):
        """Reconciliation should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {"silver": {"table": "test_table"}}  # Missing gold and silver expressions

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold_recon, cfg)

        with allure.step("Verify: should fail with missing config"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Reconciliation with join_filter restricts silver domain")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_reconciliation_with_join_filter(self, spark, gold_suite):
        """Reconciliation should restrict silver to gold domain via join_filter."""
        with allure.step("Setup test data"):
            # Gold has IDs 1, 2
            gold_data = [(1, 100), (2, 200)]
            # Silver has IDs 1, 2, 3 (3 should be excluded)
            silver_data = [(1, 100), (2, 200), (3, 999)]
            df_gold = spark.createDataFrame(gold_data, ["id", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["id", "amount"])
            df_silver.createOrReplaceTempView("test_silver_join_filter")
            cfg = {
                "silver": {"table": "test_silver_join_filter"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "gold_join_columns": "id",
                "silver_join_columns": "id",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold, cfg)

        with allure.step("Verify: reconciliation should pass (silver filtered to gold IDs)"):
            assert passed is True
            assert "Reconciliation OK" in details

    @allure.title("Reconciliation restricts silver to gold domain on composite join columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_reconciliation_with_composite_join_columns(self, spark, gold_suite):
        """List-valued gold/silver join columns restrict silver on all keys together."""
        with allure.step("Setup test data"):
            # (A, 2) exists in silver only: excluded by (tenant, id), not by id alone.
            gold_data = [("A", 1, 100), ("B", 2, 200)]
            silver_data = [("A", 1, 100), ("B", 2, 200), ("A", 2, 999)]
            df_gold = spark.createDataFrame(gold_data, ["tenant", "id", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["tenant_code", "item_id", "amount"])
            df_silver.createOrReplaceTempView("test_silver_composite_join")
            cfg = {
                "silver": {"table": "test_silver_composite_join"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "gold_join_columns": ["tenant", "id"],
                "silver_join_columns": ["tenant_code", "item_id"],
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold, cfg)

        with allure.step("Verify: silver restricted on both keys"):
            assert passed is True
            assert "Reconciliation OK" in details

    @allure.title("Reconciliation rejects join columns of different length")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_reconciliation_join_columns_length_mismatch(self, spark, gold_suite):
        """gold_join_columns and silver_join_columns must pair up one to one."""
        with allure.step("Setup test data"):
            df_gold = spark.createDataFrame([("A", 1, 100)], ["tenant", "id", "amount"])
            df_silver = spark.createDataFrame([("A", 1, 100)], ["tenant", "id", "amount"])
            df_silver.createOrReplaceTempView("test_silver_join_len")
            cfg = {
                "silver": {"table": "test_silver_join_len"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "gold_join_columns": ["tenant", "id"],
                "silver_join_columns": ["id"],
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold, cfg)

        with allure.step("Verify: config error reported"):
            assert passed is False
            assert "same length" in details

    @allure.title("Reconciliation rejects join columns given on one side only")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_reconciliation_join_columns_one_side_only(self, spark, gold_suite):
        """Without both sides the domain restriction cannot apply: fail instead of comparing all silver."""
        with allure.step("Setup test data"):
            df_gold = spark.createDataFrame([(1, 100)], ["id", "amount"])
            df_silver = spark.createDataFrame([(1, 100), (2, 999)], ["id", "amount"])
            df_silver.createOrReplaceTempView("test_silver_join_one_side")
            cfg = {
                "silver": {"table": "test_silver_join_one_side"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "gold_join_columns": ["id"],
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold, cfg)

        with allure.step("Verify: config error reported"):
            assert passed is False
            assert "gold_join_columns and silver_join_columns" in details

    @allure.title("Reconciliation with both zero values passes")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_reconciliation_both_zero(self, spark, gold_suite):
        """Reconciliation should pass when both gold and silver are zero."""
        with allure.step("Setup test data"):
            gold_data = [(1, 0)]
            silver_data = [(1, 0)]
            df_gold = spark.createDataFrame(gold_data, ["id", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["id", "amount"])
            df_silver.createOrReplaceTempView("test_silver_both_zero")
            cfg = {
                "silver": {"table": "test_silver_both_zero"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold, cfg)

        with allure.step("Verify: reconciliation should pass with both zero"):
            assert passed is True
            assert "Reconciliation OK" in details

    @allure.title("Reconciliation with silver zero fails")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_reconciliation_silver_zero(self, spark, gold_suite):
        """Reconciliation should fail when silver is zero but gold is not."""
        with allure.step("Setup test data"):
            gold_data = [(1, 100)]
            silver_data = [(1, 0)]
            df_gold = spark.createDataFrame(gold_data, ["id", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["id", "amount"])
            df_silver.createOrReplaceTempView("test_silver_zero")
            cfg = {
                "silver": {"table": "test_silver_zero"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(amount)",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute reconciliation check"):
            passed, details, _name = gold_suite._check_reconciliation_expr(df_gold, cfg)

        with allure.step("Verify: reconciliation should fail with 100% diff"):
            assert passed is False
            assert "100.0%" in details


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.GOLD_REFERENTIAL_INTEGRITY)
class TestGoldReferentialIntegrity:
    """Tests for referential_integrity check.

    Note: This check requires catalog.schema.table format which cannot be simulated
    with temp views. These tests are marked as integration tests.
    """

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Referential integrity passes when all FKs exist")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_referential_integrity_pass(self, spark, gold_suite):
        """RI should pass when all gold FKs exist in silver.

        Note: Requires actual catalog.schema.table setup (Unity Catalog).
        """
        with allure.step("Setup test data"):
            gold_data = [(1, "A"), (2, "B"), (3, "C")]
            df_gold = spark.createDataFrame(gold_data, ["id", "fk_code"])
            cfg = {
                "gold_column": "fk_code",
                "silver": {
                    "catalog": "test_catalog",
                    "schema": "test_schema",
                    "table": "test_silver_pk",
                    "column": "code",
                },
            }

        with allure.step("Execute referential integrity check"):
            passed, details, column = gold_suite._check_referential_integrity(df_gold, cfg)

        with allure.step("Verify: RI should pass"):
            assert passed is True
            assert "fk_violations=0" in details
            assert column == "fk_code"

    @pytest.mark.skip(reason="Requires Unity Catalog - run as integration test")
    @allure.title("Referential integrity fails with missing FK")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_referential_integrity_fail(self, spark, gold_suite):
        """RI should fail when gold FK doesn't exist in silver.

        Note: Requires actual catalog.schema.table setup (Unity Catalog).
        """
        with allure.step("Setup test data"):
            gold_data = [(1, "A"), (2, "B"), (3, "MISSING")]
            df_gold = spark.createDataFrame(gold_data, ["id", "fk_code"])
            cfg = {
                "gold_column": "fk_code",
                "silver": {
                    "catalog": "test_catalog",
                    "schema": "test_schema",
                    "table": "test_silver_pk",
                    "column": "code",
                },
            }

        with allure.step("Execute referential integrity check"):
            passed, details, column = gold_suite._check_referential_integrity(df_gold, cfg)

        with allure.step("Verify: RI should fail"):
            assert passed is False
            assert "fk_violations=1" in details
            assert column == "fk_code"


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.COVERAGE)
class TestGoldCoverage:
    """Tests for coverage check using temp views."""

    @allure.title("Global coverage passes when values match")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_coverage_global_pass(self, spark, gold_suite, df_coverage_gold, df_coverage_silver):
        """Global coverage should pass when aggregates match within tolerance."""
        with allure.step("Setup test data"):
            df_coverage_silver.createOrReplaceTempView("test_coverage_silver")
            cfg = {
                "silver": {"table": "test_coverage_silver"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute coverage check"):
            passed, details, _column = gold_suite._check_coverage(df_coverage_gold, cfg)

        with allure.step("Verify: global coverage should pass"):
            assert passed is True
            assert "Coverage OK" in details or ("All" in details and "keys within" in details)

    @allure.title("Global coverage fails when values mismatch")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_coverage_global_fail(self, spark, gold_suite, df_coverage_gold):
        """Global coverage should fail when difference exceeds tolerance."""
        with allure.step("Setup test data"):
            silver_data = [("G1", 999), ("G2", 999)]
            df_silver = spark.createDataFrame(silver_data, ["group_id", "total"])
            df_silver.createOrReplaceTempView("test_coverage_silver_fail")
            cfg = {
                "silver": {"table": "test_coverage_silver_fail"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute coverage check"):
            passed, details, _column = gold_suite._check_coverage(df_coverage_gold, cfg)

        with allure.step("Verify: global coverage should fail"):
            assert passed is False
            assert "exceeds" in details

    @allure.title("Keyed coverage passes")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_coverage_keyed_pass(self, spark, gold_suite, df_coverage_gold, df_coverage_silver):
        """Keyed coverage should pass when all keys match."""
        with allure.step("Setup test data"):
            df_coverage_silver.createOrReplaceTempView("test_coverage_silver_keyed")
            cfg = {
                "silver": {"table": "test_coverage_silver_keyed"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "join_columns": ["group_id"],
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute coverage check"):
            passed, details, column = gold_suite._check_coverage(df_coverage_gold, cfg)

        with allure.step("Verify: keyed coverage should pass"):
            assert passed is True
            assert "Coverage OK" in details or ("All" in details and "keys within" in details)
            assert column == "group_id"

    @allure.title("Keyed coverage accepts a single join column given as a string")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_coverage_keyed_scalar_join_columns(self, spark, gold_suite, df_coverage_gold, df_coverage_silver):
        """``join_columns: group_id`` behaves like ``[group_id]``, not like ['g', 'r', ...]."""
        with allure.step("Setup test data"):
            df_coverage_silver.createOrReplaceTempView("test_coverage_silver_scalar_key")
            cfg = {
                "silver": {"table": "test_coverage_silver_scalar_key"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "gold_join_columns": "group_id",
                "silver_join_columns": "group_id",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute coverage check"):
            passed, details, column = gold_suite._check_coverage(df_coverage_gold, cfg)

        with allure.step("Verify: keyed coverage should pass on group_id"):
            assert passed is True, details
            assert column == "group_id"

    @allure.title("Coverage with silver filter")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_coverage_with_filter(self, spark, gold_suite):
        """Coverage should apply silver_filter before comparison."""
        with allure.step("Setup test data"):
            gold_data = [("G1", 100)]
            silver_data = [("G1", "active", 100), ("G1", "inactive", 999)]
            df_gold = spark.createDataFrame(gold_data, ["group_id", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["group_id", "status", "total"])
            df_silver.createOrReplaceTempView("test_coverage_silver_filter")
            cfg = {
                "silver": {"table": "test_coverage_silver_filter"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "silver_filter": "status = 'active'",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute coverage check"):
            passed, details, _column = gold_suite._check_coverage(df_gold, cfg)

        with allure.step("Verify: filtered coverage should pass"):
            assert passed is True
            assert "Coverage OK" in details or ("All" in details and "keys within" in details)

    @allure.title("Coverage fails with mismatched join keys config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_coverage_mismatched_keys(self, spark, gold_suite, df_coverage_gold, df_coverage_silver):
        """Coverage should fail when only one of gold/silver keys is provided."""
        with allure.step("Setup test data"):
            df_coverage_silver.createOrReplaceTempView("test_coverage_silver_mismatch_keys")
            cfg = {
                "silver": {"table": "test_coverage_silver_mismatch_keys"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "gold_join_columns": ["group_id"],
                # Missing silver_join_columns
            }

        with allure.step("Execute coverage check"):
            passed, details, _column = gold_suite._check_coverage(df_coverage_gold, cfg)

        with allure.step("Verify: should fail with mismatched keys"):
            assert passed is False
            assert "gold_join_columns and silver_join_columns" in details

    @allure.title("Coverage fails with different key lengths")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_coverage_key_length_mismatch(self, spark, gold_suite, df_coverage_gold, df_coverage_silver):
        """Coverage should fail when gold and silver keys have different lengths."""
        with allure.step("Setup test data"):
            df_coverage_silver.createOrReplaceTempView("test_coverage_key_length")
            cfg = {
                "silver": {"table": "test_coverage_key_length"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "gold_join_columns": ["group_id", "extra_key"],
                "silver_join_columns": ["group_id"],
            }

        with allure.step("Execute coverage check"):
            passed, details, _column = gold_suite._check_coverage(df_coverage_gold, cfg)

        with allure.step("Verify: should fail with key length mismatch"):
            assert passed is False
            assert "same length" in details

    @allure.title("Coverage with zero silver and gold values passes")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_coverage_both_zero(self, spark, gold_suite):
        """Coverage should pass when both gold and silver values are zero."""
        with allure.step("Setup test data"):
            gold_data = [("G1", 0)]
            silver_data = [("G1", 0)]
            df_gold = spark.createDataFrame(gold_data, ["group_id", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["group_id", "total"])
            df_silver.createOrReplaceTempView("test_coverage_both_zero")
            cfg = {
                "silver": {"table": "test_coverage_both_zero"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute coverage check"):
            passed, details, _column = gold_suite._check_coverage(df_gold, cfg)

        with allure.step("Verify: coverage should pass with both zero"):
            assert passed is True
            assert "Coverage OK" in details or ("All" in details and "keys within" in details)

    @allure.title("Coverage fails when silver is zero but gold is not")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_coverage_silver_zero(self, spark, gold_suite):
        """Coverage should fail when silver is zero but gold has value."""
        with allure.step("Setup test data"):
            gold_data = [("G1", 100)]
            silver_data = [("G1", 0)]
            df_gold = spark.createDataFrame(gold_data, ["group_id", "amount"])
            df_silver = spark.createDataFrame(silver_data, ["group_id", "total"])
            df_silver.createOrReplaceTempView("test_coverage_silver_zero")
            cfg = {
                "silver": {"table": "test_coverage_silver_zero"},
                "gold_expr": "SUM(amount)",
                "silver_expr": "SUM(total)",
                "tolerance_percent": 1.0,
            }

        with allure.step("Execute coverage check"):
            passed, details, _column = gold_suite._check_coverage(df_gold, cfg)

        with allure.step("Verify: coverage should fail with 100% delta"):
            assert passed is False
            assert "100.0%" in details


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.CROSS_GOLD)
class TestGoldCrossGold:
    """Tests for cross_gold reconciliation check."""

    @allure.title("Cross-gold passes when tables match")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cross_gold_pass(self, spark, gold_suite, df_gold_left, df_gold_right):
        """Cross-gold should pass when left and right values match."""
        with allure.step("Setup test data"):
            df_gold_left.createOrReplaceTempView("test_gold_left")
            df_gold_right.createOrReplaceTempView("test_gold_right")
            cfg = {
                "left_table": "test_gold_left",
                "right_table": "test_gold_right",
                "left_expr": "SUM(amount)",
                "right_expr": "SUM(total)",
                "tolerance_abs": 0.01,
            }

        with allure.step("Execute cross-gold check"):
            passed, details, _column = gold_suite._check_cross_gold(
                df_gold_left, cfg, {"catalog": "cat", "schema": "sch", "table": "tbl"}
            )

        with allure.step("Verify: cross-gold should pass"):
            assert passed is True
            assert "Cross-gold OK" in details or ("All" in details and "within tolerance" in details)

    @allure.title("Cross-gold fails when tables mismatch")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_cross_gold_fail(self, spark, gold_suite, df_gold_left, df_gold_right_mismatch):
        """Cross-gold should fail when values exceed tolerance."""
        with allure.step("Setup test data"):
            df_gold_left.createOrReplaceTempView("test_gold_left_fail")
            df_gold_right_mismatch.createOrReplaceTempView("test_gold_right_fail")
            cfg = {
                "left_table": "test_gold_left_fail",
                "right_table": "test_gold_right_fail",
                "left_expr": "SUM(amount)",
                "right_expr": "SUM(total)",
                "tolerance_abs": 0.01,
            }

        with allure.step("Execute cross-gold check"):
            passed, details, _column = gold_suite._check_cross_gold(
                df_gold_left, cfg, {"catalog": "cat", "schema": "sch", "table": "tbl"}
            )

        with allure.step("Verify: cross-gold should fail"):
            assert passed is False
            assert "Difference" in details and "exceeds tolerance" in details

    @allure.title("Cross-gold with filters")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cross_gold_with_filters(self, spark, gold_suite):
        """Cross-gold should apply filters before comparison."""
        with allure.step("Setup test data"):
            left_data = [(1, "typeA", 100), (2, "typeA", 200), (3, "typeB", 999)]
            right_data = [(1, "typeA", 100), (2, "typeA", 200), (3, "typeB", 999)]
            df_left = spark.createDataFrame(left_data, ["id", "type", "amount"])
            df_right = spark.createDataFrame(right_data, ["id", "type", "total"])
            df_left.createOrReplaceTempView("test_gold_left_filter")
            df_right.createOrReplaceTempView("test_gold_right_filter")
            cfg = {
                "left_table": "test_gold_left_filter",
                "right_table": "test_gold_right_filter",
                "left_expr": "SUM(amount)",
                "right_expr": "SUM(total)",
                "left_filter": "type = 'typeA'",
                "right_filter": "type = 'typeA'",
                "tolerance_abs": 0.01,
            }

        with allure.step("Execute cross-gold check"):
            passed, details, _column = gold_suite._check_cross_gold(
                df_left, cfg, {"catalog": "cat", "schema": "sch", "table": "tbl"}
            )

        with allure.step("Verify: filtered cross-gold should pass"):
            assert passed is True
            assert "Cross-gold OK" in details or ("All" in details and "within tolerance" in details)

    @allure.title("Cross-gold with group_by")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cross_gold_grouped(self, spark, gold_suite):
        """Cross-gold should work with group_by aggregation."""
        with allure.step("Setup test data"):
            left_data = [("G1", 100), ("G2", 200)]
            right_data = [("G1", 100), ("G2", 200)]
            df_left = spark.createDataFrame(left_data, ["group_id", "amount"])
            df_right = spark.createDataFrame(right_data, ["group_id", "total"])
            df_left.createOrReplaceTempView("test_gold_left_grouped")
            df_right.createOrReplaceTempView("test_gold_right_grouped")
            cfg = {
                "left_table": "test_gold_left_grouped",
                "right_table": "test_gold_right_grouped",
                "left_expr": "amount",
                "right_expr": "total",
                "group_by": ["group_id"],
                "tolerance_abs": 0.01,
            }

        with allure.step("Execute cross-gold check"):
            passed, details, _column = gold_suite._check_cross_gold(
                df_left, cfg, {"catalog": "cat", "schema": "sch", "table": "tbl"}
            )

        with allure.step("Verify: grouped cross-gold should pass"):
            assert passed is True
            assert "Cross-gold OK" in details or ("All" in details and "within tolerance" in details)

    @allure.title("Cross-gold grouped with raw_expr")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cross_gold_grouped_raw_expr(self, spark, gold_suite):
        """Cross-gold with raw_expr uses expressions directly."""
        with allure.step("Setup test data"):
            left_data = [("G1", 1), ("G1", 2), ("G2", 3)]
            right_data = [("G1", 1), ("G1", 2), ("G2", 3)]
            df_left = spark.createDataFrame(left_data, ["group_id", "id"])
            df_right = spark.createDataFrame(right_data, ["group_id", "id"])
            df_left.createOrReplaceTempView("test_gold_left_raw")
            df_right.createOrReplaceTempView("test_gold_right_raw")
            cfg = {
                "left_table": "test_gold_left_raw",
                "right_table": "test_gold_right_raw",
                "left_expr": "COUNT(DISTINCT id)",
                "right_expr": "COUNT(DISTINCT id)",
                "group_by": ["group_id"],
                "raw_expr": True,
                "tolerance_abs": 0.01,
            }

        with allure.step("Execute cross-gold check"):
            passed, details, _column = gold_suite._check_cross_gold(
                df_left, cfg, {"catalog": "cat", "schema": "sch", "table": "tbl"}
            )

        with allure.step("Verify: raw expr cross-gold should pass"):
            assert passed is True
            assert "Cross-gold OK" in details or ("All" in details and "within tolerance" in details)

    @allure.title("Cross-gold with implicit left side uses the filtered table")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cross_gold_implicit_left_uses_filtered_df(self, spark, gold_suite, monkeypatch):
        """Without left_table, the df already filtered by row/partition filters is the left side."""
        with allure.step("Setup: full table total 100, filtered df total 10, right total 10"):
            full = spark.createDataFrame([(1, 10), (2, 90)], ["id", "amount"])
            filtered = full.filter("id = 1")
            right = spark.createDataFrame([(1, 10)], ["id", "total"])
            tables = {"cat.sch.tbl": full, "cat.sch.right": right}
            real = gold_suite.spark

            class _Spark:
                def table(self, name):
                    return tables[name] if name in tables else real.table(name)

                def __getattr__(self, name):
                    return getattr(real, name)

            monkeypatch.setattr(gold_suite, "spark", _Spark())
            cfg = {"right_table": "cat.sch.right", "left_expr": "SUM(amount)", "right_expr": "SUM(total)"}

        with allure.step("Execute cross-gold check"):
            passed, details, _column = gold_suite._check_cross_gold(
                filtered, cfg, {"catalog": "cat", "schema": "sch", "table": "tbl"}
            )

        with allure.step("Verify: filtered left (10) matches right (10)"):
            assert passed is True, details

    @allure.title("Cross-gold fails when config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_cross_gold_missing_config(self, gold_suite, df_gold_left):
        """Cross-gold should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {"left_table": "test_table"}  # Missing right_table, left_expr, right_expr

        with allure.step("Execute cross-gold check"):
            passed, details, _column = gold_suite._check_cross_gold(
                df_gold_left, cfg, {"catalog": "cat", "schema": "sch", "table": "tbl"}
            )

        with allure.step("Verify: should fail with missing config"):
            assert passed is False
            assert "Missing config" in details


@allure.feature(AllureFeature.GOLD_SUITE)
@allure.story(AllureStory.TREND)
class TestGoldTrend:
    """Tests for trend check using temp views."""

    @allure.title("Trend fails when config missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_trend_missing_config(self, gold_suite, df_metrics):
        """Trend should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {"metric": "COUNT(*)"}  # Missing history and name
            table_ctx = {"catalog": "test", "schema": "test", "table": "test"}

        with allure.step("Execute trend check"):
            passed, details, _column = gold_suite._check_trend(df_metrics, cfg, table_ctx)

        with allure.step("Verify: should fail with missing config"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Trend passes with insufficient history")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_trend_insufficient_history(self, spark, gold_suite, df_metrics):
        """Trend should pass (with status) when history is insufficient."""
        with allure.step("Setup test data"):
            # Create empty history table
            history_data = []
            schema = "check_name STRING, catalog STRING, schema STRING, "
            schema += "table STRING, run_timestamp TIMESTAMP, details STRING"
            df_history = spark.createDataFrame(history_data, schema)
            df_history.createOrReplaceTempView("test_history_empty")
            cfg = {
                "metric": "COUNT(*)",
                "history": {"table": "test_history_empty", "min_history": 3},
                "name": "test_trend",
            }
            table_ctx = {"catalog": "test", "schema": "test", "table": "metrics"}

        with allure.step("Execute trend check"):
            passed, details, _column = gold_suite._check_trend(df_metrics, cfg, table_ctx)

        with allure.step("Verify: should pass with insufficient_history status"):
            assert passed is True
            assert "Insufficient history" in details

    @allure.title("Trend passes when stable within tolerance")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_trend_stable_pass(self, spark, gold_suite, df_metrics):
        """Trend should pass when current value is within tolerance of baseline."""
        with allure.step("Setup test data"):
            from datetime import UTC, datetime

            # Create history with consistent values (metric=4 matches df_metrics count)
            history_data = [
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 1, tzinfo=UTC), "metric=4"),
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 2, tzinfo=UTC), "metric=4"),
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 3, tzinfo=UTC), "metric=4"),
            ]
            df_history = spark.createDataFrame(
                history_data,
                ["check_name", "catalog", "schema", "table", "run_timestamp", "details"],
            )
            df_history.createOrReplaceTempView("test_history_stable")
            cfg = {
                "metric": "COUNT(*)",
                "history": {"table": "test_history_stable", "min_history": 3, "tolerance_percent": 20},
                "name": "test_trend",
                "direction": "stable",
            }
            table_ctx = {"catalog": "test", "schema": "test", "table": "metrics"}

        with allure.step("Execute trend check"):
            passed, details, _column = gold_suite._check_trend(df_metrics, cfg, table_ctx)

        with allure.step("Verify: trend should pass when stable"):
            assert passed is True
            assert "Trend OK" in details

    @allure.title("Trend run finds its own history under the persisted check ID")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_trend_history_lookup_uses_persisted_check_id(self, spark, gold_suite, df_metrics, monkeypatch):
        """A single trend instance is persisted as ``trend``; the history lookup must use that ID, not ``name``."""
        with allure.step("Setup history persisted by previous runs"):
            from datetime import UTC, datetime

            history_data = [
                ("trend", "test", "test", "metrics", datetime(2026, 1, d, tzinfo=UTC), "metric=4") for d in (1, 2, 3)
            ]
            spark.createDataFrame(
                history_data, ["check_name", "catalog", "schema", "table", "run_timestamp", "details"]
            ).createOrReplaceTempView("test_history_persisted_trend")
            monkeypatch.setattr(gold_suite, "_read_table", lambda *_a: df_metrics)
            table_cfg = {
                "catalog": "test",
                "schema": "test",
                "table": "metrics",
                "checks": {
                    "trend": {
                        "enabled": True,
                        "metric": "COUNT(*)",
                        "name": "row_count_trend",
                        "history": {"table": "test_history_persisted_trend", "min_history": 3},
                    }
                },
            }

        with allure.step("Run the table validation"):
            result = gold_suite.validate_table(table_cfg)

        with allure.step("Verify: trend compared against history"):
            trend = next(c for c in result["checks"] if c["check_name"] == "trend")
            assert "Trend OK" in trend["details"], trend["details"]

    @allure.title("Named volume run finds its own history under the persisted check ID")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_volume_history_lookup_uses_persisted_check_id(self, spark, gold_suite, df_metrics, monkeypatch):
        """A single named volume instance is persisted as ``volume``; the lookup must not use ``name``."""
        with allure.step("Setup history persisted by previous runs"):
            from datetime import UTC, datetime

            history_data = [
                ("volume", "test", "test", "metrics", datetime(2026, 1, d, tzinfo=UTC), "value=4") for d in (1, 2, 3)
            ]
            spark.createDataFrame(
                history_data, ["check_name", "catalog", "schema", "table", "run_timestamp", "details"]
            ).createOrReplaceTempView("test_history_persisted_volume")
            monkeypatch.setattr(gold_suite, "_read_table", lambda *_a: df_metrics)
            table_cfg = {
                "catalog": "test",
                "schema": "test",
                "table": "metrics",
                "checks": {
                    "volume": {
                        "enabled": True,
                        "metric": "COUNT(*)",
                        "name": "orders_volume",
                        "history": {"table": "test_history_persisted_volume", "min_history": 3},
                    }
                },
            }

        with allure.step("Run the table validation"):
            result = gold_suite.validate_table(table_cfg)

        with allure.step("Verify: volume compared against the historical baseline"):
            volume = next(c for c in result["checks"] if c["check_name"] == "volume")
            assert "baseline" in volume["details"], volume["details"]

    @allure.title("Volume history round-trip: baseline is read from real volume results")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_volume_history_round_trip_detects_drop(self, spark, gold_suite):
        """History rows hold what volume itself emits; a large drop against them must fail."""
        with allure.step("Persist three real volume results of a 100-row table"):
            from datetime import UTC, datetime

            ctx = {"catalog": "test", "schema": "test", "table": "rt"}
            df_big = spark.range(100)
            emitted = [gold_suite._check_volume(df_big, {"metric": "COUNT(*)"}, ctx)[1] for _ in range(3)]
            history = [
                ("volume", "test", "test", "rt", datetime(2026, 1, d, tzinfo=UTC), det)
                for d, det in zip((1, 2, 3), emitted, strict=True)
            ]
            spark.createDataFrame(
                history, ["check_name", "catalog", "schema", "table", "run_timestamp", "details"]
            ).createOrReplaceTempView("test_history_volume_rt")

        with allure.step("Run volume with history on a 4-row table"):
            cfg = {"metric": "COUNT(*)", "history": {"table": "test_history_volume_rt", "min_history": 3}}
            passed, details, _column = gold_suite._check_volume(spark.range(4), cfg, ctx)

        with allure.step("Verify: the 96% drop fails against the baseline of 100"):
            assert passed is False, details
            assert "exceeds" in details

        with allure.step("Verify: the failure message still carries the current value for later runs"):
            assert gold_suite._extract_metric_from_details(details) == 4

    @staticmethod
    def _metric_df(spark, kind: str):
        """One-row table whose SUM(v) is very large, very small, or DECIMAL."""
        from decimal import Decimal

        if kind == "decimal":
            return spark.createDataFrame([(Decimal("12345.67"),)], "v decimal(12,2)")
        return spark.createDataFrame([({"large": 1e20, "small": 1e-06}[kind],)], "v double")

    @staticmethod
    def _history_view(spark, name: str, check_name: str, details: list[str]) -> str:
        from datetime import UTC, datetime

        rows = [
            (check_name, "test", "test", "rt", datetime(2026, 1, d, tzinfo=UTC), det)
            for d, det in enumerate(details, start=1)
        ]
        spark.createDataFrame(
            rows, ["check_name", "catalog", "schema", "table", "run_timestamp", "details"]
        ).createOrReplaceTempView(name)
        return name

    @pytest.mark.parametrize("kind", ["large", "small", "decimal"])
    @allure.title("Volume history round-trip keeps an unchanged metric passing")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_volume_history_round_trip_unchanged_metric(self, spark, gold_suite, kind):
        """Scientific-notation and DECIMAL values read back from history equal the current value."""
        with allure.step("Persist three real volume results"):
            ctx = {"catalog": "test", "schema": "test", "table": "rt"}
            df = self._metric_df(spark, kind)
            emitted = [gold_suite._check_volume(df, {"metric": "SUM(v)"}, ctx)[1] for _ in range(3)]
            view = self._history_view(spark, f"test_history_volume_{kind}", "volume", emitted)

        with allure.step("Run volume with history on the same data"):
            cfg = {"metric": "SUM(v)", "history": {"table": view, "min_history": 3}}
            passed, details, _column = gold_suite._check_volume(df, cfg, ctx)

        with allure.step("Verify: no deviation against its own history"):
            assert passed is True, details
            assert "baseline" in details

    @pytest.mark.parametrize("kind", ["large", "small", "decimal"])
    @allure.title("Trend history round-trip keeps an unchanged metric passing")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_trend_history_round_trip_unchanged_metric(self, spark, gold_suite, kind):
        """Scientific-notation and DECIMAL values read back from history equal the current value."""
        with allure.step("Persist three real trend results (insufficient-history runs)"):
            ctx = {"catalog": "test", "schema": "test", "table": "rt"}
            df = self._metric_df(spark, kind)
            empty = self._history_view(spark, f"test_history_trend_empty_{kind}", "trend", ["x"])
            cfg = {"metric": "SUM(v)", "name": "t", "history": {"table": empty, "min_history": 3}}
            emitted = [gold_suite._check_trend(df, cfg, ctx)[1] for _ in range(3)]
            view = self._history_view(spark, f"test_history_trend_{kind}", "t", emitted)

        with allure.step("Run trend with history on the same data"):
            cfg = {"metric": "SUM(v)", "name": "t", "history": {"table": view, "min_history": 3}}
            passed, details, _column = gold_suite._check_trend(df, cfg, ctx)

        with allure.step("Verify: trend compares against history and passes"):
            assert passed is True, details
            assert "Trend OK" in details

    @allure.title("Trend fails when deviation exceeds tolerance")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_trend_deviation_fail(self, spark, gold_suite, df_metrics):
        """Trend should fail when deviation exceeds tolerance."""
        with allure.step("Setup test data"):
            from datetime import UTC, datetime

            # Create history with values that differ significantly from current (4)
            history_data = [
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 1, tzinfo=UTC), "metric=100"),
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 2, tzinfo=UTC), "metric=100"),
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 3, tzinfo=UTC), "metric=100"),
            ]
            df_history = spark.createDataFrame(
                history_data,
                ["check_name", "catalog", "schema", "table", "run_timestamp", "details"],
            )
            df_history.createOrReplaceTempView("test_history_deviation")
            cfg = {
                "metric": "COUNT(*)",
                "history": {"table": "test_history_deviation", "min_history": 3, "tolerance_percent": 10},
                "name": "test_trend",
                "direction": "stable",
            }
            table_ctx = {"catalog": "test", "schema": "test", "table": "metrics"}

        with allure.step("Execute trend check"):
            passed, details, _column = gold_suite._check_trend(df_metrics, cfg, table_ctx)

        with allure.step("Verify: trend should fail when deviation too high"):
            assert passed is False
            assert "exceeds" in details

    @allure.title("Trend with invalid direction fails")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_trend_invalid_direction(self, spark, gold_suite, df_metrics):
        """Trend should fail with invalid direction parameter."""
        with allure.step("Setup test data"):
            from datetime import UTC, datetime

            history_data = [
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 1, tzinfo=UTC), "metric=4"),
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 2, tzinfo=UTC), "metric=4"),
                ("test_trend", "test", "test", "metrics", datetime(2026, 1, 3, tzinfo=UTC), "metric=4"),
            ]
            df_history = spark.createDataFrame(
                history_data,
                ["check_name", "catalog", "schema", "table", "run_timestamp", "details"],
            )
            df_history.createOrReplaceTempView("test_history_invalid_dir")
            cfg = {
                "metric": "COUNT(*)",
                "history": {"table": "test_history_invalid_dir", "min_history": 3},
                "name": "test_trend",
                "direction": "invalid_direction",
            }
            table_ctx = {"catalog": "test", "schema": "test", "table": "metrics"}

        with allure.step("Execute trend check"):
            passed, details, _column = gold_suite._check_trend(df_metrics, cfg, table_ctx)

        with allure.step("Verify: should fail with invalid direction"):
            assert passed is False
            assert "Invalid config" in details
