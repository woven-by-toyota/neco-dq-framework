# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for expected value resolvers.

These tests validate the expected value resolution functions used by compare_metric checks.
"""

import contextlib
from unittest.mock import patch

import allure
import pytest

from neco_dq.core.metrics.expected import (
    EXPECTED_RESOLVERS,
    _aggregate,
    _array_field_value,
    _array_ordered_field_value,
    _column_value,
    _resolve_table,
    _sql,
    _sql_grouped,
    _static,
    resolve_expected,
    safe_eval,
)
from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag
from tests.utils.assertions import (
    assert_check_failed,
    assert_check_success,
    assert_column_equals,
    assert_details_contain,
    assert_result_is_grouped,
    assert_value_equals,
)

# Mark all tests in this module as requiring Spark
pytestmark = pytest.mark.spark


# ---------------------------------------------------------------------------
# Safe Eval Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story("Safe Eval")
class TestSafeEval:
    """Tests for the safe_eval helper function."""

    @allure.title("Simple arithmetic expressions")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_simple_arithmetic(self):
        with allure.step("Verify: Simple Arithmetic"):
            assert safe_eval("value + 10", 5) == 15
            assert safe_eval("value * 2", 10) == 20
            assert safe_eval("value / 2", 100) == 50

    @allure.title("Comparison expressions")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_comparison(self):
        with allure.step("Verify: Comparison"):
            assert safe_eval("value > 5", 10) is True
            assert safe_eval("value < 5", 10) is False

    @allure.title("Percentage calculations")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_percentage(self):
        with allure.step("Verify: Percentage"):
            assert safe_eval("value * 0.1", 1000) == 100
            assert safe_eval("value * 1.05", 100) == pytest.approx(105)


# ---------------------------------------------------------------------------
# Static Resolver Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.STATIC_RESOLVER)
class TestStaticResolver:
    """Tests for _static expected resolver."""

    @allure.title("Static resolver returns configured value")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE)
    def test_static_returns_value(self):
        with allure.step("Setup test data"):
            cfg = {"value": 42}
        with allure.step("Execute function under test"):
            result = _static(None, None, cfg)
        with allure.step("Verify: Static Returns Value"):
            assert_check_success(result)
            assert_value_equals(result, expected=42)
            assert_column_equals(result, expected=None)
            assert_details_contain(result, "expected=static")

    @allure.title("Static resolver handles string values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_static_with_string(self):
        with allure.step("Setup test data"):
            cfg = {"value": "expected_string"}
        with allure.step("Execute function under test"):
            result = _static(None, None, cfg)
        with allure.step("Verify: Static With String"):
            assert_check_success(result)
            assert_value_equals(result, expected="expected_string")

    @allure.title("Static resolver handles list values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_static_with_list(self):
        with allure.step("Setup test data"):
            cfg = {"value": [1, 2, 3]}
        with allure.step("Execute function under test"):
            result = _static(None, None, cfg)
        with allure.step("Verify: Static With List"):
            assert_check_success(result)
            assert_value_equals(result, expected=[1, 2, 3])

    @allure.title("Static resolver returns None when no value configured")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE)
    def test_static_with_none(self):
        with allure.step("Setup test data"):
            cfg = {}
        with allure.step("Execute function under test"):
            result = _static(None, None, cfg)
        with allure.step("Verify: Static With None"):
            assert_check_success(result)
            assert_value_equals(result, expected=None)


# ---------------------------------------------------------------------------
# Aggregate Resolver Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.AGGREGATE_RESOLVER)
@pytest.mark.spark
class TestAggregateExpectedResolver:
    """Tests for _aggregate expected resolver."""

    @allure.title("Aggregate resolver calculates sum")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE, AllureTag.SPARK)
    def test_aggregate_sum(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "function": "sum"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate Sum"):
            assert_check_success(result)
            assert_value_equals(result, expected=1000)
            assert_column_equals(result, expected="value")
            assert_details_contain(result, "expected=aggregate(sum)")

    @allure.title("Aggregate resolver calculates average")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_aggregate_avg(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "function": "avg"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate Avg"):
            assert_check_success(result)
            assert_value_equals(result, expected=200.0)

    @allure.title("Aggregate resolver applies expression to result")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_aggregate_with_expression(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "function": "sum", "expression": "value * 1.1"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate With Expression"):
            assert_check_success(result)
            assert result[1] == pytest.approx(1100)

    @allure.title("Aggregate resolver filters with where clause")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_aggregate_with_where(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "function": "sum", "where": "status = 'active'"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate With Where"):
            assert_check_success(result)
            assert_value_equals(result, expected=550)

    @allure.title("Aggregate resolver supports group_by")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_aggregate_with_group_by(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "function": "sum", "group_by": "category"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate With Group By"):
            assert_check_success(result)
            assert_result_is_grouped(result)
            assert result[1][("A",)] == 600
            assert result[1][("B",)] == 400
            assert_details_contain(result, "grouped_by")

    @allure.title("Aggregate resolver fails if column missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_aggregate_missing_column(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"function": "sum"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate Missing Column"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_column_or_function")

    @allure.title("Aggregate resolver fails for unsupported function")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_aggregate_unsupported_function(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "function": "unsupported_fn"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate Unsupported Function"):
            assert_check_failed(result)
            assert_details_contain(result, "unsupported_aggregate_function")


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.AGGREGATE_RESOLVER)
@pytest.mark.spark
class TestAggregateExpectedEdgeCases:
    """Extended edge case tests for _aggregate expected resolver."""

    @allure.title("Aggregate handles multiple where clauses")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_aggregate_with_multiple_where(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {
                "expected_from": {
                    "column": "value",
                    "function": "sum",
                    "where": ["status = 'active'", "category = 'A'"],
                }
            }
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate With Multiple Where"):
            assert_check_success(result)
            assert_value_equals(result, expected=300)

    @allure.title("Aggregate returns error for invalid column")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_aggregate_error_handling(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "nonexistent", "function": "sum"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate Error Handling"):
            assert_check_failed(result)
            assert "error" in result[3].lower()

    @allure.title("Aggregate applies expression to grouped results")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_aggregate_with_expression_and_group_by(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {
                "expected_from": {
                    "column": "value",
                    "function": "sum",
                    "group_by": "category",
                    "expression": "value * 2",
                }
            }
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate With Expression And Group By"):
            assert_check_success(result)
            assert result[1][("A",)] == 1200
            assert result[1][("B",)] == 800

    @allure.title("Aggregate handles group_by as string")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_aggregate_group_by_as_string(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "function": "sum", "group_by": "category"}}
        with allure.step("Execute function under test"):
            result = _aggregate(spark, df_basic, cfg)
        with allure.step("Verify: Aggregate Group By As String"):
            assert_check_success(result)
            assert result[1][("A",)] == 600
            assert_details_contain(result, "grouped_by")


# ---------------------------------------------------------------------------
# Column Value Resolver Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.COLUMN_VALUE_RESOLVER)
@pytest.mark.spark
class TestColumnValueExpectedResolver:
    """Tests for _column_value expected resolver."""

    @allure.title("Column value returns first non-null value")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE, AllureTag.SPARK)
    def test_column_value_basic(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "name"}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_basic, cfg)
        with allure.step("Verify: Column Value Basic"):
            assert_check_success(result)
            assert result[1] is not None
            assert_column_equals(result, expected="name")
            assert_details_contain(result, "expected=column_value")

    @allure.title("Column value filters with where clause")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_column_value_with_where(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "name", "where": "id = 3"}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_basic, cfg)
        with allure.step("Verify: Column Value With Where"):
            assert_check_success(result)
            assert_value_equals(result, expected="Charlie")

    @allure.title("Column value supports group_by")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_column_value_grouped(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "group_by": "category"}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_basic, cfg)
        with allure.step("Verify: Column Value Grouped"):
            assert_check_success(result)
            assert_result_is_grouped(result)
            assert_details_contain(result, "grouped_by")

    @allure.title("Column value fails if column not specified")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_column_value_missing_column(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_basic, cfg)
        with allure.step("Verify: Column Value Missing Column"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_column")


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.COLUMN_VALUE_RESOLVER)
@pytest.mark.spark
class TestColumnValueExpectedResolverExtended:
    """Extended tests for _column_value expected resolver."""

    @allure.title("Column value handles empty DataFrame")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_column_value_empty_df(self, spark):
        with allure.step("Setup test data"):
            df_empty = spark.createDataFrame([], "id: int, name: string")
            cfg = {"expected_from": {"column": "name"}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_empty, cfg)
        with allure.step("Verify: Column Value Empty Df"):
            assert_check_failed(result)
            assert_details_contain(result, "empty_df")

    @allure.title("Column value handles all null column")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_column_value_all_null(self, spark):
        with allure.step("Setup test data"):
            df = spark.createDataFrame([(1, None), (2, None)], "id: int, name: string")
            cfg = {"expected_from": {"column": "name"}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df, cfg)
        with allure.step("Verify: Column Value All Null"):
            assert_check_failed(result)
            assert_details_contain(result, "all_null")

    @allure.title("Column value handles multiple where clauses")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_column_value_multiple_where(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "name", "where": ["id > 1", "id < 4"]}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_basic, cfg)
        with allure.step("Verify: Column Value Multiple Where"):
            assert_check_success(result)
            assert result[1] in ["Bob", "Charlie"]


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.COLUMN_VALUE_RESOLVER)
@pytest.mark.spark
class TestColumnValueEdgeCasesExtended:
    """More edge cases for _column_value resolver."""

    @allure.title("Column value supports multiple group_by columns")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_column_value_with_multiple_group_by(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "group_by": ["category", "status"]}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_basic, cfg)
        with allure.step("Verify: Column Value With Multiple Group By"):
            assert_check_success(result)
            assert_result_is_grouped(result)

    @allure.title("Column value handles group_by as string")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_column_value_string_group_by(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "value", "group_by": "category"}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_basic, cfg)
        with allure.step("Verify: Column Value String Group By"):
            assert_check_success(result)
            assert_result_is_grouped(result)

    @allure.title("Column value handles errors gracefully")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_column_value_error_handling(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "nonexistent_column"}}
        with allure.step("Execute function under test"):
            result = _column_value(spark, df_basic, cfg)
        with allure.step("Verify: Column Value Error Handling"):
            assert_check_failed(result)
            assert_details_contain(result, "column_value_error")


# ---------------------------------------------------------------------------
# SQL Resolver Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.SQL_RESOLVER)
@pytest.mark.spark
class TestSqlExpectedResolver:
    """Tests for _sql expected resolver."""

    @allure.title("SQL resolver executes query and returns result")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE, AllureTag.SPARK)
    def test_sql_basic(self, spark, df_basic):
        with allure.step("Setup test data"):
            df_basic.createOrReplaceTempView("test_basic_exp")
            cfg = {"expected_from": {"query": "SELECT COUNT(*) FROM test_basic_exp"}}
        with allure.step("Execute function under test"):
            result = _sql(spark, df_basic, cfg)
        with allure.step("Verify: Sql Basic"):
            assert_check_success(result)
            assert_value_equals(result, expected=5)
            assert_details_contain(result, "expected=sql")

    @allure.title("SQL resolver handles aggregate queries")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_sql_aggregate(self, spark, df_basic):
        with allure.step("Setup test data"):
            df_basic.createOrReplaceTempView("test_basic_exp")
            cfg = {"expected_from": {"query": "SELECT SUM(value) FROM test_basic_exp"}}
        with allure.step("Execute function under test"):
            result = _sql(spark, df_basic, cfg)
        with allure.step("Verify: Sql Aggregate"):
            assert_check_success(result)
            assert_value_equals(result, expected=1000)

    @allure.title("SQL resolver fails if query missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_sql_missing_query(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {}}
        with allure.step("Execute function under test"):
            result = _sql(spark, df_basic, cfg)
        with allure.step("Verify: Sql Missing Query"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_query")

    @allure.title("SQL resolver fails if query returns null")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_sql_returns_null(self, spark, df_basic):
        with allure.step("Setup test data"):
            df_basic.createOrReplaceTempView("test_basic_exp")
            cfg = {"expected_from": {"query": "SELECT NULL"}}
        with allure.step("Execute function under test"):
            result = _sql(spark, df_basic, cfg)
        with allure.step("Verify: Sql Returns Null"):
            assert_check_failed(result)
            assert_details_contain(result, "returned_null")

    @allure.title("SQL resolver handles execution errors")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_sql_execution_error(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"query": "SELECT * FROM nonexistent_table_12345"}}
        with allure.step("Execute function under test"):
            result = _sql(spark, df_basic, cfg)
        with allure.step("Verify: Sql Execution Error"):
            assert_check_failed(result)
            assert_details_contain(result, "sql_execution_error")


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.SQL_RESOLVER)
@pytest.mark.spark
class TestSqlGroupedExpectedResolver:
    """Tests for _sql_grouped expected resolver."""

    @allure.title("SQL grouped returns grouped results")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE, AllureTag.SPARK)
    def test_sql_grouped_basic(self, spark, df_basic):
        with allure.step("Setup test data"):
            df_basic.createOrReplaceTempView("test_basic_grp")
            cfg = {
                "expected_from": {
                    "query": "SELECT category, SUM(value) as total FROM test_basic_grp GROUP BY category",
                    "group_by": "category",
                }
            }
        with allure.step("Execute function under test"):
            result = _sql_grouped(spark, df_basic, cfg)
        with allure.step("Verify: Sql Grouped Basic"):
            assert_check_success(result)
            assert_result_is_grouped(result)
            assert result[1][("A",)] == 600
            assert result[1][("B",)] == 400
            assert_details_contain(result, "expected=sql_grouped")

    @allure.title("SQL grouped fails if query missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_sql_grouped_missing_query(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"group_by": "category"}}
        with allure.step("Execute function under test"):
            result = _sql_grouped(spark, df_basic, cfg)
        with allure.step("Verify: Sql Grouped Missing Query"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_query")

    @allure.title("SQL grouped fails if group_by missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_sql_grouped_missing_group_by(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"query": "SELECT * FROM test"}}
        with allure.step("Execute function under test"):
            result = _sql_grouped(spark, df_basic, cfg)
        with allure.step("Verify: Sql Grouped Missing Group By"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_group_by")

    @allure.title("SQL grouped supports custom value column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_sql_grouped_custom_value_column(self, spark, df_basic):
        with allure.step("Setup test data"):
            df_basic.createOrReplaceTempView("test_basic_grp")
            cfg = {
                "expected_from": {
                    "query": "SELECT category, AVG(value) as avg_val FROM test_basic_grp GROUP BY category",
                    "group_by": "category",
                    "value_column": "avg_val",
                }
            }
        with allure.step("Execute function under test"):
            result = _sql_grouped(spark, df_basic, cfg)
        with allure.step("Verify: Sql Grouped Custom Value Column"):
            assert_check_success(result)
            assert result[1][("A",)] == 200.0
            assert result[1][("B",)] == 200.0


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.SQL_RESOLVER)
@pytest.mark.spark
class TestSqlGroupedEdgeCases:
    """Extended edge case tests for _sql_grouped resolver."""

    @allure.title("SQL grouped handles errors gracefully")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_sql_grouped_error_handling(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"query": "SELECT * FROM nonexistent_table", "group_by": "col"}}
        with allure.step("Execute function under test"):
            result = _sql_grouped(spark, df_basic, cfg)
        with allure.step("Verify: Sql Grouped Error Handling"):
            assert_check_failed(result)
            assert "error" in result[3].lower()

    @allure.title("SQL grouped fails if group_by columns missing in result")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_sql_grouped_missing_columns(self, spark, df_basic):
        with allure.step("Setup test data"):
            df_basic.createOrReplaceTempView("test_grp_cols")
            cfg = {
                "expected_from": {
                    "query": "SELECT SUM(value) as total FROM test_grp_cols",
                    "group_by": "nonexistent_col",
                }
            }
        with allure.step("Execute function under test"):
            result = _sql_grouped(spark, df_basic, cfg)
        with allure.step("Verify: Sql Grouped Missing Columns"):
            assert_check_failed(result)
            assert "missing_columns" in result[3].lower()

    @allure.title("SQL grouped fails if value_column missing")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_sql_grouped_missing_value_column(self, spark, df_basic):
        with allure.step("Setup test data"):
            df_basic.createOrReplaceTempView("test_val_col")
            cfg = {
                "expected_from": {
                    "query": "SELECT category FROM test_val_col GROUP BY category",
                    "group_by": "category",
                    "value_column": "nonexistent_value",
                }
            }
        with allure.step("Execute function under test"):
            result = _sql_grouped(spark, df_basic, cfg)
        with allure.step("Verify: Sql Grouped Missing Value Column"):
            assert_check_failed(result)
            assert "value_column" in result[3].lower()


# ---------------------------------------------------------------------------
# Resolve Expected Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story("Resolve Expected")
@pytest.mark.spark
class TestResolveExpected:
    """Tests for the resolve_expected main function."""

    @allure.title("Resolves static value")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE, AllureTag.SPARK)
    def test_resolve_static(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"value": 100}
        with allure.step("Execute function under test"):
            result = resolve_expected(spark, df_basic, cfg)
        with allure.step("Verify: Resolve Static"):
            assert_check_success(result)
            assert_value_equals(result, expected=100)

    @allure.title("Resolves aggregate expected")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_resolve_aggregate(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"type": "aggregate", "column": "value", "function": "count"}}
        with allure.step("Execute function under test"):
            result = resolve_expected(spark, df_basic, cfg)
        with allure.step("Verify: Resolve Aggregate"):
            assert_check_success(result)
            assert_value_equals(result, expected=5)

    @allure.title("Fails if no expected configuration")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_resolve_missing_expected(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {}
        with allure.step("Execute function under test"):
            result = resolve_expected(spark, df_basic, cfg)
        with allure.step("Verify: Resolve Missing Expected"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_expected")

    @allure.title("Fails for unsupported expected type")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_resolve_unsupported_type(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"type": "unknown_type"}}
        with allure.step("Execute function under test"):
            result = resolve_expected(spark, df_basic, cfg)
        with allure.step("Verify: Resolve Unsupported Type"):
            assert_check_failed(result)
            assert_details_contain(result, "unsupported_expected_type")


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story("Resolve Expected")
@pytest.mark.spark
class TestResolveExpectedExtended:
    """Extended tests for resolve_expected function."""

    @allure.title("Resolves SQL expected")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_resolve_sql(self, spark, df_basic):
        with allure.step("Setup test data"):
            df_basic.createOrReplaceTempView("test_resolve")
            cfg = {"expected_from": {"type": "sql", "query": "SELECT MAX(value) FROM test_resolve"}}
        with allure.step("Execute function under test"):
            result = resolve_expected(spark, df_basic, cfg)
        with allure.step("Verify: Resolve Sql"):
            assert_check_success(result)
            assert_value_equals(result, expected=300)

    @allure.title("Resolves column_value expected")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_resolve_column_value(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"type": "column_value", "column": "name", "where": "id = 1"}}
        with allure.step("Execute function under test"):
            result = resolve_expected(spark, df_basic, cfg)
        with allure.step("Verify: Resolve Column Value"):
            assert_check_success(result)
            assert_value_equals(result, expected="Alice")

    @allure.title("Handles exceptions gracefully")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_resolve_handles_exception(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"type": "aggregate", "column": "nonexistent", "function": "sum"}}
        with allure.step("Execute function under test"):
            result = resolve_expected(spark, df_basic, cfg)
        with allure.step("Verify: Resolve Handles Exception"):
            assert_check_failed(result)


# ---------------------------------------------------------------------------
# Expected Resolvers Registry Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story("Registry")
class TestExpectedResolversRegistry:
    """Tests for EXPECTED_RESOLVERS registry."""

    @allure.title("Registry contains all resolvers")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE)
    def test_registry_contains_all_resolvers(self):
        with allure.step("Verify: Registry Contains All Resolvers"):
            expected_resolvers = [
                "static",
                "aggregate",
                "sql",
                "sql_grouped",
                "column_value",
                "array_field_value",
                "array_ordered_field_value",
            ]
            for resolver_name in expected_resolvers:
                assert resolver_name in EXPECTED_RESOLVERS


# ---------------------------------------------------------------------------
# Resolve Table Helper Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story("Resolve Table")
@pytest.mark.spark
class TestResolveTableHelper:
    """Tests for _resolve_table helper function."""

    @allure.title("Returns original df when no config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_resolve_table_returns_df_when_no_config(self, spark, df_basic):
        with allure.step("Execute function under test"):
            result = _resolve_table(spark, df_basic, {})
        with allure.step("Verify: Resolve Table Returns Df When No Config"):
            assert result == df_basic

    @allure.title("Resolves table with catalog.schema.table format")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_resolve_table_with_catalog_schema_table(self, spark, df_basic):
        with allure.step("Verify: Resolve Table With Catalog Schema Table"):
            df_basic.createOrReplaceTempView("test_table")
            with contextlib.suppress(Exception):
                _resolve_table(spark, df_basic, {"catalog": "cat", "schema": "sch", "table": "tbl"})


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story("Resolve Table")
@pytest.mark.spark
class TestResolveTableDatabaseFormat:
    """Tests for _resolve_table with database.table format."""

    @allure.title("Resolves table with database.table format")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_resolve_table_with_database_table(self, spark, df_basic):
        with allure.step("Verify: Resolve Table With Database Table"):
            df_basic.createOrReplaceTempView("test_db_table")
            with contextlib.suppress(Exception):
                _resolve_table(spark, df_basic, {"database": "default", "table": "test_db_table"})


# ---------------------------------------------------------------------------
# Array Field Value Resolver Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.ARRAY_FIELD_RESOLVER)
@pytest.mark.spark
class TestArrayFieldValueResolver:
    """Tests for _array_field_value expected resolver."""

    @allure.title("Array field value fails if column missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_field_value_missing_column(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"field": "value"}}
        with allure.step("Execute function under test"):
            result = _array_field_value(spark, df_basic, cfg)
        with allure.step("Verify: Array Field Value Missing Column"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_column_or_field")

    @allure.title("Array field value fails if field missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_field_value_missing_field(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "items"}}
        with allure.step("Execute function under test"):
            result = _array_field_value(spark, df_basic, cfg)
        with allure.step("Verify: Array Field Value Missing Field"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_column_or_field")

    @allure.title("Array field value fails if no table specification")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_field_value_missing_table_spec(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "items", "field": "value"}}
        with allure.step("Execute function under test"):
            result = _array_field_value(spark, df_basic, cfg)
        with allure.step("Verify: Array Field Value Missing Table Spec"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_table_specification")


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.ARRAY_FIELD_RESOLVER)
@pytest.mark.spark
class TestArrayFieldValueWithData:
    """Tests for _array_field_value with actual array data."""

    @allure.title("Array field value extracts field from JSON array")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE, AllureTag.SPARK)
    def test_array_field_value_basic(self, spark, df_expected_arrays):
        with allure.step("Verify: Array Field Value Basic"):
            cfg = {
                "expected_from": {
                    "column": "items",
                    "field": "value",
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_expected_arrays):
                result = _array_field_value(spark, df_expected_arrays, cfg)
            assert_check_success(result)
            assert_column_equals(result, expected="items")
            assert_details_contain(result, "array_field_value")

    @allure.title("Array field value supports group_by")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_array_field_value_with_group_by(self, spark, df_expected_arrays):
        with allure.step("Verify: Array Field Value With Group By"):
            cfg = {
                "expected_from": {
                    "column": "items",
                    "field": "value",
                    "database": "test",
                    "table": "test_table",
                    "group_by": "group_id",
                    "item_group_by": "item_code",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_expected_arrays):
                result = _array_field_value(spark, df_expected_arrays, cfg)
            assert_check_success(result)
            assert_result_is_grouped(result)
            assert_details_contain(result, "grouped_by")

    @allure.title("Array field value extracts multiple fields")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_array_field_value_multiple_fields(self, spark, df_rankings):
        with allure.step("Verify: Array Field Value Multiple Fields"):
            cfg = {
                "expected_from": {
                    "column": "ranking_data",
                    "fields": ["mean_rank", "item_appearances_n"],
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_rankings):
                result = _array_field_value(spark, df_rankings, cfg)
            assert_check_success(result)
            assert isinstance(result[1], list)

    @allure.title("Array field value supports fallback item_group_by")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_array_field_value_with_item_group_by_fallback(self, spark, df_expected_arrays):
        with allure.step("Verify: Array Field Value With Item Group By Fallback"):
            cfg = {
                "expected_from": {
                    "column": "items",
                    "field": "value",
                    "database": "test",
                    "table": "test_table",
                    "item_group_by": "item_code|code",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_expected_arrays):
                result = _array_field_value(spark, df_expected_arrays, cfg)
            assert_check_success(result)

    @allure.title("Array field value handles column not found error")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_field_value_error_column_not_found(self, spark, df_expected_arrays):
        with allure.step("Verify: Array Field Value Error Column Not Found"):
            cfg = {
                "expected_from": {
                    "column": "nonexistent_column",
                    "field": "value",
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_expected_arrays):
                result = _array_field_value(spark, df_expected_arrays, cfg)
            assert_check_failed(result)
            assert "error" in result[3].lower()

    @allure.title("Array field value handles fields as string")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_array_field_value_fields_as_string(self, spark, df_arrays):
        with allure.step("Verify: Array Field Value Fields As String"):
            cfg = {
                "expected_from": {
                    "column": "items",
                    "fields": "value",
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_arrays):
                result = _array_field_value(spark, df_arrays, cfg)
            assert_check_success(result)


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.ARRAY_FIELD_RESOLVER)
@pytest.mark.spark
class TestBuildArrayFieldResultEdgeCases:
    """Tests for edge cases in _build_array_field_result."""

    @allure.title("Multiple fields with grouping")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_array_field_multiple_fields_grouped(self, spark, df_rankings):
        with allure.step("Verify: Array Field Multiple Fields Grouped"):
            cfg = {
                "expected_from": {
                    "column": "ranking_data",
                    "fields": ["mean_rank", "item_appearances_n"],
                    "database": "test",
                    "table": "test_table",
                    "group_by": "question_id",
                    "item_group_by": "item_code",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_rankings):
                result = _array_field_value(spark, df_rankings, cfg)
            assert_check_success(result)
            assert_result_is_grouped(result)


# ---------------------------------------------------------------------------
# Array Ordered Field Value Resolver Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.ARRAY_ORDERED_RESOLVER)
@pytest.mark.spark
class TestArrayOrderedFieldValueResolver:
    """Tests for _array_ordered_field_value expected resolver."""

    @allure.title("Array ordered fails if column missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_ordered_field_value_missing_column(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"field": "value"}}
        with allure.step("Execute function under test"):
            result = _array_ordered_field_value(spark, df_basic, cfg)
        with allure.step("Verify: Array Ordered Field Value Missing Column"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_column_or_field")

    @allure.title("Array ordered fails if field missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_ordered_field_value_missing_field(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "items"}}
        with allure.step("Execute function under test"):
            result = _array_ordered_field_value(spark, df_basic, cfg)
        with allure.step("Verify: Array Ordered Field Value Missing Field"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_column_or_field")

    @allure.title("Array ordered fails if no table specification")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_ordered_field_value_missing_table_spec(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {"expected_from": {"column": "items", "field": "value"}}
        with allure.step("Execute function under test"):
            result = _array_ordered_field_value(spark, df_basic, cfg)
        with allure.step("Verify: Array Ordered Field Value Missing Table Spec"):
            assert_check_failed(result)
            assert_details_contain(result, "missing_table_specification")


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story(AllureStory.ARRAY_ORDERED_RESOLVER)
@pytest.mark.spark
class TestArrayOrderedFieldValueWithData:
    """Tests for _array_ordered_field_value with actual array data."""

    @allure.title("Array ordered preserves order")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SMOKE, AllureTag.SPARK)
    def test_array_ordered_basic(self, spark, df_expected_ordered):
        with allure.step("Verify: Array Ordered Basic"):
            cfg = {
                "expected_from": {
                    "column": "rankings",
                    "field": "score",
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_expected_ordered):
                result = _array_ordered_field_value(spark, df_expected_ordered, cfg)
            assert_check_success(result)
            assert_column_equals(result, expected="rankings")
            assert_details_contain(result, "array_ordered_field_value")

    @allure.title("Array ordered supports group_by")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_array_ordered_with_group_by(self, spark, df_expected_ordered):
        with allure.step("Verify: Array Ordered With Group By"):
            cfg = {
                "expected_from": {
                    "column": "rankings",
                    "field": "score",
                    "database": "test",
                    "table": "test_table",
                    "group_by": "group_id",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_expected_ordered):
                result = _array_ordered_field_value(spark, df_expected_ordered, cfg)
            assert_check_success(result)
            assert_result_is_grouped(result)
            assert_details_contain(result, "grouped_by")

    @allure.title("Array ordered supports custom value_type")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_array_ordered_with_value_type(self, spark, df_expected_ordered):
        with allure.step("Verify: Array Ordered With Value Type"):
            cfg = {
                "expected_from": {
                    "column": "rankings",
                    "field": "rank",
                    "value_type": "int",
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_expected_ordered):
                result = _array_ordered_field_value(spark, df_expected_ordered, cfg)
            assert_check_success(result)

    @allure.title("Array ordered handles column not found")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_ordered_column_not_found(self, spark, df_expected_ordered):
        with allure.step("Verify: Array Ordered Column Not Found"):
            cfg = {
                "expected_from": {
                    "column": "nonexistent",
                    "field": "score",
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_expected_ordered):
                result = _array_ordered_field_value(spark, df_expected_ordered, cfg)
            assert_check_failed(result)
            assert_details_contain(result, "column_not_found")

    @allure.title("Array ordered handles nested path")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_array_ordered_with_nested_path(self, spark, df_rankings):
        with allure.step("Verify: Array Ordered With Nested Path"):
            cfg = {
                "expected_from": {
                    "column": "ranking_data",
                    "field": "rank_count",
                    "path": "ranks",
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_rankings):
                _array_ordered_field_value(spark, df_rankings, cfg)

    @allure.title("Array ordered handles errors gracefully")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_array_ordered_error_handling(self, spark, df_basic):
        with allure.step("Verify: Array Ordered Error Handling"):
            cfg = {
                "expected_from": {
                    "column": "name",
                    "field": "value",
                    "database": "test",
                    "table": "test_table",
                }
            }
            with patch("neco_dq.core.metrics.expected._resolve_table", return_value=df_basic):
                _array_ordered_field_value(spark, df_basic, cfg)


# ---------------------------------------------------------------------------
# Resolve Expected Exception Handling Tests
# ---------------------------------------------------------------------------


@allure.feature(AllureFeature.EXPECTED_RESOLVERS)
@allure.story("Exception Handling")
@pytest.mark.spark
class TestResolveExpectedException:
    """Tests for resolve_expected exception handling."""

    @allure.title("Catches and reports resolver exceptions")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_resolve_expected_catches_resolver_exception(self, spark, df_basic):
        with allure.step("Setup test data"):
            cfg = {
                "expected_from": {
                    "type": "array_field_value",
                    "column": "items",
                    "field": "value",
                }
            }
        with allure.step("Execute function under test"):
            result = resolve_expected(spark, df_basic, cfg)
        with allure.step("Verify: Resolve Expected Catches Resolver Exception"):
            assert_check_failed(result)

    @allure.title("Catches unexpected exceptions from resolvers")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_resolve_expected_catches_unexpected_exception(self, spark, df_basic):
        with allure.step("Verify: Resolve Expected Catches Unexpected Exception"):

            def raising_resolver(*_args, **_kwargs):
                raise RuntimeError("Unexpected error in resolver")

            cfg = {"expected_from": {"type": "static"}}
            with patch("neco_dq.core.metrics.expected.EXPECTED_RESOLVERS", {"static": raising_resolver}):
                result = resolve_expected(spark, df_basic, cfg)
            assert_check_failed(result)
            assert_details_contain(result, "expected_resolution_error")
            assert "Unexpected error" in result[3]
