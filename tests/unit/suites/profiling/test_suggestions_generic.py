# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUGGEST)
class TestGenericSuggestions:
    """Characterization tests for generic suggestion builder methods.

    Real signatures confirmed from profiling_dq_test_suite.py:
      - _suggest_not_null_columns(column_stats: dict[str, dict]) -> list[str]
      - _suggest_unique_columns(column_stats: dict[str, dict], row_count: int) -> list[str]
      - _suggest_allowed_values(column_stats: dict[str, dict], row_count: int) -> list[dict]
      - _suggest_range_checks(column_stats: dict[str, dict]) -> list[dict]
      - _suggest_date_checks(column_stats: dict[str, dict]) -> list[dict]
      - _suggest_json_schema_columns(column_stats: dict[str, dict]) -> list[dict]
    """

    @allure.title("Suggest not-null columns — all columns with 0% nulls")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_not_null_columns(self, profiling_suite, df_mixed):
        """Given a DataFrame where all columns have 0% nulls, when not-null columns are suggested,
        then all 5 columns are returned."""
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Suggest not-null columns"):
            result = profiling_suite._suggest_not_null_columns(stats)

        with allure.step("Verify: all 5 columns appear (null_pct == 0 for all)"):
            assert set(result) == {"id", "name", "created", "active", "amount"}

    @allure.title("Suggest unique columns — columns with distinct_ratio >= 0.98")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_unique_columns(self, profiling_suite, df_mixed):
        """Given a DataFrame where id, created, and amount have distinct_ratio of 1.0, when unique columns
        are suggested, then exactly those three columns are returned."""
        with allure.step("Compute stats"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)

        with allure.step("Suggest unique columns"):
            result = profiling_suite._suggest_unique_columns(stats, row_count)

        with allure.step("Verify: id, created, amount have distinct_ratio == 1.0"):
            assert set(result) == {"id", "created", "amount"}

    @allure.title("Suggest unique columns — empty result when row_count is zero")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_suggest_unique_columns_zero_row_count(self, profiling_suite, df_mixed):
        """Given a row_count of 0, when unique columns are suggested, then an empty list is returned without errors."""
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Suggest unique columns with row_count=0"):
            result = profiling_suite._suggest_unique_columns(stats, 0)

        with allure.step("Verify: empty list when row_count is 0"):
            assert result == []

    @allure.title("Suggest allowed-values — low-cardinality string column 'name'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_allowed_values(self, profiling_suite, df_mixed):
        """Given a DataFrame with one low-cardinality string column ('name'), when allowed values are suggested,
        then a single suggestion for 'name' with alpha/beta/gamma is returned."""
        with allure.step("Compute stats"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)

        with allure.step("Suggest allowed values"):
            result = profiling_suite._suggest_allowed_values(stats, row_count)

        with allure.step("Verify: name column suggested with alpha/beta/gamma values"):
            assert len(result) == 1
            suggestion = result[0]
            assert suggestion["column"] == "name"
            assert set(suggestion["values"]) == {"alpha", "beta", "gamma"}

    @allure.title("Suggest allowed-values — empty result when row_count is zero")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_suggest_allowed_values_zero_row_count(self, profiling_suite, df_mixed):
        """Given a row_count of 0, when allowed values are suggested, then an empty list is returned without errors."""
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Suggest allowed values with row_count=0"):
            result = profiling_suite._suggest_allowed_values(stats, 0)

        with allure.step("Verify: empty list when row_count is 0"):
            assert result == []

    @allure.title("Suggest range checks — numeric columns with clear bounds (id excluded as ID col)")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_range_checks(self, profiling_suite, df_mixed):
        """Given a DataFrame where 'id' is an ID column and 'amount' is a regular numeric column,
        when range checks are suggested, then only 'amount' is returned with its min/max/mean/stddev."""
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Suggest range checks"):
            result = profiling_suite._suggest_range_checks(stats)

        with allure.step("Verify: only amount suggested (id skipped as ID column)"):
            assert len(result) == 1
            suggestion = result[0]
            assert suggestion["column"] == "amount"
            assert suggestion["min"] == 10.5
            assert suggestion["max"] == 40.0
            assert suggestion["mean"] == 25.125
            assert suggestion["stddev"] == 12.7173

    @allure.title("Suggest date checks — date columns with min/max bounds")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_date_checks(self, profiling_suite, df_mixed):
        """Given a DataFrame with a 'created' date column spanning 3 days, when date checks are suggested,
        then a single suggestion for 'created' with correct min/max dates and range is returned."""
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Suggest date checks"):
            result = profiling_suite._suggest_date_checks(stats)

        with allure.step("Verify: created column with correct date range"):
            assert len(result) == 1
            suggestion = result[0]
            assert suggestion["column"] == "created"
            assert suggestion["min_date"] == "2026-01-01"
            assert suggestion["max_date"] == "2026-01-04"
            assert suggestion["range_days"] == 3

    @allure.title("Suggest JSON schema columns — no JSON columns in df_mixed")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_json_schema_columns_none(self, profiling_suite, df_mixed):
        """Given a DataFrame with no JSON columns, when JSON schema columns are suggested,
        then an empty list is returned."""
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Suggest JSON schema columns"):
            result = profiling_suite._suggest_json_schema_columns(stats)

        with allure.step("Verify: empty list — df_mixed has no JSON columns"):
            assert result == []
