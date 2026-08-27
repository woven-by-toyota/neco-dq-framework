# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_COLUMN_STATS)
class TestColumnStats:
    @allure.title("Compute column stats — top-level keys match DataFrame columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_column_stats_keys(self, profiling_suite, df_mixed):
        """Given a mixed-type DataFrame, when column stats are computed, then keys match all column names."""
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Verify: top-level keys match all DataFrame columns"):
            assert set(stats.keys()) == {"id", "name", "created", "active", "amount"}

    @allure.title("Compute column stats — numeric integer column 'id'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_numeric_integer_column(self, profiling_suite, df_mixed):
        """Given a DataFrame with an integer column 'id', when stats are computed, then numeric metrics
        including min, max, mean, stddev, and percentiles are correct.
        """
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Verify: numeric stats for 'id'"):
            id_stats = stats["id"]
            assert id_stats["type"] == "numeric"
            assert id_stats["null_count"] == 0
            assert id_stats["null_pct"] == 0.0
            assert id_stats["distinct_count"] == 4
            assert id_stats["distinct_ratio"] == 1.0
            assert id_stats["min"] == 1
            assert id_stats["max"] == 4
            assert id_stats["mean"] == 2.5
            assert id_stats["stddev"] == 1.291
            assert id_stats["p25"] == 1
            assert id_stats["p50"] == 2
            assert id_stats["p75"] == 3
            assert id_stats["p95"] == 4

    @allure.title("Compute column stats — numeric double column 'amount'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_numeric_double_column(self, profiling_suite, df_mixed):
        """Given a DataFrame with a double column 'amount', when stats are computed, then numeric metrics
        including min, max, mean, stddev, and percentiles are correct.
        """
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Verify: numeric stats for 'amount'"):
            amount_stats = stats["amount"]
            assert amount_stats["type"] == "numeric"
            assert amount_stats["null_count"] == 0
            assert amount_stats["null_pct"] == 0.0
            assert amount_stats["distinct_count"] == 4
            assert amount_stats["distinct_ratio"] == 1.0
            assert amount_stats["min"] == 10.5
            assert amount_stats["max"] == 40.0
            assert amount_stats["mean"] == 25.125
            assert amount_stats["stddev"] == 12.7173
            assert amount_stats["p25"] == 10.5
            assert amount_stats["p50"] == 20.0
            assert amount_stats["p75"] == 30.0
            assert amount_stats["p95"] == 40.0

    @allure.title("Compute column stats — string column 'name'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_string_column(self, profiling_suite, df_mixed):
        """Given a DataFrame with a string column 'name', when stats are computed, then string-specific metrics
        including length stats, top values, and JSON flags are correct.
        """
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Verify: string stats for 'name'"):
            name_stats = stats["name"]
            assert name_stats["type"] == "string"
            assert name_stats["null_count"] == 0
            assert name_stats["null_pct"] == 0.0
            assert name_stats["distinct_count"] == 3
            assert name_stats["distinct_ratio"] == 0.75
            assert name_stats["min_length"] == 4
            assert name_stats["max_length"] == 5
            assert name_stats["avg_length"] == 4.75
            assert name_stats["is_json"] is False
            assert name_stats["json_is_array"] is False
            assert name_stats["top_values"][0] == {"value": "alpha", "count": 2}
            assert len(name_stats["top_values"]) == 3
            assert {"value": "beta", "count": 1} in name_stats["top_values"]
            assert {"value": "gamma", "count": 1} in name_stats["top_values"]

    @allure.title("Compute column stats — date column 'created'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_date_column(self, profiling_suite, df_mixed):
        """Given a DataFrame with a date column 'created', when stats are computed, then date-specific metrics
        including min_date, max_date, and date_range_days are correct.
        """
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Verify: date stats for 'created'"):
            created_stats = stats["created"]
            assert created_stats["type"] == "date"
            assert created_stats["null_count"] == 0
            assert created_stats["null_pct"] == 0.0
            assert created_stats["distinct_count"] == 4
            assert created_stats["distinct_ratio"] == 1.0
            assert created_stats["min_date"] == "2026-01-01"
            assert created_stats["max_date"] == "2026-01-04"
            assert created_stats["date_range_days"] == 3

    @allure.title("Compute column stats — boolean column 'active'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_boolean_column(self, profiling_suite, df_mixed):
        """Given a DataFrame with a boolean column 'active', when stats are computed, then the type is
        'boolean' and null/distinct counts are correct.
        """
        with allure.step("Compute stats"):
            stats = profiling_suite._compute_column_stats(df_mixed, df_mixed.count())

        with allure.step("Verify: boolean stats for 'active'"):
            active_stats = stats["active"]
            assert active_stats["type"] == "boolean"
            assert active_stats["null_count"] == 0
            assert active_stats["null_pct"] == 0.0
            assert active_stats["distinct_count"] == 2
            assert active_stats["distinct_ratio"] == 0.5
