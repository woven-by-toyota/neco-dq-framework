# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Characterization tests for SparkProfilingDQTestSuite.run() end-to-end.

These golden-master tests lock the structural shape of run() output in all three
modes (checks, stats, suggest) as a safety net for the upcoming refactor.

Observed output shapes (locked from real execution on df_mixed):

checks mode (32 rows):
  row keys: {'catalog', 'schema', 'table', 'column', 'run_timestamp',
             'check_name', 'passed', 'status', 'severity', 'blocking', 'details',
             'test_suite'}
  check_name set: {'empty_table', 'table_freshness', 'high_null_ratio',
                   'single_value', 'low_cardinality_suspect', 'high_distinct_ratio',
                   'sensitive_data', 'column_name_smell'}
  blocking: always False

stats mode (1 dict per table):
  keys: {'test_suite', 'mode', 'catalog', 'schema', 'table', 'run_timestamp',
         'row_count', 'schema_info', 'column_stats'}

suggest mode (1 dict per table):
  keys: {'test_suite', 'mode', 'catalog', 'schema', 'table', 'run_timestamp',
         'row_count', 'suggestions', 'column_stats'}
"""

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark

_CHECKS_ROW_KEYS = {
    "test_suite",
    "catalog",
    "schema",
    "table",
    "column",
    "run_timestamp",
    "check_name",
    "passed",
    "status",
    "severity",
    "blocking",
    "details",
}

_CHECKS_NAMES = {
    "empty_table",
    "table_freshness",
    "high_null_ratio",
    "single_value",
    "low_cardinality_suspect",
    "high_distinct_ratio",
    "sensitive_data",
    "column_name_smell",
}

_STATS_KEYS = {
    "test_suite",
    "mode",
    "catalog",
    "schema",
    "table",
    "run_timestamp",
    "row_count",
    "schema_info",
    "column_stats",
}

_SUGGEST_KEYS = {
    "test_suite",
    "mode",
    "catalog",
    "schema",
    "table",
    "run_timestamp",
    "row_count",
    "suggestions",
    "column_stats",
}


def _config_for(catalog: str, schema: str, table: str, mode: str) -> dict:
    return {
        "mode": mode,
        "tables": [{"catalog": catalog, "schema": schema, "table": table}],
    }


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_RUN_MODES)
class TestRunModes:
    @allure.title("run() in checks mode returns check rows with correct structure")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_checks_mode(self, make_suite, materialize_table, df_mixed):
        """
        Given a materialized table,
        When run() is called in checks mode (the default),
        Then it returns a non-empty list of check rows, every row has the
             expected key set, every row has blocking=False, and the set of
             check_name values matches the observed profiling check catalogue.
        """
        with allure.step("Materialize table and run checks mode"):
            catalog, schema, table = materialize_table("prof_test", "t_checks", df_mixed)
            suite = make_suite(_config_for(catalog, schema, table, "checks"))
            rows = suite.run()

        with allure.step("Verify: result is a non-empty list"):
            assert isinstance(rows, list)
            assert len(rows) > 0

        with allure.step("Verify: every row has the expected key set"):
            for row in rows:
                assert set(row.keys()) >= _CHECKS_ROW_KEYS

        with allure.step("Verify: blocking is always False for profiling"):
            assert all(row["blocking"] is False for row in rows)

        with allure.step("Verify: test_suite field is 'profiling' on every row"):
            assert all(row["test_suite"] == "profiling" for row in rows)

        with allure.step("Verify: catalog/schema/table echo back correctly"):
            assert all(row["catalog"] == catalog for row in rows)
            assert all(row["schema"] == schema for row in rows)
            assert all(row["table"] == table for row in rows)

        with allure.step("Verify: the set of check_name values matches observed profiling checks"):
            observed_check_names = {row["check_name"] for row in rows}
            assert observed_check_names == _CHECKS_NAMES

    @allure.title("run() in stats mode returns stats dict per table with correct structure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_stats_mode(self, make_suite, materialize_table, df_mixed):
        """
        Given a materialized table,
        When run() is called in stats mode,
        Then it returns a list with one stats dict whose top-level key set
             matches the observed shape and whose identity fields echo back
             the configured catalog/schema/table.
        """
        with allure.step("Materialize table and run stats mode"):
            catalog, schema, table = materialize_table("prof_test", "t_stats", df_mixed)
            suite = make_suite(_config_for(catalog, schema, table, "stats"))
            results = suite.run()

        with allure.step("Verify: result is a non-empty list"):
            assert isinstance(results, list)
            assert len(results) == 1

        with allure.step("Verify: top-level key set matches observed shape"):
            result = results[0]
            assert set(result.keys()) >= _STATS_KEYS

        with allure.step("Verify: identity fields are correct"):
            assert result["test_suite"] == "profiling"
            assert result["mode"] == "stats"
            assert result["catalog"] == catalog
            assert result["schema"] == schema
            assert result["table"] == table

        with allure.step("Verify: row_count is a positive integer"):
            assert isinstance(result["row_count"], int)
            assert result["row_count"] > 0

        with allure.step("Verify: column_stats and schema_info are present and non-empty"):
            assert isinstance(result["column_stats"], dict)
            assert len(result["column_stats"]) > 0
            assert isinstance(result["schema_info"], (dict, list))

    @allure.title("run() in suggest mode returns suggestion dict per table with correct structure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_suggest_mode(self, make_suite, materialize_table, df_mixed):
        """
        Given a materialized table,
        When run() is called in suggest mode,
        Then it returns a list with one suggestion dict whose top-level key set
             matches the observed shape and whose identity fields echo back
             the configured catalog/schema/table.
        """
        with allure.step("Materialize table and run suggest mode"):
            catalog, schema, table = materialize_table("prof_test", "t_suggest", df_mixed)
            suite = make_suite(_config_for(catalog, schema, table, "suggest"))
            results = suite.run()

        with allure.step("Verify: result is a non-empty list"):
            assert isinstance(results, list)
            assert len(results) == 1

        with allure.step("Verify: top-level key set matches observed shape"):
            result = results[0]
            assert set(result.keys()) >= _SUGGEST_KEYS

        with allure.step("Verify: identity fields are correct"):
            assert result["test_suite"] == "profiling"
            assert result["mode"] == "suggest"
            assert result["catalog"] == catalog
            assert result["schema"] == schema
            assert result["table"] == table

        with allure.step("Verify: row_count echoes materialized row count"):
            assert result["row_count"] == df_mixed.count()

        with allure.step("Verify: suggestions key is present"):
            assert "suggestions" in result

        with allure.step("Verify: column_stats is present and non-empty"):
            assert isinstance(result["column_stats"], dict)
            assert len(result["column_stats"]) > 0
