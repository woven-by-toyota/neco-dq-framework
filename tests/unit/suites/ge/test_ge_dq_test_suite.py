# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Tests for GEDQTestSuite table loading and crash handling.

Covers the Spark -> pandas conversion (only checked columns are converted) and the
engine_crash row the suite persists when a table cannot be validated.
"""

from datetime import datetime
from unittest.mock import MagicMock

import allure
import pytest

from neco_dq.test_suites.ge.ge_dq_test_suite import GEDQTestSuite
from tests.constants import AllureFeature, AllureSeverity, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.GE_SUITE)
@allure.story("Table Loading")
class TestGEColumnPruning:
    """Only the columns referenced by the expectations reach pandas."""

    @pytest.fixture
    def projects_table(self, spark):
        """Table with a far-future timestamp, past the pandas datetime64[ns] limit (2262)."""
        df = spark.createDataFrame(
            [
                (1, "A", 10, 5, datetime(2999, 9, 25, 14, 59, 59, 999000)),
                (2, "B", 20, 5, datetime(2999, 9, 25, 14, 59, 59, 999000)),
            ],
            "project_id INT, code STRING, budget INT, spent INT, project_end_date TIMESTAMP",
        )
        spark.sql("CREATE SCHEMA IF NOT EXISTS ge_prune_test")
        df.write.mode("overwrite").saveAsTable("ge_prune_test.projects")
        yield ("spark_catalog", "ge_prune_test", "projects")
        spark.sql("DROP SCHEMA IF EXISTS ge_prune_test CASCADE")

    @staticmethod
    def _table_cfg(table_ref, checks):
        catalog, schema, table = table_ref
        return {"catalog": catalog, "schema": schema, "table": table, "checks": checks}

    @allure.title("Far-future timestamp in an unchecked column does not crash the table")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_unchecked_out_of_range_timestamp_is_ignored(self, spark, projects_table):
        table_cfg = self._table_cfg(
            projects_table,
            {"project_id": {"not_null": {"enabled": True, "severity": "error"}}},
        )

        with allure.step("Validate a table whose unchecked column holds year 2999"):
            suite = GEDQTestSuite(spark, {"great_expectations": {"tables": [table_cfg]}})
            results = suite.validate_table(table_cfg)

        with allure.step("Verify: the project_id expectation ran and passed"):
            not_null = next(r for r in results if r.get("check_name") == "not_null")
            assert not_null["passed"] is True

    @allure.title("Columns named in dataset rules and row conditions are kept")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_dataset_rule_and_row_condition_columns_are_kept(self, spark, projects_table):
        table_cfg = self._table_cfg(
            projects_table,
            {
                "project_id": {
                    "not_null": {"enabled": True, "row_condition": "code == 'A'"},
                },
                "_dataset": {
                    "integrity": {"pair_gte": [{"column_A": "budget", "column_B": "spent"}]},
                },
            },
        )

        with allure.step("Validate with columns referenced outside the column keys"):
            suite = GEDQTestSuite(spark, {"great_expectations": {"tables": [table_cfg]}})
            results = suite.validate_table(table_cfg)

        with allure.step("Verify: both expectations found their columns"):
            by_name = {r.get("check_name"): r for r in results}
            assert by_name["not_null"]["passed"] is True, by_name["not_null"]
            assert by_name["pair_gte"]["passed"] is True, by_name["pair_gte"]

    @allure.title("Row-count-only suite still counts rows without converting any table column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_row_count_only_suite(self, spark, projects_table):
        table_cfg = self._table_cfg(projects_table, {"_dataset": {"min_rows": 2, "max_rows": 2}})

        with allure.step("Validate a suite with only min_rows/max_rows"):
            suite = GEDQTestSuite(spark, {"great_expectations": {"tables": [table_cfg]}})
            results = suite.validate_table(table_cfg)

        with allure.step("Verify: the row count check saw both rows"):
            row_count = next(r for r in results if r.get("check_name") == "record_count")
            assert row_count["passed"] is True, row_count

    @allure.title("A configured column missing from the table is still reported, not a crash")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_missing_column_is_reported_by_ge(self, spark, projects_table):
        table_cfg = self._table_cfg(projects_table, {"no_such_column": {"not_null": {"enabled": True}}})

        with allure.step("Validate a column that is not in the table"):
            suite = GEDQTestSuite(spark, {"great_expectations": {"tables": [table_cfg]}})
            results = suite.validate_table(table_cfg)

        with allure.step("Verify: GE reports the missing column as a failed result"):
            assert results
            assert not all(r["passed"] for r in results)


@allure.feature(AllureFeature.GE_SUITE)
@allure.story("Run Attribution")
class TestGERunId:
    """Every persisted row of a table run carries the same run_id."""

    @pytest.fixture
    def items_table(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, code STRING")
        spark.sql("CREATE SCHEMA IF NOT EXISTS ge_runid_test")
        df.write.mode("overwrite").saveAsTable("ge_runid_test.items")
        yield ("spark_catalog", "ge_runid_test", "items")
        spark.sql("DROP SCHEMA IF EXISTS ge_runid_test CASCADE")

    @allure.title("data_filter row shares the run_id of the expectation rows")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_data_filter_row_has_run_id(self, spark, items_table):
        catalog, schema, table = items_table
        config = {
            "great_expectations": {
                "data_to_save": [{"table": "history"}],
                "tables": [
                    {
                        "catalog": catalog,
                        "schema": schema,
                        "table": table,
                        "row_filter": "id > 0",
                        "checks": {"id": {"not_null": {"enabled": True}}},
                    }
                ],
            }
        }

        with allure.step("Run the suite with a row_filter"):
            suite = GEDQTestSuite(spark, config)
            suite.writer = MagicMock()
            suite.run()

        with allure.step("Verify: one run_id shared by every row, data_filter included"):
            rows = suite.writer.write.call_args.kwargs["all_rows"]
            assert "data_filter" in {r["check_name"] for r in rows}
            run_ids = {r["run_id"] for r in rows}
            assert len(run_ids) == 1, run_ids
            assert next(iter(run_ids)).startswith(f"run_{catalog}.{schema}.{table}_")


@allure.feature(AllureFeature.GE_SUITE)
@allure.story("Crash Handling")
class TestGEEngineCrashRow:
    """The engine_crash row must be attributable to its run."""

    @allure.title("engine_crash row carries run_timestamp and string details")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_engine_crash_row_has_run_timestamp(self, spark):
        config = {
            "great_expectations": {
                "data_to_save": [{"table": "history"}],
                "tables": [
                    {
                        "catalog": "spark_catalog",
                        "schema": "ge_missing_schema",
                        "table": "missing_table",
                        "checks": {"id": {"not_null": {"enabled": True}}},
                    }
                ],
            }
        }

        with allure.step("Run the suite against a table that does not exist"):
            suite = GEDQTestSuite(spark, config)
            suite.writer = MagicMock()
            suite.run()

        with allure.step("Verify: the persisted crash row has a run timestamp"):
            rows = suite.writer.write.call_args.kwargs["all_rows"]
            assert len(rows) == 1
            row = rows[0]
            assert row["check_name"] == "engine_crash"
            assert isinstance(row["run_timestamp"], datetime)
            assert isinstance(row["details"], str)
            assert "missing_table" in row["details"]
            assert row["run_id"].startswith("run_spark_catalog.ge_missing_schema.missing_table_")
