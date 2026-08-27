# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""End-to-end checks that new GE rules flow through to persisted history rows.

These tests run the real GEDQTestSuite against a materialized Spark table and
assert that the new rules (length, date_format, not_in_set, distinct_values_in_set,
multicolumn_sum) produce normalized result rows with the framework `check_name` the
DQResultWriter persists - not the raw GE expectation type.
"""

import allure
import pytest

from neco_dq.test_suites.ge.ge_dq_test_suite import GEDQTestSuite
from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_RESULT_NORMALIZER)
class TestGEHistoryPersistence:
    """Verify new GE rules reach the persisted history rows with correct check names."""

    @pytest.fixture
    def orders_table(self, spark):
        """Materialize a small table covering every new GE rule, then drop it."""
        df = spark.createDataFrame(
            [
                ("AB12", "2026-01-01", "active", 60, 40),
                ("CD34", "2026-02-15", "inactive", 70, 30),
                ("EF56", "2026-03-20", "active", 55, 45),
            ],
            "code STRING, created_date STRING, status STRING, subtotal INT, tax INT",
        )
        spark.sql("CREATE SCHEMA IF NOT EXISTS ge_hist_test")
        df.write.mode("overwrite").saveAsTable("ge_hist_test.orders")
        yield ("spark_catalog", "ge_hist_test", "orders")
        spark.sql("DROP SCHEMA IF EXISTS ge_hist_test CASCADE")

    @allure.title("New GE rules produce persisted rows with framework check names")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_new_ge_rules_reach_history_rows(self, spark, orders_table):
        catalog, schema, table = orders_table
        config = {
            "great_expectations": {
                "tables": [
                    {
                        "catalog": catalog,
                        "schema": schema,
                        "table": table,
                        "checks": {
                            "code": {
                                "length": {"enabled": True, "min": 4, "max": 4, "severity": "warning"},
                            },
                            "created_date": {
                                "date_format": {"enabled": True, "format": "%Y-%m-%d", "severity": "warning"},
                            },
                            "status": {
                                "not_in_set": {"enabled": True, "values": ["N/A"], "severity": "warning"},
                                "distinct_values_in_set": {
                                    "enabled": True,
                                    "values": ["active", "inactive"],
                                    "severity": "warning",
                                },
                            },
                            "_dataset": {
                                "multicolumn_sum": [
                                    {"column_list": ["subtotal", "tax"], "sum_total": 100, "severity": "warning"},
                                ],
                            },
                        },
                    }
                ],
            }
        }

        with allure.step("Run the GE suite end-to-end against the table"):
            suite = GEDQTestSuite(spark, config)
            results = suite.validate_table(config["great_expectations"]["tables"][0])

        with allure.step("Collect the check names that would be persisted"):
            check_names = {r.get("check_name") for r in results}

        with allure.step("Verify every new rule appears with its framework name (not raw GE type)"):
            for expected in ("length", "date_format", "not_in_set", "distinct_values_in_set", "multicolumn_sum"):
                assert expected in check_names, f"{expected} missing from persisted rows: {check_names}"
            # None of the raw GE expectation types should leak into check_name.
            assert not any(str(name).startswith("expect_") for name in check_names), check_names

    @allure.title("Persisted rows for new GE rules carry the fields the writer needs")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_new_ge_rule_rows_have_persistence_fields(self, spark, orders_table):
        catalog, schema, table = orders_table
        config = {
            "great_expectations": {
                "tables": [
                    {
                        "catalog": catalog,
                        "schema": schema,
                        "table": table,
                        "checks": {
                            "created_date": {
                                "date_format": {"enabled": True, "format": "%Y-%m-%d", "severity": "warning"},
                            },
                        },
                    }
                ],
            }
        }

        with allure.step("Run the GE suite and find the date_format row"):
            suite = GEDQTestSuite(spark, config)
            results = suite.validate_table(config["great_expectations"]["tables"][0])
            row = next(r for r in results if r.get("check_name") == "date_format")

        with allure.step("Verify the row carries the columns the writer persists"):
            assert row["column"] == "created_date"
            assert row["passed"] is True
            assert row["severity"] == "warning"
            assert "execution" in row
