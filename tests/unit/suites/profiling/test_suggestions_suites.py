# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUGGEST)
class TestBronzeSuggestions:
    @allure.title("Build bronze suggestions — top-level keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_suggestion_keys(self, profiling_suite, df_mixed):
        """Given a mixed-type DataFrame, when bronze suggestions are built, then the result contains exactly
        the expected top-level check keys.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            not_null_cols = [col for col, s in stats.items() if s.get("null_count", 0) == 0]
            date_cols = classified["date"]

        with allure.step("Build bronze suggestions"):
            result = profiling_suite._build_bronze_suggestions(stats, not_null_cols, date_cols)

        with allure.step("Verify: observed bronze suggestion keys"):
            assert set(result.keys()) == {
                "record_count",
                "mandatory_fields",
                "schema_drift",
                "freshness",
                "table_freshness",
                "record_count_anomaly",
            }

    @allure.title("Build bronze suggestions — record_count always enabled and blocking")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_record_count(self, profiling_suite, df_mixed):
        """Given bronze suggestions are built, when the record_count entry is inspected, then it is enabled,
        critical severity, and blocking.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            not_null_cols = [col for col, s in stats.items() if s.get("null_count", 0) == 0]
            date_cols = classified["date"]

        with allure.step("Build bronze suggestions"):
            result = profiling_suite._build_bronze_suggestions(stats, not_null_cols, date_cols)

        with allure.step("Verify: record_count is critical and blocking"):
            rc = result["record_count"]
            assert rc["enabled"] is True
            assert rc["severity"] == "critical"
            assert rc["blocking"] is True

    @allure.title("Build bronze suggestions — mandatory_fields lists all non-null columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_mandatory_fields(self, profiling_suite, df_mixed):
        """Given all columns are non-null, when bronze suggestions are built, then mandatory_fields lists all
        columns as a non-blocking warning.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            not_null_cols = [col for col, s in stats.items() if s.get("null_count", 0) == 0]
            date_cols = classified["date"]

        with allure.step("Build bronze suggestions"):
            result = profiling_suite._build_bronze_suggestions(stats, not_null_cols, date_cols)

        with allure.step("Verify: mandatory_fields contains all non-null columns (non-blocking warning)"):
            mf = result["mandatory_fields"]
            assert mf["enabled"] is True
            # 0% nulls is a snapshot fact, not a contract → non-blocking warning
            assert mf["severity"] == "warning"
            assert "blocking" not in mf
            assert set(mf["columns"]) == {"id", "name", "created", "active", "amount"}
            assert len(mf["columns"]) == 5

    @allure.title("Build bronze suggestions — schema_drift maps column types correctly")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_schema_drift(self, profiling_suite, df_mixed):
        """Given bronze suggestions are built, when schema_drift is inspected, then expected_columns maps
        each DataFrame column to its correct type string.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            not_null_cols = [col for col, s in stats.items() if s.get("null_count", 0) == 0]
            date_cols = classified["date"]

        with allure.step("Build bronze suggestions"):
            result = profiling_suite._build_bronze_suggestions(stats, not_null_cols, date_cols)

        with allure.step("Verify: schema_drift expected_columns type mapping"):
            sd = result["schema_drift"]
            assert sd["enabled"] is True
            assert sd["severity"] == "warning"
            expected = sd["expected_columns"]
            assert expected["id"] == "int"
            assert expected["name"] == "string"
            assert expected["created"] == "timestamp"
            assert expected["active"] == "boolean"
            assert expected["amount"] == "int"

    @allure.title("Build bronze suggestions — freshness uses 'created' column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_freshness(self, profiling_suite, df_mixed):
        """Given a DataFrame with a date column 'created', when bronze suggestions are built, then freshness
        targets 'created' with a 24-hour max delay.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            not_null_cols = [col for col, s in stats.items() if s.get("null_count", 0) == 0]
            date_cols = classified["date"]

        with allure.step("Build bronze suggestions"):
            result = profiling_suite._build_bronze_suggestions(stats, not_null_cols, date_cols)

        with allure.step("Verify: freshness uses created column with 24h max delay"):
            fr = result["freshness"]
            assert fr["enabled"] is True
            assert fr["timestamp_column"] == "created"
            assert fr["max_delay_hours"] == 24
            assert fr["severity"] == "warning"

    @allure.title("Build bronze suggestions — table_freshness always present")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_table_freshness(self, profiling_suite, df_mixed):
        """Given bronze suggestions are built, when table_freshness is inspected, then it is enabled and
        configured with a 2-day stale threshold.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            not_null_cols = [col for col, s in stats.items() if s.get("null_count", 0) == 0]
            date_cols = classified["date"]

        with allure.step("Build bronze suggestions"):
            result = profiling_suite._build_bronze_suggestions(stats, not_null_cols, date_cols)

        with allure.step("Verify: table_freshness uses Delta metadata with 2 days stale"):
            tf = result["table_freshness"]
            assert tf["enabled"] is True
            assert tf["max_days_stale"] == 2
            assert tf["severity"] == "warning"

    @allure.title("Build bronze suggestions — record_count_anomaly disabled by default")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_record_count_anomaly(self, profiling_suite, df_mixed):
        """Given bronze suggestions are built, when record_count_anomaly is inspected, then it is disabled by
        default and carries placeholder history table config.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            not_null_cols = [col for col, s in stats.items() if s.get("null_count", 0) == 0]
            date_cols = classified["date"]

        with allure.step("Build bronze suggestions"):
            result = profiling_suite._build_bronze_suggestions(stats, not_null_cols, date_cols)

        with allure.step("Verify: record_count_anomaly is disabled with placeholder config"):
            rca = result["record_count_anomaly"]
            assert rca["enabled"] is False
            assert rca["history_table"] == "<catalog.schema.dq_history_table>"
            assert rca["allowed_deviation_percent"] == 40
            assert rca["severity"] == "warning"


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUGGEST)
class TestSilverSuggestions:
    @allure.title("Build silver suggestions — top-level keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_silver_suggestion_keys(self, profiling_suite, df_mixed):
        """Given a mixed-type DataFrame, when silver suggestions are built, then the result contains exactly
        the expected top-level check keys.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build silver suggestions"):
            result = profiling_suite._build_silver_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                column_stats=stats,
                row_count=row_count,
                json_cols=None,
                pk_columns=None,
            )

        with allure.step("Verify: observed silver suggestion keys"):
            assert set(result.keys()) == {"idempotency", "freshness"}

    @allure.title("Build silver suggestions — idempotency uses unique columns as key")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_silver_idempotency(self, profiling_suite, df_mixed):
        """Given unique columns detected by profiling, when silver suggestions are built without and with a
        Unity Catalog PK, then idempotency is a disabled candidate without PK and an enabled blocking check
        with PK.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build silver suggestions without a primary key"):
            result = profiling_suite._build_silver_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                column_stats=stats,
                row_count=row_count,
                json_cols=None,
                pk_columns=None,
            )

        with allure.step("Verify: idempotency is a disabled candidate (key only from approximate profiling)"):
            idm = result["idempotency"]
            assert idm["enabled"] is False
            assert "blocking" not in idm
            # When multiple candidate cols, key_column is a list
            assert set(idm["key_columns"]) == {"id", "created", "amount"}

        with allure.step("Build silver suggestions WITH a Unity Catalog primary key"):
            result_pk = profiling_suite._build_silver_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                column_stats=stats,
                row_count=row_count,
                json_cols=None,
                pk_columns=["id"],
            )

        with allure.step("Verify: PK-backed idempotency is enabled and blocking"):
            idm_pk = result_pk["idempotency"]
            assert idm_pk["enabled"] is True
            assert idm_pk["severity"] == "error"
            assert idm_pk["blocking"] is True
            assert idm_pk["key_columns"] == "id"

    @allure.title("Build silver suggestions — freshness uses 'created' column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_silver_freshness(self, profiling_suite, df_mixed):
        """Given a DataFrame with a date column 'created', when silver suggestions are built, then freshness
        targets 'created' with a 72-hour max delay.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build silver suggestions"):
            result = profiling_suite._build_silver_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                column_stats=stats,
                row_count=row_count,
                json_cols=None,
                pk_columns=None,
            )

        with allure.step("Verify: freshness uses created column with 72h max delay"):
            fr = result["freshness"]
            assert fr["enabled"] is True
            assert fr["timestamp_column"] == "created"
            assert fr["max_delay_hours"] == 72
            assert fr["severity"] == "warning"

    @allure.title("Build silver suggestions — no cardinality or status checks for df_mixed")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_silver_no_extra_checks(self, profiling_suite, df_mixed):
        """Given a DataFrame without low-cardinality enum or JSON columns, when silver suggestions are built,
        then cardinality, status_transition, and json_schema are not included.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build silver suggestions"):
            result = profiling_suite._build_silver_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                column_stats=stats,
                row_count=row_count,
                json_cols=None,
                pk_columns=None,
            )

        with allure.step("Verify: cardinality, status_transition, json_schema not suggested"):
            assert "cardinality" not in result
            assert "status_transition" not in result
            assert "json_schema" not in result


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUGGEST)
class TestGoldSuggestions:
    @allure.title("Build gold suggestions — top-level keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_gold_suggestion_keys(self, profiling_suite, df_mixed):
        """Given a mixed-type DataFrame without enum or high-cardinality columns, when gold suggestions are
        built, then the result contains exactly uniqueness, volume, and freshness keys.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build gold suggestions"):
            result = profiling_suite._build_gold_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                row_count=row_count,
                column_stats=stats,
                json_cols=None,
            )

        with allure.step("Verify: observed gold suggestion keys"):
            # completeness NOT triggered: the only low-cardinality string column (name)
            # is free text, not enum-like, so it is excluded by the _is_enum_column gate
            # trend NOT triggered: amount has only 4 distinct values (not > 10)
            assert set(result.keys()) == {"uniqueness", "volume", "freshness"}

    @allure.title("Build gold suggestions — uniqueness uses all unique columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_gold_uniqueness(self, profiling_suite, df_mixed):
        """Given unique columns detected by profiling, when gold suggestions are built without and with a
        Unity Catalog PK, then uniqueness is a disabled candidate without PK and an enabled blocking check
        with PK.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build gold suggestions without a primary key"):
            result = profiling_suite._build_gold_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                row_count=row_count,
                column_stats=stats,
                json_cols=None,
            )

        with allure.step("Verify: uniqueness is a disabled candidate (columns only from approximate profiling)"):
            uq = result["uniqueness"]
            assert uq["enabled"] is False
            assert uq["severity"] == "critical"
            assert "blocking" not in uq
            assert set(uq["columns"]) == {"id", "created", "amount"}

        with allure.step("Build gold suggestions WITH a Unity Catalog primary key"):
            result_pk = profiling_suite._build_gold_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                row_count=row_count,
                column_stats=stats,
                json_cols=None,
                pk_columns=["id"],
            )

        with allure.step("Verify: PK-backed uniqueness is enabled and blocking"):
            uq_pk = result_pk["uniqueness"]
            assert uq_pk["enabled"] is True
            assert uq_pk["severity"] == "critical"
            assert uq_pk["blocking"] is True
            assert uq_pk["columns"] == ["id"]

    @allure.title("Build gold suggestions — volume always present when row_count > 0")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_gold_volume(self, profiling_suite, df_mixed):
        """Given a non-empty DataFrame, when gold suggestions are built, then volume is disabled by default
        and configured with a COUNT(*) metric and placeholder history table.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build gold suggestions"):
            result = profiling_suite._build_gold_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                row_count=row_count,
                column_stats=stats,
                json_cols=None,
            )

        with allure.step("Verify: volume uses COUNT(*) with history table placeholder"):
            vol = result["volume"]
            # disabled by default in v1.2.2: history.table is a placeholder needing review
            assert vol["enabled"] is False
            assert vol["metric"] == "COUNT(*)"
            assert vol["severity"] == "warning"
            hist = vol["history"]
            assert hist["table"] == "<catalog.schema.dq_history_table>"
            assert hist["tolerance_percent"] == 50

    @allure.title("Build gold suggestions — freshness uses 'created' column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_gold_freshness(self, profiling_suite, df_mixed):
        """Given a DataFrame with a date column 'created', when gold suggestions are built, then freshness
        targets 'created' with a 24-hour max delay.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build gold suggestions"):
            result = profiling_suite._build_gold_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                row_count=row_count,
                column_stats=stats,
                json_cols=None,
            )

        with allure.step("Verify: freshness uses created column with 24h max delay"):
            fr = result["freshness"]
            assert fr["enabled"] is True
            assert fr["timestamp_column"] == "created"
            assert fr["max_delay_hours"] == 24
            assert fr["severity"] == "warning"

    @allure.title("Build gold suggestions — no json_schema check without json_cols")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_gold_no_json_schema(self, profiling_suite, df_mixed):
        """Given no JSON columns are detected, when gold suggestions are built, then json_schema is not
        included in the result.
        """
        with allure.step("Compute inputs"):
            row_count = df_mixed.count()
            stats = profiling_suite._compute_column_stats(df_mixed, row_count)
            classified = profiling_suite._classify_columns(df_mixed)
            unique_cols = [col for col, s in stats.items() if s.get("distinct_ratio", 0) == 1.0]
            date_cols = classified["date"]

        with allure.step("Build gold suggestions"):
            result = profiling_suite._build_gold_suggestions(
                unique_cols=unique_cols,
                date_cols=date_cols,
                row_count=row_count,
                column_stats=stats,
                json_cols=None,
            )

        with allure.step("Verify: json_schema not suggested when no JSON columns"):
            assert "json_schema" not in result
