# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUGGEST)
class TestDistinctCountSuggestion:
    @allure.title("distinct_count suggested for moderate-cardinality column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_distinct_count_moderate_cardinality(self, profiling_suite):
        """Given a moderate-cardinality non-unique column, when suggestions are computed,
        then distinct_count is included with correct bounds and confidence.
        """
        with allure.step("Setup column stats with moderate-cardinality and unique columns"):
            column_stats = {
                "category": {"type": "string", "distinct_count": 50, "distinct_ratio": 0.05},
                "order_id": {"type": "string", "distinct_count": 1000, "distinct_ratio": 1.0},
            }

        with allure.step("Execute: suggest distinct_count checks"):
            result = profiling_suite._suggest_distinct_count_checks(
                column_stats, unique_cols=["order_id"], row_count=1000
            )

        with allure.step("Verify: category is included, order_id excluded, bounds and confidence are correct"):
            cols = [c["column"] for c in result]
            assert "category" in cols
            assert "order_id" not in cols
            cat = next(c for c in result if c["column"] == "category")
            assert cat["enabled"] is True
            assert cat["min"] <= 50 <= cat["max"]
            assert cat["_confidence"] == "medium"
            assert "_reason" in cat

    @allure.title("distinct_count not suggested for date/timestamp columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_distinct_count_excludes_date_columns(self, profiling_suite):
        """Given a date column whose distinct count would otherwise qualify, when suggestions are
        computed, then it is excluded because date distinct counts grow over time.
        """
        with allure.step("Setup column stats with a qualifying date column"):
            column_stats = {
                "batch_ingestion_at": {"type": "date", "distinct_count": 48, "distinct_ratio": 0.05},
            }

        with allure.step("Execute: suggest distinct_count checks"):
            result = profiling_suite._suggest_distinct_count_checks(column_stats, unique_cols=[], row_count=1000)

        with allure.step("Verify: no suggestions returned for date columns"):
            assert [c["column"] for c in result] == []

    @allure.title("distinct_count not suggested for id-like columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_distinct_count_excludes_id_columns(self, profiling_suite):
        """Given an id-like column with moderate cardinality, when suggestions are computed,
        then it is excluded because fixed distinct-count ranges go stale as ids grow.
        """
        with allure.step("Setup column stats with an id-like column"):
            column_stats = {
                "order_id": {"type": "string", "distinct_count": 50, "distinct_ratio": 0.05},
            }

        with allure.step("Execute: suggest distinct_count checks"):
            result = profiling_suite._suggest_distinct_count_checks(column_stats, unique_cols=[], row_count=1000)

        with allure.step("Verify: no suggestions returned for id-like columns"):
            assert [c["column"] for c in result] == []


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUGGEST)
class TestTrendSuggestion:
    @allure.title("trend suggested when date col + numeric non-id col with distinct_count > 10")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_trend_checks_returns_dict(self, profiling_suite):
        """Given a date column and a qualifying numeric non-id column, when trend checks are
        suggested, then a dict with correct fields and low confidence is returned.
        """
        with allure.step("Setup column stats with date and numeric columns"):
            column_stats = {
                "event_date": {"type": "date", "distinct_count": 30, "distinct_ratio": 0.5},
                "revenue": {"type": "numeric", "distinct_count": 500, "distinct_ratio": 0.8},
                "user_id": {"type": "numeric", "distinct_count": 500, "distinct_ratio": 0.8},
            }

        with allure.step("Execute: suggest trend checks"):
            result = profiling_suite._suggest_trend_checks(date_cols=["event_date"], column_stats=column_stats)

        with allure.step("Verify: result has correct structure, date_column, metric_column, and low confidence"):
            assert result is not None
            assert result["enabled"] is False
            assert result["date_column"] == "event_date"
            assert result["metric_column"] == "revenue"
            assert "_confidence" in result
            assert result["_confidence"] == "low"
            assert "_reason" in result

    @allure.title("trend returns None when date_cols is empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_trend_checks_no_date_cols(self, profiling_suite):
        """Given no date columns, when trend checks are suggested, then None is returned."""
        with allure.step("Setup column stats with only a numeric column and no date columns"):
            column_stats = {
                "revenue": {"type": "numeric", "distinct_count": 500, "distinct_ratio": 0.8},
            }

        with allure.step("Execute: suggest trend checks with empty date_cols"):
            result = profiling_suite._suggest_trend_checks(date_cols=[], column_stats=column_stats)

        with allure.step("Verify: result is None"):
            assert result is None

    @allure.title("trend returns None when no numeric non-id col with distinct_count > 10")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_trend_checks_no_qualifying_metric(self, profiling_suite):
        """Given only id-like or low-cardinality numeric columns, when trend checks are suggested,
        then None is returned because no qualifying metric column exists.
        """
        with allure.step("Setup column stats with date col and non-qualifying numeric columns"):
            column_stats = {
                "event_date": {"type": "date", "distinct_count": 5, "distinct_ratio": 0.5},
                "user_id": {"type": "numeric", "distinct_count": 500, "distinct_ratio": 0.8},
                "amount": {"type": "numeric", "distinct_count": 4, "distinct_ratio": 0.1},
            }

        with allure.step("Execute: suggest trend checks"):
            result = profiling_suite._suggest_trend_checks(date_cols=["event_date"], column_stats=column_stats)

        with allure.step("Verify: result is None when no qualifying metric column is found"):
            assert result is None


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUGGEST)
class TestCompletenessSuggestion:
    @allure.title("completeness suggested for enum-like column with 2-10 distinct values and top_values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_completeness_checks_returns_entry(self, profiling_suite):
        """Given an enum-like column with 2-10 distinct values and populated top_values, when
        completeness checks are suggested, then a correctly structured entry is returned.
        """
        with allure.step("Setup column stats with enum-like status column and non-qualifying id column"):
            column_stats = {
                "status": {
                    "type": "string",
                    "distinct_count": 3,
                    "top_values": [
                        {"value": "active", "count": 10},
                        {"value": "inactive", "count": 5},
                        {"value": "pending", "count": 2},
                    ],
                },
                "id": {"type": "numeric", "distinct_count": 100, "top_values": []},
            }

        with allure.step("Execute: suggest completeness checks"):
            result = profiling_suite._suggest_completeness_checks(column_stats=column_stats)

        with allure.step("Verify: status entry is present, enabled=False, expected_values correct, confidence medium"):
            assert isinstance(result, list)
            assert len(result) >= 1
            entry = next((e for e in result if e["column"] == "status"), None)
            assert entry is not None
            assert entry["enabled"] is False
            assert set(entry["expected_values"]) == {"active", "inactive", "pending"}
            assert entry["_confidence"] == "medium"
            assert "_reason" in entry

    @allure.title("completeness returns empty list when no enum-like columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_completeness_checks_no_enum_cols(self, profiling_suite):
        """Given only high-cardinality and empty top_values columns, when completeness checks are
        suggested, then an empty list is returned.
        """
        with allure.step("Setup column stats with no enum-like columns"):
            column_stats = {
                "id": {"type": "numeric", "distinct_count": 1000, "top_values": []},
                "name": {"type": "string", "distinct_count": 50, "top_values": []},
            }

        with allure.step("Execute: suggest completeness checks"):
            result = profiling_suite._suggest_completeness_checks(column_stats=column_stats)

        with allure.step("Verify: result is an empty list"):
            assert result == []

    @allure.title("completeness excludes column with distinct_count outside [2, 10]")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suggest_completeness_checks_distinct_out_of_range(self, profiling_suite):
        """Given columns whose distinct_count falls outside the [2, 10] enum range, when
        completeness checks are suggested, then all are excluded.
        """
        with allure.step("Setup column stats with high-cardinality and single-value columns"):
            column_stats = {
                "high_card": {
                    "type": "string",
                    "distinct_count": 50,
                    "top_values": [{"value": "x", "count": 1}],
                },
                "single_val": {
                    "type": "string",
                    "distinct_count": 1,
                    "top_values": [{"value": "y", "count": 10}],
                },
            }

        with allure.step("Execute: suggest completeness checks"):
            result = profiling_suite._suggest_completeness_checks(column_stats=column_stats)

        with allure.step("Verify: result is an empty list"):
            assert result == []

    @allure.title("completeness excludes free-text column despite low distinct count")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_suggest_completeness_checks_excludes_free_text(self, profiling_suite):
        """Given a free-text column with only 3 distinct values in this snapshot, when completeness
        checks are suggested, then it is excluded because asserting text values is meaningless.
        """
        with allure.step("Setup column stats with a free-text column containing long values"):
            column_stats = {
                "description": {
                    "type": "string",
                    "distinct_count": 3,
                    "top_values": [
                        {"value": "alpha", "count": 8},
                        {"value": "some longer free text value here", "count": 5},
                        {"value": "beta", "count": 2},
                    ],
                },
            }

        with allure.step("Execute: suggest completeness checks"):
            result = profiling_suite._suggest_completeness_checks(column_stats=column_stats)

        with allure.step("Verify: result is an empty list because free-text columns are excluded"):
            assert result == []


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_SUGGEST)
class TestConfidenceStripping:
    @allure.title("_confidence/_reason present in builder output, absent from YAML")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_confidence_stripped_from_yaml(self, profiling_suite):
        """Given suggestions with _confidence and _reason metadata, when formatted as YAML,
        then those private keys are stripped while real check keys survive.
        """
        with allure.step("Execute: build silver suggestions and capture raw checks"):
            checks = profiling_suite._build_silver_suggestions(
                unique_cols=["order_id"], date_cols=["updated_at"], column_stats={}, row_count=100
            )

        with allure.step("Verify: _confidence metadata is present in the raw builder output"):
            assert checks["idempotency"]["_confidence"] == "medium"

        with allure.step("Execute: format suggestions as YAML"):
            yaml_text = profiling_suite._format_yaml_suggestions({"silver": {"checks": checks}})

        with allure.step("Verify: _confidence and _reason are absent from YAML, real keys survive"):
            assert "_confidence" not in yaml_text
            assert "_reason" not in yaml_text
            assert "idempotency" in yaml_text
