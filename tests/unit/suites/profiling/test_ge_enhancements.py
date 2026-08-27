# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_GE_SUGGEST)
class TestGEEnhancements:
    """TDD tests for GE suggestion enhancements: length rule and volume band.

    Added in v1.2.2 (§7), refined afterwards:
      - length rule: string cols with narrow length range (max - min <= 2), disabled
      - volume band: min_rows/max_rows in _build_ge_dataset_rules (warning), the keys
        the GE builder actually handles. (match_like and a separate row_count key were
        removed: match_like is redundant with regex, row_count had no GE handler.)
    """

    @allure.title("GE column rules — string col with narrow length range gets a 'length' rule")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_length_rule_narrow_range(self, profiling_suite):
        """A string column with max_length - min_length <= 2 gets a disabled length rule."""
        with allure.step("Prepare length_data for a narrow string column"):
            # min_length=3, max_length=5 → range = 2, should trigger length rule
            length_data = {"code": {"min_length": 3, "max_length": 5}}

        with allure.step("Build column rules with narrow length range"):
            rules = profiling_suite._build_ge_column_rules(
                "code",
                "string",
                not_null_cols=[],
                unique_cols=[],
                allowed_values_by_col={},
                range_by_col={},
                length_data=length_data,
            )

        with allure.step("Verify: length rule present and disabled with correct min/max"):
            assert "length" in rules
            assert rules["length"]["enabled"] is False
            assert rules["length"]["min"] == 3
            assert rules["length"]["max"] == 5
            assert rules["length"]["severity"] == "warning"

    @allure.title("GE column rules — string col with wide length range does NOT get a 'length' rule")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_length_rule_wide_range_excluded(self, profiling_suite):
        """A string column with max_length - min_length > 2 should NOT get a length rule."""
        with allure.step("Prepare length_data for a wide string column"):
            # min_length=1, max_length=100 → range = 99, should NOT trigger length rule
            length_data = {"description": {"min_length": 1, "max_length": 100}}

        with allure.step("Build column rules with wide length range"):
            rules = profiling_suite._build_ge_column_rules(
                "description",
                "string",
                not_null_cols=[],
                unique_cols=[],
                allowed_values_by_col={},
                range_by_col={},
                length_data=length_data,
            )

        with allure.step("Verify: no length rule for wide range"):
            assert "length" not in rules

    @allure.title("GE column rules — string col does NOT get a 'match_like' rule")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_no_match_like_rule(self, profiling_suite):
        """match_like is not suggested: it has no GE handler and is redundant with regex."""
        with allure.step("Build column rules for a string column"):
            rules = profiling_suite._build_ge_column_rules(
                "name",
                "string",
                not_null_cols=[],
                unique_cols=[],
                allowed_values_by_col={},
                range_by_col={},
                length_data={},
            )

        with allure.step("Verify: no match_like rule"):
            assert "match_like" not in rules

    @allure.title("GE column rules — numeric col does NOT get match_like or length rules")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_non_string_no_match_like(self, profiling_suite):
        """Numeric columns should NOT get match_like or length rules."""
        with allure.step("Build column rules for a numeric column"):
            rules = profiling_suite._build_ge_column_rules(
                "amount",
                "numeric",
                not_null_cols=[],
                unique_cols=[],
                allowed_values_by_col={},
                range_by_col={},
                length_data={},
            )

        with allure.step("Verify: no match_like and no length rule for numeric"):
            assert "match_like" not in rules
            assert "length" not in rules

    @allure.title("GE column rules — string col with detected date format gets a 'date_format' rule")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_date_format_rule(self, profiling_suite):
        """A string column with a detected strftime format gets a disabled date_format rule."""
        with allure.step("Build column rules with a detected date format"):
            rules = profiling_suite._build_ge_column_rules(
                "created_at",
                "string",
                not_null_cols=[],
                unique_cols=[],
                allowed_values_by_col={},
                range_by_col={},
                length_data={},
                date_format_by_col={"created_at": "%Y-%m-%d"},
            )

        with allure.step("Verify: date_format rule present, disabled, with detected format"):
            assert "date_format" in rules
            assert rules["date_format"]["enabled"] is False
            assert rules["date_format"]["format"] == "%Y-%m-%d"
            assert rules["date_format"]["severity"] == "warning"

    @allure.title("GE column rules — string col without detected date format gets no rule")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_no_date_format_rule(self, profiling_suite):
        """A string column with no detected format does not get a date_format rule."""
        with allure.step("Build column rules with no detected date format"):
            rules = profiling_suite._build_ge_column_rules(
                "name",
                "string",
                not_null_cols=[],
                unique_cols=[],
                allowed_values_by_col={},
                range_by_col={},
                length_data={},
                date_format_by_col={},
            )

        with allure.step("Verify: no date_format rule"):
            assert "date_format" not in rules

    @allure.title("Date-format detection — consistent date strings yield a format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_date_format_consistent(self, profiling_suite, spark):
        """A string column of consistent dates returns the matching strftime format."""
        with allure.step("Build a DataFrame with consistent date strings"):
            df = spark.createDataFrame(
                [("2026-01-01",), ("2026-06-25",), ("2025-12-31",)],
                "d STRING",
            )

        with allure.step("Detect the date format"):
            detected = profiling_suite._detect_date_format(df, "d")

        with allure.step("Verify: %Y-%m-%d detected"):
            assert detected == "%Y-%m-%d"

    @allure.title("Date-format detection — free text yields no format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_detect_date_format_free_text(self, profiling_suite, spark):
        """A free-text string column returns None (no consistent date format)."""
        with allure.step("Build a DataFrame with free text"):
            df = spark.createDataFrame(
                [("alpha",), ("beta",), ("gamma",)],
                "d STRING",
            )

        with allure.step("Detect the date format"):
            detected = profiling_suite._detect_date_format(df, "d")

        with allure.step("Verify: no format detected"):
            assert detected is None

    @allure.title("GE dataset rules — volume band uses min_rows/max_rows (warning)")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_dataset_rules_volume_band(self, profiling_suite):
        """_build_ge_dataset_rules emits min_rows/max_rows (the handled keys), not a row_count key."""
        with allure.step("Build dataset rules with a known row_count"):
            row_count = 100
            result = profiling_suite._build_ge_dataset_rules(
                column_stats={},
                unique_cols=[],
                row_count=row_count,
            )

        with allure.step("Verify: min_rows/max_rows present as a non-blocking warning, no row_count key"):
            assert "row_count" not in result
            assert result["min_rows"] == int(row_count * 0.8)  # 80
            assert result["max_rows"] == int(row_count * 1.2)  # 120
            assert result["severity"] == "warning"
            assert "blocking" not in result

    @allure.title("GE dataset rules — no volume band when row_count is 0")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_dataset_rules_no_volume_when_zero(self, profiling_suite):
        """When row_count is 0, no min_rows/max_rows band should be added."""
        with allure.step("Build dataset rules with zero row_count"):
            result = profiling_suite._build_ge_dataset_rules(
                column_stats={},
                unique_cols=[],
                row_count=0,
            )

        with allure.step("Verify: no volume band when row_count is 0"):
            assert "min_rows" not in result
            assert "max_rows" not in result
