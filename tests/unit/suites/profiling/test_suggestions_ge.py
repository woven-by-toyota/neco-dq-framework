# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_GE_SUGGEST)
class TestGESuggestions:
    """Characterization tests for the Great Expectations suggestion builder methods.

    Real signatures confirmed from profiling_dq_test_suite.py:
      - _build_ge_column_rules(col, col_type, not_null_cols, unique_cols,
                               allowed_values_by_col, range_by_col) -> dict
      - _build_ge_dataset_rules(column_stats, unique_cols, row_count) -> dict
      - _build_ge_suggestions(column_stats, not_null_cols, unique_cols,
                              allowed_values_by_col, range_by_col, row_count) -> dict

    _build_ge_suggestions takes 6 positional args (beyond self), NOT just stats.
    Callers must pre-compute all suggestion lists before calling it.
    """

    def _build_inputs(self, profiling_suite, df_mixed):
        """Shared helper: compute all inputs needed by the GE builders."""
        row_count = df_mixed.count()
        stats = profiling_suite._compute_column_stats(df_mixed, row_count)
        not_null_cols = profiling_suite._suggest_not_null_columns(stats)
        unique_cols = profiling_suite._suggest_unique_columns(stats, row_count)
        allowed_values_list = profiling_suite._suggest_allowed_values(stats, row_count)
        allowed_values_by_col = {item["column"]: item["values"] for item in allowed_values_list}
        range_checks_list = profiling_suite._suggest_range_checks(stats)
        range_by_col = {item["column"]: item for item in range_checks_list}
        return stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, row_count

    @allure.title("Build GE column rules — numeric 'id' gets dtype + not_null + unique")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_id(self, profiling_suite, df_mixed):
        """Given a numeric 'id' column, when GE column rules are built, then dtype and not_null are enabled
        and unique is present as a disabled candidate."""
        with allure.step("Compute all builder inputs"):
            _stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, _ = self._build_inputs(
                profiling_suite, df_mixed
            )

        with allure.step("Build column rules for 'id'"):
            rules = profiling_suite._build_ge_column_rules(
                "id", "numeric", not_null_cols, unique_cols, allowed_values_by_col, range_by_col, length_data={}
            )

        with allure.step("Verify: dtype, not_null present; unique is a disabled candidate (no PK passed)"):
            assert rules["dtype"] == {
                "enabled": True,
                "values": ["int", "bigint", "long", "double", "float"],
                "severity": "error",
            }
            assert rules["not_null"] == {"enabled": True, "severity": "warning"}
            # No pk_columns passed → unique is a candidate, suggested disabled (not blocking)
            assert rules["unique"]["enabled"] is False
            assert rules["unique"]["severity"] == "error"
            assert "blocking" not in rules["unique"]
            assert "between" not in rules  # id excluded as ID column

    @allure.title("Build GE column rules — string 'name' gets dtype + not_null (no unique, no values_set)")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_name(self, profiling_suite, df_mixed):
        """Given a string 'name' column, when GE column rules are built, then only dtype and not_null are present
        with no unique or values_set."""
        with allure.step("Compute all builder inputs"):
            _stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, _ = self._build_inputs(
                profiling_suite, df_mixed
            )

        with allure.step("Build column rules for 'name'"):
            rules = profiling_suite._build_ge_column_rules(
                "name", "string", not_null_cols, unique_cols, allowed_values_by_col, range_by_col, length_data={}
            )

        with allure.step("Verify: dtype + not_null only (name is not an enum keyword column)"):
            assert rules["dtype"] == {"enabled": True, "values": ["str", "string"], "severity": "error"}
            assert rules["not_null"] == {"enabled": True, "severity": "warning"}
            assert "unique" not in rules
            assert "values_set" not in rules  # name not an enum keyword column
            assert "match_like" not in rules  # match_like suggestion removed (no GE handler, redundant with regex)

    @allure.title("Build GE column rules — numeric 'amount' gets dtype + not_null + unique + between")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_column_rules_amount(self, profiling_suite, df_mixed):
        """Given a numeric 'amount' column with a range, when GE column rules are built, then dtype, not_null,
        a disabled unique candidate, and a disabled between rule are present."""
        with allure.step("Compute all builder inputs"):
            _stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, _ = self._build_inputs(
                profiling_suite, df_mixed
            )

        with allure.step("Build column rules for 'amount'"):
            rules = profiling_suite._build_ge_column_rules(
                "amount", "numeric", not_null_cols, unique_cols, allowed_values_by_col, range_by_col, length_data={}
            )

        with allure.step("Verify: dtype + not_null + unique (disabled candidate) + between (disabled)"):
            assert rules["dtype"] == {
                "enabled": True,
                "values": ["int", "bigint", "long", "double", "float"],
                "severity": "error",
            }
            assert rules["not_null"] == {"enabled": True, "severity": "warning"}
            # No pk_columns passed → unique is a candidate, suggested disabled (not blocking)
            assert rules["unique"]["enabled"] is False
            assert "blocking" not in rules["unique"]
            assert rules["between"] == {"enabled": False, "min": 10.5, "max": 40.0, "severity": "warning"}

    @allure.title("Build GE dataset rules — volume band and no_duplicate candidate (no PK)")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_dataset_rules(self, profiling_suite, df_mixed):
        """Given a DataFrame with 4 rows, when GE dataset rules are built, then a warning-severity volume band
        and a disabled no_duplicate candidate are returned."""
        with allure.step("Compute all builder inputs"):
            stats, _, unique_cols, _, _, row_count = self._build_inputs(profiling_suite, df_mixed)

        with allure.step("Build dataset rules (no pk_columns passed)"):
            result = profiling_suite._build_ge_dataset_rules(stats, unique_cols, row_count)

        with allure.step("Verify: volume band (warning) + no_duplicate as a disabled candidate"):
            assert result["min_rows"] == 3  # floor(4 * 0.8) == 3
            assert result["max_rows"] == 4  # floor(4 * 1.2) == 4
            assert result["severity"] == "warning"
            assert "blocking" not in result
            assert "row_count" not in result
            no_dup = result["no_duplicate"]
            # No pk_columns → compound key is only a candidate, suggested disabled
            assert no_dup["enabled"] is False
            assert set(no_dup["column_list"]) == {"id", "created", "amount"}
            assert len(no_dup["column_list"]) == 3
            assert "blocking" not in no_dup

    @allure.title("Build GE suggestions — full output structure with all columns and _dataset key")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_suggestions_top_level_keys(self, profiling_suite, df_mixed):
        """Given a DataFrame with 5 columns, when full GE suggestions are built, then the result contains
        one key per column plus the reserved _dataset key."""
        with allure.step("Compute all builder inputs"):
            stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, row_count = self._build_inputs(
                profiling_suite, df_mixed
            )

        with allure.step("Build full GE suggestions"):
            result = profiling_suite._build_ge_suggestions(
                stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, row_count
            )

        with allure.step("Verify: top-level keys include all DataFrame columns plus _dataset"):
            assert set(result.keys()) == {"id", "name", "created", "active", "amount", "_dataset"}

    @allure.title("Build GE suggestions — per-column rules match expected shapes")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_suggestions_column_rules(self, profiling_suite, df_mixed):
        """Given a mixed DataFrame, when full GE suggestions are built, then each column's rules match
        the expected shape for its data type."""
        with allure.step("Compute all builder inputs"):
            stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, row_count = self._build_inputs(
                profiling_suite, df_mixed
            )

        with allure.step("Build full GE suggestions"):
            result = profiling_suite._build_ge_suggestions(
                stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, row_count
            )

        with allure.step("Verify: id column rules"):
            assert "dtype" in result["id"]
            assert "not_null" in result["id"]
            assert "unique" in result["id"]
            assert "between" not in result["id"]

        with allure.step("Verify: name column rules"):
            assert "dtype" in result["name"]
            assert "not_null" in result["name"]
            assert "unique" not in result["name"]
            assert "match_like" not in result["name"]  # match_like suggestion removed

        with allure.step("Verify: amount column rules"):
            assert "dtype" in result["amount"]
            assert "not_null" in result["amount"]
            assert "unique" in result["amount"]  # present as a disabled candidate (no PK passed)
            assert result["amount"]["unique"]["enabled"] is False
            assert "between" in result["amount"]
            assert result["amount"]["between"]["enabled"] is False

        with allure.step("Verify: active column rules"):
            assert result["active"]["dtype"]["values"] == ["bool", "boolean"]
            assert "not_null" in result["active"]

        with allure.step("Verify: created column rules"):
            assert result["created"]["dtype"]["values"] == ["datetime64[ns]", "timestamp"]
            assert "unique" in result["created"]

    @allure.title("Build GE suggestions — _dataset block contains min_rows and no_duplicate")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_build_ge_suggestions_dataset_block(self, profiling_suite, df_mixed):
        """Given a mixed DataFrame, when full GE suggestions are built, then the _dataset block contains a volume
        band and a disabled no_duplicate candidate with the correct composite key columns."""
        with allure.step("Compute all builder inputs"):
            stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, row_count = self._build_inputs(
                profiling_suite, df_mixed
            )

        with allure.step("Build full GE suggestions"):
            result = profiling_suite._build_ge_suggestions(
                stats, not_null_cols, unique_cols, allowed_values_by_col, range_by_col, row_count
            )

        with allure.step("Verify: _dataset block structure"):
            dataset = result["_dataset"]
            assert dataset["min_rows"] == 3
            assert dataset["max_rows"] == 4
            assert dataset["severity"] == "warning"
            assert "blocking" not in dataset
            assert "row_count" not in dataset
            assert set(dataset["no_duplicate"]["column_list"]) == {"id", "created", "amount"}
            assert len(dataset["no_duplicate"]["column_list"]) == 3
            # No pk_columns → compound key is only a candidate, suggested disabled
            assert dataset["no_duplicate"]["enabled"] is False
