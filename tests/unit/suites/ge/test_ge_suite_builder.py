# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Great Expectations Suite Builder.

Tests verify that GE expectations are correctly built from schema definitions.
"""

import tempfile

import allure
import great_expectations as gx
import pytest

from neco_dq.test_suites.ge.ge_suite_builder import (
    build_suite_from_schema,
    _handle_nullable,
    _handle_not_null,
    _handle_unique,
    _handle_dtype,
    _handle_between,
    _handle_values_set,
    _handle_regex,
    _handle_length,
    _handle_date_format,
    _handle_not_in_set,
    _handle_distinct_values_in_set,
    _handle_dataset_rules,
)
from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag


@pytest.fixture(scope="module")
def ge_context():
    """Create a temporary GE context for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        context = gx.get_context(mode="file", project_root_dir=tmpdir)
        yield context


@pytest.fixture
def ge_suite(ge_context):
    """Create a new GE suite for each test."""
    import uuid

    suite_name = f"test_suite_{uuid.uuid4().hex[:8]}"
    suite = gx.ExpectationSuite(name=suite_name)
    ge_context.suites.add(suite)
    return suite


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_SUITE_BUILDER)
class TestBuildSuiteFromSchema:
    """Tests for build_suite_from_schema function."""

    @allure.title("Build suite with column existence check")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_build_suite_adds_column_exists(self, ge_context):
        """Suite builder should add ExpectColumnToExist for each column."""
        with allure.step("Setup test data"):
            schema = {
                "id": {},
                "name": {},
            }

        with allure.step("Execute build_suite_from_schema"):
            suite = build_suite_from_schema(schema, "test_col_exists")
            ge_context.suites.add(suite)

        with allure.step("Verify: Column existence expectations are added"):
            exp_types = [e.expectation_type for e in suite.expectations]
            assert exp_types.count("expect_column_to_exist") == 2

    @allure.title("Build suite with nullable rule")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_build_suite_with_nullable(self, ge_context):
        """Suite builder should add not-null expectation when nullable=False."""
        with allure.step("Setup test data"):
            schema = {
                "id": {"nullable": {"enabled": False}},
            }

        with allure.step("Execute build_suite_from_schema"):
            suite = build_suite_from_schema(schema, "test_nullable")
            ge_context.suites.add(suite)

        with allure.step("Verify: Not-null expectation is added"):
            exp_types = [e.expectation_type for e in suite.expectations]
            assert "expect_column_values_to_not_be_null" in exp_types

    @allure.title("Build suite with dataset rules")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_build_suite_with_dataset_rules(self, ge_context):
        """Suite builder should add dataset-level expectations."""
        with allure.step("Setup test data"):
            schema = {
                "id": {},
                "_dataset": {"min_rows": 10, "max_rows": 1000},
            }

        with allure.step("Execute build_suite_from_schema"):
            suite = build_suite_from_schema(schema, "test_dataset_rules")
            ge_context.suites.add(suite)

        with allure.step("Verify: Row count expectation is added"):
            exp_types = [e.expectation_type for e in suite.expectations]
            assert "expect_table_row_count_to_be_between" in exp_types


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_COLUMN_RULES)
class TestColumnRuleHandlers:
    """Tests for column-level rule handlers."""

    @allure.title("Nullable handler adds expectation when disabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_nullable_disabled(self, ge_suite):
        """Nullable handler should add not-null expectation when enabled=False."""
        with allure.step("Setup test data"):
            cfg = {"enabled": False, "mostly": 0.95}

        with allure.step("Execute _handle_nullable"):
            _handle_nullable(ge_suite, "test_col", cfg)

        with allure.step("Verify: Not-null expectation is added with correct params"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_values_to_not_be_null"
            assert exp.column == "test_col"
            assert exp.mostly == 0.95

    @allure.title("Nullable handler skips when enabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_nullable_enabled(self, ge_suite):
        """Nullable handler should not add expectation when enabled=True."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True}

        with allure.step("Execute _handle_nullable"):
            _handle_nullable(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Not-null handler adds expectation when enabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_not_null_enabled(self, ge_suite):
        """Not-null handler should add expectation when enabled=True."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True}

        with allure.step("Execute _handle_not_null"):
            _handle_not_null(ge_suite, "test_col", cfg)

        with allure.step("Verify: Not-null expectation is added"):
            assert len(ge_suite.expectations) == 1
            assert ge_suite.expectations[0].expectation_type == "expect_column_values_to_not_be_null"

    @allure.title("Unique handler adds simple uniqueness expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_unique_simple(self, ge_suite):
        """Unique handler should add simple uniqueness expectation."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True}

        with allure.step("Execute _handle_unique"):
            _handle_unique(ge_suite, "test_col", cfg)

        with allure.step("Verify: Unique expectation is added"):
            assert len(ge_suite.expectations) == 1
            assert ge_suite.expectations[0].expectation_type == "expect_column_values_to_be_unique"

    @allure.title("Unique handler adds compound uniqueness expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_unique_compound(self, ge_suite):
        """Unique handler should add compound uniqueness expectation with column_list."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "column_list": ["col1", "col2"]}

        with allure.step("Execute _handle_unique"):
            _handle_unique(ge_suite, "test_col", cfg)

        with allure.step("Verify: Compound unique expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_compound_columns_to_be_unique"
            assert "test_col" in exp.column_list

    @allure.title("Dtype handler adds type list expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_dtype(self, ge_suite):
        """Dtype handler should add type list expectation."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "values": ["StringType", "IntegerType"]}

        with allure.step("Execute _handle_dtype"):
            _handle_dtype(ge_suite, "test_col", cfg)

        with allure.step("Verify: Type list expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_values_to_be_in_type_list"
            assert exp.type_list == ["StringType", "IntegerType"]

    @allure.title("Between handler adds range expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_between(self, ge_suite):
        """Between handler should add range expectation."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "min": 0, "max": 100}

        with allure.step("Execute _handle_between"):
            _handle_between(ge_suite, "test_col", cfg)

        with allure.step("Verify: Between expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_values_to_be_between"
            assert exp.min_value == 0
            assert exp.max_value == 100

    @allure.title("Values set handler adds in-set expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_values_set(self, ge_suite):
        """Values set handler should add in-set expectation."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "values": ["A", "B", "C"]}

        with allure.step("Execute _handle_values_set"):
            _handle_values_set(ge_suite, "test_col", cfg)

        with allure.step("Verify: In-set expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_values_to_be_in_set"
            assert exp.value_set == ["A", "B", "C"]

    @allure.title("Regex handler adds pattern expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_regex(self, ge_suite):
        """Regex handler should add pattern match expectation."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "pattern": r"^[A-Z]{2}\d{4}$"}

        with allure.step("Execute _handle_regex"):
            _handle_regex(ge_suite, "test_col", cfg)

        with allure.step("Verify: Regex expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_values_to_match_regex"
            assert exp.regex == r"^[A-Z]{2}\d{4}$"

    @allure.title("Length handler adds value-length range expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_length_enabled(self, ge_suite):
        """Length handler should add a value-length range expectation when enabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "min": 3, "max": 5}

        with allure.step("Execute _handle_length"):
            _handle_length(ge_suite, "test_col", cfg)

        with allure.step("Verify: Length expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_value_lengths_to_be_between"
            assert exp.min_value == 3
            assert exp.max_value == 5

    @allure.title("Length handler skips when disabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_length_disabled(self, ge_suite):
        """Length handler should not add an expectation when disabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": False, "min": 3, "max": 5}

        with allure.step("Execute _handle_length"):
            _handle_length(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Date format handler adds strftime match expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_date_format_enabled(self, ge_suite):
        """Date format handler should add a strftime match expectation when enabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "format": "%Y-%m-%d"}

        with allure.step("Execute _handle_date_format"):
            _handle_date_format(ge_suite, "test_col", cfg)

        with allure.step("Verify: strftime expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_values_to_match_strftime_format"
            assert exp.strftime_format == "%Y-%m-%d"

    @allure.title("Date format handler skips when disabled or format missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_date_format_skips(self, ge_suite):
        """Date format handler should not add an expectation when disabled or format absent."""
        with allure.step("Execute _handle_date_format disabled, then enabled with no format"):
            _handle_date_format(ge_suite, "test_col", {"enabled": False, "format": "%Y-%m-%d"})
            _handle_date_format(ge_suite, "test_col", {"enabled": True})

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Not-in-set handler adds blocklist expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_not_in_set_enabled(self, ge_suite):
        """Not-in-set handler should add a not-in-set expectation when enabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "values": ["N/A", "unknown", "-1"]}

        with allure.step("Execute _handle_not_in_set"):
            _handle_not_in_set(ge_suite, "test_col", cfg)

        with allure.step("Verify: not-in-set expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_values_to_not_be_in_set"
            assert exp.value_set == ["N/A", "unknown", "-1"]

    @allure.title("Not-in-set handler skips when disabled or values empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_not_in_set_skips(self, ge_suite):
        """Not-in-set handler should not add an expectation when disabled or values empty."""
        with allure.step("Execute _handle_not_in_set disabled, then enabled with no values"):
            _handle_not_in_set(ge_suite, "test_col", {"enabled": False, "values": ["N/A"]})
            _handle_not_in_set(ge_suite, "test_col", {"enabled": True, "values": []})

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Distinct-values-in-set handler adds set-membership expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_distinct_values_in_set_enabled(self, ge_suite):
        """Distinct-values-in-set handler should add a distinct-values expectation when enabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "values": ["A", "B", "C"]}

        with allure.step("Execute _handle_distinct_values_in_set"):
            _handle_distinct_values_in_set(ge_suite, "test_col", cfg)

        with allure.step("Verify: distinct-values-in-set expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_distinct_values_to_be_in_set"
            assert exp.value_set == ["A", "B", "C"]

    @allure.title("Distinct-values-in-set handler skips when disabled or values empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_distinct_values_in_set_skips(self, ge_suite):
        """Distinct-values-in-set handler should not add an expectation when disabled or values empty."""
        with allure.step("Execute _handle_distinct_values_in_set disabled, then enabled with no values"):
            _handle_distinct_values_in_set(ge_suite, "test_col", {"enabled": False, "values": ["A"]})
            _handle_distinct_values_in_set(ge_suite, "test_col", {"enabled": True, "values": []})

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_COLUMN_RULES)
class TestColumnRuleEdgeCases:
    """Tests for column rule edge cases."""

    @allure.title("Dtype handler skips when disabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_dtype_disabled(self, ge_suite):
        """Dtype handler should not add expectation when disabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": False, "values": ["StringType"]}

        with allure.step("Execute _handle_dtype"):
            _handle_dtype(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Dtype handler skips when values empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_handle_dtype_empty_values(self, ge_suite):
        """Dtype handler should not add expectation when values list is empty."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "values": []}

        with allure.step("Execute _handle_dtype"):
            _handle_dtype(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Values set handler skips when disabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_values_set_disabled(self, ge_suite):
        """Values set handler should not add expectation when disabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": False, "values": ["A", "B"]}

        with allure.step("Execute _handle_values_set"):
            _handle_values_set(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Values set handler skips when values empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_handle_values_set_empty_values(self, ge_suite):
        """Values set handler should not add expectation when values list is empty."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "values": []}

        with allure.step("Execute _handle_values_set"):
            _handle_values_set(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Regex handler skips when disabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_regex_disabled(self, ge_suite):
        """Regex handler should not add expectation when disabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": False, "pattern": r"^\d+$"}

        with allure.step("Execute _handle_regex"):
            _handle_regex(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Regex handler skips when pattern empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_handle_regex_empty_pattern(self, ge_suite):
        """Regex handler should not add expectation when pattern is empty."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "pattern": ""}

        with allure.step("Execute _handle_regex"):
            _handle_regex(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Unique handler skips when disabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_unique_disabled(self, ge_suite):
        """Unique handler should not add expectation when disabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": False}

        with allure.step("Execute _handle_unique"):
            _handle_unique(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Not-null handler skips when disabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_not_null_disabled(self, ge_suite):
        """Not-null handler should not add expectation when disabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": False}

        with allure.step("Execute _handle_not_null"):
            _handle_not_null(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Between handler skips when disabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_between_disabled(self, ge_suite):
        """Between handler should not add expectation when disabled."""
        with allure.step("Setup test data"):
            cfg = {"enabled": False, "min": 0, "max": 100}

        with allure.step("Execute _handle_between"):
            _handle_between(ge_suite, "test_col", cfg)

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_DATASET_RULES)
class TestDatasetRuleHandlers:
    """Tests for dataset-level rule handlers."""

    @allure.title("Dataset rules add row count expectation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_dataset_row_count(self, ge_suite):
        """Dataset rules should add row count expectation."""
        with allure.step("Setup test data"):
            dataset_rules = {"min_rows": 100, "max_rows": 10000}

        with allure.step("Execute _handle_dataset_rules"):
            _handle_dataset_rules(ge_suite, dataset_rules, {})

        with allure.step("Verify: Row count expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_table_row_count_to_be_between"
            assert exp.min_value == 100
            assert exp.max_value == 10000

    @allure.title("Dataset rules add pair_gte integrity check")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_dataset_pair_gte(self, ge_suite):
        """Dataset rules should add pair_gte integrity expectation."""
        with allure.step("Setup test data"):
            dataset_rules = {"integrity": {"pair_gte": [{"column_A": "end_date", "column_B": "start_date"}]}}

        with allure.step("Execute _handle_dataset_rules"):
            _handle_dataset_rules(ge_suite, dataset_rules, {})

        with allure.step("Verify: Pair comparison expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_pair_values_a_to_be_greater_than_b"
            assert exp.column_A == "end_date"
            assert exp.column_B == "start_date"
            assert exp.or_equal is True

    @allure.title("Dataset rules add pair_eq integrity check")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_dataset_pair_eq(self, ge_suite):
        """Dataset rules should add pair_eq integrity expectation."""
        with allure.step("Setup test data"):
            dataset_rules = {"integrity": {"pair_eq": [{"column_A": "total", "column_B": "computed_total"}]}}

        with allure.step("Execute _handle_dataset_rules"):
            _handle_dataset_rules(ge_suite, dataset_rules, {})

        with allure.step("Verify: Pair equality expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_pair_values_to_be_equal"
            assert exp.column_A == "total"
            assert exp.column_B == "computed_total"

    @allure.title("Dataset rules add no_duplicate check")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_dataset_no_duplicate(self, ge_suite):
        """Dataset rules should add no_duplicate compound uniqueness expectation."""
        with allure.step("Setup test data"):
            dataset_rules = {
                "no_duplicate": {
                    "enabled": True,
                    "column_list": ["id", "date"],
                }
            }

        with allure.step("Execute _handle_dataset_rules"):
            _handle_dataset_rules(ge_suite, dataset_rules, {})

        with allure.step("Verify: Compound uniqueness expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_compound_columns_to_be_unique"
            assert exp.column_list == ["id", "date"]

    @allure.title("Dataset rules add no_duplicate with schema columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_dataset_no_duplicate_from_schema(self, ge_suite):
        """Dataset rules should use schema columns for no_duplicate when configured."""
        with allure.step("Setup test data"):
            dataset_rules = {
                "no_duplicate": {
                    "enabled": True,
                    "use_schema_columns": True,
                }
            }
            schema = {"col1": {}, "col2": {}, "_dataset": {}}

        with allure.step("Execute _handle_dataset_rules"):
            _handle_dataset_rules(ge_suite, dataset_rules, schema)

        with allure.step("Verify: Compound uniqueness uses schema columns"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_compound_columns_to_be_unique"
            assert "col1" in exp.column_list
            assert "col2" in exp.column_list
            assert "_dataset" not in exp.column_list

    @allure.title("Dataset rules add pair_gt integrity check")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_dataset_pair_gt(self, ge_suite):
        """Dataset rules should add pair_gt integrity expectation (strictly greater)."""
        with allure.step("Setup test data"):
            dataset_rules = {"integrity": {"pair_gt": [{"column_A": "end_date", "column_B": "start_date"}]}}

        with allure.step("Execute _handle_dataset_rules"):
            _handle_dataset_rules(ge_suite, dataset_rules, {})

        with allure.step("Verify: Pair comparison expectation with or_equal=False"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_column_pair_values_a_to_be_greater_than_b"
            assert exp.column_A == "end_date"
            assert exp.column_B == "start_date"
            assert exp.or_equal is False

    @allure.title("Dataset rules raise error when no_duplicate enabled without columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_dataset_no_duplicate_error(self, ge_suite):
        """Dataset rules should raise ValueError when no_duplicate enabled without columns."""
        with allure.step("Setup test data"):
            dataset_rules = {
                "no_duplicate": {
                    "enabled": True,
                    # No column_list and no use_schema_columns
                }
            }
            schema = {}  # Empty schema, no columns

        with allure.step("Execute _handle_dataset_rules and verify error"):
            with pytest.raises(ValueError, match="no_duplicate enabled but no columns provided"):
                _handle_dataset_rules(ge_suite, dataset_rules, schema)

    @allure.title("Dataset rules add multicolumn_sum invariant")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_handle_dataset_multicolumn_sum(self, ge_suite):
        """Dataset rules should add a multicolumn sum expectation (e.g. subtotal + tax == total)."""
        with allure.step("Setup test data"):
            dataset_rules = {
                "multicolumn_sum": [{"column_list": ["subtotal", "tax"], "sum_total": 100}],
            }

        with allure.step("Execute _handle_dataset_rules"):
            _handle_dataset_rules(ge_suite, dataset_rules, {})

        with allure.step("Verify: multicolumn sum expectation is added"):
            assert len(ge_suite.expectations) == 1
            exp = ge_suite.expectations[0]
            assert exp.expectation_type == "expect_multicolumn_sum_to_equal"
            assert exp.column_list == ["subtotal", "tax"]
            assert exp.sum_total == 100

    @allure.title("Dataset rules skip disabled multicolumn_sum rule")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_dataset_multicolumn_sum_disabled(self, ge_suite):
        """A multicolumn_sum rule with enabled=False should be skipped."""
        with allure.step("Setup test data"):
            dataset_rules = {
                "multicolumn_sum": [
                    {"enabled": False, "column_list": ["subtotal", "tax"], "sum_total": 100},
                ],
            }

        with allure.step("Execute _handle_dataset_rules"):
            _handle_dataset_rules(ge_suite, dataset_rules, {})

        with allure.step("Verify: No expectation is added"):
            assert len(ge_suite.expectations) == 0

    @allure.title("Dataset rules raise error when multicolumn_sum missing required fields")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_handle_dataset_multicolumn_sum_error(self, ge_suite):
        """A multicolumn_sum rule without column_list/sum_total should raise ValueError."""
        with allure.step("Setup test data"):
            dataset_rules = {"multicolumn_sum": [{"column_list": ["subtotal", "tax"]}]}

        with allure.step("Execute _handle_dataset_rules and verify error"):
            with pytest.raises(ValueError, match="multicolumn_sum rule requires"):
                _handle_dataset_rules(ge_suite, dataset_rules, {})
