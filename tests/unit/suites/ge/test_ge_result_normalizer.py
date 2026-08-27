# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Great Expectations Result Normalizer.

Tests verify that GE validation results are correctly normalized into framework format.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import allure
import pytest

from neco_dq.test_suites.ge.ge_result_normalizer import (
    GE_CHECK_NAME_MAP,
    _map_check_name,
    extract_expected_actual,
    normalize_ge_results,
)
from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_validation_result():
    """Create a mock GE validation result with successful expectations."""
    result = MagicMock()

    # Create mock expectation config
    exp_config = MagicMock()
    exp_config.type = "expect_column_values_to_not_be_null"
    exp_config.meta = {"severity": "warning", "blocking": False, "rule_description": "Column must not be null"}
    exp_config.get = lambda key, default=None: {"kwargs": {"column": "test_col"}}.get(key, default)
    exp_config.to_json_dict = lambda: {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "test_col"}}

    # Create mock result item
    result_item = MagicMock()
    result_item.expectation_config = exp_config
    result_item.success = True
    result_item.result = {"element_count": 100, "unexpected_count": 0}

    result.results = [result_item]
    return result


@pytest.fixture
def mock_failed_validation_result():
    """Create a mock GE validation result with failed expectations."""
    result = MagicMock()

    # Create mock expectation config
    exp_config = MagicMock()
    exp_config.type = "expect_column_values_to_be_in_set"
    exp_config.meta = {"severity": "error", "blocking": True, "rule_description": "Value must be valid"}
    exp_config.get = lambda key, default=None: {"kwargs": {"column": "status", "value_set": ["A", "B", "C"]}}.get(
        key, default
    )
    exp_config.to_json_dict = lambda: {
        "type": "expect_column_values_to_be_in_set",
        "kwargs": {"column": "status", "value_set": ["A", "B", "C"]},
    }

    # Create mock result item
    result_item = MagicMock()
    result_item.expectation_config = exp_config
    result_item.success = False
    result_item.result = {
        "element_count": 100,
        "unexpected_count": 5,
        "partial_unexpected_list": ["X", "Y"],
        "partial_unexpected_counts": [{"value": "X", "count": 3}, {"value": "Y", "count": 2}],
    }

    result.results = [result_item]
    return result


@pytest.fixture
def mock_run_id():
    """Create a mock run ID."""
    run_id = MagicMock()
    run_id.run_name = "test_run_20240101_120000"
    return run_id


# =============================================================================
# Normalize GE Results Tests
# =============================================================================


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_RESULT_NORMALIZER)
class TestNormalizeGeResults:
    """Tests for normalize_ge_results function."""

    @allure.title("Normalize successful validation result")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_successful_result(self, mock_validation_result, mock_run_id):
        """Test normalization of successful GE validation result."""
        with allure.step("Setup test data"):
            table_fq = "catalog.schema.table"
            run_ts = datetime.now(UTC)

        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_validation_result,
                table_fq=table_fq,
                run_ts=run_ts,
                run_id=mock_run_id,
            )

        with allure.step("Verify: Successful result is normalized correctly"):
            assert len(normalized) == 1
            row = normalized[0]
            assert row["engine"] == "ge"
            assert row["table"] == table_fq
            assert row["column"] == "test_col"
            assert row["check_name"] == "not_null"  # Mapped from expect_column_values_to_not_be_null
            assert row["passed"] is True
            assert row["severity"] == "warning"
            assert row["blocking"] is False
            assert row["failure_details"] is None
            assert row["extra_metadata"]["ge_expectation_type"] == "expect_column_values_to_not_be_null"

    @allure.title("Normalize failed validation result")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_normalize_failed_result(self, mock_failed_validation_result, mock_run_id):
        """Test normalization of failed GE validation result."""
        with allure.step("Setup test data"):
            table_fq = "catalog.schema.table"
            run_ts = datetime.now(UTC)

        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_failed_validation_result,
                table_fq=table_fq,
                run_ts=run_ts,
                run_id=mock_run_id,
            )

        with allure.step("Verify: Failed result is normalized with failure details"):
            assert len(normalized) == 1
            row = normalized[0]
            assert row["passed"] is False
            assert row["severity"] == "error"
            assert row["blocking"] is True
            assert row["failure_details"] is not None
            assert row["failure_details"]["expectation_type"] == "expect_column_values_to_be_in_set"
            assert row["failure_details"]["column"] == "status"

    @allure.title("Normalize result excludes partial_unexpected_list (security)")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SECURITY)
    def test_normalize_excludes_partial_unexpected_list(self, mock_failed_validation_result, mock_run_id):
        """Test that partial_unexpected_list is NOT included (contains PII risk values)."""
        with allure.step("Setup test data"):
            table_fq = "catalog.schema.table"
            run_ts = datetime.now(UTC)

        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_failed_validation_result,
                table_fq=table_fq,
                run_ts=run_ts,
                run_id=mock_run_id,
            )

        with allure.step("Verify: partial_unexpected_list is NOT included (security)"):
            row = normalized[0]
            # details is now a human-readable string, not a dict
            assert isinstance(row["details"], str)
            assert "partial_unexpected_list" not in row["details"]
            assert "partial_unexpected_list" not in row["extra_metadata"]
            # failure_stats should not contain PII fields
            assert "partial_unexpected_list" not in row["failure_stats"]
            assert "partial_unexpected_index_list" not in str(row["failure_stats"])
            # Only aggregate counts are allowed (no actual values)
            assert "partial_unexpected_counts" in row["extra_metadata"]

    @allure.title("Normalize result escalates severity when blocking")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_escalates_severity_when_blocking(self, mock_run_id):
        """Test that severity is escalated to error when blocking=True and severity is low."""
        with allure.step("Setup test data"):
            result = MagicMock()
            exp_config = MagicMock()
            exp_config.type = "expect_column_to_exist"
            exp_config.meta = {"severity": "warning", "blocking": True}  # Low severity but blocking
            exp_config.get = lambda key, default=None: {"kwargs": {"column": "test_col"}}.get(key, default)
            exp_config.to_json_dict = lambda: {"type": "expect_column_to_exist"}

            result_item = MagicMock()
            result_item.expectation_config = exp_config
            result_item.success = True
            result_item.result = {}

            result.results = [result_item]
            run_ts = datetime.now(UTC)

        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=result,
                table_fq="catalog.schema.table",
                run_ts=run_ts,
                run_id=mock_run_id,
            )

        with allure.step("Verify: Effective severity is escalated to error"):
            row = normalized[0]
            assert row["severity"] == "warning"  # Original severity preserved
            assert row["effective_severity"] == "error"  # Escalated due to blocking

    @allure.title("Normalize result uses default severity")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_uses_default_severity(self, mock_run_id):
        """Test that default severity is used when not specified in meta."""
        with allure.step("Setup test data"):
            result = MagicMock()
            exp_config = MagicMock()
            exp_config.type = "expect_column_to_exist"
            exp_config.meta = {}  # No severity specified
            exp_config.get = lambda key, default=None: {"kwargs": {"column": "test_col"}}.get(key, default)
            exp_config.to_json_dict = lambda: {"type": "expect_column_to_exist"}

            result_item = MagicMock()
            result_item.expectation_config = exp_config
            result_item.success = True
            result_item.result = {}

            result.results = [result_item]
            run_ts = datetime.now(UTC)

        with allure.step("Execute normalize_ge_results with custom default severity"):
            normalized = normalize_ge_results(
                validation_result=result,
                table_fq="catalog.schema.table",
                run_ts=run_ts,
                run_id=mock_run_id,
                default_severity="critical",
            )

        with allure.step("Verify: Default severity is applied"):
            row = normalized[0]
            assert row["severity"] == "critical"

    @allure.title("Normalize handles empty results")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_normalize_empty_results(self, mock_run_id):
        """Test normalization of validation result with no expectations."""
        with allure.step("Setup test data"):
            result = MagicMock()
            result.results = []
            run_ts = datetime.now(UTC)

        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=result,
                table_fq="catalog.schema.table",
                run_ts=run_ts,
                run_id=mock_run_id,
            )

        with allure.step("Verify: Empty list is returned"):
            assert normalized == []

    @allure.title("Normalize handles None results attribute")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_normalize_none_results(self, mock_run_id):
        """Test normalization when validation result has None results."""
        with allure.step("Setup test data"):
            result = MagicMock()
            result.results = None
            run_ts = datetime.now(UTC)

        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=result,
                table_fq="catalog.schema.table",
                run_ts=run_ts,
                run_id=mock_run_id,
            )

        with allure.step("Verify: Empty list is returned"):
            assert normalized == []


# =============================================================================
# Check Name Mapping Tests
# =============================================================================


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_RESULT_NORMALIZER)
class TestCheckNameMapping:
    """Tests for GE expectation type to check name mapping."""

    @allure.title("Map known GE expectation types to check names")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_map_known_expectation_types(self):
        """Test mapping of all known GE expectation types."""
        with allure.step("Verify: All known mappings return correct check names"):
            assert _map_check_name("expect_column_to_exist") == "column_exists"
            assert _map_check_name("expect_column_values_to_not_be_null") == "not_null"
            assert _map_check_name("expect_column_values_to_be_unique") == "unique"
            assert _map_check_name("expect_compound_columns_to_be_unique") == "unique"
            assert _map_check_name("expect_column_values_to_be_in_set") == "allowed_values"
            assert _map_check_name("expect_column_values_to_be_between") == "range"
            assert _map_check_name("expect_column_values_to_be_in_type_list") == "dtype"
            assert _map_check_name("expect_column_values_to_match_regex") == "regex"
            assert _map_check_name("expect_column_value_lengths_to_be_between") == "length"
            assert _map_check_name("expect_column_values_to_match_strftime_format") == "date_format"
            assert _map_check_name("expect_column_values_to_not_be_in_set") == "not_in_set"
            assert _map_check_name("expect_column_distinct_values_to_be_in_set") == "distinct_values_in_set"
            assert _map_check_name("expect_multicolumn_sum_to_equal") == "multicolumn_sum"
            assert _map_check_name("expect_column_pair_values_to_be_equal") == "pair_equality"
            assert _map_check_name("expect_column_pair_values_a_to_be_greater_than_b") == "pair_gte"
            assert _map_check_name("expect_table_row_count_to_be_between") == "record_count"

    @allure.title("Unknown expectation types are passed through")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_map_unknown_expectation_type(self):
        """Test that unknown expectation types are returned as-is."""
        with allure.step("Verify: Unknown type is passed through unchanged"):
            assert _map_check_name("expect_some_custom_expectation") == "expect_some_custom_expectation"

    @allure.title("GE check name map contains required entries")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_check_name_map_completeness(self):
        """Test that the mapping covers all required GE expectations."""
        required_keys = {
            "expect_column_to_exist",
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_unique",
            "expect_column_values_to_be_in_set",
            "expect_column_values_to_be_between",
        }
        with allure.step("Verify: Map contains all required entries"):
            assert required_keys.issubset(GE_CHECK_NAME_MAP.keys())


# =============================================================================
# Extract Expected/Actual Tests
# =============================================================================


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_RESULT_NORMALIZER)
class TestExtractExpectedActual:
    """Tests for extract_expected_actual function."""

    @allure.title("Extract expected from kwargs")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_extract_expected_from_kwargs(self):
        """Test extraction of expected values from expectation kwargs."""
        with allure.step("Setup test data"):
            cfg = MagicMock()
            cfg.get = lambda key, default=None: {"kwargs": {"column": "value", "min_value": 0, "max_value": 100}}.get(
                key, default
            )
            result = {}

        with allure.step("Execute extract_expected_actual"):
            expected, _actual = extract_expected_actual(cfg, result)

        with allure.step("Verify: Expected values are extracted (excluding column)"):
            assert expected is not None
            assert "min_value" in expected
            assert "max_value" in expected
            assert "column" not in expected

    @allure.title("Extract actual from observed_value")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_extract_actual_from_observed_value(self):
        """Test extraction of actual values including observed_value."""
        with allure.step("Setup test data"):
            cfg = MagicMock()
            cfg.get = lambda key, default=None: {"kwargs": {}}.get(key, default)
            result = {"observed_value": 50}

        with allure.step("Execute extract_expected_actual"):
            _expected, actual = extract_expected_actual(cfg, result)

        with allure.step("Verify: Actual includes observed_value"):
            assert actual is not None
            assert actual["observed_value"] == 50

    @allure.title("Extract actual from unexpected_count")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_extract_actual_from_unexpected_count(self):
        """Test extraction of actual values including unexpected_count."""
        with allure.step("Setup test data"):
            cfg = MagicMock()
            cfg.get = lambda key, default=None: {"kwargs": {}}.get(key, default)
            result = {"unexpected_count": 5, "element_count": 100}

        with allure.step("Execute extract_expected_actual"):
            _expected, actual = extract_expected_actual(cfg, result)

        with allure.step("Verify: Actual includes unexpected_count and element_count"):
            assert actual is not None
            assert actual["unexpected_count"] == 5
            assert actual["element_count"] == 100

    @allure.title("Extract returns None for empty values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_extract_returns_none_for_empty(self):
        """Test that None is returned when no values to extract."""
        with allure.step("Setup test data"):
            cfg = MagicMock()
            cfg.get = lambda key, default=None: {"kwargs": {"column": "only_column"}}.get(key, default)
            result = {}  # No observed_value or unexpected_count

        with allure.step("Execute extract_expected_actual"):
            expected, actual = extract_expected_actual(cfg, result)

        with allure.step("Verify: Both are None when no values"):
            assert expected is None  # Only column was in kwargs
            assert actual is None

    @allure.title("Extract handles None kwargs")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_extract_handles_none_kwargs(self):
        """Test extraction when kwargs is None."""
        with allure.step("Setup test data"):
            cfg = MagicMock()
            cfg.get = lambda key, default=None: {"kwargs": None}.get(key, default)
            result = {"observed_value": 42}

        with allure.step("Execute extract_expected_actual"):
            expected, actual = extract_expected_actual(cfg, result)

        with allure.step("Verify: Handles None kwargs gracefully"):
            assert expected is None
            assert actual is not None
            assert actual["observed_value"] == 42


# =============================================================================
# Debug Fields Tests (failure_stats, investigation_query, execution)
# =============================================================================


@allure.feature(AllureFeature.GE_SUITE)
@allure.story(AllureStory.GE_RESULT_NORMALIZER)
class TestDebugFields:
    """Tests for debug fields: failure_stats, investigation_query, execution."""

    @allure.title("Failed result includes failure_stats")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_failed_result_includes_failure_stats(self, mock_failed_validation_result, mock_run_id):
        """Test that failed results include failure_stats with aggregate statistics."""
        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_failed_validation_result,
                table_fq="catalog.schema.table",
                run_ts=datetime.now(UTC),
                run_id=mock_run_id,
            )

        with allure.step("Verify: failure_stats is present with correct structure"):
            row = normalized[0]
            assert row["failure_stats"] is not None
            stats = row["failure_stats"]
            assert stats["failure_type"] == "expectation_failed"
            assert "affected_columns" in stats
            assert "failure_count" in stats
            assert "total_rows_checked" in stats
            assert "failure_percentage" in stats
            assert "expectation_info" in stats

    @allure.title("Passed result has no failure_stats")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_passed_result_no_failure_stats(self, mock_validation_result, mock_run_id):
        """Test that passed results do not include failure_stats."""
        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_validation_result,
                table_fq="catalog.schema.table",
                run_ts=datetime.now(UTC),
                run_id=mock_run_id,
            )

        with allure.step("Verify: failure_stats is None for passed checks"):
            row = normalized[0]
            assert row["failure_stats"] is None

    @allure.title("Failed result includes investigation_query")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_failed_result_includes_investigation_query(self, mock_failed_validation_result, mock_run_id):
        """Test that failed results include investigation_query template."""
        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_failed_validation_result,
                table_fq="catalog.schema.table",
                run_ts=datetime.now(UTC),
                run_id=mock_run_id,
            )

        with allure.step("Verify: investigation_query is present"):
            row = normalized[0]
            assert row["investigation_query"] is not None
            query = row["investigation_query"]
            assert "description" in query
            assert "query" in query
            assert "catalog.schema.table" in query["query"]

    @allure.title("Passed result has no investigation_query")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_passed_result_no_investigation_query(self, mock_validation_result, mock_run_id):
        """Test that passed results do not include investigation_query."""
        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_validation_result,
                table_fq="catalog.schema.table",
                run_ts=datetime.now(UTC),
                run_id=mock_run_id,
            )

        with allure.step("Verify: investigation_query is None for passed checks"):
            row = normalized[0]
            assert row["investigation_query"] is None

    @allure.title("All results include execution trace")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_all_results_include_execution(self, mock_validation_result, mock_run_id):
        """Test that all results include execution trace (pass or fail)."""
        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_validation_result,
                table_fq="catalog.schema.table",
                run_ts=datetime.now(UTC),
                run_id=mock_run_id,
            )

        with allure.step("Verify: execution trace is present"):
            row = normalized[0]
            assert row["execution"] is not None
            execution = row["execution"]
            assert "duration_ms" in execution
            assert "started_at" in execution
            assert "rows_scanned" in execution

    @allure.title("failure_stats contains no PII")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.SECURITY)
    def test_failure_stats_no_pii(self, mock_failed_validation_result, mock_run_id):
        """Test that failure_stats contains only aggregate statistics, no PII."""
        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_failed_validation_result,
                table_fq="catalog.schema.table",
                run_ts=datetime.now(UTC),
                run_id=mock_run_id,
            )

        with allure.step("Verify: No PII fields in failure_stats"):
            row = normalized[0]
            stats = row["failure_stats"]
            assert stats is not None
            # These fields must NEVER exist
            assert "sample_values" not in stats
            assert "sample_ids" not in stats
            assert "partial_unexpected_list" not in stats
            assert "failing_row_ids" not in stats

    @allure.title("investigation_query contains no data values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.SECURITY)
    def test_investigation_query_no_data(self, mock_failed_validation_result, mock_run_id):
        """Test that investigation_query is a template with no embedded data."""
        with allure.step("Execute normalize_ge_results"):
            normalized = normalize_ge_results(
                validation_result=mock_failed_validation_result,
                table_fq="catalog.schema.table",
                run_ts=datetime.now(UTC),
                run_id=mock_run_id,
            )

        with allure.step("Verify: Query template has no embedded row values"):
            row = normalized[0]
            query = row["investigation_query"]
            assert query is not None
            # The mock has partial_unexpected_list = ["X", "Y"] - these must NOT appear in query
            assert "['X', 'Y']" not in query["query"]
