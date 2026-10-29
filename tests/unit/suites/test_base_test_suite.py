# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Base DQ Test Suite helper methods.

Tests verify that base helper methods work correctly without requiring
a full Spark environment or Unity Catalog.
"""

import allure
import pytest

from tests.constants import AllureEpic, AllureFeature, AllureSeverity, AllureStory, AllureTag

from neco_dq.test_suites.bronze.bronze_dq_test_suite import SparkBronzeDQTestSuite

# Mark all tests in this module as requiring Spark
pytestmark = pytest.mark.spark


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def base_suite(spark):
    """Create a concrete suite instance for testing base methods."""
    config = {"bronze": {"tables": [], "default_checks": {}, "defaults": {}}}
    return SparkBronzeDQTestSuite(spark, config)


@pytest.fixture(scope="module")
def base_suite_with_defaults(spark):
    """Create suite with suite-level defaults configured."""
    config = {
        "bronze": {
            "tables": [],
            "default_checks": {},
            "defaults": {
                "threshold": {"max_failure_percent": 10},
                "row_filter": "status = 'active'",
            },
        }
    }
    return SparkBronzeDQTestSuite(spark, config)


# =============================================================================
# Suite Identity Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.SUITE_IDENTITY)
class TestBaseSuiteProperties:
    """Tests for base suite property methods."""

    @allure.title("Suite config returns correct section")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_cfg(self, base_suite):
        """Suite config should return the correct config section."""
        with allure.step("Verify: Suite config returns correct section"):
            cfg = base_suite._suite_cfg
            assert isinstance(cfg, dict)
            assert "tables" in cfg

    @allure.title("Tables config returns empty list when no tables")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_tables_config_empty(self, base_suite):
        """Tables config should return empty list when no tables configured."""
        with allure.step("Verify: Tables config returns empty list"):
            tables = base_suite._tables_config
            assert tables == []

    @allure.title("Default checks returns empty dict when not configured")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_default_checks_empty(self, base_suite):
        """Default checks should return empty dict when not configured."""
        with allure.step("Verify: Default checks returns empty dict"):
            checks = base_suite._default_checks
            assert checks == {}

    @allure.title("Data to save returns empty list when not configured")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_data_to_save_empty(self, base_suite):
        """Data to save should return empty list when not configured."""
        with allure.step("Verify: Data to save returns empty list"):
            data = base_suite._data_to_save
            assert data == []

    @allure.title("Suite defaults returns empty dict when not configured")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_defaults_empty(self, base_suite):
        """Suite defaults should return empty dict when not configured."""
        with allure.step("Verify: Suite defaults returns empty dict"):
            defaults = base_suite._suite_defaults
            assert defaults == {}

    @allure.title("Suite defaults returns configured values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_defaults_configured(self, base_suite_with_defaults):
        """Suite defaults should return configured values."""
        with allure.step("Verify: Suite defaults returns configured values"):
            defaults = base_suite_with_defaults._suite_defaults
            assert "threshold" in defaults
            assert defaults["threshold"]["max_failure_percent"] == 10


# =============================================================================
# Normalize Check Config Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestNormalizeCheckConfig:
    """Tests for _normalize_check_cfg method."""

    @allure.title("Normalize returns empty list for None")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_normalize_none(self, base_suite):
        """Normalize should return empty list for None input."""
        with allure.step("Verify: Normalize returns empty list for None"):
            result = base_suite._normalize_check_cfg(None)
            assert result == []

    @allure.title("Normalize returns list as-is")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_list(self, base_suite):
        """Normalize should return list input as-is."""
        with allure.step("Setup test data"):
            input_list = [{"enabled": True}, {"enabled": False}]

        with allure.step("Verify: Normalize returns list as-is"):
            result = base_suite._normalize_check_cfg(input_list)
            assert result == input_list

    @allure.title("Normalize wraps dict in list")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_dict(self, base_suite):
        """Normalize should wrap dict in a list."""
        with allure.step("Setup test data"):
            input_dict = {"enabled": True, "column": "test"}

        with allure.step("Verify: Normalize wraps dict in list"):
            result = base_suite._normalize_check_cfg(input_dict)
            assert result == [input_dict]

    @allure.title("Normalize raises ValueError for invalid type")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_normalize_invalid_type(self, base_suite):
        """Normalize should raise ValueError for invalid input type."""
        with allure.step("Verify: Normalize raises ValueError for invalid type"):
            with pytest.raises(ValueError, match="Invalid check configuration type"):
                base_suite._normalize_check_cfg("invalid_string")


# =============================================================================
# Build Check Instance Name Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestBuildCheckInstanceName:
    """Tests for _build_check_instance_name method."""

    @allure.title("Single instance returns plain name")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_single_instance(self, base_suite):
        """Single instance should return plain check name."""
        with allure.step("Verify: Single instance returns plain name"):
            result = base_suite._build_check_instance_name("record_count", {}, 0, single=True)
            assert result == "record_count"

    @allure.title("Multiple instances with name field")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_multiple_with_name(self, base_suite):
        """Multiple instances with name field should use name."""
        with allure.step("Setup test data"):
            cfg = {"name": "custom_name", "enabled": True}

        with allure.step("Verify: Multiple instances with name field"):
            result = base_suite._build_check_instance_name("check", cfg, 0, single=False)
            assert result == "check[name=custom_name]"

    @allure.title("Multiple instances with key_column field")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_multiple_with_key_column(self, base_suite):
        """Multiple instances with key_column should use key_column."""
        with allure.step("Setup test data"):
            cfg = {"key_column": "id,code", "enabled": True}

        with allure.step("Verify: Multiple instances with key_column field"):
            result = base_suite._build_check_instance_name("idempotency", cfg, 0, single=False)
            assert result == "idempotency[key_column=id,code]"

    @allure.title("Multiple instances with column field")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_multiple_with_column(self, base_suite):
        """Multiple instances with column should use column."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "enabled": True}

        with allure.step("Verify: Multiple instances with column field"):
            result = base_suite._build_check_instance_name("binary_type", cfg, 0, single=False)
            assert result == "binary_type[column=payload]"

    @allure.title("Multiple instances with columns list")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_multiple_with_columns_list(self, base_suite):
        """Multiple instances with columns list should join values."""
        with allure.step("Setup test data"):
            cfg = {"columns": ["col1", "col2", "col3"], "enabled": True}

        with allure.step("Verify: Multiple instances with columns list"):
            result = base_suite._build_check_instance_name("mandatory_fields", cfg, 0, single=False)
            assert result == "mandatory_fields[columns=col1,col2,col3]"

    @allure.title("Multiple instances fallback to index")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_multiple_fallback_index(self, base_suite):
        """Multiple instances without naming fields should use index."""
        with allure.step("Setup test data"):
            cfg = {"enabled": True, "some_other_field": "value"}

        with allure.step("Verify: Multiple instances fallback to index"):
            result = base_suite._build_check_instance_name("custom_check", cfg, 2, single=False)
            assert result == "custom_check[2]"


# =============================================================================
# Simple Getter Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestSimpleGetters:
    """Tests for simple getter methods."""

    @allure.title("Is enabled returns True when enabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_is_enabled_true(self, base_suite):
        """Is enabled should return True when enabled is True."""
        with allure.step("Verify: Is enabled returns True"):
            assert base_suite._is_enabled({"enabled": True}) is True

    @allure.title("Is enabled returns False when not enabled")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_is_enabled_false(self, base_suite):
        """Is enabled should return False when enabled is False or missing."""
        with allure.step("Verify: Is enabled returns False"):
            assert base_suite._is_enabled({"enabled": False}) is False
            assert base_suite._is_enabled({}) is False

    @allure.title("Severity returns configured value")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_severity_configured(self, base_suite):
        """Severity should return configured value."""
        with allure.step("Verify: Severity returns configured value"):
            assert base_suite._severity({"severity": "critical"}) == "critical"
            assert base_suite._severity({"severity": "error"}) == "error"

    @allure.title("Severity returns warning as default")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_severity_default(self, base_suite):
        """Severity should return 'warning' as default."""
        with allure.step("Verify: Severity returns warning as default"):
            assert base_suite._severity({}) == "warning"

    @allure.title("Blocking returns configured value")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_blocking_configured(self, base_suite):
        """Blocking should return configured value."""
        with allure.step("Verify: Blocking returns configured value"):
            assert base_suite._blocking({"blocking": True}) is True
            assert base_suite._blocking({"blocking": False}) is False

    @allure.title("Blocking returns False as default")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_blocking_default(self, base_suite):
        """Blocking should return False as default."""
        with allure.step("Verify: Blocking returns False as default"):
            assert base_suite._blocking({}) is False


# =============================================================================
# Register Check Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestRegisterCheck:
    """Tests for _register_check method."""

    @allure.title("Register check adds to result")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_register_check_pass(self, base_suite):
        """Register check should add check with PASS status."""
        with allure.step("Setup test data"):
            result = {"checks": []}

        with allure.step("Execute register check"):
            base_suite._register_check(
                result, "test_check", passed=True, severity="warning", blocking=False, details="ok", column="col"
            )

        with allure.step("Verify: Register check adds to result"):
            assert len(result["checks"]) == 1
            check = result["checks"][0]
            assert check["check_name"] == "test_check"
            assert check["passed"] is True
            assert check["status"] == "PASS"
            assert check["severity"] == "warning"
            assert check["blocking"] is False
            assert check["details"] == "ok"
            assert check["column"] == "col"

    @allure.title("Register check sets FAIL status")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_register_check_fail(self, base_suite):
        """Register check should set FAIL status when passed is False."""
        with allure.step("Setup test data"):
            result = {"checks": []}

        with allure.step("Execute register check"):
            base_suite._register_check(
                result, "failing_check", passed=False, severity="error", blocking=True, details="error", column=None
            )

        with allure.step("Verify: Register check sets FAIL status"):
            check = result["checks"][0]
            assert check["passed"] is False
            assert check["status"] == "FAIL"

    @allure.title("Register check initializes checks list")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_register_check_initializes_list(self, base_suite):
        """Register check should initialize checks list if not present."""
        with allure.step("Setup test data"):
            result = {}

        with allure.step("Execute register check"):
            base_suite._register_check(result, "test", True, "info", False, "ok", None)

        with allure.step("Verify: Register check initializes checks list"):
            assert "checks" in result
            assert len(result["checks"]) == 1


# =============================================================================
# Build Alert Description Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestBuildAlertDescription:
    """Tests for _build_alert_description method."""

    @allure.title("Alert description returns None for empty list")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_alert_empty(self, base_suite):
        """Alert description should return None for empty failed checks."""
        with allure.step("Verify: Alert description returns None for empty list"):
            result = base_suite._build_alert_description([])
            assert result is None

    @allure.title("Alert description formats single failure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_alert_single(self, base_suite):
        """Alert description should format single failure."""
        with allure.step("Setup test data"):
            failed = [{"check_name": "record_count", "details": "count=0"}]

        with allure.step("Verify: Alert description formats single failure"):
            result = base_suite._build_alert_description(failed)
            assert result == "[record_count] count=0"

    @allure.title("Alert description formats multiple failures")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_alert_multiple(self, base_suite):
        """Alert description should format multiple failures with pipe separator."""
        with allure.step("Setup test data"):
            failed = [
                {"check_name": "check1", "details": "error1"},
                {"check_name": "check2", "details": "error2"},
            ]

        with allure.step("Verify: Alert description formats multiple failures"):
            result = base_suite._build_alert_description(failed)
            assert "[check1] error1" in result
            assert "[check2] error2" in result
            assert " | " in result


# =============================================================================
# Require Config Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestRequireConfig:
    """Tests for _require_cfg method."""

    @allure.title("Require config passes with all keys present")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_require_all_present(self, base_suite):
        """Require config should pass when all keys are present."""
        with allure.step("Setup test data"):
            cfg = {"key1": "value1", "key2": "value2"}

        with allure.step("Verify: Require config passes with all keys"):
            ok, err = base_suite._require_cfg(cfg, "key1", "key2")
            assert ok is True
            assert err is None

    @allure.title("Require config fails with missing keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_require_missing_keys(self, base_suite):
        """Require config should fail when keys are missing."""
        with allure.step("Setup test data"):
            cfg = {"key1": "value1"}

        with allure.step("Verify: Require config fails with missing keys"):
            ok, err = base_suite._require_cfg(cfg, "key1", "key2", "key3")
            assert ok is False
            assert "Missing config" in err
            assert "key2" in err
            assert "key3" in err

    @allure.title("Require config fails with None values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_require_none_values(self, base_suite):
        """Require config should fail when values are None."""
        with allure.step("Setup test data"):
            cfg = {"key1": "value1", "key2": None}

        with allure.step("Verify: Require config fails with None values"):
            ok, err = base_suite._require_cfg(cfg, "key1", "key2")
            assert ok is False
            assert "key2" in err


# =============================================================================
# Safe Sample Tests
# =============================================================================
# Evaluate Threshold Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.FAILURE_POLICY)
class TestEvaluateThreshold:
    """Tests for _evaluate_threshold method."""

    @allure.title("No threshold - zero failures passes")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_no_threshold_zero_failures(self, base_suite):
        """Without threshold, zero failures should pass."""
        with allure.step("Verify: No threshold - zero failures passes"):
            passed, meta = base_suite._evaluate_threshold(0, 100, {})
            assert passed is True
            assert meta == {}

    @allure.title("No threshold - any failure fails")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_no_threshold_any_failure(self, base_suite):
        """Without threshold, any failure should fail."""
        with allure.step("Verify: No threshold - any failure fails"):
            passed, meta = base_suite._evaluate_threshold(1, 100, {})
            assert passed is False
            assert meta == {}

    @allure.title("Empty dataset passes with threshold")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_empty_dataset(self, base_suite):
        """Empty dataset should pass with threshold configured."""
        with allure.step("Setup test data"):
            cfg = {"threshold": {"max_failure_percent": 10}}

        with allure.step("Verify: Empty dataset passes"):
            passed, meta = base_suite._evaluate_threshold(0, 0, cfg)
            assert passed is True
            assert meta["note"] == "no_data_to_validate"

    @allure.title("Threshold percent - within limit passes")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_percent_pass(self, base_suite):
        """Should pass when failure percent is within threshold."""
        with allure.step("Setup test data"):
            cfg = {"threshold": {"max_failure_percent": 10}}

        with allure.step("Verify: Threshold percent - within limit passes"):
            passed, meta = base_suite._evaluate_threshold(5, 100, cfg)  # 5%
            assert passed is True
            assert meta["threshold_applied"] is True
            assert meta["failure_pct"] == 5.0

    @allure.title("Threshold percent - exceeds limit fails")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_threshold_percent_fail(self, base_suite):
        """Should fail when failure percent exceeds threshold."""
        with allure.step("Setup test data"):
            cfg = {"threshold": {"max_failure_percent": 10}}

        with allure.step("Verify: Threshold percent - exceeds limit fails"):
            passed, meta = base_suite._evaluate_threshold(15, 100, cfg)  # 15%
            assert passed is False
            assert meta["passed_pct_check"] is False

    @allure.title("Threshold count - within limit passes")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_count_pass(self, base_suite):
        """Should pass when failure count is within threshold."""
        with allure.step("Setup test data"):
            cfg = {"threshold": {"max_failure_count": 50}}

        with allure.step("Verify: Threshold count - within limit passes"):
            passed, meta = base_suite._evaluate_threshold(30, 100, cfg)
            assert passed is True
            assert meta["passed_cnt_check"] is True

    @allure.title("Threshold count - exceeds limit fails")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_threshold_count_fail(self, base_suite):
        """Should fail when failure count exceeds threshold."""
        with allure.step("Setup test data"):
            cfg = {"threshold": {"max_failure_count": 50}}

        with allure.step("Verify: Threshold count - exceeds limit fails"):
            passed, meta = base_suite._evaluate_threshold(60, 100, cfg)
            assert passed is False
            assert meta["passed_cnt_check"] is False

    @allure.title("Both thresholds - AND logic")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_both_and_logic(self, base_suite):
        """Should fail if either threshold is exceeded (AND logic)."""
        with allure.step("Setup test data"):
            cfg = {"threshold": {"max_failure_percent": 10, "max_failure_count": 5}}

        with allure.step("Verify: Both thresholds - AND logic (both pass)"):
            # 3% and 3 count - both pass
            passed, _ = base_suite._evaluate_threshold(3, 100, cfg)
            assert passed is True

        with allure.step("Verify: Both thresholds - AND logic (percent fails)"):
            # 15% but only 3 count - percent fails
            passed, _ = base_suite._evaluate_threshold(3, 20, cfg)  # 15%
            assert passed is False

        with allure.step("Verify: Both thresholds - AND logic (count fails)"):
            # 1% but 10 count - count fails
            passed, _ = base_suite._evaluate_threshold(10, 1000, cfg)  # 1%
            assert passed is False

    @allure.title("Suite default threshold is used")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_default_threshold(self, base_suite_with_defaults):
        """Should use suite-level default threshold if check-level not set."""
        with allure.step("Verify: Suite default threshold is used"):
            # Suite default is 10%
            passed, meta = base_suite_with_defaults._evaluate_threshold(5, 100, {})
            assert passed is True
            assert meta["threshold_applied"] is True


# =============================================================================
# Build Threshold Details Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.FAILURE_POLICY)
class TestBuildThresholdDetails:
    """Tests for _build_threshold_details method."""

    @allure.title("Empty metadata returns base details")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_empty_metadata(self, base_suite):
        """Empty metadata should return base details unchanged."""
        with allure.step("Verify: Empty metadata returns base details"):
            result = base_suite._build_threshold_details("count=10", {})
            assert result == "count=10"

    @allure.title("No data note is included")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_no_data_note(self, base_suite):
        """No data note should be included in details."""
        with allure.step("Setup test data"):
            meta = {"note": "no_data_to_validate"}

        with allure.step("Verify: No data note is included"):
            result = base_suite._build_threshold_details("count=0", meta)
            assert "no_data_to_validate" in result

    @allure.title("Threshold details with percent")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_with_percent(self, base_suite):
        """Threshold details should include percent info."""
        with allure.step("Setup test data"):
            meta = {"threshold_applied": True, "failure_pct": 5.0, "max_failure_pct": 10}

        with allure.step("Verify: Threshold details with percent"):
            result = base_suite._build_threshold_details("count=5", meta)
            assert "threshold_applied=True" in result
            assert "failure_pct=5.00%" in result
            assert "max_pct=10%" in result

    @allure.title("Threshold details with count")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_with_count(self, base_suite):
        """Threshold details should include count info."""
        with allure.step("Setup test data"):
            meta = {"threshold_applied": True, "failure_pct": 3.0, "max_failure_cnt": 50}

        with allure.step("Verify: Threshold details with count"):
            result = base_suite._build_threshold_details("count=30", meta)
            assert "max_cnt=50" in result


# =============================================================================
# Build Partition Filter Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestBuildPartitionFilter:
    """Tests for _build_partition_filter method."""

    @allure.title("Single string value")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_single_string(self, base_suite):
        """Should build filter for single string value."""
        with allure.step("Verify: Single string value"):
            result = base_suite._build_partition_filter({"date": "2024-01-15"})
            assert result == "date = '2024-01-15'"

    @allure.title("Single integer value")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_single_integer(self, base_suite):
        """Should build filter for single integer value."""
        with allure.step("Verify: Single integer value"):
            result = base_suite._build_partition_filter({"year": 2024})
            assert result == "year = 2024"

    @allure.title("List of values creates IN clause")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_list_values(self, base_suite):
        """Should build IN clause for list of values."""
        with allure.step("Verify: List of values creates IN clause"):
            result = base_suite._build_partition_filter({"date": ["2024-01-14", "2024-01-15"]})
            assert "date IN" in result
            assert "'2024-01-14'" in result
            assert "'2024-01-15'" in result

    @allure.title("Multiple columns with AND")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_multiple_columns(self, base_suite):
        """Should join multiple columns with AND."""
        with allure.step("Verify: Multiple columns with AND"):
            result = base_suite._build_partition_filter({"year": 2024, "month": 1})
            assert "year = 2024" in result
            assert "month = 1" in result
            assert " AND " in result

    @allure.title("Empty list raises ValueError")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_empty_list_raises(self, base_suite):
        """Should raise ValueError for empty list."""
        with allure.step("Verify: Empty list raises ValueError"):
            with pytest.raises(ValueError, match="empty list"):
                base_suite._build_partition_filter({"date": []})

    @allure.title("SQL injection prevention - escapes quotes")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_sql_injection_prevention(self, base_suite):
        """Should escape single quotes to prevent SQL injection."""
        with allure.step("Verify: SQL injection prevention"):
            result = base_suite._build_partition_filter({"name": "O'Brien"})
            assert "O''Brien" in result  # Escaped quote


# =============================================================================
# Validate Partition Filter Tests
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestValidatePartitionFilter:
    """Tests for _validate_partition_filter method."""

    @allure.title("Valid partition columns pass")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_valid_columns(self, base_suite):
        """Should pass when all filter columns are partition columns."""
        with allure.step("Verify: Valid partition columns pass"):
            all_valid, non_partition = base_suite._validate_partition_filter(
                {"year": 2024, "month": 1}, ["year", "month", "day"]
            )
            assert all_valid is True
            assert non_partition == []

    @allure.title("Invalid partition columns fail")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_invalid_columns(self, base_suite):
        """Should fail when filter columns are not partition columns."""
        with allure.step("Verify: Invalid partition columns fail"):
            all_valid, non_partition = base_suite._validate_partition_filter(
                {"year": 2024, "status": "active"}, ["year", "month"]
            )
            assert all_valid is False
            assert "status" in non_partition

    @allure.title("Empty partition columns returns valid")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_empty_partition_columns(self, base_suite):
        """Should return valid when partition columns list is empty."""
        with allure.step("Verify: Empty partition columns returns valid"):
            all_valid, non_partition = base_suite._validate_partition_filter({"year": 2024}, [])
            assert all_valid is True
            assert non_partition == []


# =============================================================================
# Tag-Based Filtering Tests
# =============================================================================


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.TAG_FILTERING)
class TestTagBasedFiltering:
    """Tests for tag-based check filtering."""

    @allure.title("No filter set runs all checks")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_no_filter_runs_all(self, base_suite):
        """Should run check when no tags filter is set."""
        with allure.step("Setup test data"):
            base_suite.set_tags_filter(None)
            cfg = {"enabled": True, "tags": ["critical"]}

        with allure.step("Verify: No filter runs all checks"):
            assert base_suite._should_run_check(cfg) is True

    @allure.title("Check without tags is skipped when filter is set")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_no_tags_on_check_skipped(self, base_suite):
        """Should skip check without tags when filter is set (strict filtering)."""
        with allure.step("Setup test data"):
            base_suite.set_tags_filter(["critical"])
            cfg = {"enabled": True}  # No tags

        with allure.step("Verify: Check without tags is skipped"):
            assert base_suite._should_run_check(cfg) is False

        with allure.step("Cleanup"):
            base_suite.set_tags_filter(None)

    @allure.title("Check with matching tags runs")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_matching_tags_runs(self, base_suite):
        """Should run check when tags match the filter."""
        with allure.step("Setup test data"):
            base_suite.set_tags_filter(["critical", "daily"])
            cfg = {"enabled": True, "tags": ["critical"]}

        with allure.step("Verify: Matching tags run check"):
            assert base_suite._should_run_check(cfg) is True

        with allure.step("Cleanup"):
            base_suite.set_tags_filter(None)

    @allure.title("Check with non-matching tags is skipped")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_non_matching_tags_skipped(self, base_suite):
        """Should skip check when tags don't match the filter."""
        with allure.step("Setup test data"):
            base_suite.set_tags_filter(["critical"])
            cfg = {"enabled": True, "tags": ["hourly", "smoke"]}

        with allure.step("Verify: Non-matching tags skip check"):
            assert base_suite._should_run_check(cfg) is False

        with allure.step("Cleanup"):
            base_suite.set_tags_filter(None)

    @allure.title("Partial tag match runs check (OR logic)")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_partial_match_runs(self, base_suite):
        """Should run check when ANY tag matches (OR logic, not AND)."""
        with allure.step("Setup test data"):
            base_suite.set_tags_filter(["critical"])
            cfg = {"enabled": True, "tags": ["critical", "hourly", "expensive"]}

        with allure.step("Verify: Partial match runs check"):
            assert base_suite._should_run_check(cfg) is True

        with allure.step("Cleanup"):
            base_suite.set_tags_filter(None)


# =============================================================================
# Debug Experience - Narrative Details Tests
# =============================================================================


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestDetailsCountFailures:
    """Tests for _details_count_failures method (Debug Experience)."""

    @allure.title("Count failures with percentage")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_count_failures_with_pct(self, base_suite):
        """Should format count with percentage of total."""
        with allure.step("Verify: Count with percentage"):
            result = base_suite._details_count_failures(50, "duplicate keys", 1000)
            assert result == "Found 50 duplicate keys (5.0% of 1000)"

    @allure.title("Count failures without total")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_count_failures_no_total(self, base_suite):
        """Should format count without percentage when no total."""
        with allure.step("Verify: Count without total"):
            result = base_suite._details_count_failures(25, "orphan records")
            assert result == "Found 25 orphan records"

    @allure.title("Count failures with zero total")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_count_failures_zero_total(self, base_suite):
        """Should handle zero total gracefully (no percentage shown)."""
        with allure.step("Verify: Zero total - no percentage"):
            result = base_suite._details_count_failures(0, "issues", 0)
            assert result == "Found 0 issues"

    @allure.title("Count failures - null values example")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_count_failures_null_values(self, base_suite):
        """Should format null value failures narratively."""
        with allure.step("Verify: Null values format"):
            result = base_suite._details_count_failures(150, "null values", 1000)
            assert result == "Found 150 null values (15.0% of 1000)"


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestDetailsThresholdViolation:
    """Tests for _details_threshold_violation method (Debug Experience)."""

    @allure.title("Threshold violation - exceeds limit")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_exceeds(self, base_suite):
        """Should format exceeds threshold violation."""
        with allure.step("Verify: Exceeds format"):
            result = base_suite._details_threshold_violation("Data", "48h old", "24h", "exceeds")
            assert result == "Data 48h old exceeds 24h limit"

    @allure.title("Threshold violation - below minimum")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_below(self, base_suite):
        """Should format below minimum violation."""
        with allure.step("Verify: Below format"):
            result = base_suite._details_threshold_violation("Coverage", "85%", "95%", "below minimum")
            assert result == "Coverage 85% below minimum 95%"

    @allure.title("Threshold violation - deviation example")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_deviation(self, base_suite):
        """Should format deviation threshold violation."""
        with allure.step("Verify: Deviation format"):
            result = base_suite._details_threshold_violation("Deviation", "15%", "10%", "exceeds")
            assert result == "Deviation 15% exceeds 10% limit"


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestDetailsExpectedVsObserved:
    """Tests for _details_expected_vs_observed method (Debug Experience)."""

    @allure.title("Expected vs observed - simple")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_expected_vs_observed_simple(self, base_suite):
        """Should format simple expected vs observed."""
        with allure.step("Verify: Simple format"):
            result = base_suite._details_expected_vs_observed(105, 100)
            assert result == "Mismatch: expected 105, got 100"

    @allure.title("Expected vs observed - with metric name")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_expected_vs_observed_with_metric(self, base_suite):
        """Should format with metric name."""
        with allure.step("Verify: With metric"):
            result = base_suite._details_expected_vs_observed(0, 50, "duplicates")
            assert result == "duplicates mismatch: expected 0, got 50"

    @allure.title("Expected vs observed - type mismatch")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_expected_vs_observed_type(self, base_suite):
        """Should format type mismatch."""
        with allure.step("Verify: Type mismatch"):
            result = base_suite._details_expected_vs_observed("binary", "string", "type")
            assert result == "type mismatch: expected binary, got string"


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestDetailsMissingInvalid:
    """Tests for _details_missing_invalid method (Debug Experience)."""

    @allure.title("Missing items - simple")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_missing_simple(self, base_suite):
        """Should format missing items."""
        with allure.step("Verify: Missing format"):
            result = base_suite._details_missing_invalid(3, "required columns missing")
            assert result == "Missing 3 required columns"

    @allure.title("Invalid items - with details")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_invalid_with_details(self, base_suite):
        """Should format invalid items with details."""
        with allure.step("Verify: With details"):
            result = base_suite._details_missing_invalid(12, "invalid transitions", "PENDING->SHIPPED")
            assert result == "Found 12 invalid transitions: PENDING->SHIPPED"


# =============================================================================
# Debug Experience - Legacy Comparison Details Tests (backward compat)
# =============================================================================


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestBuildComparisonDetails:
    """Tests for _build_comparison_details method (narrative format)."""

    @allure.title("Comparison details for equality failure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_comparison_equality(self, base_suite):
        """Should format equality failure narratively."""
        with allure.step("Verify: Equality failure"):
            result = base_suite._build_comparison_details(100, 105, "==")
            assert result == "Expected 105, got 100"

    @allure.title("Comparison details for >= failure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_comparison_greater_equal(self, base_suite):
        """Should format >= failure as below minimum."""
        with allure.step("Verify: >= failure"):
            result = base_suite._build_comparison_details(85, 90, ">=")
            assert result == "85 is below 90 minimum"

    @allure.title("Comparison details for <= failure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_comparison_less_equal(self, base_suite):
        """Should format <= failure as exceeds maximum."""
        with allure.step("Verify: <= failure"):
            result = base_suite._build_comparison_details(110, 100, "<=")
            assert result == "110 exceeds 100 maximum"

    @allure.title("Comparison details for between failure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_comparison_between(self, base_suite):
        """Should format between failure narratively."""
        with allure.step("Verify: Between failure"):
            result = base_suite._build_comparison_details(150, (100, 120), "between")
            assert result == "150 not in range [100, 120]"


# =============================================================================
# Debug Experience - Build Failure Stats Tests
# =============================================================================


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestBuildFailureStats:
    """Tests for _build_failure_stats method (Debug Experience)."""

    @allure.title("Failure stats includes required fields")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_failure_stats_basic(self, base_suite):
        """Should include all required fields."""
        with allure.step("Execute build failure stats"):
            result = base_suite._build_failure_stats(
                failure_type="duplicate_key",
                affected_columns=["order_id"],
                failure_count=47,
                total_checked=50000,
            )

        with allure.step("Verify: Required fields"):
            assert result["failure_type"] == "duplicate_key"
            assert result["affected_columns"] == ["order_id"]
            assert result["failure_count"] == 47
            assert result["total_rows_checked"] == 50000
            assert result["failure_percentage"] == 0.094

    @allure.title("Failure stats includes distribution")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_failure_stats_with_distribution(self, base_suite):
        """Should include distribution when provided."""
        with allure.step("Setup test data"):
            distribution = {"duplicates_2": 30, "duplicates_3_5": 12, "duplicates_6_plus": 5}

        with allure.step("Execute build failure stats"):
            result = base_suite._build_failure_stats(
                failure_type="duplicate_key",
                affected_columns=["order_id"],
                failure_count=47,
                total_checked=50000,
                distribution=distribution,
            )

        with allure.step("Verify: Distribution included"):
            assert result["distribution"] == distribution

    @allure.title("Failure stats includes diagnostic")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_failure_stats_with_diagnostic(self, base_suite):
        """Should include diagnostic when provided."""
        with allure.step("Setup test data"):
            diagnostic = {"max_duplicates_per_key": 8, "avg_duplicates_per_key": 2.4}

        with allure.step("Execute build failure stats"):
            result = base_suite._build_failure_stats(
                failure_type="duplicate_key",
                affected_columns=["order_id"],
                failure_count=47,
                total_checked=50000,
                diagnostic=diagnostic,
            )

        with allure.step("Verify: Diagnostic included"):
            assert result["diagnostic"] == diagnostic

    @allure.title("Failure stats handles zero total (no division error)")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_failure_stats_zero_total(self, base_suite):
        """Should handle zero total without division error."""
        with allure.step("Verify: Zero total handled"):
            result = base_suite._build_failure_stats(
                failure_type="null_value",
                affected_columns=["email"],
                failure_count=0,
                total_checked=0,
            )
            assert result["failure_percentage"] == 0

    @allure.title("SECURITY: Failure stats never contains values or IDs")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_failure_stats_no_pii(self, base_suite):
        """SECURITY: Failure stats must NEVER contain values or IDs."""
        with allure.step("Execute build failure stats"):
            result = base_suite._build_failure_stats(
                failure_type="duplicate_key",
                affected_columns=["order_id"],
                failure_count=47,
                total_checked=50000,
                distribution={"duplicates_2": 30},
                diagnostic={"max_duplicates_per_key": 8},
            )

        with allure.step("Verify: No PII fields"):
            result_str = str(result)
            assert "sample" not in result_str.lower()
            assert "value" not in result_str.lower() or "expected_value" not in result_str.lower()
            assert "row" not in result_str.lower() or "rows_checked" in result_str.lower()
            assert "id" not in result_str.lower() or "order_id" in result_str.lower()

        with allure.step("Verify: No PII keys"):
            forbidden_keys = ["sample_ids", "sample_values", "failing_row_ids", "partial_unexpected_list"]
            for key in forbidden_keys:
                assert key not in result


# =============================================================================
# Debug Experience - Build Investigation Query Tests
# =============================================================================


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestBuildInvestigationQuery:
    """Tests for _build_investigation_query method (Debug Experience)."""

    @allure.title("Investigation query for duplicate_key")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_investigation_query_duplicate(self, base_suite):
        """Should generate duplicate key investigation query."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="duplicate_key",
                table_fq="curated.sales.orders",
                affected_columns=["order_id"],
                cfg={"key_column": "order_id"},
            )

        with allure.step("Verify: Query structure"):
            assert "description" in result
            assert "query" in result
            assert "GROUP BY" in result["query"]
            assert "HAVING count(*) > 1" in result["query"]
            assert "curated.sales.orders" in result["query"]

    @allure.title("Investigation query for null_value")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_investigation_query_null(self, base_suite):
        """Should generate null value investigation query."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="null_value",
                table_fq="curated.sales.customers",
                affected_columns=["email", "phone"],
                cfg={},
            )

        with allure.step("Verify: Query structure"):
            assert "IS NULL" in result["query"]
            assert "email IS NULL OR phone IS NULL" in result["query"]

    @allure.title("Investigation query includes where clause")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_investigation_query_with_where(self, base_suite):
        """Should include where clause when provided."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="duplicate_key",
                table_fq="curated.sales.orders",
                affected_columns=["order_id"],
                cfg={"key_column": "order_id"},
                where_clause="status != 'deleted'",
            )

        with allure.step("Verify: Where clause included"):
            assert "status != 'deleted'" in result["query"]

    @allure.title("Investigation query falls back to generic")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_investigation_query_generic_fallback(self, base_suite):
        """Should fall back to generic template for unknown types."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="unknown_check_type",
                table_fq="curated.sales.orders",
                affected_columns=["col1"],
                cfg={},
            )

        with allure.step("Verify: Generic fallback"):
            assert "SELECT *" in result["query"]
            assert "LIMIT 100" in result["query"]

    @allure.title("Investigation query for cross_table_mismatch is fully rendered")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_investigation_query_cross_table_rendered(self, base_suite):
        """Join-based templates must not leak unresolved placeholders."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="cross_table_mismatch",
                table_fq="c.s.left",
                affected_columns=["amount"],
                cfg={
                    "join_key": "id",
                    "left_column": "amount",
                    "right_column": "amount",
                    "left_table": "c.s.left",
                    "right_table": "c.s.right",
                    "tolerance": 0.0,
                },
            )

        with allure.step("Verify: placeholders resolved"):
            query = result["query"]
            assert "{" not in query
            assert "FROM c.s.left l JOIN c.s.right r ON l.id = r.id" in query
            assert "> 0.0" in query

    @allure.title("Investigation query for cross_table_mismatch joins on composite keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_investigation_query_cross_table_composite(self, base_suite):
        """A list-valued join key renders one equality per key column."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="cross_table_mismatch",
                table_fq="c.s.left",
                affected_columns=["amount"],
                cfg={
                    "join_key": ["tenant", "id"],
                    "left_column": "amount",
                    "right_column": "amount",
                    "left_table": "c.s.left",
                    "right_table": "c.s.right",
                    "tolerance": 0.0,
                },
            )

        with allure.step("Verify: composite join rendered"):
            query = result["query"]
            assert "{" not in query
            assert query.startswith("SELECT l.tenant, l.id, ")
            assert "ON l.tenant = r.tenant AND l.id = r.id" in query

    @allure.title("Investigation query for temporal_violation joins on composite keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_investigation_query_temporal_composite(self, base_suite):
        """Temporal template renders timestamps, tables and composite join."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="temporal_violation",
                table_fq="c.s.left",
                affected_columns=["created_at"],
                cfg={
                    "join_key": ["tenant", "id"],
                    "left_timestamp": "created_at",
                    "right_timestamp": "processed_at",
                    "left_table": "c.s.left",
                    "right_table": "c.s.right",
                },
            )

        with allure.step("Verify: fully rendered"):
            query = result["query"]
            assert "{" not in query
            assert "ON l.tenant = r.tenant AND l.id = r.id" in query
            assert "WHERE l.created_at > r.processed_at" in query

    @allure.title("Investigation query for totals_mismatch joins on composite keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_investigation_query_totals_composite(self, base_suite):
        """Totals template groups and joins detail on every key column."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="totals_mismatch",
                table_fq="c.s.header",
                affected_columns=["total"],
                cfg={
                    "join_key": ["tenant", "order_id"],
                    "header_amount": "total",
                    "detail_amount": "line_amount",
                    "detail_table": "c.s.detail",
                    "tolerance": 0.01,
                },
            )

        with allure.step("Verify: fully rendered"):
            query = result["query"]
            assert "{" not in query
            assert "GROUP BY tenant, order_id) d ON h.tenant = d.tenant AND h.order_id = d.order_id" in query
            assert "FROM c.s.header h" in query

    @allure.title("Investigation query for incomplete_flow joins on composite keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_investigation_query_flow_composite(self, base_suite):
        """Flow templates anti-join source to target on every key column."""
        with allure.step("Execute build investigation query"):
            cfg = {
                "source_table": "c.s.src",
                "target_table": "c.s.tgt",
                "join_key": ["tenant", "id"],
                "source_filter": "",
            }
            plain = base_suite._build_investigation_query(
                check_type="incomplete_flow", table_fq="c.s.src", affected_columns=["tenant", "id"], cfg=cfg
            )
            late = base_suite._build_investigation_query(
                check_type="incomplete_flow_lateness",
                table_fq="c.s.src",
                affected_columns=["tenant", "id"],
                cfg={**cfg, "lateness_column": "event_ts", "max_delay_minutes": 60},
            )

        with allure.step("Verify: both fully rendered"):
            for query in (plain["query"], late["query"]):
                assert "{" not in query
                assert "LEFT JOIN c.s.tgt tgt ON src.tenant = tgt.tenant AND src.id = tgt.id" in query
                assert "WHERE tgt.tenant IS NULL" in query
            assert "ORDER BY src.tenant, src.id" in plain["query"]
            assert "<= 60" in late["query"]

    @allure.title("SECURITY: Investigation query has no embedded data")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_investigation_query_no_data(self, base_suite):
        """SECURITY: Query must not contain actual data values (PII)."""
        with allure.step("Execute build investigation query"):
            result = base_suite._build_investigation_query(
                check_type="duplicate_key",
                table_fq="curated.sales.orders",
                affected_columns=["order_id"],
                cfg={"key_column": "order_id"},
            )

        with allure.step("Verify: No embedded data"):
            query = result["query"]
            # Should not contain specific IDs or values
            assert "ORD-" not in query
            assert "john@" not in query
            assert "'active'" not in query or "= 'active'" not in query


# =============================================================================
# Debug Experience - Build Execution Trace Tests
# =============================================================================


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestBuildExecutionTrace:
    """Tests for _build_execution_trace method (Debug Experience)."""

    @allure.title("Execution trace includes duration")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_duration(self, base_suite):
        """Should calculate duration in milliseconds."""
        from datetime import datetime, UTC, timedelta

        with allure.step("Setup test data"):
            start = datetime.now(UTC) - timedelta(seconds=1.5)

        with allure.step("Execute build execution trace"):
            result = base_suite._build_execution_trace(
                start_time=start,
                rows_scanned=50000,
            )

        with allure.step("Verify: Duration calculated"):
            assert "duration_ms" in result
            assert result["duration_ms"] >= 1500
            assert result["duration_ms"] < 2000

    @allure.title("Execution trace includes started_at ISO format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_timestamp(self, base_suite):
        """Should include started_at in ISO 8601 format."""
        from datetime import datetime, UTC

        with allure.step("Setup test data"):
            start = datetime.now(UTC)

        with allure.step("Execute build execution trace"):
            result = base_suite._build_execution_trace(
                start_time=start,
                rows_scanned=50000,
            )

        with allure.step("Verify: ISO timestamp"):
            assert "started_at" in result
            assert result["started_at"].endswith("Z")
            assert "T" in result["started_at"]

    @allure.title("Execution trace includes rows scanned")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_rows(self, base_suite):
        """Should include rows_scanned."""
        from datetime import datetime, UTC

        with allure.step("Execute build execution trace"):
            result = base_suite._build_execution_trace(
                start_time=datetime.now(UTC),
                rows_scanned=1000000,
            )

        with allure.step("Verify: Rows scanned"):
            assert result["rows_scanned"] == 1000000

    @allure.title("Execution trace includes rows after filter")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_filtered_rows(self, base_suite):
        """Should include rows_after_filter when provided."""
        from datetime import datetime, UTC

        with allure.step("Execute build execution trace"):
            result = base_suite._build_execution_trace(
                start_time=datetime.now(UTC),
                rows_scanned=1000000,
                rows_after_filter=50000,
            )

        with allure.step("Verify: Filtered rows"):
            assert result["rows_after_filter"] == 50000

    @allure.title("Execution trace includes filters applied")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_filters(self, base_suite):
        """Should include filters_applied when provided."""
        from datetime import datetime, UTC

        with allure.step("Setup test data"):
            filters = {"row_filter": "status = 'active'", "partition_filter": {"date": "2026-05-12"}}

        with allure.step("Execute build execution trace"):
            result = base_suite._build_execution_trace(
                start_time=datetime.now(UTC),
                rows_scanned=1000000,
                filters_applied=filters,
            )

        with allure.step("Verify: Filters applied"):
            assert result["filters_applied"] == filters

    @allure.title("Execution trace includes table info")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_table_info(self, base_suite):
        """Should include table info when provided."""
        from datetime import datetime, UTC

        with allure.step("Setup test data"):
            table_info = {"catalog": "curated", "schema": "sales", "name": "orders"}

        with allure.step("Execute build execution trace"):
            result = base_suite._build_execution_trace(
                start_time=datetime.now(UTC),
                rows_scanned=1000000,
                table_info=table_info,
            )

        with allure.step("Verify: Table info"):
            assert result["table"] == table_info

    @allure.title("Execution trace includes secondary tables")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_secondary_tables(self, base_suite):
        """Should include secondary_tables for cross-table checks."""
        from datetime import datetime, UTC

        with allure.step("Setup test data"):
            secondary = [{"catalog": "curated", "schema": "products", "name": "catalog", "rows_read": 50000}]

        with allure.step("Execute build execution trace"):
            result = base_suite._build_execution_trace(
                start_time=datetime.now(UTC),
                rows_scanned=500000,
                secondary_tables=secondary,
            )

        with allure.step("Verify: Secondary tables"):
            assert result["secondary_tables"] == secondary

    @allure.title("Execution trace includes profile context when set")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_profile_context(self, base_suite):
        """Should include profile info when profile context is set."""
        from datetime import datetime, UTC

        try:
            with allure.step("Setup profile context"):
                base_suite.set_profile_context("prod", "analytics_prod")

            with allure.step("Execute build execution trace"):
                result = base_suite._build_execution_trace(
                    start_time=datetime.now(UTC),
                    rows_scanned=1000,
                )

            with allure.step("Verify: Profile included"):
                assert "profile" in result
                assert result["profile"]["name"] == "prod"
                assert result["profile"]["target_catalog"] == "analytics_prod"
        finally:
            # base_suite is module-scoped; reset even if an assertion fails so
            # the profile context does not leak into later tests.
            base_suite._profile_name = None
            base_suite._profile_target_catalog = None

    @allure.title("Execution trace excludes profile when not set")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_no_profile_when_not_set(self, base_suite):
        """Should NOT include profile when profile context is not set."""
        from datetime import datetime, UTC

        with allure.step("Ensure no profile context"):
            base_suite._profile_name = None
            base_suite._profile_target_catalog = None

        with allure.step("Execute build execution trace"):
            result = base_suite._build_execution_trace(
                start_time=datetime.now(UTC),
                rows_scanned=1000,
            )

        with allure.step("Verify: Profile NOT included"):
            assert "profile" not in result

    @allure.title("Execution trace profile without target_catalog")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_execution_trace_profile_without_catalog(self, base_suite):
        """Should include profile with name only when target_catalog not set."""
        from datetime import datetime, UTC

        try:
            with allure.step("Setup profile context without catalog"):
                base_suite.set_profile_context("dev", None)

            with allure.step("Execute build execution trace"):
                result = base_suite._build_execution_trace(
                    start_time=datetime.now(UTC),
                    rows_scanned=1000,
                )

            with allure.step("Verify: Profile has name only"):
                assert "profile" in result
                assert result["profile"]["name"] == "dev"
                assert "target_catalog" not in result["profile"]
        finally:
            # base_suite is module-scoped; reset even if an assertion fails so
            # the profile context does not leak into later tests.
            base_suite._profile_name = None
            base_suite._profile_target_catalog = None


# =============================================================================
# Debug Experience - Register Check with Debug Fields Tests
# =============================================================================


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.HELPER_METHODS)
class TestRegisterCheckDebugFields:
    """Tests for _register_check with debug fields (Debug Experience)."""

    @allure.title("Register check includes failure_stats on failure")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_register_check_failure_stats(self, base_suite):
        """Should include failure_stats when check fails."""
        with allure.step("Setup test data"):
            result = {"checks": []}
            failure_stats = {
                "failure_type": "duplicate_key",
                "affected_columns": ["order_id"],
                "failure_count": 47,
                "total_rows_checked": 50000,
                "failure_percentage": 0.094,
            }

        with allure.step("Execute register check"):
            base_suite._register_check(
                result,
                "idempotency",
                passed=False,
                severity="error",
                blocking=True,
                details="duplicates=47 (0.094%)",
                column="order_id",
                failure_stats=failure_stats,
            )

        with allure.step("Verify: Failure stats included"):
            check = result["checks"][0]
            assert check["failure_stats"] == failure_stats

    @allure.title("Register check includes investigation_query on failure")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_register_check_investigation_query(self, base_suite):
        """Should include investigation_query when check fails."""
        with allure.step("Setup test data"):
            result = {"checks": []}
            investigation_query = {
                "description": "Find duplicates",
                "template": "SELECT order_id, count(*) FROM t GROUP BY order_id HAVING count(*) > 1",
                "parameters": {"table": "t"},
            }

        with allure.step("Execute register check"):
            base_suite._register_check(
                result,
                "idempotency",
                passed=False,
                severity="error",
                blocking=True,
                details="duplicates=47",
                column="order_id",
                investigation_query=investigation_query,
            )

        with allure.step("Verify: Investigation query included"):
            check = result["checks"][0]
            assert check["investigation_query"] == investigation_query

    @allure.title("Register check excludes debug fields on pass")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_register_check_no_debug_fields_on_pass(self, base_suite):
        """Should NOT include failure_stats/investigation_query when check passes."""
        with allure.step("Setup test data"):
            result = {"checks": []}
            failure_stats = {"failure_type": "test", "failure_count": 0}

        with allure.step("Execute register check (passing)"):
            base_suite._register_check(
                result,
                "idempotency",
                passed=True,
                severity="warning",
                blocking=False,
                details="duplicates=0",
                column="order_id",
                failure_stats=failure_stats,
            )

        with allure.step("Verify: debug fields not included on pass"):
            check = result["checks"][0]
            assert "failure_stats" not in check
            assert "investigation_query" not in check

    @allure.title("Register check includes execution on both pass and fail")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_register_check_execution_always(self, base_suite):
        """Should include execution trace on both pass and fail."""
        from datetime import datetime, UTC

        with allure.step("Setup test data"):
            execution = {
                "duration_ms": 1234,
                "started_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "rows_scanned": 50000,
            }

        with allure.step("Execute register check (passing)"):
            result_pass = {"checks": []}
            base_suite._register_check(
                result_pass,
                "record_count",
                passed=True,
                severity="info",
                blocking=False,
                details="count=50000",
                column=None,
                execution=execution,
            )

        with allure.step("Execute register check (failing)"):
            result_fail = {"checks": []}
            base_suite._register_check(
                result_fail,
                "record_count",
                passed=False,
                severity="error",
                blocking=True,
                details="count=0",
                column=None,
                execution=execution,
            )

        with allure.step("Verify: Execution included on both"):
            assert result_pass["checks"][0]["execution"] == execution
            assert result_fail["checks"][0]["execution"] == execution
