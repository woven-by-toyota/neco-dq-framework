# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for SharedChecksMixin.

These tests validate the shared check implementations used by Silver and Gold suites.
Tests use a stub class that provides the required base methods.
"""

from datetime import UTC, datetime, timedelta

import allure
import pytest

from neco_dq.test_suites.shared_checks import SharedChecksMixin
from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag
from tests.fixtures.spark import _SPARK_AVAILABLE

# Mark all tests in this module as requiring Spark
pytestmark = pytest.mark.spark

if not _SPARK_AVAILABLE:
    pytest.skip("PySpark not available", allow_module_level=True)


class SharedChecksStub(SharedChecksMixin):
    """Stub class providing required base methods for testing SharedChecksMixin."""

    def __init__(self, spark):
        self.spark = spark
        self._current_table_fq = "test_catalog.test_schema.test_table"
        self._current_table_info = {"catalog": "test_catalog", "schema": "test_schema", "table": "test_table"}
        self._last_failure_stats = None
        self._last_investigation_query = None
        self._last_execution = None

    def _require_cfg(self, cfg: dict, *keys) -> tuple[bool, str | None]:
        """Check that required config keys are present."""
        missing = [k for k in keys if k not in cfg or cfg[k] is None]
        if missing:
            return False, self._details_config_missing(*missing)
        return True, None

    def _build_details(self, *pairs, sample_str: str = "") -> str:
        """Build details string from key-value pairs."""
        parts = []
        for key, value in pairs:
            if value is not None:
                parts.append(f"{key}={value}")
        result = "; ".join(parts)
        if sample_str:
            result = f"{result}; sample={sample_str}" if result else f"sample={sample_str}"
        return result

    def _safe_sample(self, cfg: dict, rows, limit: int = 5) -> str:
        """Return sample string for failed rows."""
        if not cfg.get("include_sample", False):
            return ""
        return str(rows[:limit])

    def _evaluate_threshold(self, failed_count: int, total_count: int, cfg: dict) -> tuple[bool, dict]:
        """Evaluate threshold - simplified version for testing."""
        threshold = cfg.get("threshold")
        if not threshold:
            return failed_count == 0, {}
        if total_count == 0:
            return True, {"threshold_applied": True, "note": "no_data_to_validate"}
        max_pct = threshold.get("max_failure_percent")
        max_cnt = threshold.get("max_failure_count")
        failure_pct = (failed_count / total_count) * 100
        passed_pct = failure_pct <= float(max_pct) if max_pct is not None else True
        passed_cnt = failed_count <= int(max_cnt) if max_cnt is not None else True
        return passed_pct and passed_cnt, {"threshold_applied": True, "failure_pct": failure_pct}

    def _build_threshold_details(self, base_details: str, threshold_meta: dict) -> str:
        """Append threshold metadata to details - simplified version for testing."""
        if not threshold_meta:
            return base_details
        return f"{base_details}; threshold_applied=True"

    def _build_failure_stats(
        self,
        failure_type: str,
        affected_columns: list,
        failure_count: int,
        total_checked: int,
        diagnostic: dict | None = None,
        distribution: dict | None = None,
        extra: dict | None = None,
    ) -> dict:
        """Build failure statistics dict - stub version for testing."""
        stats = {
            "failure_type": failure_type,
            "affected_columns": affected_columns,
            "failure_count": failure_count,
            "total_rows_checked": total_checked,
            "failure_percentage": round(failure_count / total_checked * 100, 3) if total_checked > 0 else 0,
        }
        if diagnostic:
            stats["diagnostic"] = diagnostic
        if distribution:
            stats["distribution"] = distribution
        if extra:
            stats.update(extra)
        return stats

    def _build_execution_trace(
        self,
        start_time,
        rows_scanned: int,
        table_info: dict | None = None,
        rows_after_filter: int | None = None,
        filters_applied: list | None = None,
        secondary_tables: list | None = None,
    ) -> dict:
        """Build execution trace dict - stub version for testing."""
        from datetime import datetime, UTC

        end_time = datetime.now(UTC)
        return {
            "duration_ms": int((end_time - start_time).total_seconds() * 1000),
            "started_at": start_time.isoformat(),
            "rows_scanned": rows_scanned,
            "rows_after_filter": rows_after_filter,
            "table": table_info,
            "secondary_tables": secondary_tables,
        }

    def _fmt_pct(self, count: int, total: int) -> str:
        """Format percentage with 1 decimal place."""
        if total == 0:
            return "0.0%"
        return f"{count / total * 100:.1f}%"

    # PASS helpers
    def _details_pass_count(self, metric: str, count: int, items: str) -> str:
        if items:
            return f"{metric} OK: {count} {items}"
        return f"{metric} OK: {count}"

    def _details_pass_bounds(self, metric: str, value, bounds: str) -> str:
        return f"{metric} OK: {value} (within {bounds})"

    def _details_pass_all(self, count: int, items: str, condition: str) -> str:
        return f"All {count} {items} {condition}"

    def _details_pass_comparison(self, metric: str, observed, op: str, expected) -> str:
        return f"{metric} OK: {observed} {op} {expected}"

    def _details_pass_freshness(self, metric: str, value: str, limit: str) -> str:
        return f"{metric} OK: {value} (within {limit})"

    def _details_pass_reconciliation(self, gold, silver, diff_pct: float, tolerance: float | None = None) -> str:
        return f"Reconciliation OK: gold={gold}, silver={silver} (diff {diff_pct:.1f}%)"

    # FAIL helpers
    def _details_fail_found(self, count: int, items: str, total: int) -> str:
        pct = self._fmt_pct(count, total)
        return f"Found {count} {items} ({pct} of {total})"

    def _details_fail_missing(self, count: int, items: str, total: int | None = None) -> str:
        if total is not None and total > 0:
            pct = self._fmt_pct(count, total)
            return f"Missing {count} {items} ({pct} of {total})"
        return f"Missing {count} {items}"

    def _details_fail_exceeds(self, metric: str, value: str, limit: str) -> str:
        return f"{metric} {value} exceeds {limit}"

    def _details_fail_below(self, metric: str, value, limit) -> str:
        return f"{metric} {value} below minimum {limit}"

    def _details_fail_above(self, metric: str, value, limit) -> str:
        return f"{metric} {value} above maximum {limit}"

    def _details_fail_mismatch(self, metric: str, expected, observed) -> str:
        return f"{metric} mismatch: expected {expected}, got {observed}"

    def _details_fail_deviation(self, metric: str, pct: float, limit: float) -> str:
        return f"{metric} deviation {pct:.1f}% exceeds {limit}% limit"

    # CONFIG/ERROR helpers
    def _details_config_missing(self, *keys: str) -> str:
        return f"Missing config: {', '.join(keys)}"

    def _details_not_found(self, item: str, name: str) -> str:
        return f"{item} '{name}' not found"

    def _details_error(self, message: str) -> str:
        return f"Check error: {message}"

    def _details_no_data(self, item: str) -> str:
        return f"No data in {item}"

    def _details_insufficient(self, what: str, count: int, required: int) -> str:
        return f"Insufficient {what}: {count}/{required} required"

    # Deprecated methods (for backward compat)
    def _details_count_failures(self, count: int, what: str, total: int | None = None) -> str:
        if total is not None and total > 0:
            pct = self._fmt_pct(count, total)
            return f"Found {count} {what} ({pct} of {total})"
        return f"Found {count} {what}"

    def _details_threshold_violation(self, metric: str, value: str, limit: str, violation: str) -> str:
        if violation == "exceeds":
            return f"{metric} {value} exceeds {limit} limit"
        elif violation == "below minimum":
            return f"{metric} {value} below minimum {limit}"
        elif violation == "above maximum":
            return f"{metric} {value} above maximum {limit}"
        return f"{metric} {value} {violation} {limit}"

    def _details_expected_vs_observed(self, expected: str, observed: str, context: str = "") -> str:
        if context:
            return f"{context} mismatch: expected {expected}, got {observed}"
        return f"Mismatch: expected {expected}, got {observed}"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def checker(spark):
    """Create SharedChecksMixin stub with Spark session."""
    return SharedChecksStub(spark)


@pytest.fixture(scope="module")
def df_distinct(spark):
    """DataFrame for distinct count tests."""
    data = [
        (1, "A", "cat1"),
        (2, "B", "cat1"),
        (3, "A", "cat2"),
        (4, "C", "cat2"),
        (5, "A", "cat3"),
    ]
    return spark.createDataFrame(data, ["id", "code", "category"])


@pytest.fixture(scope="module")
def df_json_simple(spark):
    """DataFrame with simple JSON array column."""
    data = [
        (1, '[{"code": "A", "value": "10"}, {"code": "B", "value": "20"}]'),
        (2, '[{"code": "C", "value": "30"}]'),
    ]
    return spark.createDataFrame(data, ["id", "items"])


@pytest.fixture(scope="module")
def df_json_nested(spark):
    """DataFrame with nested JSON structure."""
    data = [
        (1, '[{"id": "1", "rows": [{"row_code": "R1", "row_order": "1"}]}]'),
        (2, '[{"id": "2", "rows": [{"row_code": "R2", "row_order": "2"}]}]'),
    ]
    return spark.createDataFrame(data, ["id", "data"])


@pytest.fixture(scope="module")
def df_timestamp(spark):
    """DataFrame with timestamp column for freshness tests."""
    now = datetime.now(UTC)
    data = [
        (1, now - timedelta(hours=1)),
        (2, now - timedelta(hours=2)),
        (3, now - timedelta(hours=3)),
    ]
    return spark.createDataFrame(data, ["id", "updated_at"])


@pytest.fixture(scope="module")
def df_stale(spark):
    """DataFrame with stale timestamp for freshness failure tests."""
    old = datetime.now(UTC) - timedelta(hours=48)
    data = [
        (1, old),
        (2, old - timedelta(hours=1)),
    ]
    return spark.createDataFrame(data, ["id", "updated_at"])


@pytest.fixture(scope="module")
def df_business(spark):
    """DataFrame for business rules tests."""
    data = [
        (1, 100, 50, "active"),
        (2, 200, 150, "active"),
        (3, 50, 60, "inactive"),  # price < cost - violation
        (4, 300, 200, "active"),
    ]
    return spark.createDataFrame(data, ["id", "price", "cost", "status"])


@pytest.fixture(scope="module")
def df_metrics(spark):
    """DataFrame for compare_metric tests."""
    data = [
        ("G1", 100),
        ("G1", 200),
        ("G2", 150),
        ("G2", 250),
    ]
    return spark.createDataFrame(data, ["group_id", "value"])


# =============================================================================
# Distinct Count Check Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.DISTINCT_COUNT)
class TestDistinctCountCheck:
    """Tests for _check_distinct_count."""

    @allure.title("Distinct count check passes with exact expected value")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_distinct_count_exact_match(self, checker, df_distinct):
        with allure.step("Setup test data"):
            cfg = {"column": "code", "expected": 3}  # A, B, C

        with allure.step("Execute function under test"):
            passed, details, column = checker._check_distinct_count(df_distinct, cfg)

        with allure.step("Verify: Distinct Count Exact Match"):
            assert passed is True
            assert column == "code"
            assert "Distinct count OK" in details

    @allure.title("Distinct count check fails when count doesn't match expected")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_distinct_count_exact_mismatch(self, checker, df_distinct):
        with allure.step("Setup test data"):
            cfg = {"column": "code", "expected": 5}

        with allure.step("Execute function under test"):
            passed, details, _column = checker._check_distinct_count(df_distinct, cfg)

        with allure.step("Verify: Distinct Count Exact Mismatch"):
            assert passed is False
            assert "expected 5" in details

    @allure.title("Distinct count check passes within min/max range")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_distinct_count_range_pass(self, checker, df_distinct):
        with allure.step("Setup test data"):
            cfg = {"column": "category", "min_distinct_count": 2, "max_distinct_count": 5}  # 3 distinct

        with allure.step("Execute function under test"):
            passed, details, _column = checker._check_distinct_count(df_distinct, cfg)

        with allure.step("Verify: Distinct Count Range Pass"):
            assert passed is True
            assert "Distinct count OK" in details

    @allure.title("Distinct count check fails below minimum")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_distinct_count_below_min(self, checker, df_distinct):
        with allure.step("Setup test data"):
            cfg = {"column": "category", "min_distinct_count": 10}

        with allure.step("Execute function under test"):
            passed, _, _ = checker._check_distinct_count(df_distinct, cfg)

        with allure.step("Verify: Distinct Count Below Min"):
            assert passed is False

    @allure.title("Distinct count check fails above maximum")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_distinct_count_above_max(self, checker, df_distinct):
        with allure.step("Setup test data"):
            cfg = {"column": "category", "max_distinct_count": 1}

        with allure.step("Execute function under test"):
            passed, _, _ = checker._check_distinct_count(df_distinct, cfg)

        with allure.step("Verify: Distinct Count Above Max"):
            assert passed is False

    @allure.title("Distinct count check fails for missing column config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_distinct_count_missing_column_config(self, checker, df_distinct):
        with allure.step("Setup test data"):
            cfg = {"expected": 3}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_distinct_count(df_distinct, cfg)

        with allure.step("Verify: Distinct Count Missing Column Config"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Distinct count check fails for non-existent column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_distinct_count_column_not_found(self, checker, df_distinct):
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent", "expected": 3}

        with allure.step("Execute function under test"):
            passed, details, column = checker._check_distinct_count(df_distinct, cfg)

        with allure.step("Verify: Distinct Count Column Not Found"):
            assert passed is False
            assert "not found" in details
            assert column == "nonexistent"

    @allure.title("Distinct count check fails without expected, min, or max")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_distinct_count_no_bounds(self, checker, df_distinct):
        with allure.step("Setup test data"):
            cfg = {"column": "code"}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_distinct_count(df_distinct, cfg)

        with allure.step("Verify: Distinct Count No Bounds"):
            assert passed is False
            assert "Missing config" in details


# =============================================================================
# Freshness Check Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.FRESHNESS)
class TestFreshnessCheck:
    """Tests for _check_freshness."""

    @allure.title("Freshness check passes for recent data")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_freshness_pass(self, checker, df_timestamp):
        with allure.step("Setup test data"):
            cfg = {"timestamp_column": "updated_at", "max_delay_hours": 24}

        with allure.step("Execute function under test"):
            passed, details, column = checker._check_freshness(df_timestamp, cfg)

        with allure.step("Verify: Freshness Pass"):
            assert passed is True
            assert column == "updated_at"
            assert "within" in details

    @allure.title("Freshness check fails for stale data")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_freshness_fail(self, checker, df_stale):
        with allure.step("Setup test data"):
            cfg = {"timestamp_column": "updated_at", "max_delay_hours": 24}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_freshness(df_stale, cfg)

        with allure.step("Verify: Freshness Fail"):
            assert passed is False
            assert "exceeds" in details

    @allure.title("Freshness check uses default max_delay_hours")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_freshness_default_delay(self, checker, df_timestamp):
        with allure.step("Setup test data"):
            cfg = {"timestamp_column": "updated_at"}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_freshness(df_timestamp, cfg)

        with allure.step("Verify: Freshness Default Delay"):
            assert passed is True
            assert "24h" in details

    @allure.title("Freshness check fails for missing column config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_freshness_missing_column(self, checker, df_timestamp):
        with allure.step("Setup test data"):
            cfg = {"max_delay_hours": 24}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_freshness(df_timestamp, cfg)

        with allure.step("Verify: Freshness Missing Column"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Freshness check handles empty data")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_freshness_empty_data(self, checker, spark):
        with allure.step("Setup test data"):
            df_empty = spark.createDataFrame([], "id INT, updated_at TIMESTAMP")
            cfg = {"timestamp_column": "updated_at"}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_freshness(df_empty, cfg)

        with allure.step("Verify: Freshness Empty Data"):
            assert passed is False
            assert "No data" in details


# =============================================================================
# Business Rules Check Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.BUSINESS_RULES)
class TestBusinessRulesCheck:
    """Tests for _check_business_rules."""

    @allure.title("Business rule check passes when all rows satisfy rule")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_business_rule_pass(self, checker, df_business):
        with allure.step("Setup test data"):
            cfg = {"predicate": "status IN ('active', 'inactive')", "name": "valid_status"}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_business_rules(df_business, cfg)

        with allure.step("Verify: Business Rule Pass"):
            assert passed is True
            assert "passed" in details

    @allure.title("Business rule check fails when rows violate rule")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_business_rule_fail(self, checker, df_business):
        with allure.step("Setup test data"):
            cfg = {"predicate": "price >= cost", "name": "price_above_cost"}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_business_rules(df_business, cfg)

        with allure.step("Verify: Business Rule Fail"):
            assert passed is False
            assert "violating" in details

    @allure.title("Business rule check uses default name")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_business_rule_default_name(self, checker, df_business):
        with allure.step("Setup test data"):
            cfg = {"predicate": "id > 0"}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_business_rules(df_business, cfg)

        with allure.step("Verify: Business Rule Default Name"):
            assert passed is True
            assert "business_rule" in details

    @allure.title("Business rule check fails for missing check config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_business_rule_missing_check(self, checker, df_business):
        with allure.step("Setup test data"):
            cfg = {"name": "some_rule"}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_business_rules(df_business, cfg)

        with allure.step("Verify: Business Rule Missing Check"):
            assert passed is False
            assert "Missing config" in details


# =============================================================================
# JSON Schema Check Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.JSON_SCHEMA)
class TestJsonSchemaCheck:
    """Tests for _check_json_schema."""

    @allure.title("JSON schema check passes with valid required fields")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_json_schema_required_fields_pass(self, checker, df_json_simple):
        with allure.step("Setup test data"):
            cfg = {"column": "items", "required_fields": ["code", "value"]}

        with allure.step("Execute function under test"):
            passed, details, column = checker._check_json_schema(df_json_simple, cfg)

        with allure.step("Verify: Json Schema Required Fields Pass"):
            assert passed is True
            assert column == "items"
            assert "JSON schema OK" in details

    @allure.title("JSON schema check fails for missing required field")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_missing_field(self, checker, df_json_simple):
        with allure.step("Setup test data"):
            cfg = {"column": "items", "required_fields": ["code", "nonexistent"]}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df_json_simple, cfg)

        with allure.step("Verify: Json Schema Missing Field"):
            assert passed is False
            assert "missing fields" in details

    @allure.title("JSON schema check validates allowed values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_json_schema_allowed_values_pass(self, checker, df_json_simple):
        with allure.step("Setup test data"):
            cfg = {"column": "items", "allowed_values": {"code": ["A", "B", "C"]}}

        with allure.step("Execute function under test"):
            passed, _details, _ = checker._check_json_schema(df_json_simple, cfg)

        with allure.step("Verify: Json Schema Allowed Values Pass"):
            assert passed is True

    @allure.title("JSON schema check fails for disallowed values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_invalid_values(self, checker, df_json_simple):
        with allure.step("Setup test data"):
            cfg = {"column": "items", "allowed_values": {"code": ["X", "Y"]}}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df_json_simple, cfg)

        with allure.step("Verify: Json Schema Invalid Values"):
            assert passed is False
            assert "invalid values" in details

    @allure.title("JSON schema check validates field types")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_json_schema_field_types(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, '[{"count": "42", "active": "true"}]')]
            df = spark.createDataFrame(data, ["id", "items"])
            cfg = {"column": "items", "field_types": {"count": "int", "active": "boolean"}}

        with allure.step("Execute function under test"):
            passed, _details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Field Types"):
            assert passed is True

    @allure.title("JSON schema check fails for wrong field type")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_type_mismatch(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, '[{"count": "not_a_number"}]')]
            df = spark.createDataFrame(data, ["id", "items"])
            cfg = {"column": "items", "field_types": {"count": "int"}}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Type Mismatch"):
            assert passed is False
            assert "type mismatches" in details

    @allure.title("JSON schema check handles nested paths")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_json_schema_nested_paths(self, checker, df_json_nested):
        with allure.step("Setup test data"):
            cfg = {"column": "data", "required_fields": ["rows.row_code"]}

        with allure.step("Execute function under test"):
            passed, _details, _ = checker._check_json_schema(df_json_nested, cfg)

        with allure.step("Verify: Json Schema Nested Paths"):
            assert passed is True

    @allure.title("JSON schema check fails for missing column config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_json_schema_missing_column(self, checker, df_json_simple):
        with allure.step("Setup test data"):
            cfg = {"required_fields": ["code"]}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df_json_simple, cfg)

        with allure.step("Verify: Json Schema Missing Column"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("JSON schema check fails for non-existent column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_column_not_found(self, checker, df_json_simple):
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent", "required_fields": ["code"]}

        with allure.step("Execute function under test"):
            passed, details, _column = checker._check_json_schema(df_json_simple, cfg)

        with allure.step("Verify: Json Schema Column Not Found"):
            assert passed is False
            assert "not found" in details

    @allure.title("JSON schema check fails without validation rules")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_no_rules(self, checker, df_json_simple):
        with allure.step("Setup test data"):
            cfg = {"column": "items"}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df_json_simple, cfg)

        with allure.step("Verify: Json Schema No Rules"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("JSON schema check handles empty data")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_json_schema_empty_data(self, checker, spark):
        with allure.step("Setup test data"):
            df_empty = spark.createDataFrame([], "id INT, items STRING")
            cfg = {"column": "items", "required_fields": ["code"]}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df_empty, cfg)

        with allure.step("Verify: Json Schema Empty Data"):
            assert passed is True
            assert "No data to validate" in details

    @allure.title("JSON schema check validates a decoded STRUCT column")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_json_schema_struct_column_pass(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, {"code": "A", "value": "10"}), (2, {"code": "B", "value": "20"})]
            df = spark.createDataFrame(data, "id INT, items STRUCT<code: STRING, value: STRING>")
            cfg = {"column": "items", "required_fields": ["code", "value"]}

        with allure.step("Execute function under test"):
            passed, details, column = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Struct Column Pass"):
            assert passed is True
            assert column == "items"
            assert "JSON schema OK" in details

    @allure.title("JSON schema check fails for field absent from STRUCT schema")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_struct_column_missing_field(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, {"code": "A"}), (2, {"code": "B"})]
            df = spark.createDataFrame(data, "id INT, items STRUCT<code: STRING>")
            cfg = {"column": "items", "required_fields": ["code", "name"]}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Struct Missing Field"):
            assert passed is False
            assert "missing fields" in details

    @allure.title("JSON schema check fails for null required field in STRUCT")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_struct_null_required(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, {"code": "A"}), (2, {"code": None})]
            df = spark.createDataFrame(data, "id INT, items STRUCT<code: STRING>")
            cfg = {"column": "items", "required_fields": ["code"]}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Struct Null Required"):
            assert passed is False
            assert "missing fields" in details

    @allure.title("JSON schema check validates a decoded ARRAY<STRUCT> column")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_json_schema_array_struct_pass(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, [{"code": "A"}, {"code": "B"}])]
            df = spark.createDataFrame(data, "id INT, items ARRAY<STRUCT<code: STRING>>")
            cfg = {"column": "items", "required_fields": ["code"], "array_element": True}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Array Struct Pass"):
            assert passed is True
            assert "JSON schema OK" in details

    @allure.title("JSON schema validates a BINARY column holding UTF-8 JSON")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_json_schema_binary_column_pass(self, checker, spark):
        """BINARY payloads (raw Kafka value bytes) are decoded to STRING and validated."""
        with allure.step("Setup test data"):
            payload = b'{"wovenId": "w1", "sessionId": "s1", "startTimeMs": "123"}'
            data = [(1, bytearray(payload)), (2, bytearray(payload))]
            df = spark.createDataFrame(data, "id INT, value BINARY")
            cfg = {
                "column": "value",
                "array_element": False,
                "required_fields": ["wovenId", "sessionId", "startTimeMs"],
            }

        with allure.step("Execute function under test"):
            passed, details, column = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Binary Column Pass"):
            assert passed is True
            assert column == "value"
            assert "JSON schema OK" in details

    @allure.title("JSON schema flags missing field in a BINARY JSON column")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_binary_column_missing_field(self, checker, spark):
        with allure.step("Setup test data"):
            payload = b'{"wovenId": "w1"}'
            data = [(1, bytearray(payload))]
            df = spark.createDataFrame(data, "id INT, value BINARY")
            cfg = {
                "column": "value",
                "array_element": False,
                "required_fields": ["wovenId", "sessionId"],
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Binary Column Missing Field"):
            assert passed is False
            assert "missing fields" in details

    @allure.title("JSON schema check detects type mismatch in STRUCT field")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_struct_type_mismatch(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, {"count": "not_a_number"})]
            df = spark.createDataFrame(data, "id INT, items STRUCT<count: STRING>")
            cfg = {"column": "items", "field_types": {"count": "int"}}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Struct Type Mismatch"):
            assert passed is False
            assert "type mismatches" in details

    @allure.title("JSON schema check validates typed int STRUCT field")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_json_schema_struct_typed_int_pass(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, {"count": 42})]
            df = spark.createDataFrame(data, "id INT, items STRUCT<count: INT>")
            cfg = {"column": "items", "field_types": {"count": "int"}}

        with allure.step("Execute function under test"):
            passed, _details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Struct Typed Int Pass"):
            assert passed is True

    @allure.title("JSON schema check skips nested paths on STRUCT column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_json_schema_struct_nested_skipped(self, checker, spark):
        with allure.step("Setup test data"):
            data = [(1, {"code": "A"})]
            df = spark.createDataFrame(data, "id INT, items STRUCT<code: STRING>")
            cfg = {"column": "items", "required_fields": ["code", "rows.row_code"]}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Json Schema Struct Nested Skipped"):
            assert passed is True
            assert "nested paths skipped" in details


# =============================================================================
# Compare Metric Check Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.COMPARE_METRIC)
class TestCompareMetricCheck:
    """Tests for _check_compare_metric."""

    @allure.title("Compare metric check passes for matching count")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_count_pass(self, checker, df_metrics):
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "count"},
                "value": 4,  # Static expected value
                "operator": "==",
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Count Pass"):
            assert passed is True
            assert "OK" in details

    @allure.title("Compare metric check fails for mismatched count")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_compare_metric_count_fail(self, checker, df_metrics):
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "count"},
                "value": 10,
                "operator": "==",
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Count Fail"):
            assert passed is False
            assert "expected" in details

    @allure.title("Compare metric check with sum aggregation")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_sum(self, checker, df_metrics):
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "aggregate", "function": "sum", "column": "value"},
                "value": 700,  # 100+200+150+250
                "operator": "==",
            }

        with allure.step("Execute function under test"):
            passed, _details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Sum"):
            assert passed is True

    @allure.title("Compare metric check with greater than operator")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_gt_operator(self, checker, df_metrics):
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "count"},
                "value": 3,
                "operator": ">",
            }

        with allure.step("Execute function under test"):
            passed, _, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Gt Operator"):
            assert passed is True

    @allure.title("Compare metric check with tolerance")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_with_tolerance(self, checker, df_metrics):
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "aggregate", "function": "avg", "column": "value"},
                "value": 175,
                "operator": "~",
                "tolerance": 1,
            }

        with allure.step("Execute function under test"):
            passed, _, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric With Tolerance"):
            assert passed is True

    @allure.title("Compare metric check with grouped results")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_grouped(self, checker, df_metrics):
        with allure.step("Setup test data"):
            # Note: count type doesn't support group_by, use aggregate with count function
            cfg = {
                "metric": {"type": "aggregate", "function": "sum", "column": "value", "group_by": ["group_id"]},
                "value": 300,  # G1: 100+200=300, G2: 150+250=400 - using 300 to test group comparison
                "operator": ">=",  # At least 300 per group
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Grouped"):
            assert passed is True
            assert "2/2 groups" in details

    @allure.title("Compare metric check with grouped results and SQL expected")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_grouped_sql_expected(self, checker, df_metrics):
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "aggregate", "function": "sum", "column": "value", "group_by": ["group_id"]},
                "expected_from": {
                    "type": "sql_grouped",
                    "query": "SELECT 'G1' as group_id, 300 as expected UNION ALL SELECT 'G2', 400",
                    "group_by": ["group_id"],
                    "value_column": "expected",
                },
                "operator": "==",
            }

        with allure.step("Execute function under test"):
            passed, _details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Grouped Sql Expected"):
            assert passed is True

    @allure.title("Compare metric check handles failed expected resolution")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_compare_metric_expected_fail(self, checker, df_metrics):
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "count"},
                "expected_from": {"type": "sql", "query": "INVALID SQL SYNTAX"},
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Expected Fail"):
            assert passed is False
            assert "resolution" in details

    @allure.title("Compare metric check handles failed metric resolution")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_compare_metric_metric_fail(self, checker, df_metrics):
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "invalid_type"},
                "value": 0,
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Metric Fail"):
            assert passed is False
            assert "resolution" in details

    @allure.title("Compare metric check with include_details option")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_include_details(self, checker, df_metrics):
        with allure.step("Setup test data"):
            # Note: count type doesn't support group_by, use aggregate
            cfg = {
                "metric": {"type": "aggregate", "function": "sum", "column": "value", "group_by": ["group_id"]},
                "value": 300,
                "operator": ">=",
                "include_details": True,
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Compare Metric Include Details"):
            assert passed is True
            assert "groups" in details


# =============================================================================
# Helper Methods Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.HELPER_METHODS)
class TestHelperMethods:
    """Tests for helper methods in SharedChecksMixin."""

    @allure.title("Separate nested paths correctly splits fields")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_separate_nested_paths(self, checker):
        with allure.step("Setup test data"):
            required = ["code", "rows.row_code", "rows.row_order"]
            types = {"value": "int", "rows.row_order": "int"}
            allowed = {"status": ["A", "B"], "rows.status": ["active"]}

        with allure.step("Execute function under test"):
            top_level, nested = checker._separate_nested_paths(required, types, allowed)

        with allure.step("Verify: Separate Nested Paths"):
            assert "code" in top_level["required"]
            assert "value" in top_level["types"]
            assert "status" in top_level["allowed"]

            assert "rows" in nested
            assert "row_code" in nested["rows"]["required"]
            assert "row_order" in nested["rows"]["types"]
            assert "status" in nested["rows"]["allowed"]

    @allure.title("Map JSON type handles all supported types")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_map_json_type(self, checker):
        with allure.step("Verify: Map Json Type"):
            assert checker._map_json_type("string") == "string"
            assert checker._map_json_type("str") == "string"
            assert checker._map_json_type("int") == "int"
            assert checker._map_json_type("integer") == "int"
            assert checker._map_json_type("long") == "long"
            assert checker._map_json_type("bigint") == "long"
            assert checker._map_json_type("double") == "double"
            assert checker._map_json_type("float") == "float"
            assert checker._map_json_type("boolean") == "boolean"
            assert checker._map_json_type("bool") == "boolean"

            assert checker._map_json_type("unknown") == "string"

    @allure.title("Build JSON schema string creates correct schema")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_build_json_schema_str(self, checker):
        with allure.step("Setup test data"):
            schema = checker._build_json_schema_str(["code"], {"value": "int"}, {}, is_array=True)

        with allure.step("Verify: Build Json Schema Str"):
            assert schema.startswith("array<")
            assert "struct<" in schema

            schema = checker._build_json_schema_str(["code"], {}, {}, is_array=False)

            assert schema.startswith("struct<")
            assert not schema.startswith("array<")


# =============================================================================
# JSON Schema Parse Failure Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.JSON_SCHEMA)
class TestJsonSchemaParseFailures:
    """Tests for JSON schema check parse failure handling."""

    @allure.title("JSON schema check fails when parse failure exceeds threshold")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_json_schema_parse_failure_exceeds_threshold(self, checker, spark):
        """Test that JSON parse failures exceeding threshold cause check to fail."""
        with allure.step("Setup test data"):
            # Invalid JSON that will fail to parse
            data = [
                (1, "invalid_json"),
                (2, "also_not_json"),
                (3, '[{"code": "A"}]'),  # Only one valid
            ]
            df = spark.createDataFrame(data, ["id", "items"])
            cfg = {"column": "items", "required_fields": ["code"], "max_parse_failure_percent": 10}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_json_schema(df, cfg)

        with allure.step("Verify: Parse failure exceeds threshold"):
            assert passed is False
            assert "parse failed" in details


# =============================================================================
# Nested Path Processing Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.HELPER_METHODS)
class TestNestedPathProcessing:
    """Tests for nested path processing in _separate_nested_paths."""

    @allure.title("Separate nested paths creates new parent for types")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_separate_nested_paths_creates_parent_for_types(self, checker):
        """Test that nested parent is created when first encountered in types."""
        with allure.step("Setup test data"):
            # Only types have nested path, no required fields with this parent
            required = []
            types = {"rows.value": "int"}
            allowed = {}

        with allure.step("Execute function under test"):
            _top_level, nested = checker._separate_nested_paths(required, types, allowed)

        with allure.step("Verify: Nested parent created for types"):
            assert "rows" in nested
            assert "value" in nested["rows"]["types"]
            assert nested["rows"]["types"]["value"] == "int"

    @allure.title("Separate nested paths creates new parent for allowed values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_separate_nested_paths_creates_parent_for_allowed(self, checker):
        """Test that nested parent is created when first encountered in allowed."""
        with allure.step("Setup test data"):
            # Only allowed has nested path
            required = []
            types = {}
            allowed = {"cells.status": ["active", "inactive"]}

        with allure.step("Execute function under test"):
            _top_level, nested = checker._separate_nested_paths(required, types, allowed)

        with allure.step("Verify: Nested parent created for allowed"):
            assert "cells" in nested
            assert "status" in nested["cells"]["allowed"]
            assert nested["cells"]["allowed"]["status"] == ["active", "inactive"]


# =============================================================================
# Nested JSON Validation Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.JSON_SCHEMA)
class TestNestedJsonValidation:
    """Tests for _validate_nested_json_fields edge cases."""

    @allure.title("Nested validation returns early for empty config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_nested_validation_empty_config(self, checker, spark):
        """Test that empty nested config returns early."""
        with allure.step("Setup test data"):
            data = [(1, '[{"rows": [{"code": "A"}]}]')]
            df = spark.createDataFrame(data, ["id", "data"])
            nested_cfg = {"required": [], "types": {}, "allowed": {}}

        with allure.step("Execute function under test"):
            result = checker._validate_nested_json_fields(df, "data", "rows", nested_cfg, True)

        with allure.step("Verify: Returns empty result for empty config"):
            assert result["missing"] == {}
            assert result["type_mismatches"] == {}
            assert result["invalid_values"] == {}
            assert result["total_elements"] == 0

    @allure.title("Nested validation handles non-array parent")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_nested_validation_non_array_parent(self, checker, spark):
        """Test nested validation when parent JSON is not an array."""
        with allure.step("Setup test data"):
            # Object at top level, not array
            data = [(1, '{"items": [{"code": "A"}, {"code": "B"}]}')]
            df = spark.createDataFrame(data, ["id", "data"])
            nested_cfg = {"required": ["code"], "types": {}, "allowed": {}}

        with allure.step("Execute function under test"):
            result = checker._validate_nested_json_fields(df, "data", "items", nested_cfg, False)

        with allure.step("Verify: Handles non-array parent"):
            assert result["total_elements"] == 2
            assert result["missing"] == {}

    @allure.title("Nested validation returns early when no elements")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_nested_validation_no_elements(self, checker, spark):
        """Test nested validation returns early when no elements exist."""
        with allure.step("Setup test data"):
            # Empty nested array
            data = [(1, '[{"rows": []}]')]
            df = spark.createDataFrame(data, ["id", "data"])
            nested_cfg = {"required": ["code"], "types": {}, "allowed": {}}

        with allure.step("Execute function under test"):
            result = checker._validate_nested_json_fields(df, "data", "rows", nested_cfg, True)

        with allure.step("Verify: Returns early with zero elements"):
            assert result["total_elements"] == 0
            assert result["missing"] == {}

    @allure.title("Nested validation detects missing required fields")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_nested_validation_missing_required(self, checker, spark):
        """Test nested validation detects missing required fields."""
        with allure.step("Setup test data"):
            data = [(1, '[{"rows": [{"code": "A"}, {"other": "B"}]}]')]
            df = spark.createDataFrame(data, ["id", "data"])
            nested_cfg = {"required": ["code"], "types": {}, "allowed": {}}

        with allure.step("Execute function under test"):
            result = checker._validate_nested_json_fields(df, "data", "rows", nested_cfg, True)

        with allure.step("Verify: Missing field detected"):
            assert "rows.code" in result["missing"]
            assert result["missing"]["rows.code"] == 1

    @allure.title("Nested validation detects type mismatches")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_nested_validation_type_mismatch(self, checker, spark):
        """Test nested validation detects type mismatches."""
        with allure.step("Setup test data"):
            data = [(1, '[{"rows": [{"value": "123"}, {"value": "not_int"}]}]')]
            df = spark.createDataFrame(data, ["id", "data"])
            nested_cfg = {"required": [], "types": {"value": "int"}, "allowed": {}}

        with allure.step("Execute function under test"):
            result = checker._validate_nested_json_fields(df, "data", "rows", nested_cfg, True)

        with allure.step("Verify: Type mismatch detected"):
            assert "rows.value" in result["type_mismatches"]
            assert result["type_mismatches"]["rows.value"] == 1

    @allure.title("Nested validation detects invalid values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_nested_validation_invalid_values(self, checker, spark):
        """Test nested validation detects values not in allowed list."""
        with allure.step("Setup test data"):
            data = [(1, '[{"rows": [{"status": "active"}, {"status": "unknown"}]}]')]
            df = spark.createDataFrame(data, ["id", "data"])
            nested_cfg = {"required": [], "types": {}, "allowed": {"status": ["active", "inactive"]}}

        with allure.step("Execute function under test"):
            result = checker._validate_nested_json_fields(df, "data", "rows", nested_cfg, True)

        with allure.step("Verify: Invalid value detected"):
            assert "rows.status" in result["invalid_values"]
            assert result["invalid_values"]["rows.status"] == 1


# =============================================================================
# Type Mismatch Counting Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.HELPER_METHODS)
class TestTypeMismatchCounting:
    """Tests for _count_type_mismatches with different types."""

    @allure.title("Count type mismatches for double type")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_count_type_mismatches_double(self, checker, spark):
        """Test type mismatch counting for double/float type."""
        with allure.step("Setup test data"):
            from pyspark.sql import functions as func

            # Valid: 3.14, -2.5, 1e10 | Invalid: "abc"
            data = [
                (1, "3.14"),
                (2, "-2.5"),
                (3, "1e10"),
                (4, "abc"),
            ]
            df = spark.createDataFrame(data, ["id", "value"])
            df_elements = df.select(func.struct(func.col("value")).alias("_elem"))
            field_col = func.col("_elem").getField("value")

        with allure.step("Execute function under test"):
            mismatches = checker._count_type_mismatches(df_elements, field_col, "double")

        with allure.step("Verify: One mismatch for non-numeric value"):
            assert mismatches == 1

    @allure.title("Count type mismatches for boolean type")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_count_type_mismatches_boolean(self, checker, spark):
        """Test type mismatch counting for boolean type."""
        with allure.step("Setup test data"):
            from pyspark.sql import functions as func

            # Valid: true, false | Invalid: "yes", "1"
            data = [
                (1, "true"),
                (2, "false"),
                (3, "yes"),
                (4, "1"),
            ]
            df = spark.createDataFrame(data, ["id", "value"])
            df_elements = df.select(func.struct(func.col("value")).alias("_elem"))
            field_col = func.col("_elem").getField("value")

        with allure.step("Execute function under test"):
            mismatches = checker._count_type_mismatches(df_elements, field_col, "boolean")

        with allure.step("Verify: Two mismatches for non-boolean values"):
            assert mismatches == 2

    @allure.title("Count type mismatches returns zero for string type")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_count_type_mismatches_string(self, checker, spark):
        """Test type mismatch counting returns 0 for string type (no validation)."""
        with allure.step("Setup test data"):
            from pyspark.sql import functions as func

            data = [(1, "any"), (2, "123"), (3, "true")]
            df = spark.createDataFrame(data, ["id", "value"])
            df_elements = df.select(func.struct(func.col("value")).alias("_elem"))
            field_col = func.col("_elem").getField("value")

        with allure.step("Execute function under test"):
            mismatches = checker._count_type_mismatches(df_elements, field_col, "string")

        with allure.step("Verify: Zero mismatches for string type"):
            assert mismatches == 0

    @allure.title("Count type mismatches returns zero for unknown type")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_count_type_mismatches_unknown(self, checker, spark):
        """Test type mismatch counting returns 0 for unknown type."""
        with allure.step("Setup test data"):
            from pyspark.sql import functions as func

            data = [(1, "any")]
            df = spark.createDataFrame(data, ["id", "value"])
            df_elements = df.select(func.struct(func.col("value")).alias("_elem"))
            field_col = func.col("_elem").getField("value")

        with allure.step("Execute function under test"):
            mismatches = checker._count_type_mismatches(df_elements, field_col, "unknown_type")

        with allure.step("Verify: Zero mismatches for unknown type"):
            assert mismatches == 0


# =============================================================================
# Freshness Timezone Handling Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.FRESHNESS)
class TestFreshnessTimezone:
    """Tests for freshness check timezone handling."""

    @allure.title("Freshness check handles naive datetime")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_freshness_naive_datetime(self, checker, spark):
        """Test freshness check handles naive datetime without tzinfo."""
        with allure.step("Setup test data"):
            # Create DataFrame with naive datetime (no timezone)
            from datetime import datetime as dt

            now_naive = dt.now()  # noqa: DTZ005 - intentionally naive for testing
            data = [(1, now_naive)]
            df = spark.createDataFrame(data, ["id", "updated_at"])
            cfg = {"timestamp_column": "updated_at", "max_delay_hours": 24}

        with allure.step("Execute function under test"):
            passed, details, column = checker._check_freshness(df, cfg)

        with allure.step("Verify: Handles naive datetime"):
            assert passed is True
            assert column == "updated_at"
            assert "within" in details


# =============================================================================
# Compare Metric Edge Cases Tests
# =============================================================================


@allure.feature(AllureFeature.SHARED_CHECKS)
@allure.story(AllureStory.COMPARE_METRIC)
class TestCompareMetricEdgeCases:
    """Tests for compare_metric edge cases."""

    @allure.title("Compare metric fails for missing metric config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_compare_metric_missing_metric_config(self, checker, df_metrics):
        """Test compare_metric fails when neither metric nor observed is provided."""
        with allure.step("Setup test data"):
            cfg = {"value": 4, "operator": "=="}

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Fails with missing config error"):
            assert passed is False
            assert "Missing config: metric or observed" in details

    @allure.title("Compare metric with include_details shows name")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_include_details_with_name(self, checker, df_metrics):
        """Test compare_metric includes name in details when include_details is True."""
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "aggregate", "function": "sum", "column": "value", "group_by": ["group_id"]},
                "value": 300,
                "operator": ">=",
                "include_details": True,
                "name": "my_metric_check",
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Name included in details"):
            assert passed is True
            assert "my_metric_check" in details

    @allure.title("Compare metric handles single dict observed value")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_compare_metric_single_dict_observed(self, checker, spark):
        """Test compare_metric handles observed value as single-key dict."""
        with allure.step("Setup test data"):
            # Create simple data for count
            data = [(1,), (2,), (3,), (4,)]
            df = spark.createDataFrame(data, ["id"])
            cfg = {
                "metric": {"type": "count"},
                "value": 4,
                "operator": "==",
            }

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df, cfg)

        with allure.step("Verify: Handles count metric correctly"):
            assert passed is True
            assert "OK" in details

    @allure.title("Compare metric handles exception during comparison")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_compare_metric_exception_handling(self, checker, df_metrics, monkeypatch):
        """Test compare_metric handles exceptions during comparison."""
        with allure.step("Setup test data"):
            cfg = {
                "metric": {"type": "count"},
                "value": 4,
                "operator": "==",
            }

            # Monkey-patch compare to raise exception
            def raise_error(*_args, **_kwargs):
                raise ValueError("Test error")

            from neco_dq.test_suites.shared_checks import compare_metric_check

            monkeypatch.setattr(compare_metric_check, "compare", raise_error)

        with allure.step("Execute function under test"):
            passed, details, _ = checker._check_compare_metric(df_metrics, cfg)

        with allure.step("Verify: Exception handled gracefully"):
            assert passed is False
            assert "error" in details.lower()
            assert "Test error" in details
