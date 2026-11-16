# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Bronze DQ Test Suite checks.

Tests verify that Bronze-layer checks work correctly with proper configurations.
Each test uses fixtures from tests/fixtures/bronze.py.
"""

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

# Mark all tests in this module as requiring Spark
pytestmark = pytest.mark.spark


# =============================================================================
# Suite Identity Tests
# =============================================================================


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.SUITE_IDENTITY)
class TestBronzeSuiteIdentity:
    """Tests for Bronze suite identity properties."""

    @allure.title("Suite name is bronze")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_name(self, bronze_suite):
        """Suite name should be 'bronze'."""
        with allure.step("Verify: Suite name is bronze"):
            assert bronze_suite._suite_name == "bronze"

    @allure.title("Suite config key is bronze")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_suite_config_key(self, bronze_suite):
        """Suite config key should be 'bronze'."""
        with allure.step("Verify: Suite config key is bronze"):
            assert bronze_suite._suite_config_key == "bronze"


# =============================================================================
# Record Count Tests
# =============================================================================


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.RECORD_COUNT)
class TestBronzeRecordCount:
    """Tests for record_count check."""

    @allure.title("Record count passes with non-empty DataFrame")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_record_count_pass(self, bronze_suite, df_bronze_simple):
        """Record count should pass when DataFrame has rows."""
        with allure.step("Setup test data"):
            cfg = {}

        with allure.step("Execute record count check"):
            passed, details, _column = bronze_suite._check_record_count(df_bronze_simple, cfg)

        with allure.step("Verify: Record count should pass when DataFrame has rows"):
            assert passed is True
            assert "Record count OK" in details

    @allure.title("Record count fails with empty DataFrame")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_record_count_fail_empty(self, bronze_suite, df_bronze_empty):
        """Record count should fail when DataFrame is empty."""
        with allure.step("Setup test data"):
            cfg = {}

        with allure.step("Execute record count check"):
            passed, details, _column = bronze_suite._check_record_count(df_bronze_empty, cfg)

        with allure.step("Verify: Record count should fail when DataFrame is empty"):
            assert passed is False
            assert "Table empty" in details


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.FRESHNESS)
class TestBronzeFreshness:
    """Tests for freshness check."""

    @allure.title("Freshness passes with recent timestamps")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_freshness_pass(self, bronze_suite, df_timestamp):
        """Freshness should pass when timestamps are within threshold."""
        with allure.step("Setup test data"):
            cfg = {"timestamp_column": "updated_at", "max_delay_hours": 24}

        with allure.step("Execute freshness check"):
            passed, details, column = bronze_suite._check_freshness(df_timestamp, cfg)

        with allure.step("Verify: Freshness should pass when timestamps are within threshold"):
            assert passed is True
            assert column == "updated_at"
            assert "within 24h limit" in details

    @allure.title("Freshness fails with stale timestamps")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_freshness_fail_stale(self, bronze_suite, df_stale):
        """Freshness should fail when timestamps exceed threshold."""
        with allure.step("Setup test data"):
            cfg = {"timestamp_column": "updated_at", "max_delay_hours": 24}

        with allure.step("Execute freshness check"):
            passed, details, column = bronze_suite._check_freshness(df_stale, cfg)

        with allure.step("Verify: Freshness should fail when timestamps exceed threshold"):
            assert passed is False
            assert column == "updated_at"
            assert "exceeds 24h limit" in details

    @allure.title("Freshness fails when timestamp column missing from config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_freshness_missing_config(self, bronze_suite, df_timestamp):
        """Freshness should fail when required config is missing."""
        with allure.step("Setup test data"):
            cfg = {}

        with allure.step("Execute freshness check"):
            passed, details, _column = bronze_suite._check_freshness(df_timestamp, cfg)

        with allure.step("Verify: Freshness should fail when required config is missing"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Freshness fails when no timestamp found")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_freshness_no_timestamp_found(self, bronze_suite, spark):
        """Freshness should fail when max timestamp returns null."""
        with allure.step("Setup test data"):
            # DataFrame with all null timestamps - need explicit schema
            df = spark.createDataFrame([(1, None), (2, None)], "id INT, updated_at TIMESTAMP")
            cfg = {"timestamp_column": "updated_at", "max_delay_hours": 24}

        with allure.step("Execute freshness check"):
            passed, details, column = bronze_suite._check_freshness(df, cfg)

        with allure.step("Verify: Freshness fails when no timestamp found"):
            assert passed is False
            assert "No data in column" in details
            assert column == "updated_at"

    @allure.title("Freshness reads a DATE column as midnight UTC")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_freshness_date_column_today_passes(self, bronze_suite, spark):
        """A DATE max comes back as datetime.date, which has no tzinfo."""
        with allure.step("Setup test data"):
            from datetime import UTC, datetime as dt

            today = dt.now(UTC).date()
            df = spark.createDataFrame([(1, today)], "id INT, event_date DATE")
            cfg = {"timestamp_column": "event_date", "max_delay_hours": 24}

        with allure.step("Execute freshness check"):
            passed, details, column = bronze_suite._check_freshness(df, cfg)

        with allure.step("Verify: today's date is fresh"):
            assert passed is True, details
            assert column == "event_date"
            assert "within 24h limit" in details

    @allure.title("Freshness fails a stale DATE column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_freshness_date_column_stale_fails(self, bronze_suite, spark):
        with allure.step("Setup test data"):
            from datetime import UTC, datetime as dt, timedelta

            stale = dt.now(UTC).date() - timedelta(days=10)
            df = spark.createDataFrame([(1, stale)], "id INT, event_date DATE")
            cfg = {"timestamp_column": "event_date", "max_delay_hours": 48}

        with allure.step("Execute freshness check"):
            passed, details, _ = bronze_suite._check_freshness(df, cfg)

        with allure.step("Verify: stale date fails on age, not on an exception"):
            assert passed is False
            assert "exceeds 48h limit" in details


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.RECORD_COUNT_ANOMALY)
class TestBronzeRecordCountAnomaly:
    """Tests for record_count_anomaly check."""

    @pytest.fixture
    def history_with_previous_count(self, spark):
        """Factory: materialize a history table whose last record_count row holds `prev`."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS rca_test")

        def _make(prev: int) -> str:
            from datetime import UTC, datetime as dt

            spark.createDataFrame(
                [("bronze", "record_count", "cat", "sch", "tbl", dt.now(UTC), f"Record count OK: {prev} rows")],
                "test_suite STRING, check_name STRING, catalog STRING, schema STRING, "
                "table STRING, run_timestamp TIMESTAMP, details STRING",
            ).write.mode("overwrite").saveAsTable("rca_test.history")
            return "rca_test.history"

        yield _make
        spark.sql("DROP SCHEMA IF EXISTS rca_test CASCADE")

    @allure.title("Growth from a previous count of 0 is reported without a 0% deviation")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_previous_count_zero(self, bronze_suite, df_bronze_simple, history_with_previous_count):
        with allure.step("Setup: history says the previous count was 0"):
            history = history_with_previous_count(0)
            cfg = {"history_table": history, "max_deviation_percent": 50}
            table_ctx = {"catalog": "cat", "schema": "sch", "table": "tbl"}

        with allure.step("Execute record_count_anomaly"):
            passed, details, _ = bronze_suite._check_record_count_anomaly(df_bronze_simple, cfg, table_ctx)

        with allure.step("Verify: fails with a message that does not claim 0% deviation"):
            current = df_bronze_simple.count()
            assert passed is False
            assert details == f"Count went from 0 to {current} (no previous rows to compare against)"

    @allure.title("Deviation above the limit reports the percentage")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_deviation_exceeds_limit(self, bronze_suite, df_bronze_simple, history_with_previous_count):
        with allure.step("Setup: previous count far above the current one"):
            history = history_with_previous_count(1000)
            cfg = {"history_table": history, "max_deviation_percent": 50}
            table_ctx = {"catalog": "cat", "schema": "sch", "table": "tbl"}

        with allure.step("Execute record_count_anomaly"):
            passed, details, _ = bronze_suite._check_record_count_anomaly(df_bronze_simple, cfg, table_ctx)

        with allure.step("Verify: the deviation message is unchanged"):
            assert passed is False
            assert "exceeds 50% limit" in details


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.SCHEMA_DRIFT)
class TestBronzeSchemaDrift:
    """Tests for schema_drift check."""

    @allure.title("Schema drift passes when schema matches")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_schema_drift_pass(self, bronze_suite, df_schema):
        """Schema drift should pass when actual schema matches expected."""
        with allure.step("Setup test data"):
            cfg = {"expected_columns": {"id": "int", "name": "string", "count": "int"}}

        with allure.step("Execute schema drift check"):
            passed, details, _column = bronze_suite._check_schema_drift(df_schema, cfg)

        with allure.step("Verify: Schema drift should pass when actual schema matches expected"):
            assert passed is True
            assert "Schema OK" in details

    @allure.title("Schema drift fails with missing columns")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_schema_drift_fail_missing(self, bronze_suite, df_schema):
        """Schema drift should fail when expected columns are missing."""
        with allure.step("Setup test data"):
            cfg = {"expected_columns": {"id": "int", "name": "string", "extra_col": "string"}}

        with allure.step("Execute schema drift check"):
            passed, details, _column = bronze_suite._check_schema_drift(df_schema, cfg)

        with allure.step("Verify: Schema drift should fail when expected columns are missing"):
            assert passed is False
            assert "missing" in details

    @allure.title("Schema drift fails with type mismatch")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_schema_drift_fail_type_mismatch(self, bronze_suite, df_schema):
        """Schema drift should fail when column types don't match."""
        with allure.step("Setup test data"):
            cfg = {"expected_columns": {"id": "string", "name": "string"}}

        with allure.step("Execute schema drift check"):
            passed, details, _column = bronze_suite._check_schema_drift(df_schema, cfg)

        with allure.step("Verify: Schema drift should fail when column types don't match"):
            assert passed is False
            assert "mismatch" in details


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.MANDATORY_FIELDS)
class TestBronzeMandatoryFields:
    """Tests for mandatory_fields check."""

    @allure.title("Mandatory fields passes when all fields present and non-null")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_mandatory_fields_pass(self, bronze_suite, df_bronze_simple):
        """Mandatory fields should pass when all specified fields are present and non-null."""
        with allure.step("Setup test data"):
            cfg = {"columns": ["id", "code"]}

        with allure.step("Execute mandatory fields check"):
            passed, details, column = bronze_suite._check_mandatory_fields(df_bronze_simple, cfg)

        with allure.step("Verify: Mandatory fields should pass when all fields are present"):
            assert passed is True
            assert "mandatory columns present" in details
            assert column is None

    @allure.title("Mandatory fields fails with null values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_mandatory_fields_fail_nulls(self, bronze_suite, df_bronze_nulls):
        """Mandatory fields should fail when specified fields contain nulls."""
        with allure.step("Setup test data"):
            cfg = {"columns": ["name", "status"]}

        with allure.step("Execute mandatory fields check"):
            passed, details, column = bronze_suite._check_mandatory_fields(df_bronze_nulls, cfg)

        with allure.step("Verify: Mandatory fields should fail when fields contain nulls"):
            assert passed is False
            assert "null" in details.lower()
            assert column is not None

    @allure.title("Mandatory fields fails when columns config is empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_mandatory_fields_missing_config(self, bronze_suite, df_bronze_simple):
        """Mandatory fields should fail when columns config is empty."""
        with allure.step("Setup test data"):
            cfg = {"columns": []}

        with allure.step("Execute mandatory fields check"):
            passed, details, _column = bronze_suite._check_mandatory_fields(df_bronze_simple, cfg)

        with allure.step("Verify: Mandatory fields should fail when config is empty"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Mandatory fields fails when column not found in DataFrame")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_mandatory_fields_column_not_found(self, bronze_suite, df_bronze_simple):
        """Mandatory fields should fail when specified column doesn't exist."""
        with allure.step("Setup test data"):
            cfg = {"columns": ["id", "nonexistent_column"]}

        with allure.step("Execute mandatory fields check"):
            passed, details, column = bronze_suite._check_mandatory_fields(df_bronze_simple, cfg)

        with allure.step("Verify: Mandatory fields should fail when column not found"):
            assert passed is False
            assert "missing" in details.lower()
            assert column is not None

    @allure.title("Mandatory fields with threshold tolerates some nulls")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_mandatory_fields_with_threshold(self, bronze_suite, df_bronze_nulls):
        """Mandatory fields should pass when nulls are within threshold."""
        with allure.step("Setup test data"):
            # df_bronze_nulls has 3 rows, 1 null in 'name' = 33%
            cfg = {"columns": ["name"], "threshold": {"max_failure_percent": 50}}

        with allure.step("Execute mandatory fields check"):
            passed, details, _column = bronze_suite._check_mandatory_fields(df_bronze_nulls, cfg)

        with allure.step("Verify: Mandatory fields passes within threshold"):
            assert passed is True
            assert "threshold" in details


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.BINARY_NOT_NULL)
class TestBronzeBinaryNotNull:
    """Tests for binary_not_null check."""

    @allure.title("Binary not null passes with valid data")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_not_null_pass(self, bronze_suite, df_binary):
        """Binary not null should pass when no nulls in binary column."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload"}

        with allure.step("Execute binary not null check"):
            passed, details, column = bronze_suite._check_binary_not_null(df_binary, cfg)

        with allure.step("Verify: Binary not null should pass when no nulls"):
            assert passed is True
            assert "All values non-null" in details
            assert column == "payload"

    @allure.title("Binary not null fails with null values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_not_null_fail(self, bronze_suite, df_binary_with_nulls):
        """Binary not null should fail when binary column contains nulls."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload"}

        with allure.step("Execute binary not null check"):
            passed, details, column = bronze_suite._check_binary_not_null(df_binary_with_nulls, cfg)

        with allure.step("Verify: Binary not null should fail when nulls present"):
            assert passed is False
            assert "null values" in details.lower()
            assert column == "payload"

    @allure.title("Binary not null fails when column not found")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_not_null_column_not_found(self, bronze_suite, df_binary):
        """Binary not null should fail when column doesn't exist."""
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent"}

        with allure.step("Execute binary not null check"):
            passed, details, column = bronze_suite._check_binary_not_null(df_binary, cfg)

        with allure.step("Verify: Binary not null fails when column not found"):
            assert passed is False
            assert "not found" in details
            assert column == "nonexistent"


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.BINARY_NON_EMPTY)
class TestBronzeBinaryNonEmpty:
    """Tests for binary_non_empty check."""

    @allure.title("Binary non-empty passes with valid data")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_non_empty_pass(self, bronze_suite, df_binary):
        """Binary non-empty should pass when no empty values in binary column."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload"}

        with allure.step("Execute binary non-empty check"):
            passed, details, column = bronze_suite._check_binary_non_empty(df_binary, cfg)

        with allure.step("Verify: Binary non-empty should pass when no empty values"):
            assert passed is True
            assert "All values non-empty" in details
            assert column == "payload"

    @allure.title("Binary non-empty fails with empty values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_non_empty_fail(self, bronze_suite, df_binary_with_empty):
        """Binary non-empty should fail when binary column contains empty values."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload"}

        with allure.step("Execute binary non-empty check"):
            passed, details, column = bronze_suite._check_binary_non_empty(df_binary_with_empty, cfg)

        with allure.step("Verify: Binary non-empty should fail when empty values present"):
            assert passed is False
            assert "empty values" in details.lower()
            assert column == "payload"

    @allure.title("Binary non-empty fails when column not found")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_non_empty_column_not_found(self, bronze_suite, df_binary):
        """Binary non-empty should fail when column doesn't exist."""
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent"}

        with allure.step("Execute binary non-empty check"):
            passed, details, column = bronze_suite._check_binary_non_empty(df_binary, cfg)

        with allure.step("Verify: Binary non-empty fails when column not found"):
            assert passed is False
            assert "not found" in details
            assert column == "nonexistent"


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.BINARY_SIZE_GUARD)
class TestBronzeBinarySizeGuard:
    """Tests for binary_size_guard check."""

    @allure.title("Binary size guard passes within bounds")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_size_guard_pass(self, bronze_suite, df_binary_varied_sizes):
        """Binary size guard should pass when all values are within bounds."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "min_bytes": 1, "max_bytes": 100}

        with allure.step("Execute binary size guard check"):
            passed, details, column = bronze_suite._check_binary_size_guard(df_binary_varied_sizes, cfg)

        with allure.step("Verify: Binary size guard should pass within bounds"):
            assert passed is True
            assert "within" in details.lower()
            assert column == "payload"

    @allure.title("Binary size guard fails when values exceed max")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_size_guard_fail_max(self, bronze_suite, df_binary_varied_sizes):
        """Binary size guard should fail when values exceed max_bytes."""
        with allure.step("Setup test data"):
            # df_binary_varied_sizes has: 5, 16, 26 bytes
            cfg = {"column": "payload", "max_bytes": 20}

        with allure.step("Execute binary size guard check"):
            passed, details, column = bronze_suite._check_binary_size_guard(df_binary_varied_sizes, cfg)

        with allure.step("Verify: Binary size guard should fail when max exceeded"):
            assert passed is False
            assert "size violations" in details.lower()
            assert column == "payload"

    @allure.title("Binary size guard fails when values below min")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_size_guard_fail_min(self, bronze_suite, df_binary_varied_sizes):
        """Binary size guard should fail when values are below min_bytes."""
        with allure.step("Setup test data"):
            # df_binary_varied_sizes has: 5, 16, 26 bytes
            cfg = {"column": "payload", "min_bytes": 10}

        with allure.step("Execute binary size guard check"):
            passed, details, column = bronze_suite._check_binary_size_guard(df_binary_varied_sizes, cfg)

        with allure.step("Verify: Binary size guard should fail when below min"):
            assert passed is False
            assert "size violations" in details.lower()
            assert column == "payload"

    @allure.title("Binary size guard fails when column not found")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_size_guard_column_not_found(self, bronze_suite, df_binary):
        """Binary size guard should fail when column doesn't exist."""
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent", "min_bytes": 1}

        with allure.step("Execute binary size guard check"):
            passed, details, column = bronze_suite._check_binary_size_guard(df_binary, cfg)

        with allure.step("Verify: Binary size guard fails when column not found"):
            assert passed is False
            assert "not found" in details
            assert column == "nonexistent"

    @allure.title("Binary size guard fails when no size bounds defined")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_size_guard_no_bounds(self, bronze_suite, df_binary):
        """Binary size guard should fail when neither min_bytes nor max_bytes is defined."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload"}  # No min_bytes or max_bytes

        with allure.step("Execute binary size guard check"):
            passed, details, column = bronze_suite._check_binary_size_guard(df_binary, cfg)

        with allure.step("Verify: Binary size guard fails when no bounds defined"):
            assert passed is False
            assert "Missing config" in details
            assert column == "payload"


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.BINARY_TYPE)
class TestBronzeBinaryType:
    """Tests for binary_type check."""

    @allure.title("Binary type passes with BinaryType column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_type_pass(self, bronze_suite, df_binary):
        """Binary type should pass when column is BinaryType."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload"}

        with allure.step("Execute binary type check"):
            passed, details, column = bronze_suite._check_binary_type(df_binary, cfg)

        with allure.step("Verify: Binary type should pass for BinaryType column"):
            assert passed is True
            assert "is binary" in details.lower()
            assert column == "payload"

    @allure.title("Binary type fails with non-binary column")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_type_fail(self, bronze_suite, df_non_binary):
        """Binary type should fail when column is not BinaryType."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload"}

        with allure.step("Execute binary type check"):
            passed, details, column = bronze_suite._check_binary_type(df_non_binary, cfg)

        with allure.step("Verify: Binary type should fail for non-binary column"):
            assert passed is False
            assert "expected binary" in details
            assert column == "payload"

    @allure.title("Binary type fails when column not found")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_type_column_not_found(self, bronze_suite, df_binary):
        """Binary type should fail when column doesn't exist."""
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent"}

        with allure.step("Execute binary type check"):
            passed, details, column = bronze_suite._check_binary_type(df_binary, cfg)

        with allure.step("Verify: Binary type fails when column not found"):
            assert passed is False
            assert "not found" in details
            assert column == "nonexistent"


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.BINARY_DECODABLE)
class TestBronzeBinaryDecodable:
    """Tests for binary_decodable check."""

    @allure.title("Binary decodable passes with valid JSON")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_decodable_pass(self, bronze_suite, df_binary):
        """Binary decodable should pass when binary content can be decoded."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "format": "json"}

        with allure.step("Execute binary decodable check"):
            passed, details, column = bronze_suite._check_binary_decodable(df_binary, cfg)

        with allure.step("Verify: Binary decodable should pass with valid JSON"):
            assert passed is True
            assert "decodable" in details.lower()
            assert column == "payload"

    @allure.title("Binary decodable fails when column not found")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_decodable_column_not_found(self, bronze_suite, df_binary):
        """Binary decodable should fail when column doesn't exist."""
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent", "format": "json"}

        with allure.step("Execute binary decodable check"):
            passed, details, column = bronze_suite._check_binary_decodable(df_binary, cfg)

        with allure.step("Verify: Binary decodable fails when column not found"):
            assert passed is False
            assert "not found" in details
            assert column == "nonexistent"

    @allure.title("Binary decodable fails with unsupported format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_decodable_unsupported_format(self, bronze_suite, df_binary):
        """Binary decodable should fail with unsupported format."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "format": "unknown_format"}

        with allure.step("Execute binary decodable check"):
            passed, details, column = bronze_suite._check_binary_decodable(df_binary, cfg)

        with allure.step("Verify: Binary decodable fails with unsupported format"):
            assert passed is False
            assert "not supported" in details
            assert column == "payload"

    @allure.title("Binary decodable with avro format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_decodable_avro_format(self, bronze_suite, df_binary_avro):
        """Binary decodable should pass with valid avro format."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "format": "avro"}

        with allure.step("Execute binary decodable check"):
            passed, details, column = bronze_suite._check_binary_decodable(df_binary_avro, cfg)

        with allure.step("Verify: Binary decodable passes with avro format"):
            assert passed is True
            assert "avro" in details.lower()
            assert column == "payload"

    @allure.title("Binary decodable with protobuf format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_decodable_protobuf_format(self, bronze_suite, df_binary):
        """Binary decodable should pass with valid protobuf format (non-empty)."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "format": "protobuf"}

        with allure.step("Execute binary decodable check"):
            passed, details, column = bronze_suite._check_binary_decodable(df_binary, cfg)

        with allure.step("Verify: Binary decodable passes with protobuf format"):
            assert passed is True
            assert "protobuf" in details.lower()
            assert column == "payload"

    @allure.title("Binary decodable passes with valid base64-encoded JSON")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_decodable_base64_json_pass(self, bronze_suite, df_base64_json):
        """base64_json should pass when values decode from base64 to a JSON object."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "format": "base64_json"}

        with allure.step("Execute binary decodable check"):
            passed, details, column = bronze_suite._check_binary_decodable(df_base64_json, cfg)

        with allure.step("Verify: base64_json passes with valid encoded JSON"):
            assert passed is True
            assert "decodable" in details.lower()
            assert column == "payload"

    @allure.title("Binary decodable counts malformed base64 as failure (no crash under ANSI)")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_decodable_base64_json_malformed_is_counted(self, bronze_suite, df_plain_json_string):
        """Regression: malformed base64 must be counted as a failure, not raise.

        Under ANSI/Photon strict mode ``unbase64`` raises CONVERSION_INVALID_INPUT on
        non-base64 input, crashing the whole check. With ``try_to_binary`` the row is
        counted as a decode failure and evaluated against the threshold instead.
        """
        spark = bronze_suite.spark
        prev_ansi = spark.conf.get("spark.sql.ansi.enabled", "false")
        spark.conf.set("spark.sql.ansi.enabled", "true")
        try:
            with allure.step("Setup test data and enable ANSI strict mode"):
                cfg = {"column": "payload", "format": "base64_json"}

            with allure.step("Execute binary decodable check (must not raise)"):
                passed, details, column = bronze_suite._check_binary_decodable(df_plain_json_string, cfg)
        finally:
            spark.conf.set("spark.sql.ansi.enabled", prev_ansi)

        with allure.step("Verify: malformed rows counted as failures, not an error"):
            assert passed is False
            assert column == "payload"
            assert "error" not in details.lower()


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.BINARY_VERSION_HEADER)
class TestBronzeBinaryVersionHeader:
    """Tests for binary_version_header_check."""

    @allure.title("Binary version header passes with valid magic byte")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_version_header_pass(self, bronze_suite, df_binary_with_header):
        """Binary version header should pass when all rows have valid magic byte."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "magic_byte": 0, "header_size_bytes": 5}

        with allure.step("Execute binary version header check"):
            passed, details, column = bronze_suite._check_binary_version_header_check(df_binary_with_header, cfg)

        with allure.step("Verify: Binary version header should pass"):
            assert passed is True
            assert "headers valid" in details.lower()
            assert column == "payload"

    @allure.title("Binary version header fails with invalid magic byte")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_version_header_invalid_magic(self, bronze_suite, df_binary_invalid_magic):
        """Binary version header should fail when magic byte doesn't match."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "magic_byte": 0, "header_size_bytes": 5}

        with allure.step("Execute binary version header check"):
            passed, details, column = bronze_suite._check_binary_version_header_check(df_binary_invalid_magic, cfg)

        with allure.step("Verify: Binary version header should fail with invalid magic"):
            assert passed is False
            assert "invalid header" in details.lower()
            assert column == "payload"

    @allure.title("Binary version header fails with short header")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_version_header_too_short(self, bronze_suite, df_binary_short_header):
        """Binary version header should fail when header is too short."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload", "magic_byte": 0, "header_size_bytes": 5}

        with allure.step("Execute binary version header check"):
            passed, details, column = bronze_suite._check_binary_version_header_check(df_binary_short_header, cfg)

        with allure.step("Verify: Binary version header should fail with short header"):
            assert passed is False
            assert "invalid header" in details.lower()
            assert column == "payload"

    @allure.title("Binary version header fails when column not found")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_version_header_column_not_found(self, bronze_suite, df_binary_with_header):
        """Binary version header should fail when column doesn't exist."""
        with allure.step("Setup test data"):
            cfg = {"column": "nonexistent", "magic_byte": 0}

        with allure.step("Execute binary version header check"):
            passed, details, column = bronze_suite._check_binary_version_header_check(df_binary_with_header, cfg)

        with allure.step("Verify: Binary version header should fail with missing column"):
            assert passed is False
            assert "not found" in details
            assert column == "nonexistent"


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.BINARY_SCHEMA_FINGERPRINT)
class TestBronzeBinarySchemaFingerprint:
    """Tests for binary_schema_fingerprint check."""

    @allure.title("Binary schema fingerprint passes with valid fingerprint")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_schema_fingerprint_pass(self, bronze_suite, df_binary_with_fingerprint):
        """Binary schema fingerprint should pass when all rows have allowed fingerprint."""
        with allure.step("Setup test data"):
            cfg = {
                "column": "payload",
                "allowed_fingerprints": ["ABCD1234"],
                "offset": 2,
                "length_bytes": 4,
            }

        with allure.step("Execute binary schema fingerprint check"):
            passed, details, _column = bronze_suite._check_binary_schema_fingerprint(df_binary_with_fingerprint, cfg)

        with allure.step("Verify: Binary schema fingerprint should pass"):
            assert passed is True
            assert "fingerprints valid" in details.lower()

    @allure.title("Binary schema fingerprint fails with invalid fingerprint")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_schema_fingerprint_invalid(self, bronze_suite, df_binary_invalid_fingerprint):
        """Binary schema fingerprint should fail when fingerprint not in allowed list."""
        with allure.step("Setup test data"):
            cfg = {
                "column": "payload",
                "allowed_fingerprints": ["ABCD1234"],
                "offset": 2,
                "length_bytes": 4,
            }

        with allure.step("Execute binary schema fingerprint check"):
            passed, details, column = bronze_suite._check_binary_schema_fingerprint(df_binary_invalid_fingerprint, cfg)

        with allure.step("Verify: Binary schema fingerprint should fail"):
            assert passed is False
            assert "invalid fingerprint" in details.lower()
            assert column == "payload"

    @allure.title("Binary schema fingerprint with multiple allowed fingerprints")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_binary_schema_fingerprint_multiple_allowed(self, bronze_suite, df_binary_invalid_fingerprint):
        """Binary schema fingerprint should pass when fingerprint in extended allowed list."""
        with allure.step("Setup test data"):
            cfg = {
                "column": "payload",
                "allowed_fingerprints": ["ABCD1234", "DEADBEEF"],  # Both allowed
                "offset": 2,
                "length_bytes": 4,
            }

        with allure.step("Execute binary schema fingerprint check"):
            passed, details, _column = bronze_suite._check_binary_schema_fingerprint(df_binary_invalid_fingerprint, cfg)

        with allure.step("Verify: Binary schema fingerprint should pass with multiple allowed"):
            assert passed is True
            assert "fingerprints valid" in details.lower()

    @allure.title("Binary schema fingerprint fails when column missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_schema_fingerprint_missing_column(self, bronze_suite, df_binary_with_fingerprint):
        """Binary schema fingerprint should fail when column config missing."""
        with allure.step("Setup test data"):
            cfg = {"allowed_fingerprints": ["ABCD1234"]}  # Missing column

        with allure.step("Execute binary schema fingerprint check"):
            passed, details, _column = bronze_suite._check_binary_schema_fingerprint(df_binary_with_fingerprint, cfg)

        with allure.step("Verify: Binary schema fingerprint should fail with missing column"):
            assert passed is False
            assert "Missing config" in details

    @allure.title("Binary schema fingerprint fails when fingerprints missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_binary_schema_fingerprint_missing_fingerprints(self, bronze_suite, df_binary_with_fingerprint):
        """Binary schema fingerprint should fail when allowed_fingerprints config missing."""
        with allure.step("Setup test data"):
            cfg = {"column": "payload"}  # Missing allowed_fingerprints

        with allure.step("Execute binary schema fingerprint check"):
            passed, details, column = bronze_suite._check_binary_schema_fingerprint(df_binary_with_fingerprint, cfg)

        with allure.step("Verify: Binary schema fingerprint should fail with missing config"):
            assert passed is False
            assert "Missing config" in details
            assert column == "payload"


# =============================================================================
# Shared Check Mixins on Bronze (Option A composition)
# =============================================================================


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.JSON_SCHEMA)
class TestBronzeJsonSchema:
    """json_schema is composed onto Bronze via JsonSchemaCheckMixin."""

    @allure.title("Bronze json_schema passes with valid required fields")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_json_schema_pass(self, spark, bronze_suite):
        with allure.step("Setup test data"):
            df = spark.createDataFrame(
                [('{"code": "A", "value": 1}',), ('{"code": "B", "value": 2}',)],
                ["items"],
            )
            cfg = {"column": "items", "required_fields": ["code", "value"]}

        with allure.step("Execute json_schema check on Bronze"):
            passed, details, column = bronze_suite._check_json_schema(df, cfg)

        with allure.step("Verify: json_schema passes on Bronze"):
            assert passed is True
            assert column == "items"
            assert "JSON schema OK" in details


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.BUSINESS_RULES)
class TestBronzeBusinessRules:
    """business_rules is composed onto Bronze via BusinessRulesCheckMixin."""

    @allure.title("Bronze business_rules passes when all rows satisfy predicate")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_bronze_business_rules_pass(self, spark, bronze_suite):
        with allure.step("Setup test data"):
            df = spark.createDataFrame([(1, "active"), (2, "inactive")], ["id", "status"])
            cfg = {"predicate": "status IN ('active', 'inactive')", "name": "valid_status"}

        with allure.step("Execute business_rules check on Bronze"):
            passed, details, _ = bronze_suite._check_business_rules(df, cfg)

        with allure.step("Verify: business_rules passes on Bronze"):
            assert passed is True
            assert "passed" in details


@allure.feature(AllureFeature.BRONZE_SUITE)
@allure.story(AllureStory.SUITE_IDENTITY)
class TestBronzeSharedCheckSurface:
    """Option A exposes exactly json_schema + business_rules on Bronze (no leak)."""

    @allure.title("Bronze exposes json_schema and business_rules")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_bronze_exposes_two_shared_checks(self, bronze_suite):
        with allure.step("Verify: both intended shared checks are present"):
            assert hasattr(bronze_suite, "_check_json_schema")
            assert hasattr(bronze_suite, "_check_business_rules")

    @allure.title("Bronze does not leak the Silver/Gold-only shared checks")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_bronze_no_shared_check_leak(self, bronze_suite):
        with allure.step("Verify: distinct_count / compare_metric are NOT exposed on Bronze"):
            assert not hasattr(bronze_suite, "_check_distinct_count")
            assert not hasattr(bronze_suite, "_check_compare_metric")
