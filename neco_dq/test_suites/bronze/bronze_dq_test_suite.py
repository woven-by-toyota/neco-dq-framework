# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import re
from datetime import datetime, UTC
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, expr, upper
from pyspark.sql.types import BinaryType

from neco_dq.test_suites.base_test_suite import DQTestSuite
from neco_dq.test_suites.shared_checks import BusinessRulesCheckMixin, JsonSchemaCheckMixin
from neco_dq.test_suites.shared_checks.freshness_check import as_utc_datetime
from neco_dq.core.io.result_writer import DQResultWriter


class SparkBronzeDQTestSuite(DQTestSuite, JsonSchemaCheckMixin, BusinessRulesCheckMixin):
    """
    Bronze Data Quality Test Suite
    - check-centric
    - 1 row per check
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        self.spark = spark
        self.cfg = config
        self.writer = DQResultWriter(spark)

    # ------------------------------------------------------------------
    # Suite identity
    # ------------------------------------------------------------------

    @property
    def _suite_name(self) -> str:
        return "bronze"

    @property
    def _suite_config_key(self) -> str:
        return "bronze"

    def _check_record_count(self, df, _cfg: dict) -> tuple[bool, str, None]:
        """
        Verify table has at least one record.

        Config: None required.
        """
        start_time = datetime.now(UTC)
        cnt = df.count()
        passed = cnt > 0
        details = self._details_pass_count("Record count", cnt, "rows") if passed else "Table empty: 0 rows"

        investigation_query = None
        if not passed:
            investigation_query = self._build_investigation_query(
                check_type="record_count",
                table_fq=self._current_table_fq,
                affected_columns=[],
                cfg={},
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=cnt,
            table_info=self._current_table_info,
        )

        self._last_failure_stats = None
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, None

    def _check_record_count_anomaly(self, df, cfg: dict, table_ctx: dict) -> tuple[bool, str, None]:
        """
        Detect anomalies in record count compared to historical values.

        Config:
            history_table: Table with historical DQ results
            max_deviation_percent: Max allowed deviation from previous count (default: 40)
                                   (legacy alias: ``allowed_deviation_percent``)
        """
        start_time = datetime.now(UTC)
        history_table = cfg.get("history_table")
        # Canonical key is ``max_deviation_percent``; fall back to the legacy
        # ``allowed_deviation_percent`` alias for configs that bypass alias resolution.
        allowed_dev = cfg.get("max_deviation_percent", cfg.get("allowed_deviation_percent", 40))

        current = df.count()

        if not history_table:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=current,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return True, "No history configured", None

        try:
            hist = self.spark.table(history_table)

            last = (
                hist.filter(
                    (col("test_suite") == "bronze")
                    & (col("check_name") == "record_count")
                    & (col("catalog") == table_ctx["catalog"])
                    & (col("schema") == table_ctx["schema"])
                    & (col("table") == table_ctx["table"])
                )
                .orderBy(col("run_timestamp").desc())
                .select("details")
                .limit(1)
                .collect()
            )

            if not last:
                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=current,
                    table_info=self._current_table_info,
                    secondary_tables=[{"table": history_table}],
                )
                self._last_failure_stats = None
                self._last_investigation_query = None
                self._last_execution = execution
                return True, "No previous value in history", None

            # Extract count from details - supports both formats:
            # - Old: "record_count=356"
            # - New: "Record count OK: 356 rows"
            details_str = last[0]["details"]
            match = re.search(r"(\d+)", details_str)
            if not match:
                raise ValueError(f"Cannot parse count from details: {details_str}")
            prev = int(match.group(1))

            delta = abs(current - prev)
            allowed = prev * allowed_dev / 100
            deviation_pct = (delta / prev * 100) if prev > 0 else 0

            passed = delta <= allowed

            failure_stats = None
            investigation_query = None
            if not passed:
                failure_stats = self._build_failure_stats(
                    failure_type="count_anomaly",
                    affected_columns=[],
                    failure_count=1,
                    total_checked=1,
                    diagnostic={
                        "current_count": current,
                        "previous_count": prev,
                        "deviation_count": int(delta),
                        "deviation_pct": round(deviation_pct, 2),
                        "allowed_deviation_pct": allowed_dev,
                    },
                )

                investigation_query = self._build_investigation_query(
                    check_type="record_count",
                    table_fq=self._current_table_fq,
                    affected_columns=[],
                    cfg={},
                    where_clause=self._current_where_clause,
                )

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=current,
                table_info=self._current_table_info,
                secondary_tables=[{"table": history_table}],
            )

            if passed:
                details = f"Record count OK: {current} (deviation {deviation_pct:.1f}% within {allowed_dev}% limit)"
            else:
                details = self._details_fail_deviation("Count", deviation_pct, allowed_dev)

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, None

        except Exception as e:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=current,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_error(f"history lookup: {e}"), None

    def _check_freshness(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify data freshness based on a timestamp column.

        Config:
            timestamp_column: Column containing timestamps to check (required)
            max_delay_hours: Maximum allowed delay in hours (default: 24)
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "timestamp_column")
        if not ok:
            return False, err, None

        ts_col = cfg["timestamp_column"]
        max_hours = cfg.get("max_delay_hours", 24)
        total_rows = df.count()

        latest = df.selectExpr(f"max({ts_col}) as v").first()["v"]
        if not latest:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_no_data(f"column '{ts_col}'"), ts_col

        # TIMESTAMP max is naive (UTC by contract), DATE max is a date: same normalization
        latest = as_utc_datetime(latest)

        now = datetime.now(UTC)
        delta_hours = (now - latest).total_seconds() / 3600

        passed = delta_hours <= max_hours

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="stale_data",
                affected_columns=[ts_col],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "latest_timestamp": latest.isoformat(),
                    "age_hours": round(delta_hours, 2),
                    "max_allowed_hours": max_hours,
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="freshness",
                table_fq=self._current_table_fq,
                affected_columns=[ts_col],
                cfg={"timestamp_column": ts_col},
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = self._details_pass_freshness("Data", f"{delta_hours:.1f}h old", f"{max_hours}h limit")
        else:
            details = self._details_fail_exceeds("Data age", f"{delta_hours:.1f}h", f"{max_hours}h limit")

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, ts_col

    def _check_table_freshness(self, cfg: dict, table_ctx: dict) -> tuple[bool, str, str | None]:
        """
        Verify table freshness using Delta table metadata (lastModified).

        Config:
            max_staleness_days: Maximum allowed days since last modification (default: 2)
                                (legacy alias: ``max_days_stale``)
        """
        start_time = datetime.now(UTC)
        fq_table = f"{table_ctx['catalog']}.{table_ctx['schema']}.{table_ctx['table']}"
        try:
            meta = self.spark.sql(f"DESCRIBE DETAIL {fq_table}").collect()[0]
            last_modified = meta["lastModified"]

            if not last_modified:
                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=0,
                    table_info=self._current_table_info,
                )
                self._last_failure_stats = None
                self._last_investigation_query = None
                self._last_execution = execution
                return False, "Table metadata 'lastModified' not available", None

            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=UTC)

            now = datetime.now(UTC)
            days = (now - last_modified).total_seconds() / 86400
            hours = days * 24

            # Canonical key is ``max_staleness_days``; fall back to the legacy
            # ``max_days_stale`` alias for configs that bypass alias resolution.
            base_days = float(cfg.get("max_staleness_days", cfg.get("max_days_stale", 2)))
            passed = days <= base_days

            failure_stats = None
            investigation_query = None
            if not passed:
                failure_stats = self._build_failure_stats(
                    failure_type="table_stale",
                    affected_columns=[],
                    failure_count=1,
                    total_checked=1,
                    diagnostic={
                        "last_modified": last_modified.isoformat(),
                        "age_days": round(days, 2),
                        "age_hours": round(hours, 2),
                        "max_allowed_days": base_days,
                    },
                )

                investigation_query = {
                    "description": "Check Delta table history",
                    "query": f"DESCRIBE HISTORY {fq_table} LIMIT 10",
                }

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )

            if passed:
                details = self._details_pass_freshness("Table", f"updated {hours:.1f}h ago", f"{base_days}d limit")
            else:
                details = f"Table stale: {hours:.1f}h since update exceeds {base_days}d limit"

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, None

        except Exception as e:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_error(f"table freshness: {e}"), None

    def _check_schema_drift(self, df, cfg: dict) -> tuple[bool, str, None]:
        """
        Detect schema drift by comparing actual columns against expected schema.

        Config:
            expected_columns: Dict of column_name -> expected_type
        """
        start_time = datetime.now(UTC)
        expected = cfg.get("expected_columns", {})
        actual = dict(df.dtypes)
        total_rows = df.count()

        missing = [c for c in expected if c not in actual]
        mismatched = [c for c in expected if c in actual and expected[c].lower() not in actual[c].lower()]

        passed = not (missing or mismatched)

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="schema_drift",
                affected_columns=missing + mismatched,
                failure_count=len(missing) + len(mismatched),
                total_checked=len(expected),
                diagnostic={
                    "missing_columns": missing,
                    "mismatched_columns": mismatched,
                    "mismatched_details": {c: {"expected": expected[c], "actual": actual.get(c)} for c in mismatched}
                    if mismatched
                    else {},
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="schema_drift",
                table_fq=self._current_table_fq,
                affected_columns=missing + mismatched,
                cfg={},
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = f"Schema OK: {len(expected)} columns verified"
        else:
            details = f"Schema issues: {len(missing)} missing, {len(mismatched)} type mismatch"

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, None

    def _check_mandatory_fields(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify mandatory columns exist and contain no null values.

        Config:
            columns: List of column names that must be present and non-null
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of null rows per column (0-100)
                max_failure_count: Maximum allowed number of null rows per column

        Note: Threshold is applied per-column. If ANY column exceeds threshold, check fails.
        """
        start_time = datetime.now(UTC)
        columns = cfg.get("columns", [])
        if not columns:
            return False, self._details_config_missing("columns"), None

        total_count = df.count()
        violated = []
        max_null_count = 0
        column_details = {}
        missing_columns = []

        for c in columns:
            if c not in df.columns:
                violated.append(c)
                column_details[c] = "column_not_found"
                missing_columns.append(c)
            else:
                null_count = df.filter(col(c).isNull()).count()
                if null_count > 0:
                    column_details[c] = null_count
                    max_null_count = max(max_null_count, null_count)
                    # Check threshold per column
                    col_passed, _ = self._evaluate_threshold(null_count, total_count, cfg)
                    if not col_passed:
                        violated.append(c)

        # If no threshold configured, any null = violation (original behavior)
        threshold = cfg.get("threshold") or self._suite_defaults.get("threshold")
        if not threshold:
            violated = [c for c in columns if c not in df.columns or column_details.get(c, 0) > 0]

        passed = len(violated) == 0

        # Calculate total null rows for failure stats
        total_null_rows = sum(v for v in column_details.values() if isinstance(v, int))

        failure_stats = None
        investigation_query = None
        if not passed:
            # Build null distribution
            null_distribution = {c: count for c, count in column_details.items() if isinstance(count, int)}

            failure_stats = self._build_failure_stats(
                failure_type="null_value",
                affected_columns=violated,
                failure_count=total_null_rows if total_null_rows > 0 else len(missing_columns),
                total_checked=total_count,
                distribution=null_distribution if null_distribution else None,
                diagnostic={
                    "columns_with_nulls": [c for c in column_details if isinstance(column_details.get(c), int)],
                    "missing_columns": missing_columns,
                    "worst_column": max(
                        (c for c in null_distribution), key=lambda x: null_distribution.get(x, 0), default=None
                    )
                    if null_distribution
                    else None,
                    "worst_column_null_count": max(null_distribution.values()) if null_distribution else 0,
                },
            )

            # Build null conditions for investigation query
            null_conditions = " OR ".join(f"{c} IS NULL" for c in columns if c in df.columns)
            investigation_query = self._build_investigation_query(
                check_type="null_value",
                table_fq=self._current_table_fq,
                affected_columns=violated,
                cfg={"null_conditions": null_conditions if null_conditions else "1=0"},
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        # Build narrative details
        if passed:
            details = self._details_pass_all(len(columns), "mandatory columns", "present and non-null")
        elif missing_columns:
            details = self._details_fail_missing(len(missing_columns), "required columns")
        else:
            details = self._details_fail_found(total_null_rows, "null values", total_count)

        # Add threshold info if configured
        if threshold:
            _, threshold_meta = self._evaluate_threshold(max_null_count, total_count, cfg)
            details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, ",".join(violated) if violated else None

    def _check_binary_not_null(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify binary column contains no null values.

        Config:
            column: Binary column to check
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of null rows (0-100)
                max_failure_count: Maximum allowed number of null rows
        """
        start_time = datetime.now(UTC)
        col_name = cfg.get("column")

        if col_name not in df.columns:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_not_found("Column", col_name), col_name

        total_count = df.count()
        failed_count = df.filter(col(col_name).isNull()).count()

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="null_binary",
                affected_columns=[col_name],
                failure_count=failed_count,
                total_checked=total_count,
            )

            investigation_query = self._build_investigation_query(
                check_type="null_value",
                table_fq=self._current_table_fq,
                affected_columns=[col_name],
                cfg={"null_conditions": f"{col_name} IS NULL"},
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        # Build narrative details
        if passed:
            details = f"All values non-null in '{col_name}'"
        else:
            details = self._details_fail_found(failed_count, "null values", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, col_name

    def _check_binary_non_empty(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify binary column contains no empty (zero-length) values.

        Config:
            column: Binary column to check
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of empty rows (0-100)
                max_failure_count: Maximum allowed number of empty rows
        """
        start_time = datetime.now(UTC)
        col_name = cfg.get("column")

        if col_name not in df.columns:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_not_found("Column", col_name), col_name

        total_count = df.count()
        failed_count = df.filter(length(col(col_name)) == 0).count()

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="empty_binary",
                affected_columns=[col_name],
                failure_count=failed_count,
                total_checked=total_count,
            )

            investigation_query = {
                "description": "Find rows with empty binary values",
                "query": f"SELECT * FROM {self._current_table_fq} WHERE length({col_name}) = 0 LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        # Build narrative details
        if passed:
            details = f"All values non-empty in '{col_name}'"
        else:
            details = self._details_fail_found(failed_count, "empty values", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, col_name

    def _check_binary_size_guard(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify binary column values are within expected size bounds.

        Config:
            column: Binary column to check
            min_bytes: Minimum allowed size in bytes (optional)
            max_bytes: Maximum allowed size in bytes (optional)
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of invalid rows (0-100)
                max_failure_count: Maximum allowed number of invalid rows
        """
        start_time = datetime.now(UTC)
        col_name = cfg.get("column")
        min_bytes = cfg.get("min_bytes")
        max_bytes = cfg.get("max_bytes")

        if col_name not in df.columns:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_not_found("Column", col_name), col_name

        size_col = length(col(col_name))

        filters = []
        filter_conditions = []

        if min_bytes is not None:
            min_bytes = int(min_bytes)
            filters.append(size_col < min_bytes)
            filter_conditions.append(f"length({col_name}) < {min_bytes}")

        if max_bytes is not None:
            max_bytes = int(max_bytes)
            filters.append(size_col > max_bytes)
            filter_conditions.append(f"length({col_name}) > {max_bytes}")

        if not filters:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_config_missing("min_bytes or max_bytes"), col_name

        total_count = df.count()
        failed_count = df.filter(filters[0] if len(filters) == 1 else filters[0] | filters[1]).count()

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            # Get size distribution for diagnostics
            size_stats = df.agg(
                expr(f"min(length({col_name}))").alias("min_size"),
                expr(f"max(length({col_name}))").alias("max_size"),
                expr(f"avg(length({col_name}))").alias("avg_size"),
            ).first()

            failure_stats = self._build_failure_stats(
                failure_type="binary_size_violation",
                affected_columns=[col_name],
                failure_count=failed_count,
                total_checked=total_count,
                diagnostic={
                    "min_allowed_bytes": min_bytes,
                    "max_allowed_bytes": max_bytes,
                    "observed_min_size": int(size_stats["min_size"]) if size_stats["min_size"] else 0,
                    "observed_max_size": int(size_stats["max_size"]) if size_stats["max_size"] else 0,
                    "observed_avg_size": round(float(size_stats["avg_size"]), 2) if size_stats["avg_size"] else 0,
                },
            )

            where_condition = " OR ".join(filter_conditions)
            investigation_query = {
                "description": "Find rows with invalid binary size",
                "query": f"SELECT *, length({col_name}) as size FROM {self._current_table_fq} WHERE {where_condition} LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        # Build narrative details
        if passed:
            bounds = []
            if min_bytes is not None:
                bounds.append(f"min={min_bytes}B")
            if max_bytes is not None:
                bounds.append(f"max={max_bytes}B")
            details = self._details_pass_bounds("Binary size", col_name, ", ".join(bounds))
        else:
            details = self._details_fail_found(failed_count, "size violations", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, col_name

    def _check_binary_type(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify column has BinaryType data type.

        Config:
            column: Column to check
        """
        start_time = datetime.now(UTC)
        col_name = cfg.get("column")
        total_rows = df.count()

        field = next((f for f in df.schema.fields if f.name == col_name), None)
        if not field:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_not_found("Column", col_name), col_name

        passed = isinstance(field.dataType, BinaryType)
        actual_type = str(field.dataType)

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="wrong_column_type",
                affected_columns=[col_name],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "expected_type": "binary",
                    "actual_type": actual_type,
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="schema_drift",
                table_fq=self._current_table_fq,
                affected_columns=[col_name],
                cfg={},
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = f"Type OK: '{col_name}' is binary"
        else:
            details = self._details_fail_mismatch("Type", "binary", actual_type)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, col_name

    def _check_binary_decodable(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify binary column can be decoded to expected format.

        Config:
            column: Binary column to check
            format: Expected format - json, base64_json, avro, protobuf (default: json)
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of invalid payloads (0-100)
                max_failure_count: Maximum allowed number of invalid payloads
        """
        start_time = datetime.now(UTC)
        col_name = cfg.get("column")
        fmt = cfg.get("format", "json").lower()

        if col_name not in df.columns:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_not_found("Column", col_name), col_name

        total_count = df.count()
        decode_filter = ""

        try:
            if fmt == "json":
                decoded = df.select(expr(f"decode({col_name}, 'UTF-8') as decoded"))
                failed_count = decoded.filter(expr("decoded IS NULL")).count()
                decode_filter = f"decode({col_name}, 'UTF-8') IS NULL"

            elif fmt == "base64_json":
                # ``try_to_binary`` yields NULL (not an error) for malformed base64 under
                # ANSI / Photon strict mode, so a single bad row is counted as a failure
                # instead of raising CONVERSION_INVALID_INPUT and crashing the whole check.
                # JSON structure is validated via the first decoded byte ('{' = 0x7B,
                # '[' = 0x5B), which avoids charset decoding entirely.
                bin_expr = f"try_to_binary({col_name}, 'BASE64')"
                first_byte = f"hex(substring({bin_expr}, 1, 1))"
                decode_filter = f"{bin_expr} IS NULL OR {first_byte} NOT IN ('7B', '5B')"
                failed_count = df.filter(expr(decode_filter)).count()

            elif fmt == "avro":
                failed_count = df.filter(expr(f"substring({col_name},1,1) != x'00'")).count()
                decode_filter = f"substring({col_name},1,1) != x'00'"

            elif fmt == "protobuf":
                failed_count = df.filter(length(col(col_name)) == 0).count()
                decode_filter = f"length({col_name}) = 0"

            else:
                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=total_count,
                    table_info=self._current_table_info,
                )
                self._last_failure_stats = None
                self._last_investigation_query = None
                self._last_execution = execution
                return False, f"Invalid format: '{fmt}' not supported", col_name

        except Exception as e:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_count,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_error(f"decode: {e}"), col_name

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="decode_failure",
                affected_columns=[col_name],
                failure_count=failed_count,
                total_checked=total_count,
                diagnostic={
                    "format": fmt,
                },
            )

            investigation_query = {
                "description": f"Find rows that fail {fmt} decoding",
                "query": f"SELECT * FROM {self._current_table_fq} WHERE {decode_filter} LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        # Build narrative details
        if passed:
            details = f"All rows decodable as {fmt}"
        else:
            details = self._details_fail_found(failed_count, f"decode failures ({fmt})", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, col_name

    def _check_binary_version_header_check(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify binary column has valid version header (magic byte).

        Config:
            column: Binary column to check
            magic_byte: Expected magic byte value (default: 0)
            header_size_bytes: Minimum header size in bytes (default: 5)
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of invalid headers (0-100)
                max_failure_count: Maximum allowed number of invalid headers
        """
        start_time = datetime.now(UTC)
        col_name = cfg.get("column")
        magic_byte = int(cfg.get("magic_byte", 0))
        header_size = int(cfg.get("header_size_bytes", 5))

        if col_name not in df.columns:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_not_found("Column", col_name), col_name

        total_count = df.count()

        # Count each condition separately for details
        # NULL values are treated as failures (column must have data)
        null_count = df.filter(col(col_name).isNull()).count()
        too_short = df.filter(col(col_name).isNotNull() & (length(col(col_name)) < header_size)).count()
        invalid_magic = df.filter(
            col(col_name).isNotNull()
            & (length(col(col_name)) >= header_size)
            & expr(f"hex(substring({col_name}, 1, 1)) != lpad(hex({magic_byte}), 2, '0')")
        ).count()

        # Total failures = NULL + too_short + invalid_magic
        failed_count = null_count + too_short + invalid_magic

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="invalid_header",
                affected_columns=[col_name],
                failure_count=failed_count,
                total_checked=total_count,
                distribution={
                    "null_values": null_count,
                    "too_short": too_short,
                    "invalid_magic": invalid_magic,
                },
                diagnostic={
                    "expected_magic_byte": magic_byte,
                    "expected_header_size": header_size,
                },
            )

            investigation_query = {
                "description": "Find rows with invalid binary header",
                "query": (
                    f"SELECT *, length({col_name}) as size, hex(substring({col_name}, 1, 1)) as magic "
                    f"FROM {self._current_table_fq} "
                    f"WHERE {col_name} IS NULL "
                    f"OR length({col_name}) < {header_size} "
                    f"OR hex(substring({col_name}, 1, 1)) != lpad(hex({magic_byte}), 2, '0') LIMIT 100"
                ),
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        # Build narrative details
        if passed:
            details = f"All headers valid (magic={magic_byte}, size>={header_size}B)"
        else:
            details = self._details_fail_found(failed_count, "invalid headers", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, col_name

    def _check_partition_exists(self, cfg: dict, table_ctx: dict) -> tuple[bool, str, str | None]:
        """
        Verify expected partitions exist in Delta table.

        Config:
            partitions: List of partition specifications to check (required)
                        Each item is a dict like {"year": 2024, "month": 1}
            partition_column: Single partition column name (alternative to partitions)
            expected_values: List of expected values for partition_column
        """
        start_time = datetime.now(UTC)
        fq_table = f"{table_ctx['catalog']}.{table_ctx['schema']}.{table_ctx['table']}"
        try:
            # Get existing partitions from Delta
            partitions_df = self.spark.sql(f"SHOW PARTITIONS {fq_table}")
            existing = set()

            for row in partitions_df.collect():
                # Parse Hive-style partition string like "year=2024/month=1"
                # Note: split("=", 1) handles values containing "=", but values
                # containing "/" are not supported (non-standard in partition values)
                part_str = row[0] if row[0] else ""
                part_dict = {}
                for kv in part_str.split("/"):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        part_dict[k] = v
                existing.add(frozenset(part_dict.items()))

            # Check explicit partitions list
            if cfg.get("partitions"):
                missing = []
                for part in cfg["partitions"]:
                    part_key = frozenset((k, str(v)) for k, v in part.items())
                    if part_key not in existing:
                        missing.append(part)

                passed = len(missing) == 0
                expected_count = len(cfg["partitions"])

                failure_stats = None
                investigation_query = None
                if not passed:
                    failure_stats = self._build_failure_stats(
                        failure_type="missing_partition",
                        affected_columns=[],
                        failure_count=len(missing),
                        total_checked=expected_count,
                        diagnostic={
                            "missing_partitions": missing,
                            "existing_partition_count": len(existing),
                        },
                    )

                    investigation_query = {
                        "description": "Show existing partitions",
                        "query": f"SHOW PARTITIONS {fq_table}",
                    }

                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=0,
                    table_info=self._current_table_info,
                )

                if passed:
                    details = f"All {expected_count} expected partitions exist"
                else:
                    details = self._details_fail_missing(len(missing), "expected partitions")

                self._last_failure_stats = failure_stats
                self._last_investigation_query = investigation_query
                self._last_execution = execution

                return passed, details, None

            # Check single partition column with expected values
            partition_col = cfg.get("partition_column")
            expected_values = cfg.get("expected_values", [])

            if partition_col and expected_values:
                existing_values = {
                    dict(items).get(partition_col) for items in existing if dict(items).get(partition_col)
                }
                missing = [v for v in expected_values if str(v) not in existing_values]

                passed = len(missing) == 0

                failure_stats = None
                investigation_query = None
                if not passed:
                    failure_stats = self._build_failure_stats(
                        failure_type="missing_partition",
                        affected_columns=[partition_col],
                        failure_count=len(missing),
                        total_checked=len(expected_values),
                        diagnostic={
                            "missing_values": missing,
                            "existing_values_count": len(existing_values),
                        },
                    )

                    investigation_query = {
                        "description": "Show existing partitions",
                        "query": f"SHOW PARTITIONS {fq_table}",
                    }

                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=0,
                    table_info=self._current_table_info,
                )

                if passed:
                    details = f"All {len(expected_values)} expected {partition_col} values exist"
                else:
                    details = self._details_fail_missing(len(missing), f"{partition_col} partitions")

                self._last_failure_stats = failure_stats
                self._last_investigation_query = investigation_query
                self._last_execution = execution

                return passed, details, partition_col

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_config_missing("partitions or (partition_column + expected_values)"), None

        except Exception as e:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            # Table might not be partitioned
            if "not partitioned" in str(e).lower():
                return False, "Table is not partitioned", None
            return False, self._details_error(f"partition check: {e}"), None

    def _check_binary_schema_fingerprint(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify binary column contains only allowed schema fingerprints.

        Config:
            column: Binary column to check (required)
            allowed_fingerprints: List of allowed fingerprint hex values (required)
            offset: Byte offset to start reading fingerprint (default: 2)
            length_bytes: Number of bytes to read for fingerprint (default: 4)
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of invalid fingerprints (0-100)
                max_failure_count: Maximum allowed number of invalid fingerprints
        """
        start_time = datetime.now(UTC)
        col_name = cfg.get("column")
        if not col_name:
            return False, self._details_config_missing("column"), None

        allowed_fingerprints = cfg.get("allowed_fingerprints")
        if not allowed_fingerprints:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_config_missing("allowed_fingerprints"), col_name

        offset = cfg.get("offset", 2)
        length_bytes = cfg.get("length_bytes", 4)

        df_fp = df.select(
            upper(expr(f"hex(substring({col_name}, {offset}, {length_bytes}))")).alias("schema_fingerprint")
        )

        allowed_set = {fp.upper() for fp in allowed_fingerprints}

        invalid_fp_df = df_fp.filter(~col("schema_fingerprint").isin(allowed_set))

        total_count = df.count()
        failed_count = invalid_fp_df.count()

        # Get distribution of invalid fingerprints (no actual values - just counts)
        invalid_distribution = {}
        if failed_count > 0:
            fp_counts = (
                invalid_fp_df.groupBy("schema_fingerprint").count().orderBy(col("count").desc()).limit(10).collect()
            )
            invalid_distribution = {row["schema_fingerprint"]: row["count"] for row in fp_counts}

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="invalid_fingerprint",
                affected_columns=[col_name],
                failure_count=failed_count,
                total_checked=total_count,
                distribution=invalid_distribution,
                diagnostic={
                    "allowed_fingerprints_count": len(allowed_fingerprints),
                    "distinct_invalid_fingerprints": len(invalid_distribution),
                    "offset": offset,
                    "length_bytes": length_bytes,
                },
            )

            allowed_list = ", ".join(f"'{fp}'" for fp in allowed_set)
            investigation_query = {
                "description": "Find rows with invalid schema fingerprints",
                "query": (
                    f"SELECT *, upper(hex(substring({col_name}, {offset}, {length_bytes}))) as fingerprint "
                    f"FROM {self._current_table_fq} "
                    f"WHERE upper(hex(substring({col_name}, {offset}, {length_bytes}))) NOT IN ({allowed_list}) LIMIT 100"
                ),
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        # Build narrative details
        if passed:
            details = f"All fingerprints valid ({len(allowed_fingerprints)} allowed)"
        else:
            details = self._details_fail_found(failed_count, "invalid fingerprints", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, col_name
