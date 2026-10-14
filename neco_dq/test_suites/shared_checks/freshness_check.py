# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Freshness check mixin (Silver/Gold shared variant)."""

from datetime import datetime, UTC
from typing import Any

from pyspark.sql import functions as f

from neco_dq.test_suites.shared_checks.where_clause import WhereClauseMixin


class FreshnessCheckMixin(WhereClauseMixin):
    """Mixin providing the shared ``freshness`` check."""

    def _check_freshness(self, df, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
        """
        Verify data freshness based on a timestamp column.

        Config:
            timestamp_column: Column containing timestamps to check (required)
            max_delay_hours: Maximum allowed delay in hours (default: 24)
        """
        start_time = datetime.now(UTC)
        ok, _err = self._require_cfg(cfg, "timestamp_column")
        if not ok:
            return False, self._details_config_missing("timestamp_column"), None

        column = cfg["timestamp_column"]
        max_delay = cfg.get("max_delay_hours", 24)

        total_rows = df.count()
        latest = df.select(f.max(f.col(column))).first()[0]
        if latest is None:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_no_data(f"column '{column}'"), column

        # Handle naive timestamps by assuming UTC
        # This is a common pattern in data lakes where timestamps are stored without timezone
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=UTC)

        now_utc = datetime.now(UTC)
        # Convert latest to UTC if it has a different timezone
        if latest.tzinfo != UTC:
            latest = latest.astimezone(UTC)

        age_h = (now_utc - latest).total_seconds() / 3600
        passed = age_h <= max_delay

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="freshness_violation",
                affected_columns=[column],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "age_hours": round(age_h, 1),
                    "max_delay_hours": max_delay,
                    "latest_timestamp": latest.isoformat(),
                },
            )
            where = self._get_where_clause()
            investigation_query = {
                "description": "Check data freshness distribution",
                "query": f"SELECT MAX({column}) as latest, MIN({column}) as earliest, DATEDIFF(HOUR, MAX({column}), current_timestamp()) as age_hours FROM {self._current_table_fq}{where}",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = self._details_pass_freshness("Data", f"{age_h:.1f}h old", f"{max_delay}h limit")
        else:
            details = self._details_fail_exceeds("Data age", f"{age_h:.1f}h", f"{max_delay}h limit")

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, column
