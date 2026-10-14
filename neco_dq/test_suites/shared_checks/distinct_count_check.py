# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Distinct-count check mixin."""

from datetime import datetime, UTC

from neco_dq.test_suites.shared_checks.where_clause import WhereClauseMixin


class DistinctCountCheckMixin(WhereClauseMixin):
    """Mixin providing the ``distinct_count`` check."""

    def _check_distinct_count(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify the number of distinct values in a column is within expected bounds.

        Config:
            column: Column to count distinct values (required)
            min: Minimum expected distinct count (optional)
            max: Maximum expected distinct count (optional)
            expected: Exact expected distinct count (optional, overrides min/max)
        """
        start_time = datetime.now(UTC)
        ok, _err = self._require_cfg(cfg, "column")
        if not ok:
            return False, self._details_config_missing("column"), None

        column = cfg["column"]
        if column not in df.columns:
            return False, self._details_not_found("Column", column), column

        total_rows = df.count()
        distinct_count = df.select(column).distinct().count()

        # Exact match
        if cfg.get("expected") is not None:
            expected = int(cfg["expected"])
            passed = distinct_count == expected

            failure_stats = None
            investigation_query = None
            if not passed:
                failure_stats = self._build_failure_stats(
                    failure_type="distinct_count_mismatch",
                    affected_columns=[column],
                    failure_count=1,
                    total_checked=1,
                    diagnostic={
                        "distinct_count": distinct_count,
                        "expected": expected,
                        "difference": distinct_count - expected,
                    },
                )
                where = self._get_where_clause()
                investigation_query = {
                    "description": "List distinct values in column",
                    "query": f"SELECT DISTINCT {column}, COUNT(*) as cnt FROM {self._current_table_fq}{where} GROUP BY {column} ORDER BY cnt DESC LIMIT 100",
                }

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )

            if passed:
                details = f"Distinct count OK: {distinct_count} values in '{column}' (expected {expected})"
            else:
                details = self._details_fail_mismatch("Distinct count", expected, distinct_count)

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, column

        # Range check
        min_val = cfg.get("min_distinct_count")
        max_val = cfg.get("max_distinct_count")

        if min_val is None and max_val is None:
            return (
                False,
                self._details_config_missing("expected, min_distinct_count, or max_distinct_count"),
                column,
            )

        passed = True
        violation = None
        if min_val is not None and distinct_count < int(min_val):
            passed = False
            violation = "below_minimum"
        if max_val is not None and distinct_count > int(max_val):
            passed = False
            violation = "above_maximum"

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="distinct_count_out_of_range",
                affected_columns=[column],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "distinct_count": distinct_count,
                    "min": int(min_val) if min_val is not None else None,
                    "max": int(max_val) if max_val is not None else None,
                    "violation": violation,
                },
            )
            where = self._get_where_clause()
            investigation_query = {
                "description": "List distinct values in column",
                "query": f"SELECT DISTINCT {column}, COUNT(*) as cnt FROM {self._current_table_fq}{where} GROUP BY {column} ORDER BY cnt DESC LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            bounds = []
            if min_val is not None:
                bounds.append(f"min={min_val}")
            if max_val is not None:
                bounds.append(f"max={max_val}")
            details = self._details_pass_bounds("Distinct count", f"{distinct_count} in '{column}'", ", ".join(bounds))
        else:
            limit_val = min_val if violation == "below_minimum" else max_val
            if violation == "below_minimum":
                details = self._details_fail_below("Distinct count", distinct_count, limit_val)
            else:
                details = self._details_fail_above("Distinct count", distinct_count, limit_val)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, column
