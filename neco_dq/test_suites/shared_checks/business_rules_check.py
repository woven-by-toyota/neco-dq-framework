# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Business-rules check mixin (extracted from SharedChecksMixin)."""

from datetime import datetime, UTC
from typing import Any

from pyspark.sql import functions as f


class BusinessRulesCheckMixin:
    """
    Mixin providing the ``business_rules`` check (SQL-predicate validation).

    Split out of :class:`SharedChecksMixin` so a suite can compose only the
    checks it needs. Depends only on base-class helpers.
    """

    def _check_business_rules(self, df, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
        """
        Verify rows satisfy a business rule expressed as SQL expression.

        Config:
            predicate: SQL boolean expression that must be true for all rows (required)
            name: Human-readable name for the rule (default: "business_rule")
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of failing rows (0-100)
                max_failure_count: Maximum allowed number of failing rows
        """
        start_time = datetime.now(UTC)
        ok, _err = self._require_cfg(cfg, "predicate")
        if not ok:
            return False, self._details_config_missing("predicate"), None

        expr_str = cfg["predicate"]
        rule_name = cfg.get("name", "business_rule")

        total_count = df.count()
        failed_count = df.filter(~f.expr(expr_str)).count()

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="business_rule_violation",
                affected_columns=[],
                failure_count=failed_count,
                total_checked=total_count,
                diagnostic={
                    "rule_name": rule_name,
                    "rule_expression": expr_str,
                },
            )
            row_filter = getattr(self, "_current_where_clause", None)
            where_parts = [f"NOT ({expr_str})"]
            if row_filter:
                where_parts.append(row_filter)
            where_clause = " AND ".join(where_parts)
            investigation_query = {
                "description": f"Find rows violating rule '{rule_name}'",
                "query": f"SELECT * FROM {self._current_table_fq} WHERE {where_clause} LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        if passed:
            details = f"Rule '{rule_name}' passed ({total_count} rows)"
        else:
            details = self._details_fail_found(failed_count, f"rows violating rule '{rule_name}'", total_count)

        # Add threshold info if present
        if threshold_meta:
            details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, None
