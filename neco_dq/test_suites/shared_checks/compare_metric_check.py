# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Compare-metric check mixin (observed vs expected with operator)."""

from datetime import datetime, UTC
from typing import Any

from neco_dq.core.metrics.expected import resolve_expected
from neco_dq.core.metrics.observed import resolve_observed
from neco_dq.core.metrics.operators import compare
from neco_dq.test_suites.shared_checks.where_clause import WhereClauseMixin


class CompareMetricCheckMixin(WhereClauseMixin):
    """Mixin providing the ``compare_metric`` check and its investigation-query helper."""

    def _build_metric_investigation_query(
        self,
        metric_cfg: dict,
        check_name: str,
    ) -> dict:
        """Build investigation query for compare_metric failures showing observed values."""
        table_fq = self._current_table_fq
        where = self._get_where_clause()

        # Extract metric configuration
        column = metric_cfg.get("column", "*")
        function = metric_cfg.get("function", "count")
        group_by = metric_cfg.get("group_by", [])
        if isinstance(group_by, str):
            group_by = [group_by]

        # Build metric expression
        metric_expr = f"{function}({column})" if column != "*" else f"{function}(*)"

        if group_by:
            group_cols = ", ".join(group_by)
            query = f"SELECT {group_cols}, {metric_expr} AS observed_value FROM {table_fq}"
            if where:
                query += where
            query += f" GROUP BY {group_cols}"
        else:
            query = f"SELECT {metric_expr} AS observed_value FROM {table_fq}"
            if where:
                query += where

        return {
            "description": f"Show observed values for metric '{check_name}'",
            "query": query,
        }

    def _check_compare_metric(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Generic metric comparison using expected/observed resolvers and operators.

        Config:
            metric: Observed metric configuration
            expected: Expected value configuration (static, SQL, column_value, etc.)
            operator: Comparison operator (==, !=, >, <, >=, <=) (default: ==)
            tolerance_abs: Numeric tolerance for comparison (optional)
            name: Human-readable name for the check (optional)
            missing_expected_value: Value to use when expected is missing for a group (default: 0)
            include_details: Show full group-level results (default: False)
        """
        start_time = datetime.now(UTC)
        ok, expected_value, entity, msg = resolve_expected(self.spark, df, cfg)
        if not ok:
            return False, self._details_error(f"expected resolution: {msg}"), entity

        # Support both "metric" and "observed" as keys for observed configuration
        metric_cfg = cfg.get("metric") or cfg.get("observed")
        if not metric_cfg:
            return False, self._details_config_missing("metric or observed"), None

        ok, observed, entity, msg = resolve_observed(self.spark, df, metric_cfg)
        if not ok:
            return False, self._details_error(f"metric resolution: {msg}"), entity

        operator = cfg.get("operator", "==")
        tolerance = cfg.get("tolerance_abs")
        name = cfg.get("name", "compare_metric")
        total_rows = df.count()

        try:
            if isinstance(observed, list):
                results: list[dict[str, Any]] = []
                missing_expected_value = cfg.get("missing_expected_value", 0)

                for raw_group_vals, obs_val in observed:
                    group_tuple = (
                        tuple(raw_group_vals) if isinstance(raw_group_vals, (list, tuple)) else (raw_group_vals,)
                    )

                    obs_single = obs_val
                    if isinstance(obs_val, dict) and len(obs_val) == 1:
                        obs_single = next(iter(obs_val.values()))

                    if isinstance(expected_value, dict):
                        key = tuple(str(x) for x in group_tuple)
                        exp_val = expected_value.get(key, missing_expected_value)
                    else:
                        exp_val = expected_value

                    passed = compare(obs_single, exp_val, operator, tolerance=tolerance)

                    results.append(
                        {
                            "group": group_tuple,
                            "observed": obs_single,
                            "expected": exp_val,
                            "passed": passed,
                            "reason": "comparison_failed" if not passed else "ok",
                        }
                    )

                passed_all = all(r["passed"] for r in results)
                total_groups = len(results)
                passed_groups = sum(1 for r in results if r["passed"])
                failed_groups = total_groups - passed_groups

                failure_stats = None
                investigation_query = None
                if not passed_all:
                    failure_stats = self._build_failure_stats(
                        failure_type="metric_comparison_failed",
                        affected_columns=[entity] if entity else [],
                        failure_count=failed_groups,
                        total_checked=total_groups,
                        diagnostic={
                            "operator": operator,
                            "tolerance": tolerance,
                            "check_name": name,
                        },
                    )
                    investigation_query = self._build_metric_investigation_query(metric_cfg, name)

                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=total_rows,
                    table_info=self._current_table_info,
                )

                if passed_all:
                    details = f"Metric '{name}' OK: {passed_groups}/{total_groups} groups satisfy {operator}"
                else:
                    details = self._details_fail_found(failed_groups, f"groups failing metric '{name}'", total_groups)

                self._last_failure_stats = failure_stats
                self._last_investigation_query = investigation_query
                self._last_execution = execution

                return passed_all, details, entity

            if isinstance(observed, dict) and len(observed) == 1:
                observed = next(iter(observed.values()))

            passed = compare(observed, expected_value, operator, tolerance=tolerance)

            failure_stats = None
            investigation_query = None
            if not passed:
                failure_stats = self._build_failure_stats(
                    failure_type="metric_comparison_failed",
                    affected_columns=[entity] if entity else [],
                    failure_count=1,
                    total_checked=1,
                    diagnostic={
                        "observed": observed,
                        "expected": expected_value,
                        "operator": operator,
                        "tolerance": tolerance,
                    },
                )
                investigation_query = self._build_metric_investigation_query(metric_cfg, name)

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )

            if passed:
                details = self._details_pass_comparison(f"Metric '{name}'", observed, operator, expected_value)
            else:
                details = self._details_fail_mismatch(f"Metric '{name}'", expected_value, observed)

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, entity

        except Exception as e:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_error(f"metric comparison: {e!s}"), entity
