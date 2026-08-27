# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any
from collections import defaultdict


class BaseSummaryEngine:
    """
    Generic Summary Test Suite for Data Quality validation results.

    Responsibilities:
    - Aggregate raw check-level results
    - Produce a standardized, policy-ready summary
    - Remain engine-agnostic (Bronze, Silver, Gold, Profiling, GE, etc.)
    """

    def summarize(self, raw_results: list[dict[str, Any]]) -> dict[str, Any]:
        if not raw_results:
            return self._empty_summary(raw_results)

        passed, failed = self._split_results(raw_results)

        tables, tables_failed, columns_failed = self._extract_entities(raw_results, failed)

        issues_by_type = self._build_issues_by_type(failed)
        failures_by_table = self._build_failures_by_table(failed)
        failure_details = self._build_failure_details(failed)

        failed_by_severity = self._count_failed_by_severity(failed)

        blocking_failures = self._count_blocking_failures(raw_results)

        status = self._resolve_status(failed, blocking_failures)

        executed_checks, passed_checks, failed_checks = self._count_checks(
            raw_results,
            passed,
            failed,
        )

        summary = {
            "status": status,
            "total_checks": executed_checks,
            "passed": passed_checks,
            "failed": failed_checks,
            "failed_by_severity": failed_by_severity,
            "blocking_failures": blocking_failures,
            "tables_processed": len(tables),
            "tables_failed": len(tables_failed),
            "columns_with_issues": len(columns_failed),
            "issues_by_type": issues_by_type,
            "failures_by_table": failures_by_table,
            "failure_details": failure_details,
        }

        extended = self.extend_summary(summary, raw_results)
        if extended:
            summary.update(extended)

        return summary

    def extend_summary(
        self,
        _base_summary: dict[str, Any],
        _raw_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {}

    def _empty_summary(self, _raw_results: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "status": "PASS",
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "failed_by_severity": {},
            "blocking_failures": 0,
            "tables_processed": 0,
            "tables_failed": 0,
            "columns_with_issues": 0,
            "issues_by_type": {},
            "failures_by_table": {},
            "failure_details": [],
        }

    def _split_results(
        self,
        raw_results: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        passed = [r for r in raw_results if r.get("passed") is True]
        failed = [r for r in raw_results if r.get("passed") is False]
        return passed, failed

    def _extract_entities(
        self,
        raw_results: list[dict[str, Any]],
        failed: list[dict[str, Any]],
    ) -> tuple[set[str], set[str], set[str]]:
        # Use table_fullname for unique identification, fallback to table for backwards compatibility
        tables = {r.get("table_fullname") or r.get("table") for r in raw_results if r.get("table")}
        tables_failed = {r.get("table_fullname") or r.get("table") for r in failed if r.get("table")}
        columns_failed = {r.get("column") for r in failed if r.get("column")}
        return tables, tables_failed, columns_failed

    def _build_issues_by_type(
        self,
        failed: list[dict[str, Any]],
        max_cols_display: int = 3,
        max_col_length: int = 25,
    ) -> dict[str, dict[str, Any]]:
        issues = defaultdict(lambda: {"columns": [], "count": 0})

        for r in failed:
            check = r.get("check_name") or "unknown"
            col = r.get("column") or "table"

            if col not in issues[check]["columns"]:
                issues[check]["columns"].append(col)

            issues[check]["count"] += 1

        for v in issues.values():
            cols = v["columns"]
            truncated_cols = [self._truncate(c, max_col_length) for c in cols[:max_cols_display]]
            suffix = ", ..." if len(cols) > max_cols_display else ""
            v["columns"] = ", ".join(truncated_cols) + suffix

        return dict(issues)

    def _truncate(self, text: str, max_length: int) -> str:
        if len(text) <= max_length:
            return text
        return text[: max_length - 2] + ".."

    def _build_failures_by_table(
        self,
        failed: list[dict[str, Any]],
    ) -> dict[str, int]:
        table_counts: dict[str, int] = defaultdict(int)

        for r in failed:
            # Use table_fullname for unique identification, fallback to table
            table = r.get("table_fullname") or r.get("table")
            if table:
                table_counts[table] += 1

        return dict(sorted(table_counts.items(), key=lambda x: x[1], reverse=True))

    def _build_failure_details(
        self,
        failed: list[dict[str, Any]],
        max_cols_display: int = 3,
        max_col_length: int = 25,
    ) -> list[dict[str, Any]]:
        """Aggregate failures by the real (check, table) pair.

        Unlike issues_by_type (keyed by check) and failures_by_table (keyed by
        table), this preserves the link between a check and the table it failed
        on, so consumers (e.g. Slack alerts) can render accurate
        "check -> table" lines instead of a cartesian product of the two.

        Returns:
            List of {check, table, table_fullname, columns, count} dicts,
            sorted by count descending.
        """
        # Key: (check_name, table_fullname) -> {count, columns}
        grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(lambda: {"count": 0, "columns": []})

        for r in failed:
            check = r.get("check_name") or "unknown"
            table_fq = r.get("table_fullname") or r.get("table") or ""
            col = r.get("column") or "table"

            entry = grouped[(check, table_fq)]
            entry["count"] += 1
            if col not in entry["columns"]:
                entry["columns"].append(col)

        details = []
        for (check, table_fq), entry in grouped.items():
            cols = entry["columns"]
            truncated = [self._truncate(c, max_col_length) for c in cols[:max_cols_display]]
            suffix = ", ..." if len(cols) > max_cols_display else ""
            details.append(
                {
                    "check": check,
                    "table": table_fq.split(".")[-1] if "." in table_fq else table_fq,
                    "table_fullname": table_fq,
                    "columns": ", ".join(truncated) + suffix,
                    "count": entry["count"],
                }
            )

        return sorted(details, key=lambda d: d["count"], reverse=True)

    def _count_blocking_failures(self, raw_results: list[dict[str, Any]]) -> int:
        count = 0

        for r in raw_results:
            if "blocking_failures" in r:
                count += r.get("blocking_failures", 0)
            elif r.get("passed") is False and r.get("blocking") is True:
                count += 1

        return count

    def _resolve_status(
        self,
        failed: list[dict[str, Any]],
        blocking_failures: int,
    ) -> str:
        if blocking_failures > 0:
            return "FAIL"

        if failed:
            return "WARN"

        return "PASS"

    def _count_checks(
        self,
        raw_results: list[dict[str, Any]],
        passed: list[dict[str, Any]],
        failed: list[dict[str, Any]],
    ) -> tuple[int, int, int]:
        if self._is_table_level_engine(raw_results):
            executed = sum(len(r.get("checks", [])) for r in raw_results)
            failed_checks = sum(1 for r in raw_results for c in r.get("checks", []) if not c.get("passed", True))
            passed_checks = executed - failed_checks
            return executed, passed_checks, failed_checks

        return len(raw_results), len(passed), len(failed)

    def _count_failed_by_severity(
        self,
        failed_results: list[dict[str, Any]],
    ) -> dict[str, int]:
        severity_counter = defaultdict(int)

        for r in failed_results:
            severity = r.get("severity") or r.get("criticality", "error")
            severity_counter[severity] += 1

        return dict(severity_counter)

    def _detect_engine_name(self, results: list[dict[str, Any]]) -> str:
        if not results:
            return "unknown"
        return results[0].get("engine", "unknown")

    def _is_table_level_engine(self, raw_results: list[dict[str, Any]]) -> bool:
        return raw_results and isinstance(raw_results[0], dict) and "checks" in raw_results[0]
