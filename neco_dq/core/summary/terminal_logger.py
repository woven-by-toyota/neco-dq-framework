# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import defaultdict
from typing import Any

# Abbreviate suite names for display
SUITE_DISPLAY_MAP = {
    "great_expectations@bronze": "ge@bronze",
    "great_expectations@silver": "ge@silver",
    "great_expectations@gold": "ge@gold",
}


class DQSummaryTerminalLogger:
    """
    Logger-based terminal summary renderer for DQ framework.
    Produces human-readable, line-by-line logs (Databricks-safe).
    """

    def __init__(self, logger: logging.Logger, failure_threshold: float | None = None):
        self.logger = logger
        self._failure_threshold = failure_threshold

    def log(self, summary: dict[str, Any], unique_tables: int | None = None):
        if not summary:
            self.logger.warning("No DQ summary to display.")
            return

        header = f"{'Suite':<28} {'Status':<8} {'Checks':<8} {'Pass':<8} {'Fail':<8} {'Tables':<8}"
        sep = "─" * len(header)

        self.logger.info(sep)
        self.logger.info(" DATA QUALITY SUMMARY")
        self.logger.info(sep)

        self.logger.info(header)
        self.logger.info(sep)

        totals = {"checks": 0, "passed": 0, "failed": 0}
        total_blocking = 0

        for engine, s in summary.items():
            status = s.get("status", "UNKNOWN")
            checks = s.get("total_checks", 0)
            passed = s.get("passed", 0)
            failed = s.get("failed", 0)
            tables = s.get("tables_processed", 0)
            blocking = s.get("blocking_failures", 0)

            totals["checks"] += checks
            totals["passed"] += passed
            totals["failed"] += failed
            total_blocking += blocking

            display_name = SUITE_DISPLAY_MAP.get(engine, engine)
            self.logger.info(f"{display_name:<28} {status:<8} {checks:<8} {passed:<8} {failed:<8} {tables:<8}")

        self.logger.info(sep)

        # Use pre-computed unique tables count if provided
        tables_label = f"{unique_tables} unique" if unique_tables is not None else "0 unique"

        self.logger.info(
            f"{'TOTALS':<28} {'':<8} "
            f"{totals['checks']:<8} "
            f"{totals['passed']:<8} "
            f"{totals['failed']:<8} "
            f"{tables_label:<8}"
        )
        self.logger.info("")

        # Summary line with success rate and overall status
        success_pct = (totals["passed"] / totals["checks"] * 100) if totals["checks"] > 0 else 0
        overall_status = self._compute_overall_status(summary, total_blocking)
        status_detail = f"({total_blocking} blocking)" if total_blocking > 0 else ""

        # Include threshold if configured
        threshold_str = ""
        if self._failure_threshold is not None:
            threshold_str = f"  │  Threshold: {self._failure_threshold * 100:.0f}%"

        summary_line = f"Success Rate: {success_pct:.1f}%{threshold_str}  │  Status: {overall_status} {status_detail}"
        self.logger.info(summary_line.strip())
        self.logger.info("")

        self._log_issues_by_type(summary)
        self._log_tables_with_failures(summary)

        self.logger.info("Legend: PASS=all ok │ WARN=non-blocking failures │ FAIL=blocking")
        self.logger.info(sep)

    def _compute_overall_status(self, summary: dict[str, Any], total_blocking: int) -> str:
        """Compute overall status from all suites."""
        if total_blocking > 0:
            return "FAIL"
        statuses = [s.get("status", "PASS") for s in summary.values()]
        if "FAIL" in statuses:
            return "FAIL"
        # Treat unknown/error statuses as failures
        known_statuses = {"PASS", "WARN", "FAIL"}
        if any(s not in known_statuses for s in statuses):
            return "FAIL"
        if "WARN" in statuses:
            return "WARN"
        return "PASS"

    def _log_issues_by_type(self, summary: dict[str, Any]):
        checks_aggregated: dict[str, list[tuple[str, str, int]]] = defaultdict(list)

        for engine, s in summary.items():
            display_name = SUITE_DISPLAY_MAP.get(engine, engine)
            issues = s.get("issues_by_type", {})

            for check_name, details in issues.items():
                # Get individual columns (before they were joined)
                columns_str = details.get("columns", "")
                count = details.get("count", 0)

                # If multiple columns, we need to handle them
                # For now, treat as single entry with the column string
                col_display = columns_str if columns_str else "[table-level]"
                checks_aggregated[check_name].append((display_name, col_display, count))

        if not checks_aggregated:
            return

        self.logger.info("Failed checks by type:")

        # Sort by total failures descending
        sorted_checks = sorted(
            checks_aggregated.items(),
            key=lambda x: sum(item[2] for item in x[1]),
            reverse=True,
        )

        for check_name, entries in sorted_checks:
            total = sum(e[2] for e in entries)
            self.logger.info(f"  {check_name} ({total}):")

            for suite, column, count in sorted(entries, key=lambda x: x[2], reverse=True):
                self.logger.info(f"      {suite:<16} {column:<26} x{count}")

    def _log_tables_with_failures(self, summary: dict[str, Any], max_tables: int = 10):
        # Collect tables with their suites and failure count
        # Key: table_fq, Value: (set of suites, total count)
        table_failures: dict[str, tuple[set[str], int]] = {}

        for engine, s in summary.items():
            display_name = SUITE_DISPLAY_MAP.get(engine, engine)
            for table_fq, count in s.get("failures_by_table", {}).items():
                if table_fq in table_failures:
                    suites, existing_count = table_failures[table_fq]
                    suites.add(display_name)
                    table_failures[table_fq] = (suites, existing_count + count)
                else:
                    table_failures[table_fq] = ({display_name}, count)

        if not table_failures:
            return

        # Sort by count descending
        sorted_tables = sorted(table_failures.items(), key=lambda x: x[1][1], reverse=True)
        total_tables = len(sorted_tables)

        self.logger.info(f"Tables with failures ({total_tables}):")
        for table_fq, (suites, count) in sorted_tables[:max_tables]:
            # Extract just the table name from catalog.schema.table
            table_short = table_fq.split(".")[-1] if "." in table_fq else table_fq
            max_suites_inline = 2
            suite_display = ", ".join(sorted(suites)) if len(suites) <= max_suites_inline else "multiple"
            failure_word = "failure" if count == 1 else "failures"
            self.logger.info(f"      {suite_display:<16} {table_short:<30} {count} {failure_word}")

        if total_tables > max_tables:
            remaining = total_tables - max_tables
            self.logger.info(f"      ... and {remaining} more tables")

        self.logger.info("")
