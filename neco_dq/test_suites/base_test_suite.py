# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import difflib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, UTC
from typing import Any

from neco_dq.core.registry.check_param_schema import (
    CheckConfigError,
    allowed_keys,
    forbidden_keys_present,
    resolve_aliases,
    unknown_keys,
)
from neco_dq.core.summary.summary_engine import BaseSummaryEngine

logger = logging.getLogger("neco_dq")

# Expected number of elements in check result tuple: (passed, details, column)
_CHECK_RESULT_TUPLE_LEN = 3


class DQTestSuite(ABC):
    """
    Base interface for all Data Quality validation test suite.
    """

    # Runtime filter for tag-based check selection (set via set_tags_filter)
    _tags_filter: frozenset[str] | None = None

    # Debug fields set by checks, read after dispatch (reset before each check)
    _last_failure_stats: dict | None = None
    _last_investigation_query: dict | None = None
    _last_execution: dict | None = None

    # Current table context for building debug info
    _current_table_fq: str | None = None
    _current_table_info: dict | None = None
    _current_where_clause: str | None = None

    # Filter context for execution trace (set once per table, used by all checks)
    _current_rows_before: int | None = None
    _current_rows_after: int | None = None
    _current_filters_applied: dict | None = None

    # Profile context for execution trace (set once per suite, used by all checks)
    _profile_name: str | None = None
    _profile_target_catalog: str | None = None

    # Deprecated-parameter usage collected during a run (for the summary report).
    # Keyed by (check, old, new); value tracks the distinct tables where it occurred.
    _deprecations_log: dict[tuple[str, str, str], dict[str, Any]] | None = None

    # ------------------------------------------------------------------
    # Abstract properties (must be implemented by subclasses)
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def _suite_name(self) -> str:
        """Return the suite name: 'bronze', 'silver', or 'gold'."""

    @property
    @abstractmethod
    def _suite_config_key(self) -> str:
        """Return the config key (same as _suite_name)."""

    # ------------------------------------------------------------------
    # Concrete properties (derived from config)
    # ------------------------------------------------------------------

    @property
    def _suite_cfg(self) -> dict:
        """Return the suite-specific configuration section."""
        return self.cfg.get(self._suite_config_key, {})

    @property
    def _tables_config(self) -> list[dict]:
        """Return the list of table configurations."""
        return self._suite_cfg.get("tables", [])

    @property
    def _default_checks(self) -> dict:
        """Return default checks for this suite."""
        return self._suite_cfg.get("default_checks", {}) or {}

    @property
    def _data_to_save(self) -> list:
        """Return the data_to_save configuration."""
        return self._suite_cfg.get("data_to_save", [])

    @property
    def _suite_defaults(self) -> dict:
        """Return suite-level defaults for filtering and other table options."""
        return self._suite_cfg.get("defaults", {}) or {}

    # ------------------------------------------------------------------
    # Hook methods (override in subclasses as needed)
    # ------------------------------------------------------------------

    def _read_table(self, catalog: str, schema: str, table: str) -> Any:
        """
        Read table from catalog using fully-qualified name.
        No side effects on Spark session state.
        """
        return self.spark.table(f"{catalog}.{schema}.{table}")

    def _dispatch_check(
        self,
        func: Any,
        _name: str,
        df: Any,
        cfg: dict,
        table_ctx: dict,
        _fq_table: str,
    ) -> tuple[bool, str, str | None]:
        """
        Dispatch a check function with appropriate arguments.

        Design rationale:
        - Uses introspection to support multiple check signatures without requiring
          all checks to have identical signatures (df, cfg, table_ctx, etc.)
        - Allows gradual migration: existing checks continue to work while new
          patterns can be introduced
        - Subclasses can override for custom dispatch (e.g., Bronze hardcoded checks)

        Signature patterns supported:
        - (df, cfg, table_ctx) -> full context checks
        - (cfg, table_ctx) -> cross-table checks without DataFrame
        - (df, cfg) or (df_gold, cfg) -> single-table checks
        - (cfg) -> config-only checks

        Trade-offs:
        - Introspection is fragile if parameter names change
        - Standard signatures (see CHECK_IMPLEMENTATION_GUIDE.md) should be preferred
        - Consider explicit protocol/interface if signature diversity increases

        Note: _name and _fq_table are unused in default but used by subclass overrides.
        """
        params = func.__code__.co_varnames

        if "df" in params and "table_ctx" in params:
            out = func(df, cfg, table_ctx)
        elif "table_ctx" in params:
            out = func(cfg, table_ctx)
        elif any(p in params for p in ("df", "df_gold", "_df")):
            out = func(df, cfg)
        else:
            out = func(cfg)

        if len(out) == _CHECK_RESULT_TUPLE_LEN:
            return out
        return out[0], out[1], None

    def _build_row_from_check(self, result: dict, check: dict, run_timestamp: datetime) -> dict:
        """
        Build a row dict from a check result.
        Default: explicit key mapping. Override for spread pattern (Gold).
        """
        row = {
            "test_suite": self._suite_name,
            "catalog": result["catalog"],
            "schema": result["schema"],
            "table": result["table"],
            "column": check["column"],
            "run_timestamp": run_timestamp,
            "check_name": check["check_name"],
            "passed": check["passed"],
            "status": check["status"],
            "severity": check["severity"],
            "blocking": check["blocking"],
            "details": check["details"],
        }

        # Debug fields (only included if present in check result)
        if check.get("failure_stats"):
            row["failure_stats"] = check["failure_stats"]
        if check.get("investigation_query"):
            row["investigation_query"] = check["investigation_query"]
        if check.get("execution"):
            row["execution"] = check["execution"]

        return row

    def _build_failed_row(self, row: dict, result: dict) -> dict:
        """
        Build a failed row dict. Default adds alert_description.
        Override in Gold to skip alert_description.
        """
        return {**row, "alert_description": result.get("alert_description")}

    # ------------------------------------------------------------------
    # Common helper methods
    # ------------------------------------------------------------------

    def _normalize_check_cfg(self, cfg: Any) -> list[dict]:
        """Normalize check config to a list of instances."""
        if cfg is None:
            return []
        if isinstance(cfg, list):
            return cfg
        if isinstance(cfg, dict):
            return [cfg]
        raise ValueError(f"Invalid check configuration type: {type(cfg)}")

    @staticmethod
    def _as_column_list(value: str | list[str] | None) -> list[str]:
        """Normalize a ``*_columns`` value (one column name or a list) to a list."""
        if not value:
            return []
        return [value] if isinstance(value, str) else list(value)

    @property
    def _unknown_check_params_mode(self) -> str:
        """Strictness for unknown/forbidden check params (``dq_policy``, §7).

        One of ``warn`` (default, log and continue), ``error`` (fail the run),
        or ``ignore`` (restore pre-validation behavior). Honors the
        non-breaking decision: default ``warn``.
        """
        policy = self.cfg.get("dq_policy") or {}
        mode = str(policy.get("unknown_check_params", "warn")).lower()
        return mode if mode in ("warn", "error", "ignore") else "warn"

    def _suggest_key(self, check_name: str, key: str) -> str | None:
        """Closest valid key for a typo'd one, or None (edit-distance, §7)."""
        allowed = allowed_keys(self._suite_name, check_name)
        if not allowed:
            return None
        matches = difflib.get_close_matches(key, sorted(allowed), n=1, cutoff=0.6)
        return matches[0] if matches else None

    def _resolve_and_validate_cfg(self, check_name: str, cfg: dict) -> dict:
        """Resolve deprecated parameter aliases and validate the config surface.

        - Renames deprecated keys to their canonical names (recording each use,
          with the tables it occurred on, for the run summary) so existing
          configs keep working. Deprecations are NOT logged at resolution time;
          they are rendered as a dedicated summary section (verbose only).
        - Surfaces unknown top-level keys (likely typos, with a suggested
          correction) and forbidden keys per the ``unknown_check_params``
          strictness setting (§7): ``warn`` logs and continues, ``error``
          raises, ``ignore`` is silent.

        Returns the resolved config (an independent copy when aliases applied).
        """
        resolved, deprecations = resolve_aliases(self._suite_name, check_name, cfg)

        if deprecations:
            if self._deprecations_log is None:
                self._deprecations_log = {}
            table_id = getattr(self, "_current_table_fq", None) or "<unknown>"
            for dep in deprecations:
                key = (dep["check"], dep["old"], dep["new"])
                entry = self._deprecations_log.setdefault(
                    key,
                    {"check": dep["check"], "old": dep["old"], "new": dep["new"], "tables": set()},
                )
                entry["tables"].add(table_id)

        mode = self._unknown_check_params_mode
        if mode == "ignore":
            return resolved

        problems: list[str] = []
        for key in forbidden_keys_present(self._suite_name, check_name, resolved):
            problems.append(f"parameter '{key}' is not allowed")
        for key in unknown_keys(self._suite_name, check_name, resolved):
            suggestion = self._suggest_key(check_name, key)
            hint = f"did you mean '{suggestion}'?" if suggestion else "possible typo"
            problems.append(f"unknown parameter '{key}' ({hint})")

        if not problems:
            return resolved

        if mode == "error":
            raise CheckConfigError(f"[{self._suite_name}] check '{check_name}': " + "; ".join(problems))

        for problem in problems:
            logger.warning("[%s] check '%s': %s", self._suite_name, check_name, problem)
        return resolved

    def get_deprecations(self) -> list[dict[str, Any]]:
        """Return deprecated-parameter usages collected during the run.

        Each entry is ``{check, old, new, table_count}`` where ``table_count``
        is the number of distinct tables that used the deprecated key.
        """
        log = self._deprecations_log or {}
        return [
            {
                "check": entry["check"],
                "old": entry["old"],
                "new": entry["new"],
                "table_count": len(entry["tables"]),
            }
            for entry in log.values()
        ]

    def _build_check_instance_name(self, name: str, cfg: dict, idx: int, single: bool) -> str:
        """Build unique name for check instance."""
        if single:
            return name
        # Try common naming fields in order of priority
        for field in ("name", "key_columns", "key_column", "column", "gold_column", "columns"):
            if field in cfg:
                val = cfg[field]
                if isinstance(val, list):
                    val = ",".join(str(v) for v in val)
                return f"{name}[{field}={val}]"
        return f"{name}[{idx}]"

    def _is_enabled(self, cfg: dict) -> bool:
        """Check if a check instance is enabled."""
        return cfg.get("enabled", False)

    def _severity(self, cfg: dict) -> str:
        """Extract severity from check config."""
        return cfg.get("severity", "warning")

    def _blocking(self, cfg: dict) -> bool:
        """Extract blocking flag from check config."""
        return cfg.get("blocking", False)

    def _register_check(
        self,
        result: dict,
        name: str,
        passed: bool,
        severity: str,
        blocking: bool,
        details: str | None,
        column: str | None,
        failure_stats: dict | None = None,
        investigation_query: dict | None = None,
        execution: dict | None = None,
    ) -> None:
        """
        Register a check result in standardized format.

        Args:
            result: The result dict to append check to
            name: Check name/identifier
            passed: Whether the check passed
            severity: Severity level (critical, warning, info)
            blocking: Whether failure blocks pipeline
            details: Human-readable details (~50 chars)
            column: Column(s) involved in the check
            failure_stats: Aggregate failure statistics (NO values/IDs)
            investigation_query: SQL template for debugging
            execution: Execution trace (duration, row counts, filters)
        """
        check_result: dict[str, Any] = {
            "check_name": name,
            "passed": passed,
            "status": "PASS" if passed else "FAIL",
            "severity": severity,
            "blocking": blocking,
            "details": details,
            "column": column,
        }

        if not passed:
            if failure_stats:
                check_result["failure_stats"] = failure_stats
            if investigation_query:
                check_result["investigation_query"] = investigation_query

        # Execution trace - include for both pass and fail (useful for performance debugging)
        if execution:
            check_result["execution"] = execution

        result.setdefault("checks", []).append(check_result)

    def _build_alert_description(self, failed_checks: list[dict[str, Any]]) -> str | None:
        """Build alert description from failed checks."""
        if not failed_checks:
            return None
        return " | ".join(f"[{c['check_name']}] {c.get('details')}" for c in failed_checks)

    def _require_cfg(self, cfg: dict, *keys: str) -> tuple[bool, str | None]:
        """
        Validate required config keys.
        Returns (True, None) if all keys present, (False, error_msg) otherwise.
        """
        missing = [k for k in keys if k not in cfg or cfg[k] is None]
        if missing:
            return False, self._details_config_missing(*missing)
        return True, None

    # ------------------------------------------------------------------
    # Narrative Details Helpers (Uniform Message Format v1.1.0)
    # ------------------------------------------------------------------
    #
    # All messages start with a word (not a number) for consistency.
    # PASS messages: "All ...", "{Metric} OK: ..."
    # FAIL messages: "Found ...", "Missing ...", "{Metric} ..."
    # ------------------------------------------------------------------

    def _fmt_pct(self, count: int, total: int) -> str:
        """Format percentage with 1 decimal place."""
        if total == 0:
            return "0.0%"
        return f"{count / total * 100:.1f}%"

    # ------------------------------------------------------------------
    # PASS message helpers
    # ------------------------------------------------------------------

    def _details_pass_count(self, metric: str, count: int, items: str) -> str:
        """
        Build pass message for count-based checks.

        Pattern: "{Metric} OK: {count} {items}"

        Examples:
            _details_pass_count("Record count", 1000, "rows")
                -> "Record count OK: 1000 rows"
            _details_pass_count("Row count", 500, "")
                -> "Row count OK: 500"
        """
        if items:
            return f"{metric} OK: {count} {items}"
        return f"{metric} OK: {count}"

    def _details_pass_bounds(self, metric: str, value: Any, bounds: str) -> str:
        """
        Build pass message for bounded checks.

        Pattern: "{Metric} OK: {value} (within {bounds})"

        Examples:
            _details_pass_bounds("Distinct count", 15, "min=10, max=20")
                -> "Distinct count OK: 15 (within min=10, max=20)"
            _details_pass_bounds("Volume", 500, "min=100")
                -> "Volume OK: 500 (within min=100)"
        """
        return f"{metric} OK: {value} (within {bounds})"

    def _details_pass_all(self, count: int, items: str, condition: str) -> str:
        """
        Build pass message when all items satisfy a condition.

        Pattern: "All {count} {items} {condition}"

        Examples:
            _details_pass_all(50, "columns", "present and non-null")
                -> "All 50 columns present and non-null"
            _details_pass_all(1000, "rows", "unique")
                -> "All 1000 rows unique"
        """
        return f"All {count} {items} {condition}"

    def _details_pass_comparison(self, metric: str, observed: Any, op: str, expected: Any) -> str:
        """
        Build pass message for comparison checks.

        Pattern: "{Metric} OK: {observed} {op} {expected}"

        Examples:
            _details_pass_comparison("Metric 'revenue'", 1000, "==", 1000)
                -> "Metric 'revenue' OK: 1000 == 1000"
        """
        return f"{metric} OK: {observed} {op} {expected}"

    def _details_pass_freshness(self, metric: str, value: str, limit: str) -> str:
        """
        Build pass message for freshness checks.

        Pattern: "{Metric} OK: {value} (within {limit})"

        Examples:
            _details_pass_freshness("Data", "2.5h old", "24h limit")
                -> "Data OK: 2.5h old (within 24h limit)"
        """
        return f"{metric} OK: {value} (within {limit})"

    def _details_pass_reconciliation(
        self, gold: Any, silver: Any, diff_pct: float, tolerance: float | None = None
    ) -> str:
        """
        Build pass message for reconciliation checks.

        Pattern: "Reconciliation OK: gold={g}, silver={s} (diff {pct}%)"

        Examples:
            _details_pass_reconciliation(1000, 1005, 0.5)
                -> "Reconciliation OK: gold=1000, silver=1005 (diff 0.5%)"
        """
        base = f"Reconciliation OK: gold={gold}, silver={silver} (diff {diff_pct:.1f}%)"
        if tolerance is not None:
            base += f" within {tolerance}%"
        return base

    # ------------------------------------------------------------------
    # FAIL message helpers
    # ------------------------------------------------------------------

    def _details_fail_found(self, count: int, items: str, total: int) -> str:
        """
        Build fail message for violations found.

        Pattern: "Found {count} {items} ({pct}% of {total})"

        Examples:
            _details_fail_found(23, "null values", 500)
                -> "Found 23 null values (4.6% of 500)"
            _details_fail_found(50, "duplicate keys", 1000)
                -> "Found 50 duplicate keys (5.0% of 1000)"
        """
        pct = self._fmt_pct(count, total)
        return f"Found {count} {items} ({pct} of {total})"

    def _details_fail_missing(self, count: int, items: str, total: int | None = None) -> str:
        """
        Build fail message for missing items.

        Pattern: "Missing {count} {items}" or "Missing {count} {items} ({pct}% of {total})"

        Examples:
            _details_fail_missing(3, "required columns")
                -> "Missing 3 required columns"
            _details_fail_missing(50, "records in target", 1000)
                -> "Missing 50 records in target (5.0% of 1000)"
        """
        if total is not None and total > 0:
            pct = self._fmt_pct(count, total)
            return f"Missing {count} {items} ({pct} of {total})"
        return f"Missing {count} {items}"

    def _details_fail_exceeds(self, metric: str, value: str, limit: str) -> str:
        """
        Build fail message for threshold exceeded.

        Pattern: "{Metric} {value} exceeds {limit}"

        Examples:
            _details_fail_exceeds("Data age", "48.0h", "24h limit")
                -> "Data age 48.0h exceeds 24h limit"
            _details_fail_exceeds("Deviation", "45.0%", "40% limit")
                -> "Deviation 45.0% exceeds 40% limit"
        """
        return f"{metric} {value} exceeds {limit}"

    def _details_fail_below(self, metric: str, value: Any, limit: Any) -> str:
        """
        Build fail message for below minimum.

        Pattern: "{Metric} {value} below minimum {limit}"

        Examples:
            _details_fail_below("Volume", 50, 100)
                -> "Volume 50 below minimum 100"
            _details_fail_below("Distinct count", 5, 10)
                -> "Distinct count 5 below minimum 10"
        """
        return f"{metric} {value} below minimum {limit}"

    def _details_fail_above(self, metric: str, value: Any, limit: Any) -> str:
        """
        Build fail message for above maximum.

        Pattern: "{Metric} {value} above maximum {limit}"

        Examples:
            _details_fail_above("Row count", 1500, 1000)
                -> "Row count 1500 above maximum 1000"
        """
        return f"{metric} {value} above maximum {limit}"

    def _details_fail_mismatch(self, metric: str, expected: Any, observed: Any) -> str:
        """
        Build fail message for mismatches.

        Pattern: "{Metric} mismatch: expected {expected}, got {observed}"

        Examples:
            _details_fail_mismatch("Type", "binary", "string")
                -> "Type mismatch: expected binary, got string"
            _details_fail_mismatch("Distinct count", 100, 85)
                -> "Distinct count mismatch: expected 100, got 85"
        """
        return f"{metric} mismatch: expected {expected}, got {observed}"

    def _details_fail_deviation(self, metric: str, pct: float, limit: float) -> str:
        """
        Build fail message for deviation violations.

        Pattern: "{Metric} deviation {pct}% exceeds {limit}% limit"

        Examples:
            _details_fail_deviation("Count", 45.0, 40)
                -> "Count deviation 45.0% exceeds 40% limit"
        """
        return f"{metric} deviation {pct:.1f}% exceeds {limit}% limit"

    # ------------------------------------------------------------------
    # CONFIG/ERROR message helpers
    # ------------------------------------------------------------------

    def _details_config_missing(self, *keys: str) -> str:
        """
        Build error for missing config keys.

        Pattern: "Missing config: {keys}"

        Examples:
            _details_config_missing("column", "expected")
                -> "Missing config: column, expected"
        """
        return f"Missing config: {', '.join(keys)}"

    def _details_not_found(self, item: str, name: str) -> str:
        """
        Build error when item not found.

        Pattern: "{Item} '{name}' not found"

        Examples:
            _details_not_found("Column", "user_id")
                -> "Column 'user_id' not found"
        """
        return f"{item} '{name}' not found"

    def _details_error(self, message: str) -> str:
        """
        Build generic error message.

        Pattern: "Check error: {message}"

        Examples:
            _details_error("table not found")
                -> "Check error: table not found"
        """
        return f"Check error: {message}"

    def _details_no_data(self, item: str) -> str:
        """
        Build message for no data available.

        Pattern: "No data in {item}"

        Examples:
            _details_no_data("column 'timestamp'")
                -> "No data in column 'timestamp'"
        """
        return f"No data in {item}"

    def _details_insufficient(self, what: str, count: int, required: int) -> str:
        """
        Build message for insufficient data.

        Pattern: "Insufficient {what}: {count}/{required} required"

        Examples:
            _details_insufficient("history", 2, 3)
                -> "Insufficient history: 2/3 required"
        """
        return f"Insufficient {what}: {count}/{required} required"

    # ------------------------------------------------------------------
    # Deprecated methods (will be removed in v1.2.0)
    # ------------------------------------------------------------------

    def _details_count_failures(
        self,
        count: int,
        what: str,
        total: int | None = None,
    ) -> str:
        """
        DEPRECATED: Use _details_fail_found() or _details_fail_missing() instead.

        Kept for backward compatibility during migration.
        """
        if total is not None and total > 0:
            pct = self._fmt_pct(count, total)
            return f"Found {count} {what} ({pct} of {total})"
        return f"Found {count} {what}"

    def _details_threshold_violation(
        self,
        metric: str,
        value: Any,
        threshold: Any,
        direction: str = "exceeds",
    ) -> str:
        """
        DEPRECATED: Use _details_fail_exceeds(), _details_fail_below(),
        or _details_fail_above() instead.

        Kept for backward compatibility during migration.
        """
        direction_map = {
            "exceeds": f"{metric} {value} exceeds {threshold} limit",
            "below minimum": f"{metric} {value} below minimum {threshold}",
            "above maximum": f"{metric} {value} above maximum {threshold}",
        }
        return direction_map.get(direction, f"{metric} {value} {direction} {threshold}")

    def _details_expected_vs_observed(
        self,
        expected: Any,
        observed: Any,
        metric: str | None = None,
    ) -> str:
        """
        DEPRECATED: Use _details_fail_mismatch() instead.

        Kept for backward compatibility during migration.
        """
        if metric:
            return f"{metric} mismatch: expected {expected}, got {observed}"
        return f"Mismatch: expected {expected}, got {observed}"

    def _details_missing_invalid(
        self,
        count: int,
        items: str,
        details: str | None = None,
    ) -> str:
        """
        DEPRECATED: Use _details_fail_found() or specific helpers instead.

        Kept for backward compatibility during migration.
        """
        if "missing" in items.lower():
            base = f"Missing {count} {items.replace('missing', '').strip()}"
        else:
            base = f"Found {count} {items}"
        if details:
            return f"{base}: {details}"
        return base

    # ------------------------------------------------------------------
    # Deprecated methods (will be removed in v1.2.0)
    # ------------------------------------------------------------------

    def _build_details_v2(
        self,
        primary_metric: str,
        primary_value: Any,
        context: str | None = None,
    ) -> str:
        """
        DEPRECATED: Use _details_count_failures or _details_expected_vs_observed instead.
        """
        if context:
            return f"{primary_metric}={primary_value} ({context})"
        return f"{primary_metric}={primary_value}"

    def _build_comparison_details(
        self,
        observed: Any,
        expected: Any,
        operator: str,
    ) -> str:
        """
        DEPRECATED: Use _details_expected_vs_observed() instead.
        """
        if operator == "==":
            return f"Expected {expected}, got {observed}"
        elif operator == ">=":
            return f"{observed} is below {expected} minimum"
        elif operator == "<=":
            return f"{observed} exceeds {expected} maximum"
        elif operator == "between":
            return f"{observed} not in range [{expected[0]}, {expected[1]}]"
        elif operator == "~":
            return f"{observed} !≈ {expected}"
        return f"{observed} {operator} {expected}"

    def _build_failure_stats(
        self,
        failure_type: str,
        affected_columns: list[str],
        failure_count: int,
        total_checked: int,
        distribution: dict | None = None,
        diagnostic: dict | None = None,
        extra: dict | None = None,
    ) -> dict:
        """
        Build failure statistics (NO values, NO identifiers - privacy safe).

        Args:
            failure_type: Category of failure (e.g., "duplicate_key", "null_value")
            affected_columns: List of column names involved
            failure_count: Number of failures
            total_checked: Total rows/items checked
            distribution: Optional distribution buckets (statistics only)
            diagnostic: Optional diagnostic metrics (aggregates only)
            extra: Optional extra fields for specific check types

        Returns:
            Dict with failure statistics, safe for logging/storage.

        SECURITY: This method MUST NEVER include:
            - Row values (PII risk)
            - Primary keys / IDs (can be PII: SSN, email, phone)
            - Sample rows of any kind
        """
        stats: dict[str, Any] = {
            "failure_type": failure_type,
            "affected_columns": affected_columns,
            "failure_count": failure_count,
            "total_rows_checked": total_checked,
            "failure_percentage": round(failure_count / total_checked * 100, 3) if total_checked > 0 else 0,
        }

        if distribution:
            stats["distribution"] = distribution

        if diagnostic:
            stats["diagnostic"] = diagnostic

        if extra:
            stats.update(extra)

        return stats

    def _build_investigation_query(
        self,
        check_type: str,
        table_fq: str,
        affected_columns: list[str],
        cfg: dict,
        where_clause: str | None = None,
    ) -> dict:
        """
        Build investigation query template (NO actual data embedded).

        Args:
            check_type: Type of failure for template selection
            table_fq: Fully qualified table name
            affected_columns: Columns involved in the check
            cfg: Check configuration for parameter extraction
            where_clause: Optional WHERE clause to include

        Returns:
            Dict with description, template, and parameters.

        SECURITY: Templates contain placeholders, not actual data values.
        """
        templates = {
            "duplicate_key": {
                "description": "Find all duplicate keys with their occurrence counts",
                "template": (
                    "SELECT {key_columns}, count(*) as occurrence_count FROM {table} {where_clause} "
                    "GROUP BY {key_columns} HAVING count(*) > 1 ORDER BY occurrence_count DESC LIMIT 100"
                ),
            },
            "null_value": {
                "description": "Find rows with null values in required columns",
                "template": "SELECT * FROM {table} WHERE {null_conditions} {where_clause} LIMIT 100",
            },
            "orphan_reference": {
                "description": "Find orphan references (FK violations)",
                "template": (
                    "SELECT src.* FROM {table} src LEFT JOIN {target_table} tgt "
                    "ON src.{fk_column} = tgt.{pk_column} WHERE tgt.{pk_column} IS NULL {where_clause} LIMIT 100"
                ),
            },
            "invalid_transition": {
                "description": "Find invalid status transitions",
                "template": (
                    "WITH ordered AS (SELECT {key_column}, {status_column}, {timestamp_column}, "
                    "LAG({status_column}) OVER (PARTITION BY {key_column} ORDER BY {timestamp_column}) as prev_status "
                    "FROM {table}) SELECT prev_status, {status_column} as current_status, COUNT(*) as transition_count "
                    "FROM ordered WHERE prev_status IS NOT NULL "
                    "GROUP BY prev_status, {status_column} ORDER BY transition_count DESC"
                ),
            },
            "metric_mismatch": {
                "description": "Find groups where observed metric doesn't match expected",
                "template": (
                    "WITH observed AS (SELECT {group_columns}, {metric_expr} as observed_value "
                    "FROM {table} {where_clause} GROUP BY {group_columns}), "
                    "expected AS (SELECT {group_columns}, {expected_column} as expected_value FROM {expected_table}) "
                    "SELECT o.*, e.expected_value, "
                    "(o.observed_value - e.expected_value) / NULLIF(e.expected_value, 0) * 100 as deviation_pct "
                    "FROM observed o JOIN expected e USING ({group_columns}) "
                    "WHERE NOT (o.observed_value {operator} e.expected_value) "
                    "ORDER BY ABS(deviation_pct) DESC LIMIT 100"
                ),
            },
            "schema_violation": {
                "description": "Investigate JSON schema violations",
                "template": (
                    "SELECT CASE WHEN {column} IS NULL THEN 'null_value' "
                    "WHEN from_json({column}, '{schema}') IS NULL THEN 'parse_failure' END as violation_type, "
                    "COUNT(*) as violation_count FROM {table} {where_clause} "
                    "GROUP BY 1 HAVING violation_type IS NOT NULL ORDER BY violation_count DESC"
                ),
            },
            "rule_violation": {
                "description": "Find rows violating business rule",
                "template": "SELECT * FROM {table} WHERE NOT ({rule_expression}) {where_clause} LIMIT 100",
            },
            "record_count": {
                "description": "Verify record count",
                "template": "SELECT COUNT(*) as record_count FROM {table} {where_clause}",
            },
            "freshness": {
                "description": "Check data freshness",
                "template": (
                    "SELECT MAX({timestamp_column}) as latest_timestamp, "
                    "DATEDIFF(HOUR, MAX({timestamp_column}), current_timestamp()) as age_hours "
                    "FROM {table} {where_clause}"
                ),
            },
            "schema_drift": {
                "description": "Inspect table schema",
                "template": "DESCRIBE TABLE {table}",
            },
            "low_cardinality": {
                "description": "Find groups with row count below minimum",
                "template": (
                    "SELECT {group_columns}, count(*) as row_count FROM {table} {where_clause} "
                    "GROUP BY {group_columns} HAVING count(*) < {min_count} ORDER BY row_count ASC LIMIT 100"
                ),
            },
            "totals_mismatch": {
                "description": "Find header/detail total mismatches",
                "template": (
                    "SELECT h.{join_key}, h.{header_amount} as header_total, "
                    "COALESCE(d.detail_sum, 0) as detail_sum, "
                    "h.{header_amount} - COALESCE(d.detail_sum, 0) as difference "
                    "FROM {table} h LEFT JOIN (SELECT {join_key}, SUM({detail_amount}) as detail_sum "
                    "FROM {detail_table} GROUP BY {join_key}) d ON h.{join_key} = d.{join_key} "
                    "WHERE ABS(h.{header_amount} - COALESCE(d.detail_sum, 0)) > {tolerance} LIMIT 100"
                ),
            },
            "cross_table_mismatch": {
                "description": "Find rows with different values between tables",
                "template": (
                    "SELECT l.{join_key}, l.{left_column} as left_value, r.{right_column} as right_value, "
                    "l.{left_column} - r.{right_column} as difference "
                    "FROM {left_table} l JOIN {right_table} r ON l.{join_key} = r.{join_key} "
                    "WHERE ABS(l.{left_column} - r.{right_column}) > {tolerance} "
                    "ORDER BY ABS(difference) DESC LIMIT 100"
                ),
            },
            "temporal_violation": {
                "description": "Find rows violating timestamp order between tables",
                "template": (
                    "SELECT l.{join_key}, l.{left_timestamp} as left_ts, r.{right_timestamp} as right_ts, "
                    "DATEDIFF(MINUTE, r.{right_timestamp}, l.{left_timestamp}) as minutes_diff "
                    "FROM {left_table} l JOIN {right_table} r ON l.{join_key} = r.{join_key} "
                    "WHERE l.{left_timestamp} > r.{right_timestamp} "
                    "ORDER BY minutes_diff DESC LIMIT 100"
                ),
            },
            "row_temporal_violation": {
                "description": "Find rows where timestamp order is violated within same row",
                "template": (
                    "SELECT {key_columns}, {left_timestamp}, {right_timestamp}, "
                    "DATEDIFF(MINUTE, {left_timestamp}, {right_timestamp}) as minutes_diff "
                    "FROM {table} "
                    "WHERE {left_timestamp} < {right_timestamp} {where_clause} "
                    "ORDER BY minutes_diff DESC LIMIT 100"
                ),
            },
            "incomplete_flow": {
                "description": "Find source records missing in target (flow completeness)",
                "template": (
                    "SELECT src.* FROM {source_table} src "
                    "LEFT JOIN {target_table} tgt ON src.{join_key} = tgt.{join_key} "
                    "WHERE tgt.{join_key} IS NULL {source_filter} "
                    "ORDER BY src.{join_key} LIMIT 100"
                ),
            },
            "incomplete_flow_lateness": {
                "description": "Find missing records with lateness classification (late vs lost)",
                "template": (
                    "SELECT src.*, src.{lateness_column}, "
                    "DATEDIFF(MINUTE, src.{lateness_column}, current_timestamp()) as delay_minutes, "
                    "CASE WHEN DATEDIFF(MINUTE, src.{lateness_column}, current_timestamp()) <= {max_delay_minutes} "
                    "THEN 'late' ELSE 'lost' END as status "
                    "FROM {source_table} src "
                    "LEFT JOIN {target_table} tgt ON src.{join_key} = tgt.{join_key} "
                    "WHERE tgt.{join_key} IS NULL {source_filter} "
                    "ORDER BY delay_minutes DESC LIMIT 100"
                ),
            },
            "generic": {
                "description": "Investigate check failures",
                "template": "SELECT * FROM {table} {where_clause} LIMIT 100",
            },
        }

        tmpl = templates.get(check_type, templates["generic"])

        # Build parameters from config
        key_columns = cfg.get("key_columns", cfg.get("key_column", cfg.get("column", affected_columns)))
        if isinstance(key_columns, list):
            key_columns_str = ", ".join(key_columns)
        else:
            key_columns_str = key_columns or ", ".join(affected_columns)

        # Build null conditions for null checks
        null_conditions = " OR ".join(f"{col} IS NULL" for col in affected_columns) if affected_columns else "1=1"

        # Format where clause
        where_formatted = f"WHERE {where_clause}" if where_clause else ""
        if where_formatted and "{where_clause}" in tmpl["template"] and "WHERE" in tmpl["template"]:
            where_formatted = f"AND {where_clause}"

        parameters = {
            "table": table_fq,
            "key_columns": key_columns_str,
            "where_clause": where_formatted,
            "null_conditions": null_conditions,
        }

        # Add check-specific parameters
        for key in [
            "column",
            "timestamp_column",
            "status_column",
            "target_table",
            "fk_column",
            "pk_column",
            "metric_expr",
            "expected_column",
            "expected_table",
            "group_columns",
            "operator",
            "rule_expression",
            "schema",
        ]:
            if key in cfg:
                val = cfg[key]
                if isinstance(val, list):
                    parameters[key] = ", ".join(str(v) for v in val)
                else:
                    parameters[key] = val

        # Format template with available parameters to produce final query
        try:
            query = tmpl["template"].format(**{k: v for k, v in parameters.items() if v is not None})
        except KeyError:
            query = tmpl["template"]

        return {
            "description": tmpl["description"],
            "query": query,
        }

    def _build_execution_trace(
        self,
        start_time: datetime,
        rows_scanned: int | None = None,
        rows_after_filter: int | None = None,
        filters_applied: dict | None = None,
        table_info: dict | None = None,
        secondary_tables: list[dict] | None = None,
    ) -> dict:
        """
        Build execution trace for check result.

        Args:
            start_time: When the check started
            rows_scanned: Total rows in source table (uses _current_rows_before if None)
            rows_after_filter: Rows remaining after filters (uses _current_rows_after if None)
            filters_applied: Dict of filters that were applied (uses _current_filters_applied if None)
            table_info: Dict with catalog, schema, name (uses _current_table_info if None)
            secondary_tables: List of other tables read (for cross-table checks)

        Returns:
            Dict with execution context for debugging.
        """
        now = datetime.now(UTC)
        effective_rows_scanned = rows_scanned if rows_scanned is not None else self._current_rows_before
        trace: dict[str, Any] = {
            "duration_ms": int((now - start_time).total_seconds() * 1000),
            "started_at": start_time.isoformat().replace("+00:00", "Z"),
        }
        if effective_rows_scanned is not None:
            trace["rows_scanned"] = effective_rows_scanned

        effective_rows_after = rows_after_filter if rows_after_filter is not None else self._current_rows_after
        if effective_rows_after is not None:
            trace["rows_after_filter"] = effective_rows_after

        effective_filters = filters_applied if filters_applied is not None else self._current_filters_applied
        if effective_filters:
            trace["filters_applied"] = effective_filters

        effective_table_info = table_info if table_info is not None else self._current_table_info
        if effective_table_info:
            trace["table"] = effective_table_info

        if secondary_tables:
            trace["secondary_tables"] = secondary_tables

        # Include profile context only when profiles are used
        if self._profile_name:
            profile_info: dict[str, str] = {"name": self._profile_name}
            if self._profile_target_catalog:
                profile_info["target_catalog"] = self._profile_target_catalog
            trace["profile"] = profile_info

        return trace

    # ------------------------------------------------------------------
    # Profile context
    # ------------------------------------------------------------------

    def set_profile_context(self, profile_name: str, target_catalog: str | None = None) -> None:
        """
        Set the profile context for execution trace.

        When set, the profile info is included in the execution trace of each check result.
        Only set when profiles are actively used (not for default profile).

        Args:
            profile_name: Name of the active profile (e.g., "dev", "prod").
            target_catalog: Resolved catalog from profile (optional).
        """
        self._profile_name = profile_name
        self._profile_target_catalog = target_catalog

    # ------------------------------------------------------------------
    # Tag-based filtering
    # ------------------------------------------------------------------

    def set_tags_filter(self, tags: list[str] | None) -> None:
        """
        Set the tags filter for this suite.

        When set, only checks with matching tags will be executed.
        Checks without tags are skipped when filter is active.

        Args:
            tags: List of tags to filter by (e.g., ["critical", "daily"])
        """
        logger.info("Tags filter set: %s for suite %s", tags, self._suite_name)
        self._tags_filter = frozenset(tags) if tags else None

    def _should_run_check(self, cfg: dict) -> bool:
        """
        Determine if a check should run based on tags filter.

        Logic:
        - No filter set -> run all checks
        - Filter set + check has no tags -> skip (strict filtering)
        - Filter set + check has tags -> run if ANY tag matches (OR logic)

        Args:
            cfg: Check configuration dict (may contain "tags" key)

        Returns:
            True if the check should run, False to skip
        """
        if not self._tags_filter:
            return True  # No filter = run all

        check_tags = cfg.get("tags", [])
        if not check_tags:
            return False  # No tags on check = skip when filter is active

        # Run if any check tag matches any filter tag (intersection)
        return bool(set(check_tags) & self._tags_filter)

    # ------------------------------------------------------------------
    # Threshold Evaluation
    # ------------------------------------------------------------------

    def _evaluate_threshold(
        self,
        failed_count: int,
        total_count: int,
        cfg: dict[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        """
        Evaluate pass/fail considering threshold configuration.

        Args:
            failed_count: Number of rows/items that failed validation
            total_count: Total number of rows/items validated
            cfg: Check configuration (may contain 'threshold' key)

        Returns:
            tuple of (passed, threshold_metadata)
            - passed: Whether the check passes with threshold applied
            - threshold_metadata: Dict with threshold evaluation details

        Threshold precedence:
            1. Check-level threshold
            2. Suite-level default threshold (via self._suite_defaults)
            3. No threshold (zero tolerance)

        Configuration:
            threshold:
              max_failure_percent: 5.0  # Pass if <= 5% fail
              max_failure_count: 100    # Pass if <= 100 fail
              # Both can be specified (AND logic)
        """
        # Resolve effective threshold (check > suite default)
        threshold = cfg.get("threshold") or self._suite_defaults.get("threshold")

        if not threshold:
            # No threshold configured = original behavior (zero tolerance)
            return failed_count == 0, {}

        # Handle empty dataset
        if total_count == 0:
            return True, {"threshold_applied": True, "note": "no_data_to_validate"}

        max_pct = threshold.get("max_failure_percent")
        max_cnt = threshold.get("max_failure_count")

        # Calculate actual failure percentage
        failure_pct = (failed_count / total_count) * 100

        # Evaluate conditions
        passed_pct = True
        passed_cnt = True

        if max_pct is not None:
            passed_pct = failure_pct <= float(max_pct)

        if max_cnt is not None:
            passed_cnt = failed_count <= int(max_cnt)

        # Both conditions must pass (AND logic)
        passed = passed_pct and passed_cnt

        # Build metadata for details
        metadata: dict[str, Any] = {
            "threshold_applied": True,
            "failed_count": failed_count,
            "total_count": total_count,
            "failure_pct": round(failure_pct, 4),
        }

        if max_pct is not None:
            metadata["max_failure_pct"] = max_pct
            metadata["passed_pct_check"] = passed_pct

        if max_cnt is not None:
            metadata["max_failure_cnt"] = max_cnt
            metadata["passed_cnt_check"] = passed_cnt

        return passed, metadata

    def _build_threshold_details(
        self,
        base_details: str,
        threshold_metadata: dict[str, Any],
    ) -> str:
        """
        Append threshold metadata to base details string.

        Args:
            base_details: Original details string from check
            threshold_metadata: Dict from _evaluate_threshold

        Returns:
            Extended details string with threshold info
        """
        if not threshold_metadata:
            return base_details

        if threshold_metadata.get("note") == "no_data_to_validate":
            return f"{base_details}; threshold_applied=True; note=no_data_to_validate"

        threshold_parts = [
            "threshold_applied=True",
            f"failure_pct={threshold_metadata.get('failure_pct', 0):.2f}%",
        ]

        if "max_failure_pct" in threshold_metadata:
            threshold_parts.append(f"max_pct={threshold_metadata['max_failure_pct']}%")

        if "max_failure_cnt" in threshold_metadata:
            threshold_parts.append(f"max_cnt={threshold_metadata['max_failure_cnt']}")

        return f"{base_details}; {'; '.join(threshold_parts)}"

    # ------------------------------------------------------------------
    # Data Filtering (row_filter, partition_filter)
    # ------------------------------------------------------------------

    def _get_partition_columns(self, catalog: str, schema: str, table: str) -> list[str]:
        """
        Get partition columns from Delta table metadata.

        Uses DESCRIBE DETAIL to read partition information.
        Returns empty list if table is not Delta or metadata is unavailable.
        """
        try:
            fq_table = f"{catalog}.{schema}.{table}"
            detail = self.spark.sql(f"DESCRIBE DETAIL {fq_table}")
            partition_cols = detail.select("partitionColumns").first()[0]
            return partition_cols or []
        except Exception:
            # Non-Delta table, no access, or other error
            return []

    def _get_latest_partition_values(
        self,
        catalog: str,
        schema: str,
        table: str,
        partition_columns: list[str],
        latest_n: int = 1,
    ) -> dict[str, Any]:
        """
        Get the latest partition values from a table.

        For single partition column: returns the latest N values as a list (if N > 1).
        For multiple partition columns: returns the latest combination only.

        Args:
            catalog, schema, table: Table identifiers
            partition_columns: List of partition column names
            latest_n: Number of latest partitions to include (default: 1)

        Returns:
            Dict of {column: value} or {column: [values]} for the latest partition(s)
        """
        if not partition_columns:
            return {}

        fq_table = f"{catalog}.{schema}.{table}"

        if len(partition_columns) == 1:
            # Single partition column - can return multiple values
            col = partition_columns[0]
            # Safe: partition_columns comes from trusted Delta metadata (DESCRIBE DETAIL),
            # not user input. Column names are system-provided identifiers.
            query = f"""
                SELECT DISTINCT {col}
                FROM {fq_table}
                WHERE {col} IS NOT NULL
                ORDER BY {col} DESC
                LIMIT {latest_n}
            """  # nosec B608 - column names from trusted metadata
            rows = self.spark.sql(query).collect()
            if not rows:
                return {}
            values = [row[0] for row in rows]
            return {col: values[0] if len(values) == 1 else values}
        else:
            # Multiple partition columns - return latest combination
            cols_str = ", ".join(partition_columns)
            order_str = ", ".join(f"{c} DESC" for c in partition_columns)
            # Safe: partition_columns comes from trusted Delta metadata (DESCRIBE DETAIL),
            # not user input. Column names are system-provided identifiers.
            query = f"""
                SELECT DISTINCT {cols_str}
                FROM {fq_table}
                ORDER BY {order_str}
                LIMIT 1
            """  # nosec B608 - column names from trusted metadata
            row = self.spark.sql(query).first()
            if not row:
                return {}
            return {col: row[i] for i, col in enumerate(partition_columns)}

    def _validate_partition_filter(
        self,
        partition_filter: dict[str, Any],
        partition_columns: list[str],
    ) -> tuple[bool, list[str]]:
        """
        Validate that partition_filter uses actual partition columns.

        Args:
            partition_filter: Dict of column -> value(s)
            partition_columns: List of actual partition columns from metadata

        Returns:
            tuple of (all_valid, non_partition_cols)
            - all_valid: True if all filter columns are partition columns
            - non_partition_cols: List of columns that are NOT partitions
        """
        if not partition_columns:
            # Can't validate - metadata not available
            return True, []

        filter_cols = set(partition_filter.keys())
        partition_set = set(partition_columns)
        non_partition = filter_cols - partition_set

        return len(non_partition) == 0, list(non_partition)

    def _build_partition_filter(self, partition_filter: dict[str, Any]) -> str:
        """
        Build SQL WHERE condition from partition filter dict.

        Supports:
        - Single values: {date: "2024-01-15"} -> "date = '2024-01-15'"
        - Lists: {date: ["2024-01-14", "2024-01-15"]} -> "date IN ('2024-01-14', '2024-01-15')"
        - Multiple columns: {year: 2024, month: 1} -> "year = 2024 AND month = 1"
        - Integers: {year: 2024} -> "year = 2024"

        Raises:
            ValueError: If a filter value is an empty list
        """
        conditions = []
        for col_name, val in partition_filter.items():
            if isinstance(val, list):
                if not val:
                    raise ValueError(
                        f"Invalid partition_filter for column '{col_name}': empty list values are not allowed."
                    )
                # Escape single quotes in string values to prevent SQL injection
                quoted = []
                for v in val:
                    if isinstance(v, str):
                        escaped = v.replace("'", "''")
                        quoted.append(f"'{escaped}'")
                    else:
                        quoted.append(str(v))
                conditions.append(f"{col_name} IN ({', '.join(quoted)})")
            elif isinstance(val, str):
                # Escape single quotes in string values
                escaped = val.replace("'", "''")
                conditions.append(f"{col_name} = '{escaped}'")
            else:
                conditions.append(f"{col_name} = {val}")
        return " AND ".join(conditions)

    def _resolve_effective_filters(
        self, table_cfg: dict[str, Any]
    ) -> tuple[str | None, dict[str, Any] | None, str | None]:
        """
        Resolve effective row_filter and partition_filter with inheritance.

        Precedence: Table-level > Suite-level defaults

        Returns:
            tuple of (row_filter_sql, partition_filter_dict, partition_filter_sql)
        """
        defaults = self._suite_defaults

        # Row filter: table overrides suite default.
        # Use key-presence checks so an explicit None at table level disables the default.
        row_filter = table_cfg["row_filter"] if "row_filter" in table_cfg else defaults.get("row_filter")

        # Partition filter: table overrides suite default, convert dict to SQL.
        # Use key-presence checks so an explicit None at table level disables the default.
        partition_filter = (
            table_cfg["partition_filter"] if "partition_filter" in table_cfg else defaults.get("partition_filter")
        )
        partition_filter_sql = None
        if partition_filter:
            partition_filter_sql = self._build_partition_filter(partition_filter)

        return row_filter, partition_filter, partition_filter_sql

    def _apply_filters(
        self,
        df: Any,
        row_filter: str | None,
        partition_filter_sql: str | None,
    ) -> tuple[Any, dict[str, Any]]:
        """
        Apply row and partition filters to DataFrame.

        Args:
            df: Input DataFrame
            row_filter: SQL WHERE expression for row filtering
            partition_filter_sql: SQL WHERE expression for partition filtering

        Returns:
            tuple of (filtered_df, filter_metadata)
        """
        filter_metadata: dict[str, Any] = {"filters_applied": {}}

        # Apply partition filter first (for pruning optimization)
        if partition_filter_sql:
            df = df.filter(partition_filter_sql)
            filter_metadata["filters_applied"]["partition_filter"] = partition_filter_sql

        # Apply row filter
        if row_filter:
            df = df.filter(row_filter)
            filter_metadata["filters_applied"]["row_filter"] = row_filter

        return df, filter_metadata

    def _apply_table_filters(
        self,
        df: Any,
        table_cfg: dict[str, Any],
        catalog: str,
        schema: str,
        table: str,
    ) -> tuple[Any, dict[str, Any] | None, str | None]:
        """
        Apply and validate table filters, returning filtered DataFrame and check info.

        Supports auto-discovery of partitions:
        - partition_filter: {auto: true} -> filter on latest partition
        - partition_filter: {auto_latest: 7} -> filter on latest 7 partitions

        Returns:
            tuple of (filtered_df, filters_applied_dict, check_details)
            - If no filters configured, returns (df, None, None)
            - check_details contains status and any warnings/info
        """
        row_filter, partition_filter, partition_filter_sql = self._resolve_effective_filters(table_cfg)

        # Handle auto partition discovery
        auto_info = None
        if partition_filter:
            is_auto = partition_filter.get("auto", False)
            auto_latest_n = partition_filter.get("auto_latest")

            if is_auto or auto_latest_n:
                # Auto-discover partitions
                partition_columns = self._get_partition_columns(catalog, schema, table)
                if not partition_columns:
                    # No partitions found - skip partition filter
                    auto_info = "auto_partition=skipped; reason=no_partitions_found"
                    partition_filter = None
                    partition_filter_sql = None
                else:
                    # Get latest partition values
                    latest_n = auto_latest_n if auto_latest_n else 1
                    partition_filter = self._get_latest_partition_values(
                        catalog, schema, table, partition_columns, latest_n
                    )
                    if partition_filter:
                        partition_filter_sql = self._build_partition_filter(partition_filter)
                        auto_info = (
                            f"auto_partition=resolved; partitions={partition_columns}; values={partition_filter}"
                        )
                    else:
                        auto_info = "auto_partition=skipped; reason=no_partition_values_found"
                        partition_filter = None
                        partition_filter_sql = None

        if not row_filter and not partition_filter_sql:
            # Return auto_info if we tried auto but found nothing
            if auto_info:
                return df, {"auto_info": auto_info}, f"status=ok; {auto_info}"
            return df, None, None

        # Validate partition_filter columns against table metadata (skip for auto mode)
        partition_warning = None
        if partition_filter and not auto_info:
            partition_columns = self._get_partition_columns(catalog, schema, table)
            all_valid, non_partition_cols = self._validate_partition_filter(partition_filter, partition_columns)
            if not all_valid:
                partition_warning = f"non_partition_columns={non_partition_cols}; actual_partitions={partition_columns}"

        # Count rows before filtering (for metadata)
        rows_before = df.count()

        # Apply filters
        df, filter_metadata = self._apply_filters(df, row_filter, partition_filter_sql)

        # Count rows after filtering
        rows_after = df.count()

        # Calculate filter ratio
        filter_ratio = f"{rows_after / rows_before:.2%}" if rows_before > 0 else "N/A"

        # Build check details with extended metadata
        details_parts = [
            ("status", "ok"),
            ("row_filter", row_filter or "none"),
            ("partition_filter", partition_filter_sql or "none"),
            ("rows_before", rows_before),
            ("rows_after", rows_after),
            ("filter_ratio", filter_ratio),
        ]

        # Add auto-discovery info
        if auto_info:
            details_parts.append(("auto_discovery", auto_info))

        # Add partition validation warning
        if partition_warning:
            details_parts.append(("warning", partition_warning))

        check_details = "; ".join(f"{k}={v}" for k, v in details_parts)

        # Include row counts in filters_applied for execution trace
        filters_applied = filter_metadata["filters_applied"]
        filters_applied["rows_before"] = rows_before
        filters_applied["rows_after"] = rows_after

        return df, filters_applied, check_details

    # ------------------------------------------------------------------
    # Template methods
    # ------------------------------------------------------------------

    def validate_table(self, table_cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Perform quality checks on a specific table.
        Template method: uses hooks for layer-specific behavior.
        """
        catalog, schema, table = (
            table_cfg["catalog"],
            table_cfg["schema"],
            table_cfg["table"],
        )
        fq_table = f"{catalog}.{schema}.{table}"

        # Reset table-level context
        self._current_table_fq = None
        self._current_table_info = None
        self._current_where_clause = None
        self._current_rows_before = None
        self._current_rows_after = None
        self._current_filters_applied = None

        run_ts = datetime.now(UTC)
        result: dict[str, Any] = {
            "test_suite": self._suite_name,
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "table_fullname": fq_table,
            "run_timestamp": run_ts,
            "checks": [],
        }

        checks = dict(self._default_checks)
        checks.update(table_cfg.get("checks", {}))

        try:
            df = self._read_table(catalog, schema, table)
            self._register_check(
                result,
                name="table_read",
                passed=True,
                severity="critical",
                blocking=True,
                details="status=ok",
                column=None,
            )
        except Exception as e:
            self._register_check(
                result,
                "table_read",
                False,
                "critical",
                True,
                str(e),
                None,
            )
            result["passed"] = False
            return result

        # ------------------------------------------------------------------
        # Apply data filters (row_filter, partition_filter)
        # ------------------------------------------------------------------
        try:
            df, filters_applied, _ = self._apply_table_filters(df, table_cfg, catalog, schema, table)

            if filters_applied:
                result["filters_applied"] = filters_applied

                # Check if filtering returned empty DataFrame (use rows_after to avoid extra Spark action)
                rows_after = filters_applied.get("rows_after", 0)
                if rows_after == 0:
                    self._register_check(
                        result,
                        name="data_filter",
                        passed=True,
                        severity="warning",
                        blocking=False,
                        details="status=no_rows_after_filter",
                        column=None,
                    )
                    result["passed"] = True
                    return result

                # Simple marker check - detailed info is in execution trace of each check
                self._register_check(
                    result,
                    name="data_filter",
                    passed=True,
                    severity="info",
                    blocking=False,
                    details="status=ok",
                    column=None,
                )
        except Exception as e:
            self._register_check(
                result,
                "data_filter",
                False,
                "critical",
                True,
                f"filter_error: {e}",
                None,
            )
            result["passed"] = False
            return result

        table_ctx = {"catalog": catalog, "schema": schema, "table": table}

        self._current_table_fq = fq_table
        self._current_table_info = {"catalog": catalog, "schema": schema, "name": table}
        filters_applied = result.get("filters_applied", {})
        row_filter = filters_applied.get("row_filter")
        partition_filter = filters_applied.get("partition_filter")
        if row_filter and partition_filter:
            self._current_where_clause = f"({partition_filter}) AND ({row_filter})"
        elif partition_filter:
            self._current_where_clause = partition_filter
        else:
            self._current_where_clause = row_filter

        # Set filter context for execution trace (used by _build_execution_trace)
        self._current_rows_before = filters_applied.get("rows_before")
        self._current_rows_after = filters_applied.get("rows_after")
        # Exclude row counts from filters_applied dict (they go in separate trace fields)
        self._current_filters_applied = (
            {k: v for k, v in filters_applied.items() if k not in ("rows_before", "rows_after")}
            if filters_applied
            else None
        )

        for name, raw_cfg in checks.items():
            instances = self._normalize_check_cfg(raw_cfg)
            single_instance = len(instances) == 1

            for idx, cfg in enumerate(instances):
                if not self._is_enabled(cfg):
                    continue

                if not self._should_run_check(cfg):
                    continue

                resolved_cfg = self._resolve_and_validate_cfg(name, cfg)

                severity = self._severity(resolved_cfg)
                blocking = self._blocking(resolved_cfg)
                # Identity from the as-written cfg: keeps check IDs stable for legacy keys.
                check_id = self._build_check_instance_name(name, cfg, idx, single_instance)

                self._last_failure_stats = None
                self._last_investigation_query = None
                self._last_execution = None

                try:
                    func = getattr(self, f"_check_{name}")
                    passed, details, column = self._dispatch_check(func, name, df, resolved_cfg, table_ctx, fq_table)
                except Exception as e:
                    passed, details, column = False, str(e), None

                self._register_check(
                    result,
                    check_id,
                    passed,
                    severity,
                    blocking,
                    details,
                    column,
                    failure_stats=self._last_failure_stats,
                    investigation_query=self._last_investigation_query,
                    execution=self._last_execution,
                )

        failed = [c for c in result["checks"] if not c["passed"]]
        result["passed"] = len(failed) == 0
        result["alert_description"] = self._build_alert_description(failed)

        return result

    def run(self) -> list[dict[str, Any]]:
        """
        Run the full validation process for all tables.
        Template method: uses hooks for layer-specific row building.
        """
        all_rows: list[dict[str, Any]] = []
        failed_rows: list[dict[str, Any]] = []

        for table_cfg in self._tables_config:
            res = self.validate_table(table_cfg)

            for c in res["checks"]:
                row = self._build_row_from_check(res, c, res["run_timestamp"])
                all_rows.append(row)

                if not c["passed"]:
                    failed_rows.append(self._build_failed_row(row, res))

        if self._data_to_save:
            self.writer.write(all_rows=all_rows, failed_rows=failed_rows, data_to_save=self._data_to_save)

        return all_rows

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def build_summary(self, results: list[dict[str, Any]]) -> dict[str, Any]:
        """Build summary from validation results."""
        summary = BaseSummaryEngine().summarize(results)
        deprecations = self.get_deprecations()
        if deprecations:
            summary["deprecations"] = deprecations
        return summary
