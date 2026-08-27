# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections import defaultdict
from typing import Any

_SUITE_NAMES = ["great_expectations", "bronze", "silver", "gold"]
# Layer a table is grouped under in the "Tables by Layer" section. GE is an
# overlay on top of a base layer, so it is not a layer of its own.
_LAYER_ORDER = ["bronze", "silver", "gold"]

# Column-name keywords that hint at personal data (best-effort, name-based only).
_PII_KEYWORDS = ("email", "phone", "ssn", "passport", "address", "first_name", "last_name", "full_name", "birth")
# A column whose null percentage is at or above this is flagged as a potential issue.
_HIGH_NULL_PCT = 40.0
# distinct_ratio at or above this hints at a primary-key candidate.
_PK_CANDIDATE_RATIO = 0.98
# distinct_count at or below this (for non-numeric, non-pk columns) hints at an enum.
_ENUM_MAX_DISTINCT = 20


def _check_meta(value: Any) -> tuple[bool | None, str | None]:
    """Extract (enabled, confidence) from a single check value.

    Handles three shapes:
    - Native check dict: {"enabled": bool, ...} — direct flag at top level.
    - GE column entry: {"not_null": {"enabled": bool}, "dtype": {"enabled": bool}, ...}
      — a dict of named sub-rules each carrying their own enabled flag. Treated as
      enabled if any sub-rule is enabled.
    - List of dicts (e.g. distinct_count, json_schema): enabled if any entry is enabled.
    """
    if isinstance(value, dict):
        if "enabled" in value:
            return value.get("enabled"), value.get("_confidence")
        # GE column entry: sub-rules keyed by rule name
        sub_rules = [v for v in value.values() if isinstance(v, dict) and "enabled" in v]
        if sub_rules:
            return any(r.get("enabled") for r in sub_rules), None
        return None, None
    if isinstance(value, list):
        entries = [e for e in value if isinstance(e, dict)]
        if not entries:
            return None, None
        enabled = any(e.get("enabled") for e in entries)
        return enabled, entries[0].get("_confidence")
    return None, None


def _ge_column_has_disabled_rules(value: Any) -> bool:
    """Return True if a GE column check value has at least one disabled sub-rule."""
    if not isinstance(value, dict):
        return False
    if "enabled" in value:
        return value.get("enabled") is False
    return any(isinstance(v, dict) and v.get("enabled") is False for v in value.values())


def _layer_of(suggestions: dict) -> str:
    """Return the base layer for a table based on which suite has checks."""
    for layer in _LAYER_ORDER:
        if layer in suggestions and suggestions[layer].get("checks"):
            return layer
    return "other"


class ProfilingSummaryMixin:
    """Generates a PROFILING_SUMMARY.md markdown report from suggest_results."""

    def _write_suggest_summary(self, suggest_results: list[dict], output_path: str) -> str:
        """Build and write PROFILING_SUMMARY.md from suggest_results.

        Args:
            suggest_results: List of profiling result dicts. Each has keys:
                catalog, schema, table, row_count, suggestions (dict with suite keys).
                Results with an "error" key are skipped.
            output_path: Directory path where PROFILING_SUMMARY.md will be written.

        Returns:
            The markdown string written to disk.
        """
        valid_results = [r for r in suggest_results if "error" not in r]
        error_count = len(suggest_results) - len(valid_results)

        total_tables = len(valid_results)
        total_rows = sum(r.get("row_count", 0) for r in valid_results)

        total_checks = 0
        total_enabled = 0
        total_disabled = 0
        for r in valid_results:
            for suite in _SUITE_NAMES:
                checks = r.get("suggestions", {}).get(suite, {}).get("checks", {})
                for value in checks.values():
                    total_checks += 1
                    enabled, _ = _check_meta(value)
                    if enabled:
                        total_enabled += 1
                    else:
                        total_disabled += 1

        layer_counts: dict[str, int] = dict.fromkeys(_LAYER_ORDER, 0)
        for r in valid_results:
            layer = _layer_of(r.get("suggestions", {}))
            if layer in layer_counts:
                layer_counts[layer] += 1

        lines: list[str] = []
        lines.append("# DQ Config Suggestions Summary")
        lines.append("")

        self._append_overview(
            lines, total_tables, error_count, total_rows, total_checks, total_enabled, total_disabled, layer_counts
        )
        self._append_tables_by_layer(lines, valid_results)
        self._append_checks_by_suite(lines, valid_results)
        self._append_manual_config(lines, valid_results)
        self._append_schema_highlights(lines, valid_results)
        self._append_data_quality_observations(lines, valid_results)
        self._append_quick_start(lines, output_path)
        self._append_review_checklist(lines)

        md = "\n".join(lines)
        self._write_text_file(f"{output_path}/PROFILING_SUMMARY.md", md)  # type: ignore[attr-defined]
        return md

    # ------------------------------------------------------------------
    # Sections
    # ------------------------------------------------------------------

    def _append_overview(
        self,
        lines: list[str],
        total_tables: int,
        error_count: int,
        total_rows: int,
        total_checks: int,
        total_enabled: int,
        total_disabled: int,
        layer_counts: dict[str, int],
    ) -> None:
        layer_parts = [f"{count} {layer}" for layer, count in layer_counts.items() if count]
        layer_str = " · ".join(layer_parts) if layer_parts else "0 tables"

        lines.append("## Overview")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Tables profiled | {total_tables} ({layer_str}) |")
        lines.append(f"| Total rows scanned | {total_rows:,} |")
        lines.append(f"| Checks suggested | {total_checks} — {total_enabled} enabled, {total_disabled} disabled |")
        if error_count:
            lines.append(f"| Tables with errors | {error_count} |")
        lines.append("")

    def _append_tables_by_layer(self, lines: list[str], valid_results: list[dict]) -> None:
        lines.append("## Tables by Layer")
        lines.append("")

        grouped: dict[str, list[dict]] = {}
        for r in valid_results:
            layer = _layer_of(r.get("suggestions", {}))
            grouped.setdefault(layer, []).append(r)

        for layer in (*_LAYER_ORDER, "other"):
            results = grouped.get(layer, [])
            if not results:
                continue
            lines.append(f"### {layer.capitalize()} ({len(results)} tables)")
            lines.append("")
            lines.append("| Table | Rows | Columns | Suites |")
            lines.append("|-------|------|---------|--------|")
            for r in results:
                suggestions = r.get("suggestions", {})
                fq_table = f"{r['catalog']}.{r['schema']}.{r['table']}"
                rows = r.get("row_count", 0)
                cols = suggestions.get("column_count", 0)
                suites_present = [s for s in _SUITE_NAMES if s in suggestions and suggestions[s].get("checks")]
                suites_str = ", ".join(suites_present) if suites_present else "-"
                lines.append(f"| {fq_table} | {rows:,} | {cols} | {suites_str} |")
            lines.append("")

    def _append_checks_by_suite(self, lines: list[str], valid_results: list[dict]) -> None:
        lines.append("## Checks by Suite")
        lines.append("")
        lines.append("| Suite | Tables | Checks | Enabled | Disabled |")
        lines.append("|-------|--------|--------|---------|----------|")
        for suite in _SUITE_NAMES:
            suite_tables = 0
            suite_checks = 0
            enabled_count = 0
            disabled_count = 0
            for r in valid_results:
                checks = r.get("suggestions", {}).get(suite, {}).get("checks", {})
                if not checks:
                    continue
                suite_tables += 1
                suite_checks += len(checks)
                for value in checks.values():
                    enabled, _confidence = _check_meta(value)
                    if enabled:
                        enabled_count += 1
                    else:
                        disabled_count += 1
            lines.append(f"| {suite} | {suite_tables} | {suite_checks} | {enabled_count} | {disabled_count} |")
        lines.append("")

    def _append_manual_config(self, lines: list[str], valid_results: list[dict]) -> None:
        # Group by (suite, check_name) → table count. GE disabled rules are summarized as one row.
        grouped: dict[tuple[str, str], int] = {}
        ge_tables_with_disabled: set[str] = set()

        for r in valid_results:
            suggestions = r.get("suggestions", {})
            fq_table = f"{r['catalog']}.{r['schema']}.{r['table']}"
            for suite in _SUITE_NAMES:
                checks = suggestions.get(suite, {}).get("checks", {})
                for check_name, value in checks.items():
                    if suite == "great_expectations":
                        if _ge_column_has_disabled_rules(value):
                            ge_tables_with_disabled.add(fq_table)
                    else:
                        enabled, _ = _check_meta(value)
                        if enabled is False:
                            key = (suite, check_name)
                            grouped[key] = grouped.get(key, 0) + 1

        lines.append("## Disabled Checks to Review")
        lines.append("")
        lines.append("Grouped by check — enable after manual validation.")
        lines.append("")

        if not grouped and not ge_tables_with_disabled:
            lines.append("None.")
            lines.append("")
            return

        lines.append("| Check | Suite | Tables affected |")
        lines.append("|-------|-------|----------------|")

        suite_order = {s: i for i, s in enumerate(_SUITE_NAMES)}
        for (suite, check_name), count in sorted(
            grouped.items(), key=lambda x: (suite_order.get(x[0][0], 99), x[0][1])
        ):
            lines.append(f"| {check_name} | {suite} | {count} |")

        if ge_tables_with_disabled:
            lines.append(f"| (all column rules) | great_expectations | {len(ge_tables_with_disabled)} |")

        lines.append("")

    def _append_data_quality_observations(self, lines: list[str], valid_results: list[dict]) -> None:
        # Group by (issue_text, severity, column) → list of short table names to avoid repetition.
        obs: dict[tuple[str, str, str], list[str]] = defaultdict(list)

        for r in valid_results:
            short_table = f"{r['schema']}.{r['table']}"
            row_count = r.get("row_count", 0)
            for col, stats in r.get("column_stats", {}).items():
                if not isinstance(stats, dict):
                    continue
                null_pct = stats.get("null_pct", 0) or 0
                distinct_count = stats.get("distinct_count", 0) or 0
                if null_pct >= _HIGH_NULL_PCT:
                    obs[(f"High null ratio ({null_pct:.0f}%)", "warning", col)].append(short_table)
                if any(kw in col.lower() for kw in _PII_KEYWORDS):
                    obs[("Possible PII (column name)", "warning", col)].append(short_table)
                if row_count > 0 and distinct_count == 1:
                    obs[("Single value column", "info", col)].append(short_table)

        inline_limit = 2
        rows: list[str] = []
        for (issue, severity, col), tables in obs.items():
            table_str = ", ".join(tables) if len(tables) <= inline_limit else f"{len(tables)} tables"
            rows.append(f"| {issue} | {severity} | {col} | {table_str} |")

        lines.append("## Data Quality Observations")
        lines.append("")
        if not rows:
            lines.append("No potential issues detected.")
            lines.append("")
            return
        lines.append("| Issue | Severity | Column | Tables |")
        lines.append("|-------|----------|--------|--------|")
        lines.extend(rows)
        lines.append("")

    def _append_schema_highlights(self, lines: list[str], valid_results: list[dict]) -> None:
        json_cols: list[str] = []
        date_cols: list[str] = []
        pk_cols: list[str] = []
        enum_cols: list[str] = []

        for r in valid_results:
            table_name = r["table"]
            for col, stats in r.get("column_stats", {}).items():
                if not isinstance(stats, dict):
                    continue
                qualified = f"{table_name}.{col}"
                if stats.get("is_json"):
                    json_cols.append(qualified)
                if stats.get("type") == "date":
                    date_cols.append(qualified)
                distinct_ratio = stats.get("distinct_ratio", 0) or 0
                distinct_count = stats.get("distinct_count", 0) or 0
                if distinct_ratio >= _PK_CANDIDATE_RATIO:
                    pk_cols.append(qualified)
                elif stats.get("type") != "numeric" and 1 < distinct_count <= _ENUM_MAX_DISTINCT:
                    enum_cols.append(qualified)

        lines.append("## Schema Highlights")
        lines.append("")
        lines.append("| Observation | Count |")
        lines.append("|-------------|-------|")
        lines.append(f"| JSON columns | {len(json_cols)} |")
        lines.append(f"| Date/timestamp columns | {len(date_cols)} |")
        lines.append(f"| PK candidates | {len(pk_cols)} |")
        lines.append(f"| Enum-like columns | {len(enum_cols)} |")
        lines.append("")

    def _append_quick_start(self, lines: list[str], output_path: str) -> None:
        lines.append("## Quick Start")
        lines.append("")
        lines.append("```python")
        lines.append("from neco_dq.runner import run_dq")
        lines.append("")
        lines.append("# Run DQ with the generated config")
        lines.append(f'run_dq("{output_path}")')
        lines.append("")
        lines.append("# Run a specific suite only")
        lines.append(f'run_dq("{output_path}", run_suites=["silver"])')
        lines.append("```")
        lines.append("")

    def _append_review_checklist(self, lines: list[str]) -> None:
        lines.append("## Review Checklist")
        lines.append("")
        lines.append("- [ ] Verify `idempotency` key columns match actual primary keys")
        lines.append("- [ ] Configure `history_table` for anomaly/volume checks")
        lines.append("- [ ] Check `freshness` thresholds match your SLAs")
        lines.append("- [ ] Review disabled checks listed under Disabled Checks to Review")
        lines.append("- [ ] Adjust `between` ranges for numeric columns")
        lines.append("")
