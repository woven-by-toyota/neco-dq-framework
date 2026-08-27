# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from neco_dq.test_suites.profiling.constants import _MIN_DATE_COLS_FOR_PAIR, _NARROW_LENGTH_RANGE


class GeSuggestionsMixin:
    """Mixin providing Great Expectations suggestion-builder methods."""

    def _build_ge_column_rules(
        self,
        col: str,
        col_type: str,
        not_null_cols: list[str],
        unique_cols: list[str],
        allowed_values_by_col: dict[str, list[str]],
        range_by_col: dict[str, dict],
        length_data: dict[str, dict] | None = None,
        pk_columns: list[str] | None = None,
        date_format_by_col: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Build GE rules for a single column."""
        col_rules: dict[str, Any] = {}
        pk_set = set(pk_columns or [])
        date_format_by_col = date_format_by_col or {}

        # dtype: validate data type
        # Note: Spark reports LongType as "bigint", not "long"
        dtype_map = {
            "numeric": ["int", "bigint", "long", "double", "float"],
            "string": ["str", "string"],
            "date": ["datetime64[ns]", "timestamp"],
            "boolean": ["bool", "boolean"],
        }
        if col_type in dtype_map:
            col_rules["dtype"] = {"enabled": True, "values": dtype_map[col_type], "severity": "error"}

        # not_null: columns with 0% nulls
        if col in not_null_cols:
            col_rules["not_null"] = {"enabled": True, "severity": "warning"}

        # unique: distinct ratio is approximate (approx_count_distinct), so a column
        # that merely looks unique in the snapshot is only a candidate. Enable a
        # blocking unique check ONLY when the column is part of a Unity Catalog
        # primary key (authoritative). Otherwise suggest it disabled for review,
        # so we never block a pipeline on a guess.
        if col in pk_set:
            col_rules["unique"] = {"enabled": True, "severity": "error", "blocking": True}
        elif col in unique_cols:
            col_rules["unique"] = {
                "enabled": False,
                "severity": "error",
                "_comment": "Candidate unique column (from approximate profiling). Review, then enable.",
            }

        # values_set: ONLY for columns that look like enumerations
        if col in allowed_values_by_col and self._is_enum_column(col):
            col_rules["values_set"] = {"enabled": True, "values": allowed_values_by_col[col], "severity": "warning"}

        # between: numeric columns with clear bounds
        # Disabled by default - values are from profiling snapshot and may need adjustment
        if col in range_by_col:
            rc = range_by_col[col]
            col_rules["between"] = {
                "enabled": False,
                "min": rc["min"],
                "max": rc["max"],
                "severity": "warning",
            }

        # regex: suggest for string columns that look like IDs/emails
        if col_type == "string":
            regex_rule = self._suggest_regex_rule(col)
            if regex_rule:
                col_rules["regex"] = regex_rule

            # length: narrow range string columns (added in v1.2.2, §7)
            # Only when length stats available and max_length - min_length <= 2
            ld = (length_data or {}).get(col)
            if ld is not None:
                min_len = ld.get("min_length")
                max_len = ld.get("max_length")
                if min_len is not None and max_len is not None and (max_len - min_len) <= _NARROW_LENGTH_RANGE:
                    col_rules["length"] = {
                        "enabled": False,
                        "min": min_len,
                        "max": max_len,
                        "severity": "warning",
                    }

            # date_format: string columns holding dates in a consistent strftime format.
            # Disabled by default - the detected format is inferred from a sample and
            # the reviewer should confirm it before enabling.
            detected_format = date_format_by_col.get(col)
            if detected_format:
                col_rules["date_format"] = {
                    "enabled": False,
                    "format": detected_format,
                    "severity": "warning",
                    "_reason": f"Sampled values match the strftime format {detected_format}",
                }

        return col_rules

    def _suggest_regex_rule(self, col: str) -> dict[str, Any] | None:
        """Suggest regex rule based on column name patterns."""
        col_lower = col.lower()
        if "email" in col_lower:
            return {
                "enabled": False,
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "severity": "warning",
            }
        if "id" in col_lower or "uuid" in col_lower:
            return {"enabled": False, "pattern": r"^[a-zA-Z0-9_-]+$", "severity": "warning"}
        return None

    def _build_ge_dataset_rules(
        self,
        column_stats: dict[str, dict],
        unique_cols: list[str],
        row_count: int,
        pk_columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Build GE dataset-level rules."""
        dataset_rules: dict[str, Any] = {}
        pk_columns = pk_columns or []

        # Volume band: min_rows/max_rows are the keys the GE builder actually
        # handles (both map to ExpectTableRowCountToBeBetween). Bounds are derived
        # from a profiling snapshot, so this is a non-blocking warning rather than a
        # blocking error - the real row count moves over time.
        if row_count > 0:
            dataset_rules["min_rows"] = int(row_count * 0.8)
            dataset_rules["max_rows"] = int(row_count * 1.2)
            dataset_rules["severity"] = "warning"

        # integrity.pair_gte: date pairs where one should be >= another
        date_cols = [col for col, stats in column_stats.items() if stats.get("type") == "date"]
        pair_gte_rules = self._find_date_pair_rules(date_cols)
        if pair_gte_rules:
            dataset_rules["integrity"] = {"pair_gte": pair_gte_rules}

        # no_duplicate: compound uniqueness. A real composite key comes from a
        # multi-column Unity Catalog primary key - enable that as blocking. Multiple
        # columns that merely look unique in the snapshot are independent candidates,
        # not a compound key, so suggest disabled for review instead of blocking.
        if len(pk_columns) > 1:
            dataset_rules["no_duplicate"] = {
                "enabled": True,
                "column_list": pk_columns,
                "severity": "error",
                "blocking": True,
            }
        elif len(unique_cols) > 1:
            dataset_rules["no_duplicate"] = {
                "enabled": False,
                "column_list": unique_cols,
                "severity": "error",
                "_comment": "Candidate compound key (from approximate profiling). Review, then enable.",
            }

        return dataset_rules

    def _find_date_pair_rules(self, date_cols: list[str]) -> list[dict[str, Any]]:
        """Find date column pairs for integrity checks (updated >= created)."""
        if len(date_cols) < _MIN_DATE_COLS_FOR_PAIR:
            return []

        pair_rules = []
        for i, col_a in enumerate(date_cols):
            for col_b in date_cols[i + 1 :]:
                col_a_lower, col_b_lower = col_a.lower(), col_b.lower()
                if "updated" in col_a_lower and "created" in col_b_lower:
                    pair_rules.append({"column_A": col_a, "column_B": col_b, "severity": "warning"})
                elif "created" in col_a_lower and "updated" in col_b_lower:
                    pair_rules.append({"column_A": col_b, "column_B": col_a, "severity": "warning"})
        return pair_rules

    def _build_ge_suggestions(
        self,
        column_stats: dict[str, dict],
        not_null_cols: list[str],
        unique_cols: list[str],
        allowed_values_by_col: dict[str, list[str]],
        range_by_col: dict[str, dict],
        row_count: int,
        length_data: dict[str, dict] | None = None,
        pk_columns: list[str] | None = None,
        date_format_by_col: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Build Great Expectations suggestions based on actual GE rule types.

        GE Rules: not_null, unique, dtype, between, values_set, regex, length, date_format
        Dataset Rules: min_rows, max_rows, integrity.pair_gte, no_duplicate
        """
        ge_checks: dict[str, Any] = {}

        # Per-column rules
        for col, stats in column_stats.items():
            col_type = stats.get("type", "other")
            col_rules = self._build_ge_column_rules(
                col,
                col_type,
                not_null_cols,
                unique_cols,
                allowed_values_by_col,
                range_by_col,
                length_data,
                pk_columns,
                date_format_by_col,
            )
            if col_rules:
                ge_checks[col] = col_rules

        # Dataset-level rules
        dataset_rules = self._build_ge_dataset_rules(column_stats, unique_cols, row_count, pk_columns)
        if dataset_rules:
            ge_checks["_dataset"] = dataset_rules

        return ge_checks
