# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from neco_dq.test_suites.profiling.constants import (
    _LOW_CARDINALITY_THRESHOLD,
    _NULLABLE_THRESHOLD,
    _TOP_VALUE_DOMINANCE_THRESHOLD,
    _UNIQUE_RATIO_THRESHOLD,
)


class GenericSuggestionsMixin:
    """Generic suggestion-builder methods for profiling config generation."""

    def _suggest_not_null_columns(self, column_stats: dict[str, dict[str, Any]]) -> list[str]:
        """
        Suggest columns for not_null check (columns with 0% nulls).
        """
        return [col for col, stats in column_stats.items() if stats.get("null_pct", 100) == _NULLABLE_THRESHOLD]

    def _suggest_unique_columns(self, column_stats: dict[str, dict[str, Any]], row_count: int) -> list[str]:
        """
        Suggest columns for uniqueness check (distinct_ratio >= threshold).
        """
        if row_count == 0:
            return []
        return [col for col, stats in column_stats.items() if stats.get("distinct_ratio", 0) >= _UNIQUE_RATIO_THRESHOLD]

    def _suggest_allowed_values(self, column_stats: dict[str, dict[str, Any]], row_count: int) -> list[dict[str, Any]]:
        """
        Suggest allowed_values checks for low cardinality columns.

        Returns list of {column, values} dicts.
        """
        if row_count == 0:
            return []

        suggestions = []
        for col, stats in column_stats.items():
            distinct_count = stats.get("distinct_count", 0)
            top_values = stats.get("top_values", [])

            # Check if low cardinality
            if distinct_count > _LOW_CARDINALITY_THRESHOLD:
                continue

            # Check if top_values cover most of the data
            if top_values:
                total_top_count = sum(tv.get("count", 0) for tv in top_values)
                coverage = total_top_count / row_count if row_count > 0 else 0

                if coverage >= _TOP_VALUE_DOMINANCE_THRESHOLD:
                    values = [tv["value"] for tv in top_values if tv.get("value")]
                    if values:
                        suggestions.append({"column": col, "values": values})

        return suggestions

    def _suggest_range_checks(self, column_stats: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Suggest range checks for numeric columns with clear bounds.

        Returns list of {column, min, max} dicts.

        Note: ID columns are excluded because their values grow over time
        and hardcoded ranges become invalid quickly.
        """
        suggestions = []
        for col, stats in column_stats.items():
            if stats.get("type") != "numeric":
                continue

            # Skip ID columns - their values grow over time
            if self._is_id_column(col):
                continue

            min_val = stats.get("min")
            max_val = stats.get("max")

            if min_val is not None and max_val is not None:
                suggestions.append(
                    {
                        "column": col,
                        "min": min_val,
                        "max": max_val,
                        "mean": stats.get("mean"),
                        "stddev": stats.get("stddev"),
                    }
                )

        return suggestions

    def _suggest_date_checks(self, column_stats: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Suggest freshness/date range checks for date columns.

        Returns list of {column, min_date, max_date, range_days} dicts.
        """
        suggestions = []
        for col, stats in column_stats.items():
            if stats.get("type") != "date":
                continue

            min_date = stats.get("min_date")
            max_date = stats.get("max_date")
            range_days = stats.get("date_range_days")

            if min_date and max_date:
                suggestions.append(
                    {
                        "column": col,
                        "min_date": min_date,
                        "max_date": max_date,
                        "range_days": range_days,
                    }
                )

        return suggestions

    def _suggest_json_schema_columns(self, column_stats: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Suggest json_schema checks for columns that contain JSON data.

        Returns list of {column, is_array, sample_fields, field_types} dicts.
        """
        suggestions = []
        for col, stats in column_stats.items():
            if stats.get("is_json", False):
                suggestion = {
                    "column": col,
                    "is_array": stats.get("json_is_array", False),
                }
                if stats.get("json_sample_fields"):
                    suggestion["sample_fields"] = stats["json_sample_fields"]
                if stats.get("field_types"):
                    suggestion["field_types"] = stats["field_types"]
                suggestions.append(suggestion)
        return suggestions
