# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from neco_dq.test_suites.profiling.constants import (
    _CARDINALITY_FK_MAX_RATIO,
    _CARDINALITY_FK_MIN_DISTINCT,
    _CARDINALITY_FK_MIN_RATIO,
    _DISTINCT_COUNT_MAX_DISTINCT,
    _DISTINCT_COUNT_MAX_MULTIPLIER,
    _DISTINCT_COUNT_MIN_MULTIPLIER,
    _MIN_DATE_COLS_FOR_PAIR,
    _STATUS_MAX_DISTINCT,
    _STATUS_MIN_DISTINCT,
    _TREND_METRIC_MIN_DISTINCT,
)


class SuiteSuggestionsMixin:
    """Bronze, Silver, and Gold suite suggestion-builder methods and predicate helpers."""

    def _build_bronze_suggestions(
        self,
        column_stats: dict[str, dict],
        not_null_cols: list[str],
        date_cols: list[str],
    ) -> dict[str, Any]:
        """
        Build Bronze suite suggestions based on actual Bronze check implementations.

        Suggests single-table checks:
        - record_count: always (verify table not empty)
        - mandatory_fields: columns with 0% nulls
        - schema_drift: all columns with their types
        - freshness: ingestion/batch timestamp columns
        - table_freshness: always (uses Delta metadata)
        - record_count_anomaly: disabled by default (requires history_table config)

        Template checks (partition_exists) are NOT suggested
        as they require additional configuration that cannot be inferred.
        """
        bronze_checks: dict[str, Any] = {}

        # record_count: verify table has at least one record
        bronze_checks["record_count"] = {
            "enabled": True,
            "severity": "critical",
            "blocking": True,
        }

        # mandatory_fields: columns that must exist and be non-null.
        # 0% nulls is observed in the profiling snapshot - an optional field can be
        # fully populated today and have nulls later, so this is suggested as a
        # non-blocking warning rather than a blocking error.
        if not_null_cols:
            bronze_checks["mandatory_fields"] = {
                "enabled": True,
                "columns": not_null_cols,
                "severity": "warning",
            }

        # schema_drift: compare actual schema against expected
        # Note: schema_drift uses substring match, so "int" matches both "int" and "bigint"
        if column_stats:
            type_map = {
                "numeric": "int",
                "string": "string",
                "date": "timestamp",
                "boolean": "boolean",
                "other": "string",
            }
            expected_schema = {
                col: type_map.get(stats.get("type", "other"), "string") for col, stats in column_stats.items()
            }
            bronze_checks["schema_drift"] = {
                "enabled": True,
                "expected_columns": expected_schema,
                "severity": "warning",
            }

        # freshness: verify data freshness based on timestamp column
        # Look for ingestion/batch timestamp columns
        freshness_keywords = ["ingestion", "batch", "load", "ingest", "created", "timestamp"]
        for col in date_cols:
            if any(kw in col.lower() for kw in freshness_keywords):
                bronze_checks["freshness"] = {
                    "enabled": True,
                    "timestamp_column": col,
                    "max_delay_hours": 24,
                    "severity": "warning",
                }
                break

        # table_freshness: verify table freshness using Delta metadata
        bronze_checks["table_freshness"] = {
            "enabled": True,
            "max_days_stale": 2,
            "severity": "warning",
        }

        # record_count_anomaly: detect anomalies compared to historical values
        # Disabled by default - requires history_table to be configured
        bronze_checks["record_count_anomaly"] = {
            "enabled": False,
            "history_table": "<catalog.schema.dq_history_table>",
            "allowed_deviation_percent": 40,
            "severity": "warning",
            "_comment": "Enable and set history_table to detect record count anomalies",
        }

        return bronze_checks

    def _build_silver_core_checks(
        self, unique_cols: list[str], date_cols: list[str], pk_columns: list[str] | None = None
    ) -> dict[str, Any]:
        """Build core Silver checks: idempotency, freshness, row_temporal_consistency."""
        checks: dict[str, Any] = {}
        pk_columns = pk_columns or []

        # idempotency: distinct ratio is approximate, so a key detected only from the
        # snapshot is a candidate. Enable a blocking idempotency check ONLY for an
        # authoritative Unity Catalog primary key. Otherwise suggest it disabled for
        # review, so we never block a pipeline on a guess.
        if pk_columns:
            key_col = pk_columns[0] if len(pk_columns) == 1 else pk_columns
            checks["idempotency"] = {
                "enabled": True,
                "key_column": key_col,
                "severity": "error",
                "blocking": True,
                "_confidence": "high",
                "_reason": "Primary key from Unity Catalog constraints",
            }
        elif unique_cols:
            key_col = unique_cols[0] if len(unique_cols) == 1 else unique_cols
            checks["idempotency"] = {
                "enabled": False,
                "key_column": key_col,
                "severity": "error",
                "_confidence": "medium",
                "_reason": "Candidate key (from approximate profiling). Review, then enable.",
            }

        # freshness: verify data freshness based on timestamp column
        freshness_kw = ["created", "updated", "timestamp", "time"]
        for col in date_cols:
            if any(kw in col.lower() for kw in freshness_kw):
                checks["freshness"] = {
                    "enabled": True,
                    "column": col,
                    "max_delay_hours": 72,
                    "severity": "warning",
                    "_confidence": "medium",
                    "_reason": "Column name contains a freshness keyword",
                }
                break

        # row_temporal_consistency: verify timestamp ordering within same row
        if len(date_cols) >= _MIN_DATE_COLS_FOR_PAIR:
            temporal_check = self._find_temporal_consistency_pair(date_cols)
            if temporal_check:
                checks["row_temporal_consistency"] = temporal_check

        return checks

    def _find_temporal_consistency_pair(self, date_cols: list[str]) -> dict[str, Any] | None:
        """Find a pair of date columns for temporal consistency check."""
        for i, col_a in enumerate(date_cols):
            for col_b in date_cols[i + 1 :]:
                if "created" in col_a.lower() and "created" in col_b.lower():
                    return {
                        "enabled": True,
                        "left_timestamp": col_a,
                        "right_timestamp": col_b,
                        "severity": "error",
                        "blocking": True,
                    }
        return None

    def _suggest_cardinality_checks(
        self,
        column_stats: dict[str, dict[str, Any]],
        unique_cols: list[str],
        row_count: int,
    ) -> list[dict[str, Any]]:
        """
        Suggest cardinality checks for grouping columns.

        Identifies columns that look like grouping keys:
        - Columns with "_id" suffix that are NOT unique (foreign keys)
        - Columns with moderate cardinality (not too low, not unique)

        Returns list of cardinality check configurations.
        """
        if row_count == 0:
            return []

        suggestions = []
        id_suffix_keywords = ["_id", "_key", "_fk"]

        for col, stats in column_stats.items():
            # Skip columns that are already identified as unique
            if col in unique_cols:
                continue

            distinct_count = stats.get("distinct_count", 0)
            distinct_ratio = stats.get("distinct_ratio", 0)

            # Look for foreign key patterns: columns with _id suffix
            is_fk_pattern = any(col.lower().endswith(kw) for kw in id_suffix_keywords)

            # Moderate cardinality: not too low (enum) and not unique
            is_moderate_cardinality = (
                distinct_count >= _CARDINALITY_FK_MIN_DISTINCT
                and distinct_ratio >= _CARDINALITY_FK_MIN_RATIO
                and distinct_ratio < _CARDINALITY_FK_MAX_RATIO
            )

            if is_fk_pattern and is_moderate_cardinality:
                suggestions.append(
                    {
                        "enabled": True,
                        "group_by": col,
                        "min": 1,
                        "severity": "warning",
                        "_comment": f"Detected FK pattern: {distinct_count} distinct values",
                        "_confidence": "low",
                        "_reason": "FK pattern detected",
                    }
                )

        return suggestions

    def _find_status_column(self, column_stats: dict[str, dict[str, Any]]) -> tuple[str | None, list[str]]:
        """Find a status/state column with low cardinality. Returns (column_name, values)."""
        status_keywords = ["status", "state", "phase", "stage"]

        for col, stats in column_stats.items():
            if not any(kw in col.lower() for kw in status_keywords):
                continue

            distinct_count = stats.get("distinct_count", 0)
            if not (_STATUS_MIN_DISTINCT <= distinct_count <= _STATUS_MAX_DISTINCT):
                continue

            top_values = stats.get("top_values", [])
            if top_values:
                values = [tv["value"] for tv in top_values if tv.get("value")]
                return col, values

        return None, []

    def _find_timestamp_column(self, date_cols: list[str]) -> str | None:
        """Find a timestamp column suitable for ordering."""
        timestamp_keywords = ["created", "updated", "timestamp", "time", "date"]
        for col in date_cols:
            if any(kw in col.lower() for kw in timestamp_keywords):
                return col
        return None

    def _find_key_columns(
        self, unique_cols: list[str], column_stats: dict[str, dict[str, Any]], pk_columns: list[str] | None = None
    ) -> list[str]:
        """Find key columns (entity identifiers).

        Priority order:
            1. UC primary key constraint columns (if provided)
            2. First unique column from stats
            3. Columns matching naming convention (*_id, id)

        Args:
            unique_cols: Columns detected as unique from stats
            column_stats: Column statistics
            pk_columns: Primary key columns from UC constraints (optional)

        Returns:
            List of key columns, empty if none found.
        """
        # 1. Use PK columns if available (from UC constraints)
        if pk_columns:
            return pk_columns

        # 2. Use first unique column
        if unique_cols:
            return [unique_cols[0]]

        # 3. Fall back to naming convention
        for col in column_stats:
            col_lower = col.lower()
            if col_lower.endswith("_id") or col_lower == "id":
                return [col]

        return []

    def _find_key_column(
        self, unique_cols: list[str], column_stats: dict[str, dict[str, Any]], pk_columns: list[str] | None = None
    ) -> str | None:
        """Find a single key column (entity identifier).

        Convenience wrapper around _find_key_columns for cases needing single column.
        For composite keys, returns the first column.
        """
        key_cols = self._find_key_columns(unique_cols, column_stats, pk_columns)
        return key_cols[0] if key_cols else None

    def _suggest_status_transition_check(
        self,
        column_stats: dict[str, dict[str, Any]],
        unique_cols: list[str],
        date_cols: list[str],
        pk_columns: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """
        Suggest status_transition check when detecting status workflow pattern.

        Requires:
        - A status/state column with low cardinality
        - A timestamp column for ordering
        - A key column (entity identifier)

        Returns status_transition check configuration or None.
        """
        status_col, status_values = self._find_status_column(column_stats)
        if not status_col:
            return None

        timestamp_col = self._find_timestamp_column(date_cols)
        if not timestamp_col:
            return None

        key_col = self._find_key_column(unique_cols, column_stats, pk_columns)
        if not key_col:
            return None

        return {
            "enabled": False,  # Disabled by default - needs manual review of allowed_flow
            "key_column": key_col,
            "status_column": status_col,
            "timestamp_column": timestamp_col,
            "allowed_flow": status_values,
            "severity": "error",
            "blocking": True,
            "_comment": "Review and order allowed_flow based on your workflow",
        }

    def _build_silver_suggestions(
        self,
        unique_cols: list[str],
        date_cols: list[str],
        column_stats: dict[str, dict[str, Any]],
        row_count: int,
        json_cols: list[dict[str, Any]] | None = None,
        pk_columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Build Silver suite suggestions based on actual Silver check implementations.

        Only suggests single-table checks that can be fully configured from profiling data:
        - idempotency: unique key columns
        - freshness: timestamp columns
        - row_temporal_consistency: date column pairs
        - cardinality: grouping columns (FK patterns)
        - status_transition: workflow status columns
        - json_schema: JSON columns with structure validation

        Cross-table checks (compare_metric) are NOT suggested as they require knowledge
        of related tables or historical data that cannot be inferred from single-table profiling.
        """
        # Core checks (idempotency, freshness, row_temporal_consistency)
        checks = self._build_silver_core_checks(unique_cols, date_cols, pk_columns)

        # cardinality: verify minimum rows per group (FK patterns)
        cardinality_checks = self._suggest_cardinality_checks(column_stats, unique_cols, row_count)
        if cardinality_checks:
            checks["cardinality"] = cardinality_checks

        # distinct_count: verify distinct value counts for moderate-cardinality columns
        dc = self._suggest_distinct_count_checks(column_stats, unique_cols, row_count)
        if dc:
            checks["distinct_count"] = dc

        # status_transition: verify status workflow transitions
        status_check = self._suggest_status_transition_check(column_stats, unique_cols, date_cols, pk_columns)
        if status_check:
            checks["status_transition"] = status_check

        # json_schema: validate JSON column structure
        if json_cols:
            json_schema_checks = []
            for json_info in json_cols:
                # required_fields/field_types are inferred from a small sample, so an
                # optional field absent from the sample would make the check fail.
                # When we inferred a schema, suggest the check disabled for review;
                # with no inferred schema there is nothing to get wrong, so enable it.
                inferred_schema = bool(json_info.get("sample_fields") or json_info.get("field_types"))
                check_cfg: dict[str, Any] = {
                    "enabled": not inferred_schema,
                    "column": json_info["column"],
                    "array_element": json_info.get("is_array", True),
                    "severity": "error",
                }
                # Add sample fields as required_fields suggestion
                if json_info.get("sample_fields"):
                    check_cfg["required_fields"] = json_info["sample_fields"]
                # Add inferred field types
                if json_info.get("field_types"):
                    check_cfg["field_types"] = json_info["field_types"]
                if inferred_schema:
                    check_cfg["_comment"] = "Review and adjust required_fields/field_types, then enable."
                json_schema_checks.append(check_cfg)

            if json_schema_checks:
                checks["json_schema"] = json_schema_checks

        return checks

    def _suggest_distinct_count_checks(
        self,
        column_stats: dict[str, dict[str, Any]],
        unique_cols: list[str],
        row_count: int,
    ) -> list[dict[str, Any]]:
        """
        Suggest distinct_count checks for moderate-cardinality columns.

        Identifies columns worth monitoring for distinct value counts:
        - Columns with moderate cardinality (10 <= distinct_count <= 1000)
        - Columns with moderate distinct ratio (0.01 <= distinct_ratio <= 0.5)
        - Columns NOT already identified as unique

        Returns list of distinct_count check configurations.
        """
        if row_count == 0:
            return []

        suggestions = []

        for col, stats in column_stats.items():
            # Skip columns that are already identified as unique
            if col in unique_cols:
                continue

            # Skip date/timestamp columns: their distinct count grows monotonically
            # over time (each new batch adds new timestamps), so a hardcoded max
            # produces guaranteed false-positive warnings as the table accumulates.
            if stats.get("type") == "date":
                continue

            # Skip id-like columns for the same reason their `between` check is skipped:
            # values grow over time and a fixed distinct-count range goes stale fast.
            if self._is_id_column(col):
                continue

            distinct_count = stats.get("distinct_count", 0)
            distinct_ratio = stats.get("distinct_ratio", 0)

            # Moderate cardinality: not too low, not unique
            is_moderate = (
                _CARDINALITY_FK_MIN_DISTINCT <= distinct_count <= _DISTINCT_COUNT_MAX_DISTINCT
                and _CARDINALITY_FK_MIN_RATIO <= distinct_ratio <= _CARDINALITY_FK_MAX_RATIO
            )
            if is_moderate:
                suggestions.append(
                    {
                        "enabled": True,
                        "column": col,
                        "min": int(distinct_count * _DISTINCT_COUNT_MIN_MULTIPLIER),
                        "max": int(distinct_count * _DISTINCT_COUNT_MAX_MULTIPLIER),
                        "severity": "warning",
                        "_confidence": "medium",
                        "_reason": f"Moderate cardinality ({distinct_count} distinct)",
                    }
                )

        return suggestions

    def _is_enum_column(self, col: str) -> bool:
        """Check if column name suggests an enumeration/categorical field."""
        enum_keywords = ["status", "type", "category", "severity", "level", "state", "mode", "kind", "flag"]
        return any(kw in col.lower() for kw in enum_keywords)

    def _is_id_column(self, col: str) -> bool:
        """Check if column name suggests an ID/key field that grows over time.

        ID columns should not have `between` checks because their values
        grow over time and hardcoded ranges become invalid quickly.
        """
        col_lower = col.lower()
        # Exact matches for common ID patterns
        id_exact = ["id", "pk", "key", "uuid", "guid"]
        if col_lower in id_exact:
            return True
        # Suffix patterns (e.g., user_id, response_id, batch_id)
        id_suffixes = ["_id", "_pk", "_key", "_uuid", "_guid", "_seq"]
        if any(col_lower.endswith(suffix) for suffix in id_suffixes):
            return True
        # Prefix patterns (e.g., id_user, pk_order)
        id_prefixes = ["id_", "pk_", "key_", "uuid_", "guid_"]
        return any(col_lower.startswith(prefix) for prefix in id_prefixes)

    def _suggest_trend_checks(self, date_cols: list[str], column_stats: dict[str, Any]) -> dict[str, Any] | None:
        """
        Suggest a trend check when a date column and a qualifying numeric metric column are detected.

        Returns None if no date columns are present or no qualifying metric column is found.
        Qualifying numeric column: type=="numeric", not id-like (not ending in _id/_key/_fk), distinct_count > 10.
        """
        if not date_cols:
            return None

        id_suffixes = ["_id", "_key", "_fk"]
        metric_col = None
        for col, stats in column_stats.items():
            if stats.get("type") != "numeric":
                continue
            if any(col.lower().endswith(sfx) for sfx in id_suffixes):
                continue
            if stats.get("distinct_count", 0) > _TREND_METRIC_MIN_DISTINCT:
                metric_col = col
                break

        if metric_col is None:
            return None

        date_col = date_cols[0]
        return {
            "enabled": False,
            "date_column": date_col,
            "metric_column": metric_col,
            "metric": f"SUM({metric_col})",
            "granularity": "day",
            "lookback_periods": 7,
            "tolerance_pct": 30,
            "severity": "warning",
            "_confidence": "low",
            "_reason": f"Date column ({date_col}) + numeric column ({metric_col}) detected",
        }

    def _suggest_completeness_checks(self, column_stats: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Suggest completeness checks for enum-like columns.

        A column qualifies when its name looks like an enumeration, it has 2-10
        distinct values, and top_values is populated. The expected_values list is
        derived from the top_values entries.
        """
        suggestions = []
        for col, stats in column_stats.items():
            # Only enum-like columns: completeness asserts every expected value must
            # always exist, which is meaningless for free-text columns (e.g. a title)
            # whose small distinct count is just a snapshot artifact. Mirrors the gate
            # the GE values_set suggestion already applies.
            if not self._is_enum_column(col):
                continue
            distinct_count = stats.get("distinct_count", 0)
            top_values = stats.get("top_values", [])
            if not (_STATUS_MIN_DISTINCT <= distinct_count <= _STATUS_MAX_DISTINCT) or not top_values:
                continue
            expected_values = [tv["value"] for tv in top_values if tv.get("value") is not None]
            if not expected_values:
                continue
            suggestions.append(
                {
                    "enabled": False,
                    "column": col,
                    "expected_values": expected_values,
                    "severity": "warning",
                    "_confidence": "medium",
                    "_reason": f"Enum column with {distinct_count} values (use if ALL values must exist)",
                }
            )
        return suggestions

    def _build_gold_suggestions(
        self,
        unique_cols: list[str],
        date_cols: list[str],
        row_count: int,
        column_stats: dict[str, Any],
        json_cols: list[dict[str, Any]] | None = None,
        pk_columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Build Gold suite suggestions based on actual Gold check implementations.

        Only suggests single-table checks that can be fully configured from profiling data:
        - uniqueness: columns that appear unique
        - volume: row count with historical tolerance
        - freshness: timestamp columns
        - trend: date + numeric metric column detected (disabled by default)
        - completeness: enum-like columns with 2-10 distinct values (disabled by default)
        - json_schema: JSON columns with structure validation

        Cross-table checks and template checks are NOT suggested.
        """
        gold_checks: dict[str, Any] = {}
        pk_columns = pk_columns or []

        # uniqueness: distinct ratio is approximate, so columns that merely look
        # unique in the snapshot are only candidates. Enable a blocking uniqueness
        # check ONLY for an authoritative Unity Catalog primary key. Otherwise
        # suggest it disabled for review, so we never block on a guess.
        if pk_columns:
            gold_checks["uniqueness"] = {
                "enabled": True,
                "columns": pk_columns,
                "severity": "critical",
                "blocking": True,
            }
        elif unique_cols:
            gold_checks["uniqueness"] = {
                "enabled": False,
                "columns": unique_cols,
                "severity": "critical",
                "_comment": "Candidate unique column(s) (from approximate profiling). Review, then enable.",
            }

        # volume: verify row count is within historical tolerance.
        # Disabled by default - the history table is a placeholder that must be set first.
        if row_count > 0:
            gold_checks["volume"] = {
                "enabled": False,
                "metric": "COUNT(*)",
                "history": {
                    "table": "<catalog.schema.dq_history_table>",  # e.g. catalog.schema.dq_test_history
                    "tolerance_pct": 50,
                },
                "severity": "warning",
                "_comment": "Enable and set history.table to verify row volume against history",
            }

        # freshness: verify data freshness based on timestamp column
        freshness_keywords = ["created", "updated", "timestamp", "time", "run"]
        for col in date_cols:
            if any(kw in col.lower() for kw in freshness_keywords):
                gold_checks["freshness"] = {
                    "enabled": True,
                    "column": col,
                    "max_delay_hours": 24,
                    "severity": "warning",
                }
                break

        # trend: date + numeric metric column pattern (added in v1.2.2 §3)
        trend = self._suggest_trend_checks(date_cols, column_stats)
        if trend:
            gold_checks["trend"] = trend

        # completeness: enum-like columns with 2-10 distinct values (added in v1.2.2 §3)
        completeness = self._suggest_completeness_checks(column_stats)
        if completeness:
            gold_checks["completeness"] = completeness

        # json_schema: validate JSON column structure
        if json_cols:
            json_schema_checks = []
            for json_info in json_cols:
                # required_fields/field_types are inferred from a small sample, so an
                # optional field absent from the sample would make the check fail.
                # When we inferred a schema, suggest the check disabled for review;
                # with no inferred schema there is nothing to get wrong, so enable it.
                inferred_schema = bool(json_info.get("sample_fields") or json_info.get("field_types"))
                check_cfg: dict[str, Any] = {
                    "enabled": not inferred_schema,
                    "column": json_info["column"],
                    "array_element": json_info.get("is_array", True),
                    "severity": "error",
                }
                # Add sample fields as required_fields suggestion
                if json_info.get("sample_fields"):
                    check_cfg["required_fields"] = json_info["sample_fields"]
                # Add inferred field types
                if json_info.get("field_types"):
                    check_cfg["field_types"] = json_info["field_types"]
                if inferred_schema:
                    check_cfg["_comment"] = "Review and adjust required_fields/field_types, then enable."
                json_schema_checks.append(check_cfg)

            if json_schema_checks:
                gold_checks["json_schema"] = json_schema_checks

        return gold_checks
