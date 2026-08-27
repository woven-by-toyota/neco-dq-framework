# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any
from datetime import datetime, UTC

# Map GE expectation types to framework check names for consistent reporting
GE_CHECK_NAME_MAP = {
    "expect_column_to_exist": "column_exists",
    "expect_column_values_to_not_be_null": "not_null",
    "expect_column_values_to_be_unique": "unique",
    "expect_compound_columns_to_be_unique": "unique",
    "expect_column_values_to_be_in_set": "allowed_values",
    "expect_column_values_to_be_between": "range",
    "expect_column_values_to_be_in_type_list": "dtype",
    "expect_column_values_to_match_regex": "regex",
    "expect_column_value_lengths_to_be_between": "length",
    "expect_column_values_to_match_strftime_format": "date_format",
    "expect_column_values_to_not_be_in_set": "not_in_set",
    "expect_column_distinct_values_to_be_in_set": "distinct_values_in_set",
    "expect_multicolumn_sum_to_equal": "multicolumn_sum",
    "expect_column_pair_values_to_be_equal": "pair_equality",
    "expect_column_pair_values_a_to_be_greater_than_b": "pair_gte",
    "expect_table_row_count_to_be_between": "record_count",
}

# Investigation query templates by expectation type
GE_INVESTIGATION_QUERIES = {
    "expect_column_values_to_not_be_null": {
        "description": "Find rows with null values",
        "template": "SELECT * FROM {table} WHERE {column} IS NULL LIMIT 100",
    },
    "expect_column_values_to_be_unique": {
        "description": "Find duplicate values",
        "template": "SELECT {column}, COUNT(*) as cnt FROM {table} GROUP BY {column} HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 100",
    },
    "expect_compound_columns_to_be_unique": {
        "description": "Find duplicate compound keys",
        "template": "SELECT {columns}, COUNT(*) as cnt FROM {table} GROUP BY {columns} HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 100",
    },
    "expect_column_values_to_be_in_set": {
        "description": "Find values not in allowed set",
        "template": "SELECT {column}, COUNT(*) as cnt FROM {table} WHERE {column} NOT IN ({value_set}) GROUP BY {column} ORDER BY cnt DESC LIMIT 100",
    },
    "expect_column_values_to_be_between": {
        "description": "Find values outside range",
        "template": "SELECT {column}, COUNT(*) as cnt FROM {table} WHERE {column} < {min_value} OR {column} > {max_value} GROUP BY {column} ORDER BY cnt DESC LIMIT 100",
    },
    "expect_column_values_to_match_regex": {
        "description": "Find values not matching pattern",
        "template": "SELECT {column} FROM {table} WHERE NOT {column} RLIKE '{regex}' LIMIT 100",
    },
    "expect_column_value_lengths_to_be_between": {
        "description": "Find values with length outside range",
        "template": "SELECT {column}, LENGTH({column}) as len FROM {table} WHERE LENGTH({column}) < {min_value} OR LENGTH({column}) > {max_value} LIMIT 100",
    },
    "expect_column_values_to_not_be_in_set": {
        "description": "Find values present in the forbidden set",
        "template": "SELECT {column}, COUNT(*) as cnt FROM {table} WHERE {column} IN ({value_set}) GROUP BY {column} ORDER BY cnt DESC LIMIT 100",
    },
    "expect_column_distinct_values_to_be_in_set": {
        "description": "Find distinct values not in the allowed set",
        "template": "SELECT {column}, COUNT(*) as cnt FROM {table} WHERE {column} NOT IN ({value_set}) GROUP BY {column} ORDER BY cnt DESC LIMIT 100",
    },
    "expect_table_row_count_to_be_between": {
        "description": "Check row count",
        "template": "SELECT COUNT(*) as row_count FROM {table}",
    },
    "expect_column_to_exist": {
        "description": "Check table schema",
        "template": "DESCRIBE TABLE {table}",
    },
}


def _map_check_name(ge_expectation_type: str) -> str:
    """Map GE expectation type to framework check name."""
    return GE_CHECK_NAME_MAP.get(ge_expectation_type, ge_expectation_type)


def _fmt_pct(count: int, total: int) -> str:
    """Format percentage with 1 decimal place."""
    if total == 0:
        return "0.0%"
    return f"{count / total * 100:.1f}%"


def _build_narrative_details(
    ge_expectation_type: str,
    column: str | None,
    result_detail: dict,
    kwargs: dict,
    success: bool,
) -> str:
    """
    Build human-readable details string for GE expectations.

    All messages start with a word (not a number) for consistency.
    PASS: "All ...", "Row count OK: ...", "Column '...' exists"
    FAIL: "Found ...", "Row count ...", "Column '...' not found", "Type mismatch: ..."
    """
    unexpected_count = result_detail.get("unexpected_count")
    element_count = result_detail.get("element_count")
    observed_value = result_detail.get("observed_value")

    if success:
        # Success messages - all start with a word
        if ge_expectation_type == "expect_table_row_count_to_be_between":
            return f"Row count OK: {observed_value}"
        if ge_expectation_type == "expect_column_to_exist":
            return f"Column '{column}' exists"
        if unexpected_count is not None and element_count:
            return f"All {element_count} values valid"
        return "Check passed"

    # Failure messages by expectation type - all start with a word
    if ge_expectation_type == "expect_column_values_to_not_be_null":
        if unexpected_count is not None and element_count:
            pct = _fmt_pct(unexpected_count, element_count)
            return f"Found {unexpected_count} null values ({pct} of {element_count})"
        return "Null values found"

    if ge_expectation_type in ("expect_column_values_to_be_unique", "expect_compound_columns_to_be_unique"):
        if unexpected_count is not None and element_count:
            pct = _fmt_pct(unexpected_count, element_count)
            return f"Found {unexpected_count} duplicates ({pct} of {element_count})"
        return "Duplicate values found"

    if ge_expectation_type == "expect_column_values_to_be_in_set":
        if unexpected_count is not None and element_count:
            pct = _fmt_pct(unexpected_count, element_count)
            return f"Found {unexpected_count} invalid values ({pct} of {element_count})"
        return "Values not in allowed set"

    if ge_expectation_type == "expect_column_values_to_be_between":
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        if unexpected_count is not None and element_count:
            pct = _fmt_pct(unexpected_count, element_count)
            return f"Found {unexpected_count} values out of range [{min_val}, {max_val}] ({pct} of {element_count})"
        return f"Values outside range [{min_val}, {max_val}]"

    if ge_expectation_type == "expect_column_values_to_match_regex":
        if unexpected_count is not None and element_count:
            pct = _fmt_pct(unexpected_count, element_count)
            return f"Found {unexpected_count} pattern mismatches ({pct} of {element_count})"
        return "Values don't match regex pattern"

    if ge_expectation_type == "expect_table_row_count_to_be_between":
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        return f"Row count {observed_value} outside expected range [{min_val}, {max_val}]"

    if ge_expectation_type == "expect_column_to_exist":
        return f"Column '{column}' not found"

    if ge_expectation_type == "expect_column_values_to_be_in_type_list":
        expected_types = kwargs.get("type_list", [])
        return f"Type mismatch: expected {expected_types}"

    if ge_expectation_type in (
        "expect_column_pair_values_to_be_equal",
        "expect_column_pair_values_a_to_be_greater_than_b",
    ):
        col_a = kwargs.get("column_A")
        col_b = kwargs.get("column_B")
        if unexpected_count is not None and element_count:
            pct = _fmt_pct(unexpected_count, element_count)
            return f"Found {unexpected_count} pair violations ({pct} of {element_count})"
        return f"Pair constraint violated: {col_a} vs {col_b}"

    # Generic fallback
    if unexpected_count is not None and element_count:
        pct = _fmt_pct(unexpected_count, element_count)
        return f"Found {unexpected_count} failures ({pct} of {element_count})"
    return "Expectation failed"


def _build_failure_stats(
    ge_expectation_type: str,
    column: str | None,
    result_detail: dict,
    kwargs: dict,
) -> dict | None:
    """
    Build failure_stats from GE result (statistics only, no PII).

    Includes all aggregate statistics from GE result, excluding any PII fields.
    """
    unexpected_count = result_detail.get("unexpected_count")
    element_count = result_detail.get("element_count")

    stats = {
        "failure_type": "expectation_failed",
        "affected_columns": [column] if column else [],
        "expectation_info": {
            "expectation_type": ge_expectation_type,
        },
    }

    # Add counts if available
    if element_count is not None:
        stats["total_rows_checked"] = element_count

    if unexpected_count is not None:
        stats["failure_count"] = unexpected_count
        if element_count:
            stats["failure_percentage"] = round(unexpected_count / element_count * 100, 4)

    # Add observed_value for aggregate expectations
    observed_value = result_detail.get("observed_value")
    if observed_value is not None:
        stats["observed_value"] = observed_value

    # Add expectation-specific parameters (no PII, just config)
    if ge_expectation_type == "expect_column_values_to_be_between":
        stats["expectation_info"]["min_value"] = kwargs.get("min_value")
        stats["expectation_info"]["max_value"] = kwargs.get("max_value")
    elif ge_expectation_type == "expect_table_row_count_to_be_between":
        stats["expectation_info"]["min_value"] = kwargs.get("min_value")
        stats["expectation_info"]["max_value"] = kwargs.get("max_value")
    elif ge_expectation_type == "expect_column_values_to_be_in_type_list":
        stats["expectation_info"]["type_list"] = kwargs.get("type_list")
    elif ge_expectation_type == "expect_compound_columns_to_be_unique":
        stats["affected_columns"] = kwargs.get("column_list", [])

    # Add missing_count and missing_percent if available
    missing_count = result_detail.get("missing_count")
    if missing_count is not None:
        stats["missing_count"] = missing_count
        if element_count:
            stats["missing_percentage"] = round(missing_count / element_count * 100, 4)

    # Add unexpected_percent if available (GE sometimes computes this)
    unexpected_percent = result_detail.get("unexpected_percent")
    if unexpected_percent is not None:
        stats["unexpected_percent_ge"] = unexpected_percent

    return stats


def _build_investigation_query(
    ge_expectation_type: str,
    table_fq: str,
    column: str | None,
    kwargs: dict,
    where_clause: str | None = None,
) -> dict | None:
    """Build investigation query template (no data embedded)."""
    template_info = GE_INVESTIGATION_QUERIES.get(ge_expectation_type)
    if not template_info:
        base_query = f"SELECT * FROM {table_fq}"
        if where_clause:
            base_query += f" WHERE {where_clause}"
        return {
            "description": "Investigate check failures",
            "query": f"{base_query} LIMIT 100",
        }

    params = {
        "table": table_fq,
        "column": column or "*",
    }

    # Handle compound columns
    if ge_expectation_type == "expect_compound_columns_to_be_unique":
        columns = kwargs.get("column_list", [])
        params["columns"] = ", ".join(columns) if columns else "*"

    # Handle value set - use placeholder to avoid embedding potentially sensitive values
    if ge_expectation_type in (
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_not_be_in_set",
        "expect_column_distinct_values_to_be_in_set",
    ):
        params["value_set"] = "<allowed_set>"

    # Handle between (numeric range and value-length range)
    if ge_expectation_type in (
        "expect_column_values_to_be_between",
        "expect_column_value_lengths_to_be_between",
    ):
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        if ge_expectation_type == "expect_column_value_lengths_to_be_between":
            params["min_value"] = 0 if min_value is None else min_value
            params["max_value"] = 999999 if max_value is None else max_value
        else:
            params["min_value"] = -1e308 if min_value is None else min_value
            params["max_value"] = 1e308 if max_value is None else max_value

    # Handle regex
    if ge_expectation_type == "expect_column_values_to_match_regex":
        params["regex"] = kwargs.get("regex", ".*")

    try:
        query = template_info["template"].format(**params)
    except KeyError:
        query = f"SELECT * FROM {table_fq} LIMIT 100"

    # Append where_clause to query if present
    if where_clause:
        query_upper = query.upper()
        if "WHERE" in query_upper:
            # Find the position to insert AND clause (before GROUP BY, ORDER BY, or LIMIT)
            for keyword in [" GROUP BY ", " ORDER BY ", " HAVING ", " LIMIT "]:
                if keyword in query_upper:
                    idx = query_upper.index(keyword)
                    query = query[:idx] + f" AND {where_clause}" + query[idx:]
                    break
            else:
                # No GROUP BY/ORDER BY/LIMIT found, append at end
                query = query + f" AND {where_clause}"
        else:
            # No WHERE clause, insert before GROUP BY/ORDER BY/LIMIT
            for keyword in [" GROUP BY ", " ORDER BY ", " HAVING ", " LIMIT "]:
                if keyword in query_upper:
                    idx = query_upper.index(keyword)
                    query = query[:idx] + f" WHERE {where_clause}" + query[idx:]
                    break
            else:
                query = query + f" WHERE {where_clause}"

    return {
        "description": template_info["description"],
        "query": query,
    }


def _build_execution_trace(
    start_time: datetime,
    element_count: int | None,
    profile_name: str | None = None,
    profile_target_catalog: str | None = None,
) -> dict:
    """Build execution trace for GE check.

    Args:
        start_time: When the check started.
        element_count: Number of rows scanned.
        profile_name: Active profile name (optional, only when profiles are used).
        profile_target_catalog: Resolved catalog from profile (optional).

    Returns:
        Dict with execution context for debugging.
    """
    now = datetime.now(UTC)
    trace = {
        "duration_ms": int((now - start_time).total_seconds() * 1000),
        "started_at": start_time.isoformat().replace("+00:00", "Z"),
    }
    if element_count is not None:
        trace["rows_scanned"] = element_count

    # Include profile context only when profiles are used
    if profile_name:
        profile_info: dict[str, str] = {"name": profile_name}
        if profile_target_catalog:
            profile_info["target_catalog"] = profile_target_catalog
        trace["profile"] = profile_info

    return trace


def normalize_ge_results(
    validation_result,
    table_fq: str,
    run_ts: datetime,
    run_id: Any,
    default_severity: str = "warning",
    start_time: datetime | None = None,
    where_clause: str | None = None,
    profile_name: str | None = None,
    profile_target_catalog: str | None = None,
) -> list[dict[str, Any]]:
    """
    Convert GE ValidationResult into framework raw results.
    Preserves severity, blocking, and sets alert_description as expectation type.
    Adds failure_stats, investigation_query, and execution trace.

    Args:
        where_clause: Optional WHERE clause (row_filter + partition_filter) to include in investigation queries.
        profile_name: Active profile name (optional, only when profiles are used).
        profile_target_catalog: Resolved catalog from profile (optional).

    SECURITY: Does NOT include partial_unexpected_list (contains PII).
    """
    normalized: list[dict[str, Any]] = []
    check_start_time = start_time or datetime.now(UTC)

    results = getattr(validation_result, "results", []) or []

    for r in results:
        cfg = getattr(r, "expectation_config", {})
        success = getattr(r, "success", False)

        # -----------------------------
        # Meta info from GE
        # -----------------------------
        meta = getattr(cfg, "meta", {}) or {}
        severity = meta.get("severity", default_severity)
        blocking = meta.get("blocking", False)
        rule_description = meta.get("rule_description", "")

        # Escalate severity if blocking
        effective_severity = severity
        if blocking and severity not in ["critical", "error"]:
            effective_severity = "error"

        ge_expectation_type = getattr(cfg, "type", "unknown_check")
        check_name = _map_check_name(ge_expectation_type)
        kwargs = cfg.get("kwargs", {}) or {}
        column = kwargs.get("column")

        result_detail_raw = getattr(r, "result", {})
        element_count = result_detail_raw.get("element_count")

        # Build human-readable details string
        details = _build_narrative_details(ge_expectation_type, column, result_detail_raw, kwargs, success)

        # Build debug fields (only on failure)
        failure_stats = None
        investigation_query = None
        if not success:
            expected, actual = extract_expected_actual(cfg, result_detail_raw)
            failure_stats = _build_failure_stats(ge_expectation_type, column, result_detail_raw, kwargs)
            investigation_query = _build_investigation_query(
                ge_expectation_type, table_fq, column, kwargs, where_clause
            )
        else:
            expected, actual = None, None

        execution = _build_execution_trace(check_start_time, element_count, profile_name, profile_target_catalog)

        row = {
            "engine": "ge",
            "table": table_fq,
            "table_fullname": table_fq,
            "column": column,
            "check_name": check_name,
            "passed": success,
            "severity": severity,
            "effective_severity": effective_severity,
            "blocking": blocking,
            "run_id": run_id,
            "run_ts": run_ts,
            "rule_description": rule_description,
            "details": details,
            "alert_description": ge_expectation_type,
            "extra_metadata": {"ge_expectation_type": ge_expectation_type},
            "failure_details": {
                "expected": expected,
                "actual": actual,
                "expectation_type": ge_expectation_type,
                "check_name": check_name,
                "column": column,
                "rule_description": rule_description,
            }
            if not success
            else None,
            "failure_stats": failure_stats,
            "investigation_query": investigation_query,
            "execution": execution,
        }

        # SECURITY: Do NOT include partial_unexpected_list (contains actual values = PII risk)
        # Only include aggregate counts (no PII)
        if "partial_unexpected_counts" in result_detail_raw:
            row["extra_metadata"]["partial_unexpected_counts"] = result_detail_raw["partial_unexpected_counts"]

        normalized.append(row)

    return normalized


def extract_expected_actual(cfg, result):
    expected = {}
    actual = {}

    kwargs = cfg.get("kwargs", {}) or {}

    # Expected
    for k, v in kwargs.items():
        if k != "column":
            expected[k] = v

    # Actual
    if "observed_value" in result:
        actual["observed_value"] = result["observed_value"]

    if "unexpected_count" in result:
        actual["unexpected_count"] = result["unexpected_count"]
        actual["element_count"] = result.get("element_count")

    return expected or None, actual or None
