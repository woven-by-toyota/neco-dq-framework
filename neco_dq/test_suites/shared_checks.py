# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""
Shared check implementations used by multiple test suites.

This mixin provides common checks that are shared between Silver and Gold suites.
Suites inherit from this mixin to gain access to these checks without duplicating code.

Usage:
    class MyTestSuite(DQTestSuite, SharedChecksMixin):
        ...
"""

from datetime import datetime, UTC
from typing import Any

from pyspark.sql import functions as f

from neco_dq.core.metrics.expected import resolve_expected
from neco_dq.core.metrics.observed import resolve_observed
from neco_dq.core.metrics.operators import compare


class SharedChecksMixin:
    """
    Mixin providing shared check implementations for Silver and Gold suites.

    Requires the base class to provide:
    - spark: SparkSession instance
    - _require_cfg(cfg, *keys) -> tuple[bool, str | None]
    - _build_failure_stats(...) -> dict
    - _build_investigation_query(...) -> dict

    Also uses from base class (if available):
    - _current_where_clause: Optional row filter applied to table
    - _current_table_fq: Fully qualified table name
    - _evaluate_threshold(failed_count, total_count, cfg) -> tuple[bool, dict]
    - _build_threshold_details(base_details, threshold_meta) -> str
    """

    def _get_where_clause(self) -> str:
        """Get WHERE clause from row_filter if set, empty string otherwise."""
        row_filter = getattr(self, "_current_where_clause", None)
        return f" WHERE {row_filter}" if row_filter else ""

    # ------------------------------------------------------------------
    # Distinct Count Check
    # ------------------------------------------------------------------

    def _check_distinct_count(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify the number of distinct values in a column is within expected bounds.

        Config:
            column: Column to count distinct values (required)
            min: Minimum expected distinct count (optional)
            max: Maximum expected distinct count (optional)
            expected: Exact expected distinct count (optional, overrides min/max)
        """
        start_time = datetime.now(UTC)
        ok, _err = self._require_cfg(cfg, "column")
        if not ok:
            return False, self._details_config_missing("column"), None

        column = cfg["column"]
        if column not in df.columns:
            return False, self._details_not_found("Column", column), column

        total_rows = df.count()
        distinct_count = df.select(column).distinct().count()

        # Exact match
        if cfg.get("expected") is not None:
            expected = int(cfg["expected"])
            passed = distinct_count == expected

            failure_stats = None
            investigation_query = None
            if not passed:
                failure_stats = self._build_failure_stats(
                    failure_type="distinct_count_mismatch",
                    affected_columns=[column],
                    failure_count=1,
                    total_checked=1,
                    diagnostic={
                        "distinct_count": distinct_count,
                        "expected": expected,
                        "difference": distinct_count - expected,
                    },
                )
                where = self._get_where_clause()
                investigation_query = {
                    "description": "List distinct values in column",
                    "query": f"SELECT DISTINCT {column}, COUNT(*) as cnt FROM {self._current_table_fq}{where} GROUP BY {column} ORDER BY cnt DESC LIMIT 100",
                }

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )

            if passed:
                details = f"Distinct count OK: {distinct_count} values in '{column}' (expected {expected})"
            else:
                details = self._details_fail_mismatch("Distinct count", expected, distinct_count)

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, column

        # Range check
        min_val = cfg.get("min")
        max_val = cfg.get("max")

        if min_val is None and max_val is None:
            return False, self._details_config_missing("expected, min, or max"), column

        passed = True
        violation = None
        if min_val is not None and distinct_count < int(min_val):
            passed = False
            violation = "below_minimum"
        if max_val is not None and distinct_count > int(max_val):
            passed = False
            violation = "above_maximum"

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="distinct_count_out_of_range",
                affected_columns=[column],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "distinct_count": distinct_count,
                    "min": int(min_val) if min_val is not None else None,
                    "max": int(max_val) if max_val is not None else None,
                    "violation": violation,
                },
            )
            where = self._get_where_clause()
            investigation_query = {
                "description": "List distinct values in column",
                "query": f"SELECT DISTINCT {column}, COUNT(*) as cnt FROM {self._current_table_fq}{where} GROUP BY {column} ORDER BY cnt DESC LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            bounds = []
            if min_val is not None:
                bounds.append(f"min={min_val}")
            if max_val is not None:
                bounds.append(f"max={max_val}")
            details = self._details_pass_bounds("Distinct count", f"{distinct_count} in '{column}'", ", ".join(bounds))
        else:
            limit_val = min_val if violation == "below_minimum" else max_val
            if violation == "below_minimum":
                details = self._details_fail_below("Distinct count", distinct_count, limit_val)
            else:
                details = self._details_fail_above("Distinct count", distinct_count, limit_val)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, column

    # ------------------------------------------------------------------
    # JSON Schema Check
    # ------------------------------------------------------------------

    def _check_json_schema(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Validate JSON column structure, required fields, field types, and allowed values.

        Supports nested path validation using dot-notation (e.g., "rows.row_code").

        Config:
            column: JSON column name (required)
            required_fields: List of field names that must exist (optional)
                             Supports dot-notation for nested fields: "rows.row_code"
            field_types: Dict mapping field name -> expected type (optional)
                         Supported types: string, int, long, double, float, boolean
                         Supports dot-notation: {"rows.row_order": "int"}
            allowed_values: Dict mapping field name -> list of allowed values (optional)
                            Supports dot-notation: {"cells.status": ["active", "inactive"]}
            array_element: If true, validates elements of JSON array (default: true)
            sample_size: Number of rows to sample for validation (default: 1000, 0 = all)
            max_parse_failure_pct: Max percentage of rows allowed to fail JSON parsing (default: 0)
            threshold: Optional threshold configuration for validation failures
                max_failure_percent: Maximum allowed percentage of validation failures (0-100)
                max_failure_count: Maximum allowed number of validation failures
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "column")
        if not ok:
            return False, err, None

        column = cfg["column"]
        required_fields = cfg.get("required_fields", [])
        field_types = cfg.get("field_types", {})
        allowed_values = cfg.get("allowed_values", {})
        is_array = cfg.get("array_element", True)
        sample_size = cfg.get("sample_size", 1000)
        max_parse_failure_pct = cfg.get("max_parse_failure_pct", 0)

        if column not in df.columns:
            return False, self._details_not_found("Column", column), column

        if not required_fields and not field_types and not allowed_values:
            return False, self._details_config_missing("required_fields, field_types, or allowed_values"), column

        # Separate top-level fields from nested paths
        top_level, nested = self._separate_nested_paths(required_fields, field_types, allowed_values)

        # Prepare data
        df_check = self._prepare_json_check_df(df, column, sample_size)
        total_rows = df_check.count()
        if total_rows == 0:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            # Empty data is a warning - may indicate missing data or overly strict filters
            failure_stats = self._build_failure_stats(
                failure_type="json_no_data",
                affected_columns=[column],
                failure_count=0,
                total_checked=0,
                diagnostic={"reason": "No non-null JSON data found in column"},
            )
            self._last_failure_stats = failure_stats
            self._last_investigation_query = None
            self._last_execution = execution
            return True, f"Warning: No data to validate in column '{column}'", column

        all_missing = {}
        all_type_mismatches = {}
        all_invalid_values = {}
        total_elements = 0
        parse_failures = 0

        # Validate top-level fields if any
        if top_level["required"] or top_level["types"] or top_level["allowed"]:
            schema_str = self._build_json_schema_str(
                top_level["required"], top_level["types"], top_level["allowed"], is_array
            )
            df_parsed = df_check.withColumn("_parsed", f.from_json(f.col(column), schema_str))
            parse_failures = df_parsed.filter(f.col("_parsed").isNull()).count()

            parse_failure_pct = (parse_failures / total_rows) * 100 if total_rows > 0 else 0
            if parse_failure_pct > max_parse_failure_pct:
                failure_stats = self._build_failure_stats(
                    failure_type="json_parse_failure",
                    affected_columns=[column],
                    failure_count=parse_failures,
                    total_checked=total_rows,
                    diagnostic={
                        "parse_failure_pct": round(parse_failure_pct, 1),
                        "max_allowed_pct": max_parse_failure_pct,
                    },
                )
                row_filter = getattr(self, "_current_where_clause", None)
                where_parts = [f"from_json({column}, 'struct<>') IS NULL"]
                if row_filter:
                    where_parts.append(row_filter)
                where_clause = " AND ".join(where_parts)
                investigation_query = {
                    "description": "Sample rows with JSON parse failures",
                    "query": f"SELECT {column} FROM {self._current_table_fq} WHERE {where_clause} LIMIT 100",
                }
                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=total_rows,
                    table_info=self._current_table_info,
                )
                self._last_failure_stats = failure_stats
                self._last_investigation_query = investigation_query
                self._last_execution = execution
                details = f"JSON parse failed: {parse_failures} rows ({parse_failure_pct:.1f}%) exceed {max_parse_failure_pct}% limit"
                return False, details, column

            df_valid = df_parsed.filter(f.col("_parsed").isNotNull())
            df_elements = self._get_json_elements(df_valid, is_array)
            total_elements = df_elements.count()

            all_missing.update(self._check_json_required_fields(df_elements, top_level["required"]))
            all_type_mismatches.update(self._check_json_field_types(df_elements, top_level["types"]))
            all_invalid_values.update(self._check_json_allowed_values(df_elements, top_level["allowed"]))

        # Validate nested fields for each parent path
        for parent_path, nested_cfg in nested.items():
            nested_result = self._validate_nested_json_fields(df_check, column, parent_path, nested_cfg, is_array)
            all_missing.update(nested_result["missing"])
            all_type_mismatches.update(nested_result["type_mismatches"])
            all_invalid_values.update(nested_result["invalid_values"])
            total_elements = max(total_elements, nested_result["total_elements"])

        # Calculate total validation failures (sum of all field-level failures)
        total_validation_failures = (
            sum(all_missing.values()) + sum(all_type_mismatches.values()) + sum(all_invalid_values.values())
        )

        # Calculate total field validations for accurate threshold denominator
        num_fields_checked = len(top_level["required"]) + len(top_level["types"]) + len(top_level["allowed"])
        for nested_cfg in nested.values():
            num_fields_checked += (
                len(nested_cfg.get("required", []))
                + len(nested_cfg.get("types", {}))
                + len(nested_cfg.get("allowed", {}))
            )
        total_validations = total_elements * num_fields_checked if num_fields_checked > 0 else total_elements

        # Evaluate with threshold (based on field-level validations)
        passed, threshold_meta = self._evaluate_threshold(total_validation_failures, total_validations, cfg)

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="json_schema_violation",
                affected_columns=[column],
                failure_count=total_validation_failures,
                total_checked=total_validations,
                diagnostic={
                    "missing_fields": len(all_missing),
                    "type_mismatches": len(all_type_mismatches),
                    "invalid_values": len(all_invalid_values),
                    "total_elements": total_elements,
                },
            )
            row_filter = getattr(self, "_current_where_clause", None)
            investigation_query = self._build_json_investigation_query(
                column=column,
                table_fq=self._current_table_fq,
                all_missing=all_missing,
                all_type_mismatches=all_type_mismatches,
                all_invalid_values=all_invalid_values,
                row_filter=row_filter,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = f"JSON schema OK: {total_elements} elements validated"
        else:
            issues = []
            if all_missing:
                issues.append(f"{len(all_missing)} missing fields")
            if all_type_mismatches:
                issues.append(f"{len(all_type_mismatches)} type mismatches")
            if all_invalid_values:
                issues.append(f"{len(all_invalid_values)} invalid values")
            details = f"JSON validation failed: {', '.join(issues)} in '{column}'"

        # Add threshold info if present
        if threshold_meta:
            details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, column

    def _separate_nested_paths(
        self, required_fields: list, field_types: dict, allowed_values: dict
    ) -> tuple[dict, dict]:
        """
        Separate top-level fields from nested paths (dot-notation).

        Returns:
            (top_level, nested) where:
            - top_level: {"required": [...], "types": {...}, "allowed": {...}}
            - nested: {"parent_path": {"required": [...], "types": {...}, "allowed": {...}}, ...}
        """
        top_level = {"required": [], "types": {}, "allowed": {}}
        nested: dict[str, dict] = {}

        # Process required_fields
        for field in required_fields:
            if "." in field:
                parent, child = field.split(".", 1)
                if parent not in nested:
                    nested[parent] = {"required": [], "types": {}, "allowed": {}}
                nested[parent]["required"].append(child)
            else:
                top_level["required"].append(field)

        # Process field_types
        for field, ftype in field_types.items():
            if "." in field:
                parent, child = field.split(".", 1)
                if parent not in nested:
                    nested[parent] = {"required": [], "types": {}, "allowed": {}}
                nested[parent]["types"][child] = ftype
            else:
                top_level["types"][field] = ftype

        # Process allowed_values
        for field, values in allowed_values.items():
            if "." in field:
                parent, child = field.split(".", 1)
                if parent not in nested:
                    nested[parent] = {"required": [], "types": {}, "allowed": {}}
                nested[parent]["allowed"][child] = values
            else:
                top_level["allowed"][field] = values

        return top_level, nested

    def _validate_nested_json_fields(
        self, df_check, column: str, parent_path: str, nested_cfg: dict, parent_is_array: bool
    ) -> dict:
        """
        Validate fields inside a nested JSON array.

        Args:
            df_check: DataFrame with JSON column
            column: JSON column name
            parent_path: Path to the nested array (e.g., "rows", "cells")
            nested_cfg: {"required": [...], "types": {...}, "allowed": {...}}
            parent_is_array: Whether the top-level JSON is an array

        Returns:
            {"missing": {...}, "type_mismatches": {...}, "invalid_values": {...}, "total_elements": int}
        """
        result = {"missing": {}, "type_mismatches": {}, "invalid_values": {}, "total_elements": 0}

        required = nested_cfg.get("required", [])
        types = nested_cfg.get("types", {})
        allowed = nested_cfg.get("allowed", {})

        if not required and not types and not allowed:
            return result

        # Build schema for nested array elements
        all_fields = set(required) | set(types.keys()) | set(allowed.keys())
        nested_schema = f"array<struct<{','.join(f'{name}:string' for name in all_fields)}>>"

        # Extract nested array using get_json_object
        if parent_is_array:
            # Top-level is array, need to explode first then get nested field
            df_exploded = df_check.select(f.explode(f.from_json(f.col(column), "array<string>")).alias("_item"))
            df_nested = df_exploded.withColumn(
                "_nested", f.from_json(f.get_json_object(f.col("_item"), f"$.{parent_path}"), nested_schema)
            )
        else:
            # Top-level is object, directly get nested field
            df_nested = df_check.withColumn(
                "_nested", f.from_json(f.get_json_object(f.col(column), f"$.{parent_path}"), nested_schema)
            )

        # Filter valid rows and explode nested array
        df_valid = df_nested.filter(f.col("_nested").isNotNull())
        df_elements = df_valid.select(f.explode(f.col("_nested")).alias("_elem"))
        result["total_elements"] = df_elements.count()

        if result["total_elements"] == 0:
            return result

        # Validate nested fields
        for field_name in required:
            null_count = df_elements.filter(f.col("_elem").getField(field_name).isNull()).count()
            if null_count > 0:
                result["missing"][f"{parent_path}.{field_name}"] = null_count

        for field_name, expected_type in types.items():
            spark_type = self._map_json_type(expected_type)
            field_col = f.col("_elem").getField(field_name)
            mismatch_count = self._count_type_mismatches(df_elements, field_col, spark_type)
            if mismatch_count > 0:
                result["type_mismatches"][f"{parent_path}.{field_name}"] = mismatch_count

        for field_name, valid_list in allowed.items():
            field_col = f.col("_elem").getField(field_name)
            valid_str_list = [str(v) for v in valid_list]
            invalid_count = df_elements.filter(
                field_col.isNotNull() & ~field_col.cast("string").isin(valid_str_list)
            ).count()
            if invalid_count > 0:
                result["invalid_values"][f"{parent_path}.{field_name}"] = invalid_count

        return result

    def _build_json_investigation_query(
        self,
        column: str,
        table_fq: str,
        all_missing: dict,
        all_type_mismatches: dict,
        all_invalid_values: dict,
        row_filter: str | None,
    ) -> dict:
        """Build investigation query for JSON schema violations."""
        where_parts = [f"{column} IS NOT NULL"]

        # Build conditions for missing required fields
        missing_conditions = []
        for field_name in all_missing:
            if "." in field_name:
                # Nested field - use json path
                missing_conditions.append(f"get_json_object({column}, '$.{field_name}') IS NULL")
            else:
                # Top-level field
                missing_conditions.append(f"get_json_object({column}, '$.{field_name}') IS NULL")

        # Build conditions for type mismatches (show the problematic records)
        # Type mismatches are harder to detect in SQL, so we just note them
        type_fields = list(all_type_mismatches.keys())

        # Build conditions for invalid values
        # Invalid values are also complex, just note them

        # Combine conditions
        if missing_conditions:
            where_parts.append(f"({' OR '.join(missing_conditions)})")

        if row_filter:
            where_parts.append(row_filter)

        where_clause = " AND ".join(where_parts)

        # Build description
        desc_parts = []
        if all_missing:
            desc_parts.append(f"missing fields: {list(all_missing.keys())}")
        if all_type_mismatches:
            desc_parts.append(f"type mismatches: {type_fields}")
        if all_invalid_values:
            desc_parts.append(f"invalid values in: {list(all_invalid_values.keys())}")

        description = f"Find JSON rows with violations ({', '.join(desc_parts) or 'schema errors'})"

        return {
            "description": description,
            "query": f"SELECT {column} FROM {table_fq} WHERE {where_clause} LIMIT 100",
        }

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

    def _prepare_json_check_df(self, df, column: str, sample_size: int):
        """Prepare DataFrame for JSON schema validation."""
        df_check = df.select(column).filter(f.col(column).isNotNull())
        return df_check.limit(sample_size) if sample_size > 0 else df_check

    def _build_json_schema_str(
        self, required_fields: list, field_types: dict, allowed_values: dict, is_array: bool
    ) -> str:
        """Build Spark schema string for JSON parsing - always parse as strings to preserve original values."""
        all_fields = set(required_fields) | set(field_types.keys()) | set(allowed_values.keys())
        # Always parse as string to preserve original format for type validation
        schema_parts = [f"{name}:string" for name in all_fields]
        schema_str = f"struct<{','.join(schema_parts)}>"
        return f"array<{schema_str}>" if is_array else schema_str

    def _get_json_elements(self, df_parsed, is_array: bool):
        """Get DataFrame with JSON elements (exploded if array)."""
        if is_array:
            return df_parsed.select(f.explode("_parsed").alias("_elem"))
        return df_parsed.select(f.col("_parsed").alias("_elem"))

    def _check_json_required_fields(self, df_elements, required_fields: list) -> dict:
        """Check for missing required fields in JSON elements."""
        missing = {}
        for field_name in required_fields:
            null_count = df_elements.filter(f.col("_elem").getField(field_name).isNull()).count()
            if null_count > 0:
                missing[field_name] = null_count
        return missing

    def _check_json_field_types(self, df_elements, field_types: dict) -> dict:
        """Check field type mismatches in JSON elements by validating value format."""
        mismatches = {}
        for field_name, expected_type in field_types.items():
            spark_type = self._map_json_type(expected_type)
            field_col = f.col("_elem").getField(field_name)
            mismatch_count = self._count_type_mismatches(df_elements, field_col, spark_type)
            if mismatch_count > 0:
                mismatches[field_name] = mismatch_count
        return mismatches

    def _count_type_mismatches(self, df_elements, field_col, expected_type: str) -> int:
        """Count values that don't match the expected type format."""
        # Patterns for type detection (values parsed as strings from JSON)
        int_pattern = r"^-?\d+$"  # Integer: optional minus, digits only
        number_pattern = r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$"  # Any number including scientific notation
        bool_pattern = r"^(true|false)$"  # Boolean

        if expected_type in ("int", "long"):
            # Expected int: value must be integer format (no decimal point)
            mismatch_filter = field_col.isNotNull() & ~field_col.rlike(int_pattern)
        elif expected_type in ("double", "float"):
            # Expected numeric: value must be a valid number
            mismatch_filter = field_col.isNotNull() & ~field_col.rlike(number_pattern)
        elif expected_type == "boolean":
            # Expected boolean: value must be "true" or "false"
            mismatch_filter = field_col.isNotNull() & ~field_col.rlike(bool_pattern)
        elif expected_type == "string":
            # For strings, do not infer type mismatches from the lexical content.
            # JSON strings may legitimately contain numeric or boolean text (e.g. "true", "123"),
            # and we cannot reliably detect type mismatches here.
            return 0
        else:
            # Unknown type, skip validation
            return 0

        return df_elements.filter(mismatch_filter).count()

    def _check_json_allowed_values(self, df_elements, allowed_values: dict) -> dict:
        """Check for values not in allowed list."""
        invalid = {}
        for field_name, valid_list in allowed_values.items():
            field_col = f.col("_elem").getField(field_name)
            # Cast values to string for comparison to handle mixed types
            valid_str_list = [str(v) for v in valid_list]
            invalid_count = df_elements.filter(
                field_col.isNotNull() & ~field_col.cast("string").isin(valid_str_list)
            ).count()
            if invalid_count > 0:
                invalid[field_name] = invalid_count
        return invalid

    def _map_json_type(self, type_name: str) -> str:
        """Map user-friendly type names to Spark SQL types."""
        type_mapping = {
            "string": "string",
            "str": "string",
            "int": "int",
            "integer": "int",
            "long": "long",
            "bigint": "long",
            "double": "double",
            "float": "float",
            "boolean": "boolean",
            "bool": "boolean",
        }
        return type_mapping.get(type_name.lower(), "string")

    # ------------------------------------------------------------------
    # Freshness Check
    # ------------------------------------------------------------------

    def _check_freshness(self, df, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
        """
        Verify data freshness based on a timestamp column.

        Config:
            column: Column containing timestamps to check (required)
            max_delay_hours: Maximum allowed delay in hours (default: 24)
        """
        start_time = datetime.now(UTC)
        ok, _err = self._require_cfg(cfg, "column")
        if not ok:
            return False, self._details_config_missing("column"), None

        column = cfg["column"]
        max_delay = cfg.get("max_delay_hours", 24)

        total_rows = df.count()
        latest = df.select(f.max(f.col(column))).first()[0]
        if latest is None:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_no_data(f"column '{column}'"), column

        # Handle naive timestamps by assuming UTC
        # This is a common pattern in data lakes where timestamps are stored without timezone
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=UTC)

        now_utc = datetime.now(UTC)
        # Convert latest to UTC if it has a different timezone
        if latest.tzinfo != UTC:
            latest = latest.astimezone(UTC)

        age_h = (now_utc - latest).total_seconds() / 3600
        passed = age_h <= max_delay

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="freshness_violation",
                affected_columns=[column],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "age_hours": round(age_h, 1),
                    "max_delay_hours": max_delay,
                    "latest_timestamp": latest.isoformat(),
                },
            )
            where = self._get_where_clause()
            investigation_query = {
                "description": "Check data freshness distribution",
                "query": f"SELECT MAX({column}) as latest, MIN({column}) as earliest, DATEDIFF(HOUR, MAX({column}), current_timestamp()) as age_hours FROM {self._current_table_fq}{where}",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = self._details_pass_freshness("Data", f"{age_h:.1f}h old", f"{max_delay}h limit")
        else:
            details = self._details_fail_exceeds("Data age", f"{age_h:.1f}h", f"{max_delay}h limit")

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, column

    # ------------------------------------------------------------------
    # Business Rules Check
    # ------------------------------------------------------------------

    def _check_business_rules(self, df, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
        """
        Verify rows satisfy a business rule expressed as SQL expression.

        Config:
            check: SQL boolean expression that must be true for all rows (required)
            name: Human-readable name for the rule (default: "business_rule")
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of failing rows (0-100)
                max_failure_count: Maximum allowed number of failing rows
        """
        start_time = datetime.now(UTC)
        ok, _err = self._require_cfg(cfg, "check")
        if not ok:
            return False, self._details_config_missing("check"), None

        expr_str = cfg["check"]
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

    # ------------------------------------------------------------------
    # Compare Metric Check
    # ------------------------------------------------------------------

    def _check_compare_metric(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Generic metric comparison using expected/observed resolvers and operators.

        Config:
            metric: Observed metric configuration
            expected: Expected value configuration (static, SQL, column_value, etc.)
            operator: Comparison operator (==, !=, >, <, >=, <=) (default: ==)
            tolerance: Numeric tolerance for comparison (optional)
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
        tolerance = cfg.get("tolerance")
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
