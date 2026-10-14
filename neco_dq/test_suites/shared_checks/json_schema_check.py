# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""JSON schema validation check mixin (extracted from SharedChecksMixin)."""

from datetime import datetime, UTC

from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, BinaryType, StructType


class JsonSchemaCheckMixin:
    """
    Mixin providing the ``json_schema`` check and its private helpers.

    Split out of :class:`SharedChecksMixin` so a suite can compose only the
    checks it needs (e.g. Bronze exposes json_schema + business_rules without
    pulling in distinct_count / freshness / compare_metric).

    Requires the host class to provide base helpers: ``_require_cfg``,
    ``_build_failure_stats``, ``_build_execution_trace``, ``_evaluate_threshold``,
    ``_build_threshold_details``, ``_details_*``, ``_current_table_*``, ``_last_*``.
    """

    def _check_json_schema(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Validate JSON column structure, required fields, field types, and allowed values.

        Works for raw JSON stored as a STRING (parsed via ``from_json``), raw UTF-8 JSON
        bytes stored as ``BINARY`` (common in Bronze for raw Kafka payloads; decoded to
        STRING before parsing), and columns already decoded as ``STRUCT`` / ``ARRAY<STRUCT>``.
        The column type is detected at runtime; structured columns are validated directly
        without ``from_json``.

        Supports nested path validation using dot-notation (e.g., "rows.row_code"). Nested
        dot-notation is currently supported only for STRING columns; on already-parsed
        STRUCT columns, nested paths are skipped and reported in the result details.

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
        max_parse_failure_pct = cfg.get("max_parse_failure_percent", 0)

        if column not in df.columns:
            return False, self._details_not_found("Column", column), column

        if not required_fields and not field_types and not allowed_values:
            return False, self._details_config_missing("required_fields, field_types, or allowed_values"), column

        # BINARY payloads are decoded to STRING for validation; investigation-query
        # templates must do the same so they stay runnable against the physical table.
        source_is_binary = isinstance(df.schema[column].dataType, BinaryType)
        column_sql = f"decode({column}, 'UTF-8')" if source_is_binary else column

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

        column_dtype = df_check.schema[column].dataType
        is_structured_column = isinstance(column_dtype, (StructType, ArrayType))

        # Validate top-level fields if any
        if top_level["required"] or top_level["types"] or top_level["allowed"]:
            if is_structured_column:
                # Column is already a decoded STRUCT / ARRAY<STRUCT> (common in Bronze when the
                # ingestion decodes the payload); validate fields directly, without from_json.
                df_elements = self._get_structured_elements(df_check, column, column_dtype)
                total_elements = df_elements.count()
                self._validate_struct_elements(
                    df_elements,
                    self._struct_element_fields(column_dtype),
                    total_elements,
                    top_level,
                    all_missing,
                    all_type_mismatches,
                    all_invalid_values,
                )
            else:
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
                    where_parts = [f"from_json({column_sql}, 'struct<>') IS NULL"]
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

        # Validate nested fields for each parent path.
        # Nested dot-notation on already-decoded STRUCT columns is not yet supported
        # (needs getField chains instead of get_json_object); skip to avoid a type error.
        skipped_nested = bool(nested) and is_structured_column
        if nested and not is_structured_column:
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

        # Calculate total field validations for accurate threshold denominator.
        # Skipped nested paths (structured columns) are not counted since they were not validated.
        num_fields_checked = len(top_level["required"]) + len(top_level["types"]) + len(top_level["allowed"])
        if not skipped_nested:
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
                column_expr=column_sql,
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

        if skipped_nested:
            details += f" (nested paths skipped: unsupported on STRUCT column, {list(nested.keys())})"

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
        column_expr: str | None = None,
    ) -> dict:
        """Build investigation query for JSON schema violations.

        ``column_expr`` is the SQL expression used to read the JSON text (e.g.
        ``decode(value, 'UTF-8')`` for BINARY columns); it defaults to ``column`` for
        plain STRING columns. The raw ``column`` is still projected in the SELECT.
        """
        json_expr = column_expr or column
        where_parts = [f"{column} IS NOT NULL"]

        # Build conditions for missing required fields
        missing_conditions = []
        for field_name in all_missing:
            if "." in field_name:
                # Nested field - use json path
                missing_conditions.append(f"get_json_object({json_expr}, '$.{field_name}') IS NULL")
            else:
                # Top-level field
                missing_conditions.append(f"get_json_object({json_expr}, '$.{field_name}') IS NULL")

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

    def _prepare_json_check_df(self, df, column: str, sample_size: int):
        """Prepare DataFrame for JSON schema validation.

        ``BINARY`` columns (common in Bronze when the Kafka payload is stored as the raw
        bytes of UTF-8 JSON) are decoded to ``STRING`` so the ``from_json`` string path
        applies unchanged. STRING and STRUCT / ARRAY<STRUCT> columns are left untouched.
        """
        df_check = df.select(column).filter(f.col(column).isNotNull())
        if isinstance(df_check.schema[column].dataType, BinaryType):
            df_check = df_check.withColumn(column, f.decode(f.col(column), "UTF-8"))
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

    def _get_structured_elements(self, df_check, column: str, col_dtype):
        """
        Return a DataFrame with a struct ``_elem`` column from an already-decoded column.

        For ``ARRAY<STRUCT>`` the array is exploded (one row per element); for a plain
        ``STRUCT`` the column is aliased directly. Output is shaped like the string path's
        ``_elem`` so the shared field helpers (``getField``) apply unchanged.
        """
        if isinstance(col_dtype, ArrayType):
            return df_check.select(f.explode(f.col(column)).alias("_elem"))
        return df_check.select(f.col(column).alias("_elem"))

    def _struct_element_fields(self, col_dtype) -> set:
        """Return the field names of the element struct for a STRUCT / ARRAY<STRUCT> column."""
        element_type = col_dtype.elementType if isinstance(col_dtype, ArrayType) else col_dtype
        if isinstance(element_type, StructType):
            return {field.name for field in element_type.fields}
        return set()

    def _validate_struct_elements(
        self,
        df_elements,
        present_fields: set,
        total_elements: int,
        top_level: dict,
        all_missing: dict,
        all_type_mismatches: dict,
        all_invalid_values: dict,
    ) -> None:
        """
        Validate top-level fields directly on a decoded STRUCT / ARRAY<STRUCT> column.

        Fields declared in the config but absent from the struct schema are reported as
        entirely missing (count = total_elements). Present fields are checked for nulls,
        type format, and allowed values via the shared helpers; ``getField`` would raise on
        an absent field, so type/allowed checks are restricted to fields in ``present_fields``.
        """
        # Required: absent-from-schema => every element missing; present => count nulls.
        present_required = []
        for field_name in top_level["required"]:
            if field_name not in present_fields:
                all_missing[field_name] = total_elements
            else:
                present_required.append(field_name)
        all_missing.update(self._check_json_required_fields(df_elements, present_required))

        # Type / allowed checks only apply to fields that exist in the struct schema.
        present_types = {k: v for k, v in top_level["types"].items() if k in present_fields}
        present_allowed = {k: v for k, v in top_level["allowed"].items() if k in present_fields}
        all_type_mismatches.update(self._check_json_field_types(df_elements, present_types))
        all_invalid_values.update(self._check_json_allowed_values(df_elements, present_allowed))

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

        # Cast to string so the lexical patterns work both for JSON values parsed as
        # strings and for already-typed fields of a decoded STRUCT column.
        field_str = field_col.cast("string")

        if expected_type in ("int", "long"):
            # Expected int: value must be integer format (no decimal point)
            mismatch_filter = field_col.isNotNull() & ~field_str.rlike(int_pattern)
        elif expected_type in ("double", "float"):
            # Expected numeric: value must be a valid number
            mismatch_filter = field_col.isNotNull() & ~field_str.rlike(number_pattern)
        elif expected_type == "boolean":
            # Expected boolean: value must be "true" or "false"
            mismatch_filter = field_col.isNotNull() & ~field_str.rlike(bool_pattern)
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
