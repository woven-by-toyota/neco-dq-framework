# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any
from collections.abc import Callable, Mapping
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count, countDistinct, sum as sql_sum, avg, col, explode
from pyspark.sql.types import ArrayType

import pyspark.sql.functions as f

from asteval import Interpreter

# ============================
# Types
# ============================

ExpectedResult = tuple[
    bool,  # success
    Any | None,  # expected value
    str | None,  # column (if applicable)
    str,  # reason / metadata
]

ExpectedFn = Callable[
    [SparkSession, DataFrame, dict],
    ExpectedResult,
]

# ============================
# Spark aggregation registry
# ============================

SPARK_AGG_FUNCTIONS: Mapping[str, Callable] = {
    "count": count,
    "countDistinct": countDistinct,
    "sum": sql_sum,
    "avg": avg,
}

# ============================
# Safe expression evaluator
# ============================

SAFE_EVAL = Interpreter()


def _resolve_table(spark: SparkSession, df: DataFrame, config: dict) -> DataFrame:
    """
    Resolve target DataFrame from config supporting both formats:
    - New: catalog.schema.table
    - Old: database.table (with table potentially containing schema.table_name)
    """
    if "catalog" in config and "schema" in config and "table" in config:
        table_path = f"{config['catalog']}.{config['schema']}.{config['table']}"
        return spark.table(table_path)
    if "database" in config and "table" in config:
        table_path = f"{config['database']}.{config['table']}"
        return spark.table(table_path)
    return df


def safe_eval(expression: str, value: Any) -> Any:
    """Evaluate a simple expression safely with `value` in scope."""
    SAFE_EVAL.symtable["value"] = value
    result = SAFE_EVAL(expression)
    SAFE_EVAL.symtable.clear()
    return result


# ============================
# Resolvers
# ============================


def _static(_: SparkSession, __: DataFrame, cfg: dict) -> ExpectedResult:
    """Static expected value."""
    return True, cfg.get("value"), None, "expected=static"


def _aggregate(spark: SparkSession, df: DataFrame, cfg: dict) -> ExpectedResult:
    """Expected value derived from Spark aggregation."""
    ef = cfg.get("expected_from") or cfg.get("expected") or {}
    column = ef.get("column")
    function = ef.get("function")

    if not column or not function:
        return False, None, None, "aggregate_missing_column_or_function"

    df_target = _resolve_table(spark, df, ef)

    if "where" in ef:
        where_clauses = ef["where"]
        if isinstance(where_clauses, str):
            where_clauses = [where_clauses]

        for clause in where_clauses:
            df_target = df_target.filter(clause)

    # Resolve Spark function
    spark_fn = SPARK_AGG_FUNCTIONS.get(function) if isinstance(function, str) else function
    if not spark_fn:
        return False, None, column, f"unsupported_aggregate_function={function}"

    try:
        # Grouped aggregation
        if "group_by" in ef:
            group_cols: Sequence[str] = ef["group_by"]
            if isinstance(group_cols, str):
                group_cols = [group_cols]

            value_df = df_target.groupBy(*group_cols).agg(spark_fn(column).alias("metric_value"))

            if "expression" in ef:
                values = {
                    tuple(str(r[c]) for c in group_cols): safe_eval(ef["expression"], r["metric_value"])
                    for r in value_df.collect()
                }
            else:
                values = {tuple(str(r[c]) for c in group_cols): r["metric_value"] for r in value_df.collect()}

            return True, values, column, f"expected=aggregate({function})_grouped_by({list(group_cols)})"

        # Global aggregation
        value = df_target.select(spark_fn(column).alias("metric_value")).collect()[0]["metric_value"]

        if "expression" in ef:
            value = safe_eval(ef["expression"], value)

        return True, value, column, f"expected=aggregate({function})"

    except Exception as exc:
        return False, None, column, f"aggregate_error={exc!s}"


def _sql(spark: SparkSession, _: DataFrame, cfg: dict) -> ExpectedResult:
    """Expected value derived from SQL query."""
    ef = cfg.get("expected_from") or cfg.get("expected") or {}
    query = ef.get("query")

    if not query:
        return False, None, None, "sql_missing_query"

    try:
        value = spark.sql(query).collect()[0][0]
    except Exception as exc:
        return False, None, None, f"sql_execution_error={exc!s}"

    if value is None:
        return False, None, None, "sql_returned_null"

    return True, value, None, "expected=sql"


def _sql_grouped(spark: SparkSession, _: DataFrame, cfg: dict) -> ExpectedResult:
    """
    Expected value derived from SQL query with grouping support.

    Returns dict with tuple keys matching group_by columns.
    """
    ef = cfg.get("expected_from") or cfg.get("expected") or {}
    query = ef.get("query")
    group_by: Sequence[str] = ef.get("group_by", [])
    value_column = ef.get("value_column")

    if not query:
        return False, None, None, "sql_grouped_missing_query"

    if isinstance(group_by, str):
        group_by = [group_by]

    if not group_by:
        return False, None, None, "sql_grouped_missing_group_by"

    try:
        df_result = spark.sql(query)

        # Determine value column (last column if not specified)
        if not value_column:
            value_column = df_result.columns[-1]

        # Verify all group_by columns exist in result
        missing_cols = [col for col in group_by if col not in df_result.columns]
        if missing_cols:
            return False, None, None, f"sql_grouped_missing_columns={missing_cols}"

        # Verify value column exists
        if value_column not in df_result.columns:
            return False, None, None, f"sql_grouped_missing_value_column={value_column}"

        # Build dict with tuple keys
        values = {tuple(str(r[col]) for col in group_by): r[value_column] for r in df_result.collect()}

        return True, values, None, f"expected=sql_grouped({group_by})"

    except Exception as exc:
        return False, None, None, f"sql_grouped_error={exc!s}"


def _column_value(spark: SparkSession, df: DataFrame, cfg: dict) -> ExpectedResult:
    """
    Return the value(s) of a column, optionally grouped by one or more columns.
    Supports optional where clause filtering.
    """
    ef = cfg.get("expected_from") or cfg.get("expected") or {}
    column = ef.get("column")
    if not column:
        return False, None, None, "column_value_missing_column"

    df_target = _resolve_table(spark, df, ef)

    group_by: Sequence[str] = ef.get("group_by", [])
    if isinstance(group_by, str):
        group_by = [group_by]

    where_clauses = ef.get("where", [])
    if isinstance(where_clauses, str):
        where_clauses = [where_clauses]

    try:
        # Apply where filters
        for clause in where_clauses:
            df_target = df_target.filter(clause)

        if group_by:
            df_grouped = df_target.select(*group_by, column).dropna(subset=[column])
            values = {tuple(str(r[g]) for g in group_by): r[column] for r in df_grouped.collect()}
            return True, values, column, f"expected=column_value_grouped_by({group_by})"

        # ---- non-grouped case ----
        df_non_null = df_target.select(column).dropna(subset=[column])

        if df_target.limit(1).count() == 0:
            return False, None, column, "column_value_empty_df"

        if df_non_null.limit(1).count() == 0:
            return False, None, column, "column_value_all_null"

        value = df_non_null.limit(1).collect()[0][column]
        return True, value, column, "expected=column_value"

    except Exception as exc:
        return False, None, column, f"column_value_error={exc!s}"


def _prepare_array_field_extraction(
    df_target: DataFrame,
    column: str,
    fields: list[str],
    item_group_by: str | None,
    group_by: list[str],
) -> tuple[DataFrame, list[str], list[str]]:
    """Prepare DataFrame for array field extraction."""
    schema_field = next((fld for fld in df_target.schema.fields if fld.name == column), None)
    if schema_field is None:
        raise ValueError(f"column_not_found={column}")

    dt = schema_field.dataType
    parsed_col = col(column) if isinstance(dt, ArrayType) else f.from_json(col(column), "array<map<string,string>>")

    df_exploded = df_target.withColumn("_elem", explode(parsed_col))

    # Extract all requested fields and cast to double
    # These fields are expected to be numeric (e.g., borda_points, mean_rank, borda_norm_pct)
    for field_name in fields:
        field_value = df_exploded["_elem"].getItem(field_name)
        df_exploded = df_exploded.withColumn(f"_field_{field_name}", field_value.cast("double"))

    # Extract item_group_by field if specified
    if item_group_by:
        if "|" in item_group_by:
            fallback_fields = [fld.strip() for fld in item_group_by.split("|")]
            # Build coalesce BEFORE casting (so NULL stays NULL for coalesce to work)
            group_col = df_exploded["_elem"].getItem(fallback_fields[0])
            for fallback in fallback_fields[1:]:
                group_col = f.coalesce(group_col, df_exploded["_elem"].getItem(fallback))
            # Cast to string AFTER coalesce
            df_exploded = df_exploded.withColumn("_item_group", group_col.cast("string"))
        else:
            df_exploded = df_exploded.withColumn(
                "_item_group", df_exploded["_elem"].getItem(item_group_by).cast("string")
            )

    effective_group_by = list(group_by)
    if item_group_by:
        effective_group_by.append("_item_group")

    field_cols = [f"_field_{field_name}" for field_name in fields]

    return df_exploded, effective_group_by, field_cols


def _build_array_field_result(
    df_exploded: DataFrame,
    fields: list[str],
    field_cols: list[str],
    effective_group_by: list[str],
    item_group_by: str | None,
) -> dict | list:
    """Build result from extracted array fields."""
    if effective_group_by:
        select_cols = [*effective_group_by, *field_cols]
        df_result = df_exploded.select(*select_cols)

        if len(fields) == 1:
            # Single field - maintain backward compatibility
            return {
                tuple(str(r[g]) for g in effective_group_by): r[field_cols[0]]
                for r in df_result.collect()
                if r[field_cols[0]] is not None
            }
        # Multiple fields
        return {
            tuple(str(r[g]) for g in effective_group_by): {
                "item_id": r["_item_group"] if item_group_by else None,
                **{
                    field_name: r[f"_field_{field_name}"]
                    for field_name in fields
                    if r[f"_field_{field_name}"] is not None
                },
            }
            for r in df_result.collect()
        }

    # No grouping
    if len(fields) == 1:
        return [r[field_cols[0]] for r in df_exploded.select(field_cols[0]).collect() if r[field_cols[0]] is not None]

    return [
        {field_name: r[f"_field_{field_name}"] for field_name in fields if r[f"_field_{field_name}"] is not None}
        for r in df_exploded.select(*field_cols).collect()
    ]


def _array_field_value(spark: SparkSession, df: DataFrame, cfg: dict) -> ExpectedResult:
    """
    Extract field values from an array column, optionally grouped.

    Returns values as list of tuples matching the observed format:
        [(group_keys, {"field_name": value}), ...]
    """
    ef = cfg.get("expected_from") or cfg.get("expected") or {}
    column = ef.get("column")
    field = ef.get("field")
    fields = ef.get("fields")

    if not column or (not field and not fields):
        return False, None, None, "array_field_value_missing_column_or_field"

    # Normalize to list
    if field:
        fields = [field]
    elif isinstance(fields, str):
        fields = [fields]

    # Resolve target table
    if "catalog" not in ef and "schema" not in ef and "database" not in ef:
        return False, None, None, "array_field_value_missing_table_specification"

    df_target = _resolve_table(spark, df, ef)

    group_by: Sequence[str] = ef.get("group_by", [])
    if isinstance(group_by, str):
        group_by = [group_by]

    item_group_by = ef.get("item_group_by")

    try:
        df_exploded, effective_group_by, field_cols = _prepare_array_field_extraction(
            df_target, column, fields, item_group_by, group_by
        )

        values = _build_array_field_result(df_exploded, fields, field_cols, effective_group_by, item_group_by)

        metric_desc = f"expected=array_field_value({fields})"
        if effective_group_by:
            metric_desc += f"_grouped_by({effective_group_by})"

        return True, values, column, metric_desc

    except Exception as exc:
        return False, None, column, f"array_field_value_error={exc!s}"


def _array_ordered_field_value(spark: SparkSession, df: DataFrame, cfg: dict) -> ExpectedResult:
    """
    Extract field values from an array column preserving original order.
    Unlike _array_field_value, this preserves the exact order of elements in the array.

    Useful for verifying that arrays match an expected ordering from a reference table.

    Returns ordered list of values for each group, matching observed format.
    """
    ef = cfg.get("expected_from") or cfg.get("expected") or {}
    column = ef.get("column")
    field = ef.get("field")
    value_type = ef.get("value_type", "double")
    path = ef.get("path")  # Optional path to nested array

    if not column or not field:
        return False, None, None, "array_ordered_field_value_missing_column_or_field"

    # Resolve target table
    if "catalog" not in ef and "schema" not in ef and "database" not in ef:
        return False, None, None, "array_ordered_field_value_missing_table_specification"

    df_target = _resolve_table(spark, df, ef)

    group_by: Sequence[str] = ef.get("group_by", [])
    if isinstance(group_by, str):
        group_by = [group_by]

    try:
        schema_field = next((fld for fld in df_target.schema.fields if fld.name == column), None)
        if schema_field is None:
            return False, None, column, f"column_not_found={column}"

        dt = schema_field.dataType
        parsed_col = col(column) if isinstance(dt, ArrayType) else f.from_json(col(column), "array<map<string,string>>")

        # Use posexplode which returns (pos, col) as two separate columns
        df_exploded = df_target.withColumn("_parsed", parsed_col).select(
            "*", f.posexplode("_parsed").alias("_idx", "_elem")
        )

        # Handle nested array via path
        if path:
            # Store parent index for ordering
            df_exploded = df_exploded.withColumn("_parent_idx", col("_idx"))
            nested_col = df_exploded["_elem"].getItem(path)
            nested_as_string = nested_col.cast("string")
            nested_parsed = f.from_json(nested_as_string, "array<map<string,string>>")
            df_exploded = df_exploded.withColumn("_nested", nested_parsed)
            df_exploded = df_exploded.filter(col("_nested").isNotNull() & (f.size(col("_nested")) > 0))
            # posexplode nested array
            df_exploded = df_exploded.select("*", f.posexplode("_nested").alias("_nested_idx", "_elem"))
            # Create combined index for ordering: parent_idx * 1000 + nested_idx
            df_exploded = df_exploded.withColumn("_idx", col("_parent_idx") * 1000 + col("_nested_idx"))

        df_exploded = df_exploded.withColumn("_value", col("_elem")[field].cast(value_type)).filter(
            col("_value").isNotNull()
        )

        if group_by:
            # Collect values as ordered list preserving array order
            df_agg = df_exploded.groupBy(*group_by).agg(
                f.array_sort(f.collect_list(f.struct(col("_idx"), col("_value")))).alias("_ordered_values")
            )

            # Build dict with tuple keys matching observed format
            values = {
                tuple(str(r[g]) for g in group_by): [item["_value"] for item in r["_ordered_values"]]
                for r in df_agg.collect()
            }
            return True, values, column, f"expected=array_ordered_field_value({field})_grouped_by({group_by})"

        # No grouping - return ordered list
        ordered_rows = df_exploded.select("_idx", "_value").orderBy("_idx").collect()
        value = [r["_value"] for r in ordered_rows]
        return True, value, column, f"expected=array_ordered_field_value({field})"

    except Exception as exc:
        return False, None, column, f"array_ordered_field_value_error={exc!s}"


# ============================
# Resolver registry
# ============================

EXPECTED_RESOLVERS: dict[str, ExpectedFn] = {
    "static": _static,
    "aggregate": _aggregate,
    "sql": _sql,
    "sql_grouped": _sql_grouped,
    "column_value": _column_value,
    "array_field_value": _array_field_value,
    "array_ordered_field_value": _array_ordered_field_value,
}


def resolve_expected(spark: SparkSession, df: DataFrame, cfg: dict) -> ExpectedResult:
    """Resolve expected value based on configuration.

    Resolution priority:
      1. static value
      2. expected_from.type (or expected.type as alias)
    """
    if "value" in cfg:
        expected_type = "static"
    else:
        # Support both "expected_from" and "expected" as keys
        ef = cfg.get("expected_from") or cfg.get("expected")
        if not ef:
            return False, None, None, "missing_expected"
        expected_type = ef.get("type")

    resolver = EXPECTED_RESOLVERS.get(expected_type)
    if not resolver:
        return False, None, None, f"unsupported_expected_type={expected_type}"

    try:
        return resolver(spark, df, cfg)
    except Exception as exc:
        return False, None, None, f"expected_resolution_error={exc!s}"
