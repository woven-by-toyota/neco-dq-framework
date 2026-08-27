# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any
from collections.abc import Callable, Sequence
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as sql_sum,
    avg,
    when,
    trim,
    isnan,
    size,
    from_json,
    any_value,
    explode,
    collect_set,
)
from pyspark.sql.types import NumericType, ArrayType, DoubleType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import math

MetricFn = Callable[
    [SparkSession, DataFrame, dict],
    tuple[bool, Any | None, str | None, str],
]

# JSON_ARRAY_SCHEMA: array of objects with 'text' and 'value' fields
JSON_ARRAY_SCHEMA = "array<struct<text:string,value:string>>"
GENERIC_JSON_ARRAY_SCHEMA = "array<map<string,string>>"

# Kendall's W default thresholds for consensus labels
# Can be overridden via 'thresholds' config parameter
DEFAULT_KENDALL_W_THRESHOLDS = {
    "strong": 0.9,
    "moderate": 0.7,
    "fair": 0.5,
    "weak": 0.3,
    "low": 0.2,  # Alternative label for weak consensus
    "none": 0.0,
}

# Minimum requirements for Kendall's W calculation
MIN_ITEMS = 2  # Minimum number of items to rank
MIN_RATERS = 2  # Minimum number of raters

# Maximum number of label mismatches to report in error message
MAX_LABEL_MISMATCH_REPORT = 5


def completed_condition(df: DataFrame, column: str):
    field = next(f for f in df.schema.fields if f.name == column)
    dt = field.dataType

    # NUMERIC
    if isinstance(dt, NumericType):
        return col(column).isNotNull() & ~isnan(col(column))

    # ARRAY
    if isinstance(dt, ArrayType):
        return col(column).isNotNull() & (size(col(column)) > 0)

    parsed = from_json(col(column), JSON_ARRAY_SCHEMA)

    return col(column).isNotNull() & (trim(col(column)) != "") & parsed.isNotNull() & (size(parsed) > 0)


# Mapping string -> Spark function
SPARK_AGG_FUNCTIONS = {
    "count": count,
    "countDistinct": countDistinct,
    "sum": sql_sum,
    "avg": avg,
}


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


def _count(_: SparkSession, df: DataFrame, __: dict):
    return True, df.count(), None, "metric=count"


def _aggregate(spark: SparkSession, df: DataFrame, cfg: dict):
    column = cfg["column"]
    df_target = _resolve_table(spark, df, cfg)
    function = cfg["function"]

    spark_fn = SPARK_AGG_FUNCTIONS.get(function) if isinstance(function, str) else function
    if not spark_fn:
        return False, None, column, f"unsupported_aggregate_function={function}"

    try:
        where_clauses = cfg.get("where", [])
        if isinstance(where_clauses, str):
            where_clauses = [where_clauses]

        for clause in where_clauses:
            df_target = df_target.filter(clause)

        group_by: Sequence[str] = cfg.get("group_by", [])
        if isinstance(group_by, str):
            group_by = [group_by]

        if group_by:
            df_agg = df_target.groupBy(*group_by).agg(spark_fn(column).alias("metric_value"))
            values = [(tuple(r[c] for c in group_by), r["metric_value"]) for r in df_agg.collect()]
            return True, values, column, f"metric=aggregate({function})_grouped_by({group_by})"

        value = df_target.select(spark_fn(column).alias("metric_value")).collect()[0]["metric_value"]
        return True, value, column, f"metric=aggregate({function})"

    except Exception as e:
        return False, None, column, f"aggregate_error={e!s}"


def _sql(spark: SparkSession, _: DataFrame, cfg: dict):
    value = spark.sql(cfg["query"]).collect()[0][0]
    if value is None:
        return False, None, None, "sql_metric_returned_null"
    return True, value, None, "metric=sql"


def _ratio(_: SparkSession, df: DataFrame, cfg: dict):
    column = cfg["column"]
    metric_type = cfg.get("type")
    group_by: Sequence[str] = cfg.get("group_by", [])
    if isinstance(group_by, str):
        group_by = [group_by]

    if metric_type != "completion_ratio":
        return False, None, column, f"unsupported_ratio_type={metric_type}"

    try:
        condition = completed_condition(df, column)

        if group_by:
            df_agg = df.groupBy(*group_by).agg(
                count("*").alias("total"),
                count(when(condition, 1)).alias("completed"),
            )
            values = [(tuple(r[c] for c in group_by), r["completed"] / r["total"]) for r in df_agg.collect()]
            return True, values, column, f"metric=completion_ratio_grouped_by({group_by})"

        row = df.select(
            count("*").alias("total"),
            count(when(condition, 1)).alias("completed"),
        ).collect()[0]

        if row["total"] == 0:
            return False, None, column, "empty_dataframe"

        return True, row["completed"] / row["total"], column, "metric=completion_ratio"

    except Exception as e:
        return False, None, column, f"ratio_error={e!s}"


def _size_array(_: SparkSession, df: DataFrame, cfg: dict):
    column = cfg["column"]
    group_by: Sequence[str] = cfg.get("group_by", [])
    if isinstance(group_by, str):
        group_by = [group_by]

    field = next(f for f in df.schema.fields if f.name == column)
    dt = field.dataType

    parsed_col = col(column) if isinstance(dt, ArrayType) else from_json(col(column), JSON_ARRAY_SCHEMA)

    try:
        if group_by:
            df_agg = df.groupBy(*group_by).agg(size(any_value(parsed_col)).alias("metric_value"))
            values = [(tuple(r[c] for c in group_by), r["metric_value"]) for r in df_agg.collect()]
            return True, values, column, f"metric=size_array_grouped_by({group_by})"

        value = df.select(size(parsed_col).alias("metric_value")).collect()[0]["metric_value"]
        return True, value, column, "metric=size_array"

    except Exception as e:
        return False, None, column, f"size_array_error={e!s}"


def _sum_array_field(_: SparkSession, df: DataFrame, cfg: dict):
    column = cfg["column"]
    field = cfg["field"]
    path = cfg.get("path")  # Path to array field (e.g., "cells" for matrix_data.cells[])
    item_group_by = cfg.get("item_group_by")  # Field to group by within array items
    group_by: Sequence[str] = cfg.get("group_by", [])
    value_type = cfg.get("value_type", "long")

    if isinstance(group_by, str):
        group_by = [group_by]

    try:
        if path:
            # Case 1: Column is an object/map with an array field at 'path'
            # E.g., matrix_data = {"rows": [...], "cells": [...]}
            # Parse as string and extract the array field
            parsed_obj = from_json(col(column), "map<string,string>")
            array_field = parsed_obj.getItem(path)
            array_parsed = from_json(array_field.cast("string"), GENERIC_JSON_ARRAY_SCHEMA)
            df_parsed = df.withColumn("_array", array_parsed)
            df_parsed = df_parsed.filter(col("_array").isNotNull() & (size(col("_array")) > 0))
            df_parsed = df_parsed.withColumn("_elem", explode(col("_array")))
        else:
            # Case 2: Column is directly an array
            # E.g., column = [{"field": value}, ...]
            parsed = from_json(col(column), GENERIC_JSON_ARRAY_SCHEMA)
            df_parsed = df.withColumn("_parsed", parsed).withColumn("_elem", explode("_parsed"))

        # Extract item_group_by field if specified (e.g., row_code from each cell)
        if item_group_by:
            if "|" in item_group_by:
                fallback_fields = [fld.strip() for fld in item_group_by.split("|")]
                # Build coalesce BEFORE casting (so NULL stays NULL for coalesce to work)
                group_col = df_parsed["_elem"].getItem(fallback_fields[0])
                for fallback in fallback_fields[1:]:
                    group_col = f.coalesce(group_col, df_parsed["_elem"].getItem(fallback))
                # Cast to string AFTER coalesce
                df_parsed = df_parsed.withColumn("_item_group", group_col.cast("string"))
            else:
                df_parsed = df_parsed.withColumn(
                    "_item_group", df_parsed["_elem"].getItem(item_group_by).cast("string")
                )

        df_parsed = df_parsed.withColumn("_value", col("_elem")[field].cast(value_type))

        # Build effective group_by (include item_group if specified)
        effective_group_by = list(group_by)
        if item_group_by:
            effective_group_by.append("_item_group")

        if effective_group_by:
            df_agg = df_parsed.groupBy(*effective_group_by).agg(sql_sum(col("_value")).alias("metric_value"))

            values = [(tuple(str(r[g]) for g in effective_group_by), r["metric_value"]) for r in df_agg.collect()]

            return (
                True,
                values,
                column,
                f"metric=sum_array_field({field})_grouped_by({effective_group_by})",
            )

        value = df_parsed.select(sql_sum(col("_value")).alias("metric_value")).collect()[0]["metric_value"]

        return True, value, column, f"metric=sum_array_field({field})"

    except Exception as e:
        return False, None, column, f"sum_array_field_error={e!s}"


def _array_field_values(_: SparkSession, df: DataFrame, cfg: dict):
    column = cfg["column"]
    field = cfg["field"]
    group_by: Sequence[str] = cfg.get("group_by", [])

    if isinstance(group_by, str):
        group_by = [group_by]

    parsed = from_json(col(column), GENERIC_JSON_ARRAY_SCHEMA)

    try:
        df_exploded = (
            df.withColumn("_parsed", parsed)
            .withColumn("_elem", explode("_parsed"))
            .withColumn("_value", col("_elem")[field].cast("string"))
            .filter(col("_value").isNotNull())
        )

        if group_by:
            df_agg = df_exploded.groupBy(*group_by).agg(collect_set("_value").alias("metric_value"))
            values = [(tuple(str(r[g]) for g in group_by), sorted(r["metric_value"])) for r in df_agg.collect()]
            return True, values, column, f"metric=array_field_values_grouped_by({group_by})"

        value = sorted([r["_value"] for r in df_exploded.select("_value").distinct().collect()])
        return True, value, column, "metric=array_field_values"

    except Exception as e:
        return False, None, column, f"array_field_values_error={e!s}"


def _array_sorted_field_values(_: SparkSession, df: DataFrame, cfg: dict):
    """
    Extract field values from array column and sort them in the specified order.

    This metric extracts values and automatically sorts them.
    Useful for verifying that arrays should be sorted by a specific field.

    Returns list of values sorted in the specified order for each group.
    """
    column = cfg["column"]
    field = cfg["field"]
    group_by: Sequence[str] = cfg.get("group_by", [])
    value_type = cfg.get("value_type", "double")
    order = cfg.get("order", "desc").lower()  # 'desc' or 'asc', default to 'desc'
    path = cfg.get("path")  # Optional path to nested array

    if order not in ("desc", "asc"):
        return False, None, column, f"invalid_order={order}_expected_desc_or_asc"

    if isinstance(group_by, str):
        group_by = [group_by]

    schema_field = next((fld for fld in df.schema.fields if fld.name == column), None)
    if schema_field is None:
        return False, None, column, f"column_not_found={column}"

    dt = schema_field.dataType
    parsed_col = col(column) if isinstance(dt, ArrayType) else from_json(col(column), GENERIC_JSON_ARRAY_SCHEMA)

    # Determine sort direction
    reverse_sort = order == "desc"

    try:
        # Extract values from array
        df_exploded = df.withColumn("_parsed", parsed_col).select("*", f.explode("_parsed").alias("_elem"))

        # Handle nested array via path
        if path:
            nested_col = df_exploded["_elem"].getItem(path)
            nested_as_string = nested_col.cast("string")
            nested_parsed = from_json(nested_as_string, GENERIC_JSON_ARRAY_SCHEMA)
            df_exploded = df_exploded.withColumn("_nested", nested_parsed)
            df_exploded = df_exploded.filter(col("_nested").isNotNull() & (size(col("_nested")) > 0))
            df_exploded = df_exploded.withColumn("_elem", f.explode(col("_nested")))

        df_exploded = df_exploded.withColumn("_value", col("_elem")[field].cast(value_type)).filter(
            col("_value").isNotNull()
        )

        if group_by:
            # Collect values and sort in specified order
            df_agg = df_exploded.groupBy(*group_by).agg(f.collect_list("_value").alias("_values"))

            values = [
                (
                    tuple(str(r[g]) for g in group_by),
                    sorted(r["_values"], reverse=reverse_sort),
                )
                for r in df_agg.collect()
            ]
            return True, values, column, f"metric=array_sorted_field_values({order})_grouped_by({group_by})"

        # No grouping - collect and sort
        all_values = [r["_value"] for r in df_exploded.select("_value").collect()]
        value = sorted(all_values, reverse=reverse_sort)
        return True, value, column, f"metric=array_sorted_field_values({order})"

    except Exception as e:
        return False, None, column, f"array_sorted_field_values_error={e!s}"


def _column_value(_: SparkSession, df: DataFrame, cfg: dict):
    column = cfg["column"]
    group_by = cfg.get("group_by", [])
    where_clauses = cfg.get("where", [])

    if isinstance(group_by, str):
        group_by = [group_by]

    if isinstance(where_clauses, str):
        where_clauses = [where_clauses]

    try:
        # Apply where filters
        df_filtered = df
        for clause in where_clauses:
            df_filtered = df_filtered.filter(clause)

        if group_by:
            df_agg = df_filtered.groupBy(*group_by).agg(any_value(col(column)).alias("metric_value"))

            values = [(tuple(r[g] for g in group_by), r["metric_value"]) for r in df_agg.collect()]

            return True, values, column, f"metric=column_value_grouped_by({group_by})"

        value = df_filtered.select(any_value(col(column)).alias("metric_value")).collect()[0]["metric_value"]

        return True, value, column, "metric=column_value"

    except Exception as e:
        return False, None, column, f"column_value_error={e!s}"


# ----------------------------------------------------------------------
# Function group: Array statistics
# ----------------------------------------------------------------------
def _parse_array_column(
    df: DataFrame,
    column: str,
    field: str,
    weight_field: str | None = None,
    path: str | None = None,
    item_group_by: str | None = None,
) -> DataFrame:
    """
    Parse an array column and extract field/weight values.

    Args:
        df: Input DataFrame
        column: The array column name
        field: The field to extract from array elements
        weight_field: Optional weight field
        path: Optional path to nested array (e.g., "ranks" to access items[].ranks[])
        item_group_by: Optional field to extract from parent array element for grouping

    Returns:
        DataFrame with _value, _weight, and optionally _item_group columns
    """
    schema_field = next((fld for fld in df.schema.fields if fld.name == column), None)
    if schema_field is None:
        raise ValueError(f"column_not_found={column}")
    dt = schema_field.dataType
    parsed_col = f.col(column) if isinstance(dt, ArrayType) else f.from_json(f.col(column), "array<map<string,string>>")
    df_parsed = df.withColumn("_elem", f.explode(parsed_col))

    # Extract item_group_by field from parent array element BEFORE navigating into nested path
    # Supports fallback syntax: "item_code|item_label" tries item_code first, falls back to item_label
    if item_group_by:
        if "|" in item_group_by:
            fields = [fld.strip() for fld in item_group_by.split("|")]
            # Build coalesce BEFORE casting (so NULL stays NULL for coalesce to work)
            group_col = df_parsed["_elem"].getItem(fields[0])
            for fallback in fields[1:]:
                group_col = f.coalesce(group_col, df_parsed["_elem"].getItem(fallback))
            # Cast to string AFTER coalesce
            df_parsed = df_parsed.withColumn("_item_group", group_col.cast("string"))
        else:
            df_parsed = df_parsed.withColumn("_item_group", df_parsed["_elem"].getItem(item_group_by).cast("string"))

    # Handle nested array via path
    if path:
        # Navigate into nested array using path
        # Supports nested structures like items[].ranks[] where path="ranks"
        nested_col = df_parsed["_elem"].getItem(path)
        # Always cast to string and parse as JSON array
        # This handles both string JSON and already-parsed arrays uniformly
        nested_as_string = nested_col.cast("string")
        nested_parsed = f.from_json(nested_as_string, "array<map<string,string>>")
        df_parsed = df_parsed.withColumn("_nested", nested_parsed)
        df_parsed = df_parsed.filter(f.col("_nested").isNotNull() & (f.size(f.col("_nested")) > 0))
        df_parsed = df_parsed.withColumn("_elem", f.explode(f.col("_nested")))

    df_parsed = df_parsed.withColumn("_value", df_parsed["_elem"].getItem(field).cast(DoubleType()))
    if weight_field:
        df_parsed = df_parsed.withColumn("_weight", df_parsed["_elem"].getItem(weight_field).cast(DoubleType()))
    else:
        df_parsed = df_parsed.withColumn("_weight", f.lit(1.0))
    return df_parsed.filter(f.col("_value").isNotNull() & f.col("_weight").isNotNull())


def _weighted_mean_std_ci_grouped(df_parsed: DataFrame, group_by: Sequence[str] = []) -> list[tuple[tuple, dict]]:
    """
    Returns a list of tuples: (group_values, {mean, std, ci_95_low, ci_95_high})
    Values are replicated according to their weight to be consistent with median calculation.
    If no group_by, group_values is ().
    """

    df_rep = df_parsed.withColumn("_replicate", f.expr("array_repeat(_value, cast(_weight as int))"))
    df_rep = df_rep.withColumn("_value_weighted", f.explode("_replicate"))

    if group_by:
        df_grouped = df_rep.groupBy(*group_by).agg(f.collect_list("_value_weighted").alias("_vals"))
    else:
        df_grouped = df_rep.agg(f.collect_list("_value_weighted").alias("_vals"))

    results = []
    for r in df_grouped.collect():
        vals = r["_vals"]
        if not vals:
            metrics = {"mean": None, "std": None, "ci_95_low": None, "ci_95_high": None}
        else:
            n = len(vals)
            mean_val = sum(vals) / n
            # Explicit implementation of sample standard deviation (n-1),
            # aligned with Spark stddev_samp semantics and easier to extend
            # (e.g. weighted std, confidence intervals).
            std_val = math.sqrt(sum((v - mean_val) ** 2 for v in vals) / (n - 1)) if n > 1 else 0.0

            ci_multiplier = 1.96
            ci_low = mean_val - ci_multiplier * std_val / math.sqrt(n) if n > 1 else mean_val
            ci_high = mean_val + ci_multiplier * std_val / math.sqrt(n) if n > 1 else mean_val
            metrics = {"mean": mean_val, "std": std_val, "ci_95_low": ci_low, "ci_95_high": ci_high}

        group_vals = tuple(r[g] for g in group_by) if group_by else ()
        results.append((group_vals, metrics))

    return results


def _median(arr: list[float], method: str = "discrete") -> float | None:
    n = len(arr)
    if n == 0:
        return None

    if method == "continuous":
        return arr[n // 2] if n % 2 == 1 else (arr[n // 2 - 1] + arr[n // 2]) / 2

    if method == "lower":
        return arr[(n - 1) // 2]

    if method == "upper":
        return arr[n // 2]

    return arr[n // 2]


def _kendall_w_label(w: float, thresholds: dict | None = None) -> str:
    """Convert Kendall's W value to consensus label using thresholds.

    Args:
        w: Kendall's W value (0.0 to 1.0)
        thresholds: Optional custom thresholds dict {label: min_value}.
                   If None, uses DEFAULT_KENDALL_W_THRESHOLDS.

    Returns:
        Label string (e.g., "strong", "moderate", "low", "none")
    """
    if thresholds is None:
        thresholds = DEFAULT_KENDALL_W_THRESHOLDS

    # Sort thresholds by value descending to check from highest to lowest
    sorted_labels = sorted(thresholds.items(), key=lambda x: x[1], reverse=True)
    for label, threshold in sorted_labels:
        if w >= threshold:
            return label
    return "none"


def _compute_kendall_w(
    ranks: list[float],
    m: float,
    thresholds: dict | None = None,
    min_raters: int = MIN_RATERS,
) -> tuple[float | None, str]:
    """
    Compute Kendall's W from ranks and number of raters.

    Args:
        ranks: List of mean ranks for each item
        m: Number of raters
        thresholds: Optional custom thresholds for label classification
        min_raters: Minimum number of raters required (default: 2)

    Returns:
        Tuple of (W value, label)
    """
    n = len(ranks)
    if n < MIN_ITEMS or m < min_raters:
        return None, "insufficient_data"

    # Convert mean_rank to rank sums: R_i = mean_rank_i * m
    rank_sums = [rank * m for rank in ranks]
    r_mean = sum(rank_sums) / n
    s = sum((r_i - r_mean) ** 2 for r_i in rank_sums)

    # W = 12 * S / (m² * (n³ - n))
    denominator = (m**2) * (n**3 - n)
    w = (12 * s) / denominator if denominator > 0 else 0.0
    w = min(max(w, 0.0), 1.0)  # Clamp to [0, 1]
    return w, _kendall_w_label(w, thresholds)


def _build_expected_labels_map(df: DataFrame, label_column: str, group_by: Sequence[str]) -> dict[tuple, str]:
    """Build a map of group keys to expected labels from a DataFrame column."""
    expected_labels_map: dict[tuple, str] = {}
    if group_by:
        df_labels = df.select(*group_by, label_column).distinct()
        for r in df_labels.collect():
            group_vals = tuple(r[g] for g in group_by)
            expected_labels_map[group_vals] = r[label_column]
    else:
        label_row = df.select(label_column).first()
        if label_row:
            expected_labels_map[()] = label_row[label_column]
    return expected_labels_map


def _format_label_mismatch_error(label_mismatches: list[str], total_groups: int) -> str:
    """Format label mismatch errors for reporting with totals."""
    failed = len(label_mismatches)
    passed = total_groups - failed
    header = f"kendall_w_label_mismatch: {failed}/{total_groups} failed ({passed} passed); "
    details = "; ".join(label_mismatches[:MAX_LABEL_MISMATCH_REPORT])
    if failed > MAX_LABEL_MISMATCH_REPORT:
        return header + details + f" (+{failed - MAX_LABEL_MISMATCH_REPORT} more)"
    return header + details


def _prepare_kendall_w_dataframe(
    df: DataFrame,
    column: str,
    rank_sum_field: str,
    appearances_field: str,
    group_by: Sequence[str],
    label_column: str | None,
) -> DataFrame:
    """Prepare DataFrame for Kendall W calculation by exploding and extracting fields."""
    schema_field = next((fld for fld in df.schema.fields if fld.name == column), None)
    if schema_field is None:
        raise ValueError(f"column_not_found={column}")

    dt = schema_field.dataType
    parsed_col = f.col(column) if isinstance(dt, ArrayType) else f.from_json(f.col(column), "array<map<string,string>>")

    df_exploded = df.withColumn("_elem", f.explode(parsed_col))
    df_exploded = df_exploded.withColumn("_mean_rank", df_exploded["_elem"].getItem(rank_sum_field).cast(DoubleType()))
    df_exploded = df_exploded.withColumn(
        "_appearances", df_exploded["_elem"].getItem(appearances_field).cast(DoubleType())
    )

    select_cols = [*list(group_by), "_mean_rank", "_appearances"]
    if label_column:
        select_cols.append(label_column)
    return df_exploded.select(*select_cols).filter(f.col("_mean_rank").isNotNull())


def _kendall_w(_: SparkSession, df: DataFrame, cfg: dict) -> tuple[bool, dict | list, str, str]:
    """
    Calculate Kendall's W (coefficient of concordance) from ranking data.

    Kendall's W formula:
        W = 12 * S / (m² * (n³ - n))
    Where:
        n = number of items
        m = number of raters
        R_i = sum of ranks for item i
        S = Σ(R_i - R̄)² where R̄ = mean of R_i

    Label validation:
        If 'label' or 'label_column' is specified, validates that the observed
        consensus level matches the expected label. Uses configurable thresholds.

    Config parameters:
        column: Array column with ranking data
        rank_sum_field: Field name for mean rank (default: "mean_rank")
        appearances_field: Field name for rater count (default: "item_appearances_n")
        group_by: Optional grouping columns
        label: Static expected label (e.g., "strong", "moderate", "low")
        label_column: Column name containing expected label per row/group
        thresholds: Custom thresholds dict (e.g., {"strong": 0.9, "moderate": 0.3})
        min_raters: Minimum raters required (default: 2, set to 1 for single-rater support)
    """
    column = cfg["column"]
    rank_sum_field = cfg.get("rank_sum_field", "mean_rank")
    appearances_field = cfg.get("appearances_field", "item_appearances_n")
    group_by: Sequence[str] = cfg.get("group_by", [])
    if isinstance(group_by, str):
        group_by = [group_by]

    expected_label_static = cfg.get("label")
    label_column = cfg.get("label_column")
    thresholds = cfg.get("thresholds")
    min_raters_raw = cfg.get("min_raters", MIN_RATERS)
    try:
        min_raters = int(min_raters_raw)
    except (TypeError, ValueError):
        return False, None, column, f"kendall_w: invalid 'min_raters' value {min_raters_raw!r}, expected int >= 1"
    if min_raters < 1:
        return False, None, column, f"kendall_w: 'min_raters' must be >= 1, got {min_raters}"

    try:
        df_items = _prepare_kendall_w_dataframe(df, column, rank_sum_field, appearances_field, group_by, label_column)

        expected_labels_map: dict[tuple, str] = {}
        if label_column:
            expected_labels_map = _build_expected_labels_map(df, label_column, group_by)

        if group_by:
            return _kendall_w_grouped(
                df_items, column, group_by, thresholds, expected_label_static, expected_labels_map, min_raters
            )
        return _kendall_w_ungrouped(
            df_items, column, thresholds, expected_label_static, expected_labels_map, min_raters
        )

    except Exception as e:
        return False, None, column, f"kendall_w_error={e!s}"


def _kendall_w_grouped(
    df_items: DataFrame,
    column: str,
    group_by: Sequence[str],
    thresholds: dict | None,
    expected_label_static: str | None,
    expected_labels_map: dict[tuple, str],
    min_raters: int = MIN_RATERS,
) -> tuple[bool, list, str, str]:
    """Calculate Kendall W with grouping and label validation."""
    df_grouped = df_items.groupBy(*group_by).agg(
        f.collect_list("_mean_rank").alias("_ranks"),
        f.first("_appearances").alias("_m"),
    )

    results = []
    label_mismatches = []
    rows = df_grouped.collect()
    for r in rows:
        ranks = r["_ranks"]
        m = r["_m"]
        w, observed_label = _compute_kendall_w(ranks, m, thresholds, min_raters)
        group_vals = tuple(r[g] for g in group_by)

        expected_label = expected_label_static or expected_labels_map.get(group_vals)
        if expected_label and observed_label != expected_label:
            label_mismatches.append(f"group={group_vals}: observed={observed_label}, expected={expected_label}")

        results.append((group_vals, w))

    if label_mismatches:
        total_groups = len(rows)
        return False, results, column, _format_label_mismatch_error(label_mismatches, total_groups)

    return True, results, column, "metric=kendall_w"


def _kendall_w_ungrouped(
    df_items: DataFrame,
    column: str,
    thresholds: dict | None,
    expected_label_static: str | None,
    expected_labels_map: dict[tuple, str],
    min_raters: int = MIN_RATERS,
) -> tuple[bool, float | None, str, str]:
    """Calculate Kendall W without grouping and with label validation."""
    rows = df_items.collect()
    ranks = [r["_mean_rank"] for r in rows]
    m = rows[0]["_appearances"] if rows else 0
    w, observed_label = _compute_kendall_w(ranks, m, thresholds, min_raters)

    expected_label = expected_label_static or expected_labels_map.get(())
    if expected_label and observed_label != expected_label:
        return (
            False,
            w,
            column,
            f"kendall_w_label_mismatch: observed={observed_label}, expected={expected_label}",
        )

    return True, w, column, f"metric=kendall_w(consensus={observed_label})"


def _prepare_borda_dataframes(
    df: DataFrame,
    column: str,
    ranks_field: str,
    rank_position_field: str,
    rank_count_field: str,
    item_id_field: str,
    group_by: Sequence[str],
) -> tuple[DataFrame, DataFrame]:
    """Prepare DataFrames for borda count calculation.

    item_id_field supports fallback syntax with "|":
        - "item_code" - use item_code field
        - "item_code|item_label" - use item_code, fallback to item_label if null
    """
    schema_field = next((fld for fld in df.schema.fields if fld.name == column), None)
    if schema_field is None:
        raise ValueError(f"column_not_found={column}")

    dt = schema_field.dataType
    parsed_col = f.col(column) if isinstance(dt, ArrayType) else f.from_json(f.col(column), "array<map<string,string>>")

    df_items = df.withColumn("_item", f.explode(parsed_col))

    # Support fallback syntax: "item_code|item_label"
    if "|" in item_id_field:
        fallback_fields = [fld.strip() for fld in item_id_field.split("|")]
        id_col = df_items["_item"].getItem(fallback_fields[0])
        for fallback in fallback_fields[1:]:
            id_col = f.coalesce(id_col, df_items["_item"].getItem(fallback))
        df_items = df_items.withColumn("_item_id", id_col.cast("string"))
    else:
        df_items = df_items.withColumn("_item_id", df_items["_item"].getItem(item_id_field).cast("string"))

    ranks_col = df_items["_item"].getItem(ranks_field)
    ranks_parsed = f.from_json(ranks_col.cast("string"), "array<map<string,string>>")
    df_items = df_items.withColumn("_ranks", ranks_parsed)
    df_items = df_items.filter(f.col("_ranks").isNotNull() & (f.size(f.col("_ranks")) > 0))

    df_ranks = df_items.withColumn("_rank_elem", f.explode(f.col("_ranks")))
    df_ranks = df_ranks.withColumn(
        "_rank_position", df_ranks["_rank_elem"].getItem(rank_position_field).cast(DoubleType())
    )
    df_ranks = df_ranks.withColumn("_rank_count", df_ranks["_rank_elem"].getItem(rank_count_field).cast(DoubleType()))

    select_cols = [*list(group_by), "_item_id", "_rank_position", "_rank_count"]
    df_ranks = df_ranks.select(*select_cols).filter(
        f.col("_rank_position").isNotNull() & f.col("_rank_count").isNotNull()
    )

    return df_items, df_ranks


def _calculate_borda_grouped(
    df_items: DataFrame, df_ranks: DataFrame, group_by: Sequence[str], fields: Sequence[str] | None = None
) -> list[tuple]:
    """Calculate borda count with grouping."""
    df_n = df_items.groupBy(*group_by).agg(f.countDistinct("_item_id").alias("_n"))
    df_ranks_with_n = df_ranks.join(df_n, on=list(group_by), how="left")
    df_ranks_with_n = df_ranks_with_n.withColumn(
        "_borda_contrib", f.col("_rank_count") * (f.col("_n") - f.col("_rank_position") + 1)
    )

    df_borda = df_ranks_with_n.groupBy(*[*list(group_by), "_item_id"]).agg(
        f.sum("_borda_contrib").alias("_borda_points")
    )
    df_total = df_borda.groupBy(*group_by).agg(f.sum("_borda_points").alias("_total_points"))
    df_borda = df_borda.join(df_total, on=list(group_by), how="left")
    df_borda = df_borda.withColumn("_borda_norm_pct", (f.col("_borda_points") / f.col("_total_points")) * 100)

    window_spec = Window.partitionBy(*group_by).orderBy(f.col("_borda_points").desc(), f.col("_item_id"))
    # Use rank() so items with equal Borda points receive the same rank
    df_borda = df_borda.withColumn("_borda_rank", f.rank().over(window_spec))

    # Default fields if not specified
    all_fields = ["item_id", "borda_points", "borda_norm_pct", "borda_rank"]
    output_fields = fields if fields else all_fields

    results = []
    for r in df_borda.collect():
        # Include item_id in the group key for consistency with array_field_value
        group_vals = tuple(str(r[g]) for g in [*group_by, "_item_id"])
        value = {}
        if "item_id" in output_fields:
            value["item_id"] = r["_item_id"]
        if "borda_points" in output_fields:
            value["borda_points"] = r["_borda_points"]
        if "borda_norm_pct" in output_fields:
            value["borda_norm_pct"] = r["_borda_norm_pct"]
        if "borda_rank" in output_fields:
            value["borda_rank"] = r["_borda_rank"]
        results.append((group_vals, value))
    return results


def _calculate_borda_ungrouped(
    df_items: DataFrame, df_ranks: DataFrame, fields: Sequence[str] | None = None
) -> list[tuple]:
    """Calculate borda count without grouping."""
    n = df_items.select(f.countDistinct("_item_id").alias("n")).collect()[0]["n"]
    df_ranks = df_ranks.withColumn("_borda_contrib", f.col("_rank_count") * (f.lit(n) - f.col("_rank_position") + 1))

    df_borda = df_ranks.groupBy("_item_id").agg(f.sum("_borda_contrib").alias("_borda_points"))
    total_points = df_borda.select(f.sum("_borda_points").alias("total")).collect()[0]["total"]
    df_borda = df_borda.withColumn("_borda_norm_pct", (f.col("_borda_points") / f.lit(total_points)) * 100)

    window_spec = Window.orderBy(f.col("_borda_points").desc(), f.col("_item_id"))
    # Use rank() so items with equal Borda points receive the same rank
    df_borda = df_borda.withColumn("_borda_rank", f.rank().over(window_spec))

    # Default fields if not specified
    all_fields = ["item_id", "borda_points", "borda_norm_pct", "borda_rank"]
    output_fields = fields if fields else all_fields

    results = []
    for r in df_borda.collect():
        value = {}
        if "item_id" in output_fields:
            value["item_id"] = r["_item_id"]
        if "borda_points" in output_fields:
            value["borda_points"] = r["_borda_points"]
        if "borda_norm_pct" in output_fields:
            value["borda_norm_pct"] = r["_borda_norm_pct"]
        if "borda_rank" in output_fields:
            value["borda_rank"] = r["_borda_rank"]
        results.append(((r["_item_id"],), value))
    return results


def _borda_count(_: SparkSession, df: DataFrame, cfg: dict) -> tuple[bool, dict | list, str, str]:
    """
    Calculate Borda Count scores from ranking data.

    Borda Count formula for each item:
        borda_points = Σ(rank_count * (N - rank_position + 1))
    Where:
        N = total number of items
        rank_position = position in ranking (1 = first place)
        rank_count = how many times the item received that position

    Also calculates:
        - borda_norm_pct: (borda_points / total_points) * 100
        - borda_rank: ranking based on borda_points (1 = highest)

    Optional fields parameter to select which fields to include in output:
        - fields: list of field names to include (default: all fields)
        - Available fields: item_id, borda_points, borda_norm_pct, borda_rank
    """
    column = cfg["column"]
    ranks_field = cfg.get("ranks_field", "ranks")
    rank_position_field = cfg.get("rank_position_field", "rank_position")
    rank_count_field = cfg.get("rank_count_field", "rank_count")
    item_id_field = cfg.get("item_id_field", "item_code")
    group_by: Sequence[str] = cfg.get("group_by", [])
    fields: Sequence[str] | None = cfg.get("fields")
    if isinstance(group_by, str):
        group_by = [group_by]
    if isinstance(fields, str):
        fields = [fields]

    try:
        df_items, df_ranks = _prepare_borda_dataframes(
            df, column, ranks_field, rank_position_field, rank_count_field, item_id_field, group_by
        )

        if group_by:
            results = _calculate_borda_grouped(df_items, df_ranks, group_by, fields)
        else:
            results = _calculate_borda_ungrouped(df_items, df_ranks, fields)

        return True, results, column, "metric=borda_count"

    except Exception as e:
        return False, None, column, f"borda_count_error={e!s}"


def _compute_max_stat(df_parsed: DataFrame, effective_group_by: Sequence[str]) -> dict | list:
    """Compute maximum value statistic."""
    if effective_group_by:
        df_grouped = df_parsed.groupBy(*effective_group_by).agg(f.max("_value").alias("max_value"))
        return [(tuple(r[g] for g in effective_group_by), {"max": r["max_value"]}) for r in df_grouped.collect()]
    max_value = df_parsed.select(f.max("_value").alias("max_value")).collect()[0]["max_value"]
    return {"max": max_value}


def _compute_min_stat(df_parsed: DataFrame, effective_group_by: Sequence[str]) -> dict | list:
    """Compute minimum value statistic."""
    if effective_group_by:
        df_grouped = df_parsed.groupBy(*effective_group_by).agg(f.min("_value").alias("min_value"))
        return [(tuple(r[g] for g in effective_group_by), {"min": r["min_value"]}) for r in df_grouped.collect()]
    min_value = df_parsed.select(f.min("_value").alias("min_value")).collect()[0]["min_value"]
    return {"min": min_value}


def _compute_median_stat(
    df_parsed: DataFrame, effective_group_by: Sequence[str], weight_field: str | None, median_method: str
) -> dict | list:
    """Compute median value statistic."""
    if weight_field:
        df_parsed = df_parsed.withColumn(
            "_replicate",
            f.expr("array_repeat(_value, cast(_weight as int))"),
        ).withColumn(
            "_value_weighted",
            f.explode("_replicate"),
        )
        value_col = "_value_weighted"
    else:
        value_col = "_value"

    if effective_group_by:
        df_grouped = df_parsed.groupBy(*effective_group_by).agg(f.collect_list(value_col).alias("_vals"))
        return [
            (tuple(r[g] for g in effective_group_by), {"median": _median(sorted(r["_vals"]), median_method)})
            for r in df_grouped.collect()
        ]
    arr = sorted(r[value_col] for r in df_parsed.select(value_col).collect())
    return {"median": _median(arr, median_method)}


def _array_stats(_: SparkSession, df: DataFrame, cfg: dict) -> tuple[bool, dict | list, str, str]:
    column = cfg["column"]
    field = cfg["field"]
    stat = cfg.get("stat")
    median_method = cfg.get("median_method", "discrete")
    weight_field = cfg.get("weight")
    path = cfg.get("path")  # Optional path to nested array
    item_group_by = cfg.get("item_group_by")  # Optional field to group by within array
    group_by: Sequence[str] = cfg.get("group_by", [])
    if isinstance(group_by, str):
        group_by = [group_by]

    # If item_group_by is specified, add it to the effective group_by
    effective_group_by = list(group_by)
    if item_group_by:
        effective_group_by.append("_item_group")

    try:
        df_parsed = _parse_array_column(df, column, field, weight_field, path, item_group_by)

        if stat == "max":
            values = _compute_max_stat(df_parsed, effective_group_by)
        elif stat == "min":
            values = _compute_min_stat(df_parsed, effective_group_by)
        elif stat == "median":
            values = _compute_median_stat(df_parsed, effective_group_by, weight_field, median_method)
        else:
            values = _weighted_mean_std_ci_grouped(df_parsed, effective_group_by)
            if stat:
                values = [(g, {stat: m.get(stat)}) for g, m in values]

        return True, values, column, f"metric=array_stats({field})"

    except Exception as e:
        return False, None, column, f"array_stats_error={e!s}"


OBSERVED_METRICS: dict[str, MetricFn] = {
    "count": _count,
    "aggregate": _aggregate,
    "sql": _sql,
    "null_ratio": _ratio,
    "completeness_ratio": _ratio,
    "distinct_ratio": _ratio,
    "completion_ratio": _ratio,
    "size_array": _size_array,
    "sum_array_field": _sum_array_field,
    "array_field_values": _array_field_values,
    "array_sorted_field_values": _array_sorted_field_values,
    "column_value": _column_value,
    "array_stats": _array_stats,
    "kendall_w": _kendall_w,
    "borda_count": _borda_count,
}


def resolve_observed(spark: SparkSession, df: DataFrame, cfg: dict):
    metric_type = cfg.get("type")
    fn = OBSERVED_METRICS.get(metric_type)
    if not fn:
        return False, None, None, f"unsupported_metric_type={metric_type}"

    try:
        return fn(spark, df, cfg)
    except Exception as e:
        return False, None, None, f"metric_resolution_error={e!s}"
