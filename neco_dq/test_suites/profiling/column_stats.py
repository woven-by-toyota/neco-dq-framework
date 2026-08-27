# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from neco_dq.test_suites.profiling.constants import (
    BOOLEAN_TYPES,
    DATE_TYPES,
    NUMERIC_TYPES,
    STRING_TYPES,
    _P25_IDX,
    _P50_IDX,
    _P75_IDX,
    _P95_IDX,
)


class ColumnStatsMixin:
    """Column classification, sampling, statistics, and schema-info helpers."""

    def _classify_columns(self, df: DataFrame) -> dict[str, list[str]]:
        """
        Classify DataFrame columns by data type.

        Returns:
            dict with keys: numeric, string, date, boolean, other
            Each value is a list of column names.
        """
        classified: dict[str, list[str]] = {
            "numeric": [],
            "string": [],
            "date": [],
            "boolean": [],
            "other": [],
        }

        for field in df.schema.fields:
            if isinstance(field.dataType, NUMERIC_TYPES):
                classified["numeric"].append(field.name)
            elif isinstance(field.dataType, STRING_TYPES):
                classified["string"].append(field.name)
            elif isinstance(field.dataType, DATE_TYPES):
                classified["date"].append(field.name)
            elif isinstance(field.dataType, BOOLEAN_TYPES):
                classified["boolean"].append(field.name)
            else:
                classified["other"].append(field.name)

        return classified

    def _get_sample(self, df: DataFrame, row_count: int) -> DataFrame:
        """
        Apply sampling to DataFrame if configured or if table exceeds auto-sample threshold.

        Config options:
            sampling:
                enabled: false
                fraction: 0.1
                max_rows: 1000000
                seed: 42
            auto_sample_threshold: 10000000  # auto-sample if > 10M rows

        Returns:
            Sampled DataFrame or original if sampling not applicable.
        """
        sampling_cfg = self._suite_cfg.get("sampling", {})
        auto_threshold = self._suite_cfg.get("auto_sample_threshold")

        # Check if sampling is explicitly enabled or auto-triggered
        sampling_enabled = sampling_cfg.get("enabled", False)
        auto_triggered = auto_threshold is not None and row_count > auto_threshold

        if not sampling_enabled and not auto_triggered:
            return df

        fraction = sampling_cfg.get("fraction", 0.1)
        max_rows = sampling_cfg.get("max_rows")
        seed = sampling_cfg.get("seed", 42)

        # Calculate effective fraction based on max_rows if specified
        if max_rows and row_count > 0:
            max_fraction = max_rows / row_count
            fraction = min(fraction, max_fraction)

        return df.sample(withReplacement=False, fraction=fraction, seed=seed)

    def _compute_common_stats(self, df: DataFrame, columns: list[str], row_count: int) -> dict[str, dict[str, Any]]:
        """
        Compute common statistics for all columns in a single pass.

        Statistics computed:
            - null_count: count of null values
            - null_pct: percentage of null values
            - distinct_count: approximate distinct count
            - distinct_ratio: distinct_count / row_count

        Args:
            df: DataFrame to analyze
            columns: list of column names
            row_count: total row count for percentage calculations

        Returns:
            dict mapping column name to stats dict
        """
        if not columns or row_count == 0:
            return {col: self._empty_common_stats() for col in columns}

        # Build aggregation expressions for all columns at once
        agg_exprs = []
        for col in columns:
            agg_exprs.extend(
                [
                    f.sum(f.when(f.col(col).isNull(), 1).otherwise(0)).alias(f"{col}__null_count"),
                    f.approx_count_distinct(col).alias(f"{col}__distinct_count"),
                ]
            )

        # Execute single aggregation
        result = df.agg(*agg_exprs).collect()[0]

        # Build stats dict for each column
        stats: dict[str, dict[str, Any]] = {}
        for col in columns:
            null_count = result[f"{col}__null_count"] or 0
            distinct_count = result[f"{col}__distinct_count"] or 0
            null_pct = (null_count / row_count * 100) if row_count > 0 else 0.0
            distinct_ratio = (distinct_count / row_count) if row_count > 0 else 0.0

            stats[col] = {
                "null_count": null_count,
                "null_pct": round(null_pct, 2),
                "distinct_count": distinct_count,
                "distinct_ratio": round(distinct_ratio, 4),
            }

        return stats

    def _empty_common_stats(self) -> dict[str, Any]:
        """Return empty common stats dict for edge cases."""
        return {
            "null_count": 0,
            "null_pct": 0.0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }

    def _compute_numeric_stats(self, df: DataFrame, columns: list[str]) -> dict[str, dict[str, Any]]:
        """
        Compute numeric statistics for numeric columns.

        Statistics: min, max, mean, stddev, p25, p50, p75, p95

        Returns:
            dict mapping column name to numeric stats dict
        """
        if not columns:
            return {}

        agg_exprs = []
        for col in columns:
            agg_exprs.extend(
                [
                    f.min(col).alias(f"{col}__min"),
                    f.max(col).alias(f"{col}__max"),
                    f.mean(col).alias(f"{col}__mean"),
                    f.stddev(col).alias(f"{col}__stddev"),
                    f.percentile_approx(col, [0.25, 0.5, 0.75, 0.95]).alias(f"{col}__percentiles"),
                ]
            )

        result = df.agg(*agg_exprs).collect()[0]

        stats: dict[str, dict[str, Any]] = {}
        for col in columns:
            percentiles = result[f"{col}__percentiles"] or [None, None, None, None]
            stats[col] = {
                "min": result[f"{col}__min"],
                "max": result[f"{col}__max"],
                "mean": round(result[f"{col}__mean"], 4) if result[f"{col}__mean"] is not None else None,
                "stddev": round(result[f"{col}__stddev"], 4) if result[f"{col}__stddev"] is not None else None,
                "p25": percentiles[_P25_IDX] if len(percentiles) > _P25_IDX else None,
                "p50": percentiles[_P50_IDX] if len(percentiles) > _P50_IDX else None,
                "p75": percentiles[_P75_IDX] if len(percentiles) > _P75_IDX else None,
                "p95": percentiles[_P95_IDX] if len(percentiles) > _P95_IDX else None,
            }

        return stats

    def _compute_string_stats(self, df: DataFrame, columns: list[str]) -> dict[str, dict[str, Any]]:
        """
        Compute string statistics for string columns.

        Statistics: min_length, max_length, avg_length, top_values (top 10), is_json

        Returns:
            dict mapping column name to string stats dict
        """
        if not columns:
            return {}

        # Length statistics (single aggregation)
        agg_exprs = []
        for col in columns:
            agg_exprs.extend(
                [
                    f.min(f.length(col)).alias(f"{col}__min_length"),
                    f.max(f.length(col)).alias(f"{col}__max_length"),
                    f.avg(f.length(col)).alias(f"{col}__avg_length"),
                ]
            )

        length_result = df.agg(*agg_exprs).collect()[0]

        stats: dict[str, dict[str, Any]] = {}
        for col in columns:
            stats[col] = {
                "min_length": length_result[f"{col}__min_length"],
                "max_length": length_result[f"{col}__max_length"],
                "avg_length": (
                    round(length_result[f"{col}__avg_length"], 2)
                    if length_result[f"{col}__avg_length"] is not None
                    else None
                ),
                "top_values": [],
                "is_json": False,
                "json_is_array": False,
            }

        # Top values and JSON detection (separate queries per column)
        for col in columns:
            try:
                top_df = df.filter(f.col(col).isNotNull()).groupBy(col).count().orderBy(f.desc("count")).limit(10)
                top_rows = top_df.collect()
                stats[col]["top_values"] = [{"value": str(row[col]), "count": row["count"]} for row in top_rows]

                # JSON detection: sample values and check if they look like JSON
                json_info = self._detect_json_column(df, col)
                stats[col]["is_json"] = json_info["is_json"]
                stats[col]["json_is_array"] = json_info["is_array"]
                if json_info["sample_fields"]:
                    stats[col]["json_sample_fields"] = json_info["sample_fields"]
                if json_info.get("field_types"):
                    stats[col]["field_types"] = json_info["field_types"]

                # Date-format detection: dates stored as strings (not JSON)
                if not json_info["is_json"]:
                    date_format = self._detect_date_format(df, col)
                    if date_format:
                        stats[col]["date_format"] = date_format
            except Exception:
                stats[col]["top_values"] = []

        return stats

    def _compute_date_stats(self, df: DataFrame, columns: list[str]) -> dict[str, dict[str, Any]]:
        """
        Compute date statistics for date/timestamp columns.

        Statistics: min_date, max_date, date_range_days

        Returns:
            dict mapping column name to date stats dict
        """
        if not columns:
            return {}

        agg_exprs = []
        for col in columns:
            agg_exprs.extend(
                [
                    f.min(col).alias(f"{col}__min_date"),
                    f.max(col).alias(f"{col}__max_date"),
                ]
            )

        result = df.agg(*agg_exprs).collect()[0]

        stats: dict[str, dict[str, Any]] = {}
        for col in columns:
            min_date = result[f"{col}__min_date"]
            max_date = result[f"{col}__max_date"]

            # Calculate date range in days
            date_range_days = None
            if min_date and max_date:
                try:
                    delta = max_date - min_date
                    date_range_days = delta.days if hasattr(delta, "days") else int(delta.total_seconds() / 86400)
                except (TypeError, ValueError, AttributeError):
                    # Incompatible date/timestamp arithmetic (e.g. mixed Spark Row types); keep None
                    date_range_days = None

            stats[col] = {
                "min_date": str(min_date) if min_date else None,
                "max_date": str(max_date) if max_date else None,
                "date_range_days": date_range_days,
            }

        return stats

    def _compute_column_stats(self, df: DataFrame, row_count: int) -> dict[str, dict[str, Any]]:
        """
        Entry point: compute all statistics for all columns.

        Combines common stats with type-specific stats.

        Returns:
            dict mapping column name to complete stats dict including:
                - type: column type category
                - common stats (null_count, null_pct, distinct_count, distinct_ratio)
                - type-specific stats (numeric/string/date)
        """
        classified = self._classify_columns(df)
        all_columns = df.columns

        # Compute common stats for all columns
        common_stats = self._compute_common_stats(df, all_columns, row_count)

        # Compute type-specific stats
        numeric_stats = self._compute_numeric_stats(df, classified["numeric"])
        string_stats = self._compute_string_stats(df, classified["string"])
        date_stats = self._compute_date_stats(df, classified["date"])

        # Merge all stats
        result: dict[str, dict[str, Any]] = {}
        for col in all_columns:
            col_stats = {"type": self._get_column_type_category(col, classified)}
            col_stats.update(common_stats.get(col, self._empty_common_stats()))

            # Add type-specific stats
            if col in numeric_stats:
                col_stats.update(numeric_stats[col])
            elif col in string_stats:
                col_stats.update(string_stats[col])
            elif col in date_stats:
                col_stats.update(date_stats[col])

            result[col] = col_stats

        return result

    def _get_column_type_category(self, column: str, classified: dict[str, list[str]]) -> str:
        """Get the type category for a column."""
        for category, columns in classified.items():
            if column in columns:
                return category
        return "other"

    def _get_partitions(self, fq_table: str) -> list[str]:
        """
        Get partition columns for a table using SHOW PARTITIONS.

        Returns:
            List of partition column names, empty list if not partitioned.
        """
        try:
            partitions_df = self.spark.sql(f"SHOW PARTITIONS {fq_table}")
            if partitions_df.columns:
                # Partition column names are in the schema
                # Format: "col1=val1/col2=val2", so we parse the first row
                first_row = partitions_df.first()
                if first_row:
                    partition_str = first_row[0]
                    # Parse "col1=val1/col2=val2" to get column names
                    parts = partition_str.split("/")
                    return [p.split("=")[0] for p in parts if "=" in p]
            return []
        except Exception:
            # Table is not partitioned or SHOW PARTITIONS not supported
            return []

    def _get_delta_properties(self, fq_table: str) -> dict[str, Any] | None:
        """
        Get Delta table properties using DESCRIBE DETAIL.

        Returns:
            dict with: size_bytes, num_files, last_modified, format
            None if DESCRIBE DETAIL fails (non-Delta table).
        """
        try:
            detail_df = self.spark.sql(f"DESCRIBE DETAIL {fq_table}")
            row = detail_df.collect()[0]

            # Extract relevant properties
            properties: dict[str, Any] = {}

            if hasattr(row, "sizeInBytes"):
                properties["size_bytes"] = row.sizeInBytes
            if hasattr(row, "numFiles"):
                properties["num_files"] = row.numFiles
            if hasattr(row, "lastModified"):
                properties["last_modified"] = str(row.lastModified) if row.lastModified else None
            if hasattr(row, "format"):
                properties["format"] = row.format
            if hasattr(row, "partitionColumns"):
                properties["partition_columns"] = list(row.partitionColumns) if row.partitionColumns else []

            return properties if properties else None
        except Exception:
            return None

    def _extract_schema_info(self, df: DataFrame, fq_table: str) -> dict[str, Any]:
        """
        Extract comprehensive schema information for a table.

        Returns:
            dict with:
                columns: [{name, type, nullable}, ...]
                partitions: [col1, col2, ...]
                delta_properties: {size_bytes, num_files, last_modified, ...}
        """
        # Extract column info from DataFrame schema
        columns_info = [
            {
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable,
            }
            for field in df.schema.fields
        ]

        # Get partitions
        partitions = self._get_partitions(fq_table)

        # Get Delta properties (if applicable)
        delta_properties = self._get_delta_properties(fq_table)

        return {
            "columns": columns_info,
            "partitions": partitions,
            "delta_properties": delta_properties,
        }
