# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import re
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from neco_dq.test_suites.profiling.constants import HIGH_DISTINCT_RATIO_THRESHOLD


class ProfilingChecksMixin:
    """Mixin providing checks-mode methods for the profiling test suite."""

    def _check_empty_table(self, result: dict, total_rows: int):
        if not self._enabled("empty_table"):
            return

        self._register(
            result,
            "empty_table",
            total_rows > 0,
            self._severity("empty_table"),
            f"total_rows={total_rows}",
        )

    def _check_table_freshness(self, result: dict, fq_table: str):
        if not self._enabled("table_freshness"):
            return

        try:
            meta = self.spark.sql(f"DESCRIBE DETAIL {fq_table}").collect()[0]

            last_modified = getattr(meta, "lastModified", None)
            if last_modified is None:
                raise ValueError("lastModified not available")

            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=UTC)

            days_stale = (datetime.now(UTC) - last_modified).total_seconds() / 86400
            tf_cfg = self.checks_cfg.get("table_freshness", {})
            max_days = float(tf_cfg.get("max_staleness_days", tf_cfg.get("max_days_stale", 2)))

            passed = days_stale <= max_days
            details = f"days_stale={round(days_stale, 2)} threshold={max_days}"

        except Exception as exc:
            passed = False
            details = f"DESCRIBE DETAIL failed: {exc!s}"

        self._register(
            result,
            "table_freshness",
            passed,
            self._severity("table_freshness"),
            details,
        )

    def _detect_sensitive(self, value: Any) -> str | None:
        if value is None:
            return None

        s = str(value)
        if re.fullmatch(r"\d{12}", s):
            return "PII_MyNumber"
        if re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", s):
            return "PII_Email"
        if re.fullmatch(r"(\+81|0)\d{9,10}", s):
            return "PII_Phone"
        if re.fullmatch(r"\d{16}", s):
            return "Financial_Card"
        return None

    def _profile_column(
        self,
        df: DataFrame,
        result: dict,
        col: str,
        total_rows: int,
        precomputed_stats: dict[str, Any] | None = None,
    ):
        """
        Profile a single column using pre-computed stats when available.

        Args:
            df: DataFrame to analyze
            result: result dict to append checks to
            col: column name
            total_rows: total row count
            precomputed_stats: optional pre-computed stats for this column
        """
        # Use pre-computed stats if available, otherwise compute
        if precomputed_stats:
            null_count = precomputed_stats.get("null_count", 0)
            null_pct = precomputed_stats.get("null_pct", 0.0)
            distinct_count = precomputed_stats.get("distinct_count", 0)
        else:
            null_count = df.filter(f.col(col).isNull()).count()
            distinct_count = df.select(col).distinct().count()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0

        self._check_high_null_ratio(result, col, null_pct)
        self._check_single_value(result, col, distinct_count, total_rows)
        self._check_low_cardinality(result, col, distinct_count, total_rows)
        self._check_high_distinct_ratio(result, col, distinct_count, null_count, total_rows)
        self._check_sensitive_data(result, df, col)
        self._check_column_name_smell(result, col)

    def _check_high_null_ratio(self, result, col, null_pct):
        if not self._enabled("high_null_ratio"):
            return

        threshold = self.checks_cfg.get("high_null_ratio", {}).get("threshold_pct", 50)
        self._register(
            result,
            "high_null_ratio",
            null_pct <= threshold,
            self._severity("high_null_ratio"),
            f"null_pct={round(null_pct, 2)} threshold={threshold}",
            col,
        )

    def _check_single_value(self, result, col, distinct_count, total_rows):
        if not self._enabled("single_value"):
            return

        passed = not (total_rows > 0 and distinct_count == 1)
        self._register(
            result,
            "single_value",
            passed,
            self._severity("single_value"),
            f"distinct_count={distinct_count}",
            col,
        )

    def _check_low_cardinality(self, result, col, distinct_count, total_rows):
        if not self._enabled("low_cardinality_suspect"):
            return

        max_distinct = self.checks_cfg.get("low_cardinality_suspect", {}).get("max_distinct", 5)

        passed = not (distinct_count <= max_distinct and total_rows > max_distinct * 10)

        self._register(
            result,
            "low_cardinality_suspect",
            passed,
            self._severity("low_cardinality_suspect"),
            f"distinct_count={distinct_count}",
            col,
        )

    def _check_high_distinct_ratio(
        self,
        result,
        col,
        distinct_count,
        null_count,
        total_rows,
    ):
        if not self._enabled("high_distinct_ratio") or total_rows == 0:
            return

        ratio = distinct_count / total_rows
        passed = not (ratio >= HIGH_DISTINCT_RATIO_THRESHOLD and null_count == 0)

        self._register(
            result,
            "high_distinct_ratio",
            passed,
            self._severity("high_distinct_ratio"),
            f"distinct_ratio={round(ratio, 3)}",
            col,
        )

    def _check_sensitive_data(self, result, df, col):
        if not self._enabled("sensitive_data"):
            return

        try:
            sample_rows = df.select(col).dropna().limit(100).collect()
            sensitive = next(
                (self._detect_sensitive(r[col]) for r in sample_rows if self._detect_sensitive(r[col])),
                None,
            )
        except Exception:
            sensitive = None

        self._register(
            result,
            "sensitive_data",
            sensitive is None,
            self._severity("sensitive_data"),
            sensitive or "no_sensitive_data",
            col,
        )

    def _check_column_name_smell(self, result, col):
        if not self._enabled("column_name_smell"):
            return

        bad = bool(re.search(r"(col\d+|value|field|tmp|test)", col.lower()))
        self._register(
            result,
            "column_name_smell",
            not bad,
            self._severity("column_name_smell"),
            f"column_name={col}",
            col,
        )

    def validate_table(self, table_cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Perform profiling checks on a table.

        Uses batch statistics computation for efficiency.
        """
        catalog = table_cfg["catalog"]
        schema = table_cfg["schema"]
        table = table_cfg["table"]
        fq_table = f"{catalog}.{schema}.{table}"

        result: dict[str, Any] = {
            "test_suite": self._suite_name,
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "run_timestamp": datetime.now(UTC),
            "checks": [],
        }

        try:
            df = self._load_table(catalog, schema, table)
        except Exception as exc:
            self._register(
                result,
                "table_read",
                False,
                "critical",
                str(exc),
            )
            return result

        total_rows = df.count()

        # Table-level checks
        self._check_empty_table(result, total_rows)
        self._check_table_freshness(result, fq_table)

        # Pre-compute common stats for all columns in batch (single scan)
        columns = df.columns
        common_stats = self._compute_common_stats(df, columns, total_rows)

        # Column-level profiling with pre-computed stats
        for col in columns:
            self._profile_column(df, result, col, total_rows, precomputed_stats=common_stats.get(col))

        return result
