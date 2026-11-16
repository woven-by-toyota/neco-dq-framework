# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime
from functools import reduce
from operator import and_
from typing import Any

from pyspark.sql import Column, SparkSession, Window
from pyspark.sql.functions import col, abs as sql_abs, sum as sql_sum, count as sql_count
from pyspark.sql import functions as f

from neco_dq.test_suites.base_test_suite import DQTestSuite
from neco_dq.test_suites.shared_checks import SharedChecksMixin
from neco_dq.core.io.result_writer import DQResultWriter

# Distribution bucket thresholds for duplicate counts
_DUP_BUCKET_SINGLE = 2
_DUP_BUCKET_LOW_MIN = 3
_DUP_BUCKET_LOW_MAX = 5


class SparkSilverDQTestSuite(DQTestSuite, SharedChecksMixin):
    """
    YAML-driven Silver Data Quality Test Suite
    - 1 row per check
    - column-aware
    """

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        self.spark = spark
        self.cfg = config
        self.writer = DQResultWriter(spark)

    # ------------------------------------------------------------------
    # Suite identity
    # ------------------------------------------------------------------

    @property
    def _suite_name(self) -> str:
        return "silver"

    @property
    def _suite_config_key(self) -> str:
        return "silver"

    def _check_idempotency(self, df, cfg: dict) -> tuple[bool, str, str]:
        """
        Verify no duplicate records exist based on key column(s).

        Config:
            key_columns: Column name or comma-separated list of columns to check for uniqueness (required)
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of duplicate keys (0-100)
                max_failure_count: Maximum allowed number of duplicate keys
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "key_columns")
        if not ok:
            return False, err, None

        key_cols = cfg["key_columns"]
        if isinstance(key_cols, str):
            key_cols = [k.strip() for k in key_cols.split(",")]

        total_count = df.count()
        dupes = df.groupBy(*key_cols).agg(sql_count("*").alias("_dup_count")).filter(col("_dup_count") > 1)
        failed_count = dupes.count()

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            # Compute distribution buckets
            bucket_df = (
                dupes.withColumn(
                    "_bucket",
                    f.when(col("_dup_count") == _DUP_BUCKET_SINGLE, "duplicates_2")
                    .when(
                        (col("_dup_count") >= _DUP_BUCKET_LOW_MIN) & (col("_dup_count") <= _DUP_BUCKET_LOW_MAX),
                        "duplicates_3_5",
                    )
                    .otherwise("duplicates_6_plus"),
                )
                .groupBy("_bucket")
                .count()
            )
            distribution = {row["_bucket"]: row["count"] for row in bucket_df.collect()}

            # Compute diagnostic stats
            stats_row = dupes.agg(
                f.max("_dup_count").alias("max_dups"),
                f.avg("_dup_count").alias("avg_dups"),
                f.sum("_dup_count").alias("total_dup_rows"),
            ).first()

            failure_stats = self._build_failure_stats(
                failure_type="duplicate_key",
                affected_columns=key_cols,
                failure_count=failed_count,
                total_checked=total_count,
                distribution=distribution,
                diagnostic={
                    "max_duplicates_per_key": int(stats_row["max_dups"]) if stats_row["max_dups"] else 0,
                    "avg_duplicates_per_key": round(float(stats_row["avg_dups"]), 2) if stats_row["avg_dups"] else 0,
                    "total_duplicate_rows": int(stats_row["total_dup_rows"]) if stats_row["total_dup_rows"] else 0,
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="duplicate_key",
                table_fq=self._current_table_fq,
                affected_columns=key_cols,
                cfg={"key_column": ", ".join(key_cols)},
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        if passed:
            details = self._details_pass_all(total_count, "keys", "unique")
        else:
            details = self._details_fail_found(failed_count, "duplicate keys", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, ",".join(key_cols)

    def _check_status_transition(self, df, cfg: dict) -> tuple[bool, str, str]:
        """
        Verify status transitions follow an allowed sequential flow.

        Config:
            key_columns: Column identifying the entity (required)
            status_column: Column containing the status value (required)
            timestamp_column: Column for ordering transitions chronologically (required)
            allowed_flow: List of statuses in valid progression order (e.g., ["pending", "active", "completed"])
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of invalid transitions (0-100)
                max_failure_count: Maximum allowed number of invalid transitions
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "key_columns", "status_column", "timestamp_column")
        if not ok:
            return False, err, None

        flow = cfg.get("allowed_flow", [])
        key_col = cfg["key_columns"]
        status_col = cfg["status_column"]
        ts_col = cfg["timestamp_column"]

        # Build status -> index mapping for allowed flow
        status_to_idx = {s: i for i, s in enumerate(flow)}

        # Window to get previous status per key, ordered by timestamp
        window_spec = Window.partitionBy(key_col).orderBy(ts_col)

        # Add index for each status (NULL if not in allowed_flow)
        df_with_idx = df.select(key_col, status_col, ts_col).withColumn(
            "_status_idx",
            f.when(col(status_col).isin(flow), f.lit(None)).otherwise(f.lit(-1)),
        )
        # Map status to its index in allowed_flow
        for status, idx in status_to_idx.items():
            df_with_idx = df_with_idx.withColumn(
                "_status_idx",
                f.when(col(status_col) == status, f.lit(idx)).otherwise(col("_status_idx")),
            )

        # Add previous status index
        df_with_prev = df_with_idx.withColumn("_prev_status_idx", f.lag("_status_idx").over(window_spec))

        # Identify violations:
        # 1. Unknown status: _status_idx == -1
        # 2. Backward transition: _status_idx < _prev_status_idx (both must be valid, i.e., >= 0)
        df_violations = (
            df_with_prev.withColumn("_is_unknown", f.when(col("_status_idx") == -1, f.lit(1)).otherwise(f.lit(0)))
            .withColumn(
                "_is_backward",
                f.when(
                    (col("_status_idx") >= 0)
                    & (col("_prev_status_idx") >= 0)
                    & (col("_status_idx") < col("_prev_status_idx")),
                    f.lit(1),
                ).otherwise(f.lit(0)),
            )
            .withColumn(
                # A transition only counts when BOTH current and previous status
                # are valid (>= 0). Transitions out of an unknown state are not
                # counted here, mirroring _is_backward and avoiding double-counting
                # unknown rows (they already contribute via unknown_count).
                "_is_transition",
                f.when((col("_status_idx") >= 0) & (col("_prev_status_idx") >= 0), f.lit(1)).otherwise(f.lit(0)),
            )
        )

        # Aggregate counts in Spark (no .collect())
        agg_result = df_violations.agg(
            f.sum("_is_unknown").alias("unknown_count"),
            f.sum("_is_backward").alias("backward_count"),
            f.sum("_is_transition").alias("transition_count"),
            f.count("*").alias("total_rows"),
        ).first()

        unknown_count = int(agg_result["unknown_count"] or 0)
        violations = int(agg_result["backward_count"] or 0)
        total_transitions = int(agg_result["transition_count"] or 0)
        total_rows = int(agg_result["total_rows"] or 0)

        # Get distinct unknown statuses (small set, safe to collect)
        unknown_statuses: set[str] = set()
        if unknown_count > 0:
            unknown_df = df_violations.filter(col("_is_unknown") == 1).select(status_col).distinct().limit(100)
            unknown_statuses = {row[0] for row in unknown_df.collect()}

        failed_count = violations + unknown_count
        total_count = total_transitions + unknown_count if (total_transitions + unknown_count) > 0 else total_rows

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="invalid_transition",
                affected_columns=[cfg["status_column"]],
                failure_count=failed_count,
                total_checked=total_count,
                diagnostic={
                    "backward_transitions": violations,
                    "unknown_status_occurrences": unknown_count,
                    "unknown_statuses": list(unknown_statuses) if unknown_statuses else [],
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="invalid_transition",
                table_fq=self._current_table_fq,
                affected_columns=[cfg["status_column"]],
                cfg={
                    "key_column": cfg["key_columns"],
                    "status_column": cfg["status_column"],
                    "timestamp_column": cfg["timestamp_column"],
                },
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = self._details_pass_all(total_count, "transitions", "valid")
        else:
            details = self._details_fail_found(failed_count, "invalid transitions", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, cfg["status_column"]

    def _check_referential_integrity(self, cfg: dict, table_ctx: dict) -> tuple[bool, str, str]:
        """
        Verify foreign key references exist in target table.

        Config:
            source_schema: Source table schema (default: current table's schema)
            source_table: Source table name (default: current table's table)
            source_catalog: Catalog for source/target tables (default: current table's catalog)
            source_column: Column in source table containing foreign key (required)
            target: Nested {catalog, schema, table, column} block for the target
                table (required). ``catalog`` defaults to the source catalog.
            min_valid_percent: Minimum percent (0-100) of valid references (default: 100)
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "source_column", "target")
        if not ok:
            return False, err, None

        target = cfg["target"] if isinstance(cfg.get("target"), dict) else {}
        tgt_missing = [k for k in ("schema", "table", "column") if not target.get(k)]
        if tgt_missing:
            return False, self._details_config_missing(*(f"target.{k}" for k in tgt_missing)), None
        target_column = target["column"]

        catalog = cfg.get("source_catalog", table_ctx["catalog"])
        src_schema = cfg.get("source_schema", table_ctx["schema"])
        src_table = cfg.get("source_table", table_ctx["table"])
        src_fq = f"{catalog}.{src_schema}.{src_table}"
        tgt_fq = f"{target.get('catalog', catalog)}.{target['schema']}.{target['table']}"
        src = self.spark.table(src_fq)
        tgt = self.spark.table(tgt_fq)

        total = src.count() or 1
        orphans = src.join(tgt, src[cfg["source_column"]] == tgt[target_column], "left_anti")
        invalid = orphans.count()

        ratio = 1 - invalid / total
        min_percent = cfg.get("min_valid_percent", 100.0)
        passed = ratio * 100 >= min_percent

        failure_stats = None
        investigation_query = None
        if not passed and invalid > 0:
            # Compute diagnostic stats
            distinct_orphan_fks = orphans.select(cfg["source_column"]).distinct().count()

            failure_stats = self._build_failure_stats(
                failure_type="orphan_reference",
                affected_columns=[cfg["source_column"]],
                failure_count=invalid,
                total_checked=total,
                extra={
                    "reference_info": {
                        "target_table": tgt_fq,
                        "target_column": target_column,
                    },
                },
                diagnostic={
                    "distinct_orphan_fks": distinct_orphan_fks,
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="orphan_reference",
                table_fq=src_fq,
                affected_columns=[cfg["source_column"]],
                cfg={
                    "fk_column": cfg["source_column"],
                    "pk_column": target_column,
                    "target_table": tgt_fq,
                },
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total,
            table_info=self._current_table_info,
            secondary_tables=[{"table": tgt_fq}],
        )

        if passed:
            details = self._details_pass_all(total, "references", "valid")
        else:
            details = self._details_fail_found(invalid, "orphan records", total)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, cfg["source_column"]

    def _check_cardinality(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify minimum cardinality (row count) per group.

        Config:
            group_by: Column or list of columns to group by (required)
            min_count: Minimum number of rows per group (default: 1)
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of violated groups (0-100)
                max_failure_count: Maximum allowed number of violated groups
        """
        start_time = datetime.now(UTC)
        gb = cfg.get("group_by")
        if not gb:
            return False, self._details_config_missing("group_by"), None

        gb_cols = [gb] if isinstance(gb, str) else gb
        min_count = cfg.get("min_count", 1)
        total_rows = df.count()

        grouped = df.groupBy(*gb_cols).count()

        # Single aggregation to avoid double shuffle
        agg_row = grouped.agg(
            f.sum(f.when(col("count") < min_count, f.lit(1)).otherwise(f.lit(0))).alias("failed_count"),
            f.count(f.lit(1)).alias("total_groups"),
            f.min("count").alias("min_count_observed"),
        ).collect()[0]
        failed_count = int(agg_row["failed_count"])
        total_groups = int(agg_row["total_groups"])

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_groups, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="low_cardinality",
                affected_columns=gb_cols,
                failure_count=failed_count,
                total_checked=total_groups,
                diagnostic={
                    "min_required": min_count,
                    "min_observed": int(agg_row["min_count_observed"]) if agg_row["min_count_observed"] else 0,
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="low_cardinality",
                table_fq=self._current_table_fq,
                affected_columns=gb_cols,
                cfg={"group_columns": ", ".join(gb_cols), "min_count": min_count},
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = self._details_pass_all(total_groups, "groups", "meet minimum")
        else:
            details = self._details_fail_found(failed_count, "groups below minimum", total_groups)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, ",".join(gb_cols) if isinstance(gb, list) else gb

    def _check_totals_consistency(self, df, cfg: dict, table_ctx: dict) -> tuple[bool, str, str | None]:
        """
        Verify header totals match sum of detail rows.

        Config:
            detail_schema: Schema of detail table (required)
            detail_table: Name of detail table (required)
            join_columns: Column to join header and detail tables (required)
            detail_amount_column: Column in detail table to sum (required)
            header_amount_column: Column in header table to compare against (required)
            tolerance_abs: Allowed numeric difference (default: 0.0)
            detail_catalog: Catalog for detail table (default: current table's catalog)
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(
            cfg, "detail_schema", "detail_table", "join_columns", "detail_amount_column", "header_amount_column"
        )
        if not ok:
            return False, err, None

        catalog = cfg.get("detail_catalog", table_ctx["catalog"])
        detail_fq = f"{catalog}.{cfg['detail_schema']}.{cfg['detail_table']}"
        detail = self.spark.table(detail_fq)
        sums = detail.groupBy(cfg["join_columns"]).agg(sql_sum(cfg["detail_amount_column"]).alias("detail_sum"))

        total_headers = df.count()
        joined = df.join(sums, cfg["join_columns"], "left")
        tolerance = cfg.get("tolerance_abs", 0.0)
        header_amt_col = cfg["header_amount_column"]

        # Mismatch conditions:
        # 1. Header has non-zero amount but no detail rows (detail_sum IS NULL)
        # 2. Difference exceeds tolerance (both values present)
        mismatches = joined.filter(
            # Headers with non-zero amount but no detail rows
            (col("detail_sum").isNull() & (sql_abs(col(header_amt_col)) > tolerance))
            # Or difference exceeds tolerance
            | (col("detail_sum").isNotNull() & (sql_abs(col("detail_sum") - col(header_amt_col)) > tolerance))
        )

        count = mismatches.count()
        passed = count == 0

        failure_stats = None
        investigation_query = None
        if not passed and count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="totals_mismatch",
                affected_columns=[cfg["header_amount_column"], cfg["detail_amount_column"]],
                failure_count=count,
                total_checked=total_headers,
                extra={
                    "detail_table": detail_fq,
                    "join_key": cfg["join_columns"],
                    "tolerance": cfg.get("tolerance_abs", 0.0),
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="totals_mismatch",
                table_fq=self._current_table_fq,
                affected_columns=[cfg["header_amount_column"]],
                cfg={
                    "join_key": cfg["join_columns"],
                    "header_amount": cfg["header_amount_column"],
                    "detail_amount": cfg["detail_amount_column"],
                    "detail_table": detail_fq,
                    "tolerance": cfg.get("tolerance_abs", 0.0),
                },
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_headers,
            table_info=self._current_table_info,
            secondary_tables=[{"table": detail_fq}],
        )

        if passed:
            details = self._details_pass_all(total_headers, "totals", "consistent")
        else:
            details = self._details_fail_found(count, "header/detail mismatches", total_headers)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, cfg["header_amount_column"]

    def _join_on(self, join_columns: str | list[str]) -> Column:
        """Equality join condition between aliases ``l`` and ``r`` on every join column."""
        conditions = [col(f"l.{c}") == col(f"r.{c}") for c in self._as_column_list(join_columns)]
        return reduce(and_, conditions)

    def _check_cross_table_equality(self, cfg: dict, table_ctx: dict) -> tuple[bool, str, str | None]:
        """
        Verify column values match between two tables after joining.

        Config:
            left_schema: Schema of left table (default: current table's schema)
            left_table: Name of left table (default: current table's table)
            right_schema: Schema of right table (required)
            right_table: Name of right table (required)
            join_columns: Column to join tables on (required)
            left_column: Column in left table to compare (required)
            right_column: Column in right table to compare (required)
            tolerance_abs: Allowed numeric difference (default: 0.0)
            catalog: Catalog for tables (default: current table's catalog)
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "right_schema", "right_table", "join_columns", "left_column", "right_column")
        if not ok:
            return False, err, None

        catalog = cfg.get("catalog", table_ctx["catalog"])
        left_schema = cfg.get("left_schema", table_ctx["schema"])
        left_table = cfg.get("left_table", table_ctx["table"])
        left_fq = f"{catalog}.{left_schema}.{left_table}"
        right_fq = f"{catalog}.{cfg['right_schema']}.{cfg['right_table']}"
        left = self.spark.table(left_fq)
        right = self.spark.table(right_fq)

        joined = left.alias("l").join(right.alias("r"), self._join_on(cfg["join_columns"]), "inner")
        total_joined = joined.count()

        mismatches = joined.filter(
            sql_abs(col(f"l.{cfg['left_column']}") - col(f"r.{cfg['right_column']}")) > cfg.get("tolerance_abs", 0.0)
        )

        count = mismatches.count()
        passed = count == 0

        failure_stats = None
        investigation_query = None
        if not passed and count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="cross_table_mismatch",
                affected_columns=[cfg["left_column"], cfg["right_column"]],
                failure_count=count,
                total_checked=total_joined,
                extra={
                    "left_table": left_fq,
                    "right_table": right_fq,
                    "join_key": cfg["join_columns"],
                    "tolerance": cfg.get("tolerance_abs", 0.0),
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="cross_table_mismatch",
                table_fq=left_fq,
                affected_columns=[cfg["left_column"]],
                cfg={
                    "join_key": cfg["join_columns"],
                    "left_column": cfg["left_column"],
                    "right_column": cfg["right_column"],
                    "left_table": left_fq,
                    "right_table": right_fq,
                    "tolerance": cfg.get("tolerance_abs", 0.0),
                },
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_joined,
            table_info=self._current_table_info,
            secondary_tables=[{"table": left_fq}, {"table": right_fq}],
        )

        if passed:
            details = self._details_pass_all(total_joined, "values", "match")
        else:
            details = self._details_fail_found(count, "mismatched rows", total_joined)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, cfg["left_column"]

    def _check_temporal_consistency(self, cfg: dict, table_ctx: dict) -> tuple[bool, str, str | None]:
        """
        Verify timestamp ordering between two tables (left timestamp must be <= right timestamp).

        Config:
            left_schema: Schema of left table (default: current table's schema)
            left_table: Name of left table (default: current table's table)
            right_schema: Schema of right table (required)
            right_table: Name of right table (required)
            join_columns: Column to join tables on (required)
            left_timestamp_column: Timestamp column in left table (required)
            right_timestamp_column: Timestamp column in right table (required)
            catalog: Catalog for tables (default: current table's catalog)
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(
            cfg,
            "right_schema",
            "right_table",
            "join_columns",
            "left_timestamp_column",
            "right_timestamp_column",
        )
        if not ok:
            return False, err, None

        catalog = cfg.get("catalog", table_ctx["catalog"])
        left_schema = cfg.get("left_schema", table_ctx["schema"])
        left_table = cfg.get("left_table", table_ctx["table"])
        left_fq = f"{catalog}.{left_schema}.{left_table}"
        right_fq = f"{catalog}.{cfg['right_schema']}.{cfg['right_table']}"
        left = self.spark.table(left_fq)
        right = self.spark.table(right_fq)

        joined = left.alias("l").join(right.alias("r"), self._join_on(cfg["join_columns"]), "inner")
        total_joined = joined.count()

        violations = joined.filter(col(f"l.{cfg['left_timestamp_column']}") > col(f"r.{cfg['right_timestamp_column']}"))
        count = violations.count()
        passed = count == 0

        failure_stats = None
        investigation_query = None
        if not passed and count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="temporal_violation",
                affected_columns=[cfg["left_timestamp_column"], cfg["right_timestamp_column"]],
                failure_count=count,
                total_checked=total_joined,
                extra={
                    "left_table": left_fq,
                    "right_table": right_fq,
                    "join_key": cfg["join_columns"],
                },
            )

            investigation_query = self._build_investigation_query(
                check_type="temporal_violation",
                table_fq=left_fq,
                affected_columns=[cfg["left_timestamp_column"]],
                cfg={
                    "join_key": cfg["join_columns"],
                    "left_timestamp": cfg["left_timestamp_column"],
                    "right_timestamp": cfg["right_timestamp_column"],
                    "left_table": left_fq,
                    "right_table": right_fq,
                },
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_joined,
            table_info=self._current_table_info,
            secondary_tables=[{"table": left_fq}, {"table": right_fq}],
        )

        if passed:
            details = self._details_pass_all(total_joined, "timestamps", "ordered")
        else:
            details = self._details_fail_found(count, "temporal violations", total_joined)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, cfg["left_timestamp_column"]

    def _check_row_temporal_consistency(self, df, cfg: dict) -> tuple[bool, str, str | None]:
        """
        Verify timestamp ordering within same row (left timestamp must be >= right timestamp).

        Config:
            left_timestamp_column: First timestamp column (required)
            right_timestamp_column: Second timestamp column (required)
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of violations (0-100)
                max_failure_count: Maximum allowed number of violations
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "left_timestamp_column", "right_timestamp_column")
        if not ok:
            return False, err, None

        total_count = df.count()
        violations = df.filter(col(cfg["left_timestamp_column"]) < col(cfg["right_timestamp_column"]))
        failed_count = violations.count()

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed and failed_count > 0:
            failure_stats = self._build_failure_stats(
                failure_type="row_temporal_violation",
                affected_columns=[cfg["left_timestamp_column"], cfg["right_timestamp_column"]],
                failure_count=failed_count,
                total_checked=total_count,
            )

            investigation_query = self._build_investigation_query(
                check_type="row_temporal_violation",
                table_fq=self._current_table_fq,
                affected_columns=[cfg["left_timestamp_column"], cfg["right_timestamp_column"]],
                cfg={
                    "left_timestamp": cfg["left_timestamp_column"],
                    "right_timestamp": cfg["right_timestamp_column"],
                },
                where_clause=self._current_where_clause,
            )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        if passed:
            details = self._details_pass_all(total_count, "row timestamps", "ordered")
        else:
            details = self._details_fail_found(failed_count, "timestamp inconsistencies", total_count)
        details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, cfg["left_timestamp_column"]

    def _check_flow_completeness(self, cfg: dict, table_ctx: dict) -> tuple[bool, str, str | None]:
        """
        Verify all source records have corresponding target records (flow completeness).

        Config:
            source_schema: Schema of source table (required)
            source_table: Name of source table (required)
            join_columns: Column to join source and target (required)
            source_catalog: Catalog of source table (default: current table's catalog)
            source_filter: SQL filter to apply to source (optional)
            target_filter: SQL filter to apply to target (optional)
            tolerance_percent: Allowed percentage of missing records (default: 0.0)
            lateness: Lateness configuration (optional):
                timestamp_column: Timestamp column for lateness calculation
                max_delay_minutes: Maximum allowed delay in minutes

        Note: Target table is derived from table_ctx (current table being validated).
        """
        start_time = datetime.now(UTC)

        # Validation
        required = {"source_schema", "source_table", "join_columns"}
        missing_keys = required - cfg.keys()
        if missing_keys:
            raise ValueError(f"flow_completeness missing keys: {missing_keys}")

        forbidden = {"target_catalog", "target_schema", "target_table"}
        illegal = forbidden.intersection(cfg.keys())
        if illegal:
            raise ValueError(f"flow_completeness must not define target_* keys: {illegal}")

        tolerance_pct = float(cfg.get("tolerance_percent", 0.0))

        lateness_cfg = cfg.get("lateness")
        lateness_col = None
        max_delay_min = None

        if lateness_cfg:
            lateness_col = lateness_cfg["timestamp_column"]
            max_delay_min = int(lateness_cfg["max_delay_minutes"])

        # Source
        src_fq = f"{cfg.get('source_catalog', table_ctx['catalog'])}.{cfg['source_schema']}.{cfg['source_table']}"
        src = self.spark.table(src_fq)

        if cfg.get("source_filter"):
            src = src.where(cfg["source_filter"])

        total_src = src.count()

        # Target
        trg_fq = f"{table_ctx['catalog']}.{table_ctx['schema']}.{table_ctx['table']}"
        trg = self.spark.table(trg_fq)

        if cfg.get("target_filter"):
            trg = trg.where(cfg["target_filter"])

        # Missing logic
        missing_df = src.join(trg, cfg["join_columns"], "left_anti")

        if lateness_col:
            missing_df = missing_df.withColumn(
                "_delay_min", (f.current_timestamp().cast("long") - f.col(lateness_col).cast("long")) / 60
            )
            late_df = missing_df.filter(f.col("_delay_min") <= max_delay_min)
            lost_df = missing_df.filter(f.col("_delay_min") > max_delay_min)
        else:
            late_df = self.spark.createDataFrame([], missing_df.schema)
            lost_df = missing_df

        late_cnt = late_df.count()
        lost_cnt = lost_df.count()
        total_missing = late_cnt + lost_cnt

        missing_pct = total_missing / total_src * 100 if total_src else 0

        # Pass logic
        if lateness_cfg:
            passed = lost_cnt == 0 or (lost_cnt / total_src * 100 if total_src else 0) <= tolerance_pct
        else:
            passed = missing_pct <= tolerance_pct

        failure_stats = None
        investigation_query = None
        if not passed and total_missing > 0:
            failure_stats = self._build_failure_stats(
                failure_type="incomplete_flow",
                affected_columns=self._as_column_list(cfg["join_columns"]),
                failure_count=total_missing,
                total_checked=total_src,
                diagnostic={
                    "late_count": late_cnt,
                    "lost_count": lost_cnt,
                    "tolerance_pct": tolerance_pct,
                },
                extra={
                    "source_table": src_fq,
                    "target_table": trg_fq,
                },
            )

            # Use lateness-aware template if lateness is configured
            if lateness_cfg:
                investigation_query = self._build_investigation_query(
                    check_type="incomplete_flow_lateness",
                    table_fq=src_fq,
                    affected_columns=self._as_column_list(cfg["join_columns"]),
                    cfg={
                        "source_table": src_fq,
                        "target_table": trg_fq,
                        "join_key": cfg["join_columns"],
                        "lateness_column": lateness_col,
                        "max_delay_minutes": max_delay_min,
                        "source_filter": f"AND {cfg['source_filter']}" if cfg.get("source_filter") else "",
                    },
                    where_clause=None,
                )
            else:
                investigation_query = self._build_investigation_query(
                    check_type="incomplete_flow",
                    table_fq=src_fq,
                    affected_columns=self._as_column_list(cfg["join_columns"]),
                    cfg={
                        "source_table": src_fq,
                        "target_table": trg_fq,
                        "join_key": cfg["join_columns"],
                        "source_filter": f"AND {cfg['source_filter']}" if cfg.get("source_filter") else "",
                    },
                    where_clause=None,
                )

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_src,
            table_info=self._current_table_info,
            secondary_tables=[{"table": src_fq}, {"table": trg_fq}],
        )

        if passed:
            details = self._details_pass_all(total_src, "records", "present in target")
        else:
            details = self._details_fail_missing(total_missing, "records in target", total_src)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, cfg["join_columns"]
