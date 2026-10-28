# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import contextlib
import re
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from neco_dq.test_suites.base_test_suite import DQTestSuite
from neco_dq.test_suites.shared_checks import SharedChecksMixin
from neco_dq.core.io.result_writer import DQResultWriter


class SparkGoldDQTestSuite(DQTestSuite, SharedChecksMixin):
    """
    YAML-driven Gold Data Quality Test Suite
    Business & reconciliation focused
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
        return "gold"

    @property
    def _suite_config_key(self) -> str:
        return "gold"

    def _get_gold_where_clause(self) -> str:
        """Get WHERE clause from row_filter if set, empty string otherwise."""
        row_filter = getattr(self, "_current_where_clause", None)
        return f" WHERE {row_filter}" if row_filter else ""

    # ------------------------------------------------------------------
    # Hook overrides
    # ------------------------------------------------------------------

    def _severity(self, cfg: dict[str, Any]) -> str:
        """Gold layer defaults to critical severity."""
        return cfg.get("severity", "critical")

    def _check_reconciliation_expr(
        self,
        df_gold: DataFrame,
        cfg: dict[str, Any],
    ) -> tuple[bool, str, str | None]:
        """
        Metric-level reconciliation between Gold and Silver.

        The expressions provided in `gold_expr` and `silver_expr` MUST be
        fully-aggregated Spark SQL expressions (e.g. COUNT(*), COUNT(DISTINCT id),
        SUM(metric)).

        Supports:
        - optional filters per layer
        - optional group_by reconciliation
        - optional join_filter to restrict Silver rows to Gold domain
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "silver", "gold_expr", "silver_expr")
        if not ok:
            return False, err, None

        # ------------------------------------------------------------------
        # Config
        # ------------------------------------------------------------------
        silver_block = cfg["silver"] if isinstance(cfg.get("silver"), dict) else {}
        if not silver_block.get("table"):
            return False, self._details_config_missing("silver.table"), None
        silver_table = ".".join(
            str(p) for p in (silver_block.get("catalog"), silver_block.get("schema"), silver_block["table"]) if p
        )
        df_silver = self.spark.table(silver_table)

        gold_expr = cfg["gold_expr"]
        silver_expr = cfg["silver_expr"]
        tolerance_pct = float(cfg.get("tolerance_percent", 1.0))

        gold_filter = cfg.get("gold_filter")
        silver_filter = cfg.get("silver_filter")
        group_by = cfg.get("group_by", [])
        gold_join_col = cfg.get("gold_join_columns")
        silver_join_col = cfg.get("silver_join_columns")
        name = cfg.get("name")

        g_df, s_df = df_gold, df_silver
        gold_rows = g_df.count()

        # ------------------------------------------------------------------
        # Optional domain restriction (Gold → Silver)
        # ------------------------------------------------------------------
        if gold_join_col and silver_join_col:
            # Extract distinct gold IDs with consistent column name for join
            gold_ids = g_df.select(f.col(gold_join_col).cast("string").alias("_join_key")).distinct()

            # Cast silver column and join on the common key
            s_df = (
                s_df.withColumn("_join_key", f.col(silver_join_col).cast("string"))
                .join(gold_ids, "_join_key", "inner")
                .drop("_join_key")
            )

        # ------------------------------------------------------------------
        # Optional filters
        # ------------------------------------------------------------------
        if gold_filter:
            g_df = g_df.filter(f.expr(gold_filter))

        if silver_filter:
            s_df = s_df.filter(f.expr(silver_filter))

        # ------------------------------------------------------------------
        # Grouped reconciliation
        # ------------------------------------------------------------------
        if group_by:
            g_agg = g_df.groupBy(*group_by).agg(f.expr(gold_expr).alias("gold_value"))

            s_agg = s_df.groupBy(*group_by).agg(f.expr(silver_expr).alias("silver_value"))

            diff = (
                g_agg.join(s_agg, group_by, "outer")
                .withColumn("gold_value", f.coalesce("gold_value", f.lit(0)))
                .withColumn("silver_value", f.coalesce("silver_value", f.lit(0)))
                .withColumn(
                    "diff_pct",
                    f.when(
                        (f.col("gold_value") == 0) & (f.col("silver_value") == 0),
                        f.lit(0.0),
                    )
                    .when(
                        f.col("silver_value") == 0,
                        f.lit(100.0),
                    )
                    .otherwise(f.abs(f.col("gold_value") - f.col("silver_value")) / f.col("silver_value") * 100),
                )
            )

            failing = diff.filter(f.col("diff_pct") > tolerance_pct).count()
            total = diff.count()

            passed = failing == 0

            failure_stats = None
            investigation_query = None
            if not passed and failing > 0:
                # Get deviation stats
                dev_stats = (
                    diff.filter(f.col("diff_pct") > tolerance_pct)
                    .agg(
                        f.max("diff_pct").alias("max_diff"),
                        f.avg("diff_pct").alias("avg_diff"),
                    )
                    .first()
                )

                failure_stats = self._build_failure_stats(
                    failure_type="reconciliation_mismatch",
                    affected_columns=group_by,
                    failure_count=failing,
                    total_checked=total,
                    diagnostic={
                        "max_deviation_pct": round(float(dev_stats["max_diff"]), 2) if dev_stats["max_diff"] else 0,
                        "avg_deviation_pct": round(float(dev_stats["avg_diff"]), 2) if dev_stats["avg_diff"] else 0,
                        "tolerance_pct": tolerance_pct,
                    },
                    extra={
                        "silver_table": silver_table,
                        "gold_expr": gold_expr,
                        "silver_expr": silver_expr,
                    },
                )

                gold_filter_parts = []
                if self._current_where_clause:
                    gold_filter_parts.append(self._current_where_clause)
                if gold_filter:
                    gold_filter_parts.append(gold_filter)
                gold_where = f" WHERE {' AND '.join(gold_filter_parts)}" if gold_filter_parts else ""
                silver_where = f" WHERE {silver_filter}" if silver_filter else ""
                investigation_query = {
                    "description": "Find groups with reconciliation mismatch",
                    "query": f"WITH gold AS (SELECT {', '.join(group_by)}, {gold_expr} as gold_value FROM {self._current_table_fq}{gold_where} GROUP BY {', '.join(group_by)}), silver AS (SELECT {', '.join(group_by)}, {silver_expr} as silver_value FROM {silver_table}{silver_where} GROUP BY {', '.join(group_by)}) SELECT g.*, s.silver_value, ABS(g.gold_value - s.silver_value) / NULLIF(s.silver_value, 0) * 100 as diff_pct FROM gold g JOIN silver s USING ({', '.join(group_by)}) WHERE ABS(g.gold_value - s.silver_value) / NULLIF(s.silver_value, 0) * 100 > {tolerance_pct} ORDER BY diff_pct DESC LIMIT 100",
                }

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=gold_rows,
                table_info=self._current_table_info,
                secondary_tables=[{"table": silver_table}],
            )

            if passed:
                details = f"All {total} groups reconcile (within {tolerance_pct}%)"
            else:
                details = self._details_fail_found(failing, f"groups exceeding {tolerance_pct}% deviation", total)

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, name

        # ------------------------------------------------------------------
        # Global reconciliation
        # ------------------------------------------------------------------
        g_val = g_df.selectExpr(f"{gold_expr} AS v").first()["v"] or 0
        s_val = s_df.selectExpr(f"{silver_expr} AS v").first()["v"] or 0

        if g_val == 0 and s_val == 0:
            diff_pct = 0.0
        elif s_val == 0:
            diff_pct = 100.0
        else:
            diff_pct = abs(g_val - s_val) / s_val * 100

        passed = diff_pct <= tolerance_pct

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="reconciliation_mismatch",
                affected_columns=[],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "gold_value": float(g_val),
                    "silver_value": float(s_val),
                    "difference": abs(float(g_val) - float(s_val)),
                    "deviation_pct": round(diff_pct, 2),
                    "tolerance_pct": tolerance_pct,
                },
                extra={
                    "silver_table": silver_table,
                    "gold_expr": gold_expr,
                    "silver_expr": silver_expr,
                },
            )

            gold_filter_parts = []
            if self._current_where_clause:
                gold_filter_parts.append(self._current_where_clause)
            if gold_filter:
                gold_filter_parts.append(gold_filter)
            gold_where = f" WHERE {' AND '.join(gold_filter_parts)}" if gold_filter_parts else ""
            silver_where = f" WHERE {silver_filter}" if silver_filter else ""
            investigation_query = {
                "description": "Verify reconciliation values",
                "query": f"SELECT '{gold_expr}' as metric, ({gold_expr}) as gold_value FROM {self._current_table_fq}{gold_where} UNION ALL SELECT '{silver_expr}' as metric, ({silver_expr}) as silver_value FROM {silver_table}{silver_where}",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=gold_rows,
            table_info=self._current_table_info,
            secondary_tables=[{"table": silver_table}],
        )

        if passed:
            details = self._details_pass_reconciliation(g_val, s_val, diff_pct)
        else:
            details = self._details_fail_deviation("Reconciliation", diff_pct, tolerance_pct)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, name

    def _check_referential_integrity(self, df_gold: DataFrame, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
        """
        Verify foreign key references exist in Silver table.

        Config:
            gold_column: Column in Gold table containing foreign key
            silver: Silver table configuration:
                catalog: Catalog name
                schema: Schema name
                table: Table name
                column: Column to match against
        """
        start_time = datetime.now(UTC)
        gold_col = cfg["gold_column"]
        silver = cfg["silver"]

        silver_fq = f"{silver['catalog']}.{silver['schema']}.{silver['table']}"
        silver_col = silver["column"]

        gold_rows = df_gold.count()
        df_pk = self.spark.table(silver_fq).select(silver_col).distinct()

        missing = df_gold.join(
            df_pk,
            df_gold[gold_col].cast("string") == df_pk[silver_col].cast("string"),
            "left_anti",
        ).count()

        passed = missing == 0

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="referential_integrity_violation",
                affected_columns=[gold_col],
                failure_count=missing,
                total_checked=gold_rows,
                diagnostic={
                    "silver_table": silver_fq,
                    "silver_column": silver_col,
                    "orphan_rate_pct": round(missing / gold_rows * 100, 2) if gold_rows > 0 else 0,
                },
            )

            where_parts = [f"{gold_col} NOT IN (SELECT {silver_col} FROM {silver_fq})"]
            if self._current_where_clause:
                where_parts.append(self._current_where_clause)
            where_clause = " AND ".join(where_parts)
            investigation_query = {
                "description": "Find orphaned foreign keys not in Silver table",
                "query": f"SELECT DISTINCT {gold_col} FROM {self._current_table_fq} WHERE {where_clause} LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=gold_rows,
            table_info=self._current_table_info,
            secondary_tables=[{"table": silver_fq}],
        )

        if passed:
            details = self._details_pass_all(gold_rows, "FK references", f"valid in {silver_fq}")
        else:
            details = self._details_fail_found(missing, f"orphaned FK values in '{gold_col}'", gold_rows)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, gold_col

    def _check_uniqueness(self, df: DataFrame, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
        """
        Verify no duplicate rows exist based on specified columns.

        Config:
            columns: List of column names that must be unique together (required)
            threshold: Optional threshold configuration
                max_failure_percent: Maximum allowed percentage of duplicate rows (0-100)
                max_failure_count: Maximum allowed number of duplicate rows
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "columns")
        if not ok:
            return False, err, None

        cols = cfg["columns"]
        total_count = df.count()
        unique_count = df.dropDuplicates(cols).count()
        failed_count = total_count - unique_count

        # Evaluate with threshold
        passed, threshold_meta = self._evaluate_threshold(failed_count, total_count, cfg)

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="uniqueness_violation",
                affected_columns=cols,
                failure_count=failed_count,
                total_checked=total_count,
                diagnostic={
                    "unique_count": unique_count,
                    "duplicate_rate_pct": round(failed_count / total_count * 100, 2) if total_count > 0 else 0,
                },
            )

            cols_str = ", ".join(cols)
            gold_where = self._get_gold_where_clause()
            investigation_query = {
                "description": "Find duplicate key combinations and their counts",
                "query": f"SELECT {cols_str}, COUNT(*) as cnt FROM {self._current_table_fq}{gold_where} GROUP BY {cols_str} HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_count,
            table_info=self._current_table_info,
        )

        if passed:
            details = self._details_pass_all(total_count, "rows", f"unique on [{', '.join(cols)}]")
        else:
            details = self._details_fail_found(failed_count, f"duplicate rows on [{', '.join(cols)}]", total_count)

        # Add threshold info if present
        if threshold_meta:
            details = self._build_threshold_details(details, threshold_meta)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, ",".join(cols)

    def _check_coverage(self, df_gold: DataFrame, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
        """
        Verify coverage between Gold and Silver tables.

        Config:
            silver: Nested {catalog, schema, table} block for the Silver table
                (required). ``catalog`` is optional.
            gold_expr: SQL expression for Gold aggregation
            silver_expr: SQL expression for Silver aggregation
            tolerance_percent: Allowed percentage difference (default: 1.0)
            join_type: Type of join (left, inner, outer) (default: left)
            join_columns: Columns to join on (optional, for keyed coverage)
            gold_join_columns: Gold-specific join columns (optional)
            silver_join_columns: Silver-specific join columns (optional)
            silver_filter: SQL filter to apply to Silver (optional)

        Supports:
        - Global coverage (no join keys): compares aggregate values
        - Keyed coverage (with join keys): compares per-key aggregates
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "silver", "gold_expr", "silver_expr")
        if not ok:
            return False, err, None

        silver_block = cfg["silver"] if isinstance(cfg.get("silver"), dict) else {}
        if not silver_block.get("table"):
            return False, self._details_config_missing("silver.table"), None
        silver_table = ".".join(
            str(p) for p in (silver_block.get("catalog"), silver_block.get("schema"), silver_block["table"]) if p
        )
        tol_pct = float(cfg.get("tolerance_percent", 1.0))
        join_type = cfg.get("join_type", "left")

        # ------------------------------------------------------------------
        # Resolve join keys
        # ------------------------------------------------------------------
        join_keys = cfg.get("join_columns")
        gold_keys = cfg.get("gold_join_columns", join_keys)
        silver_keys = cfg.get("silver_join_columns", join_keys)

        if bool(gold_keys) ^ bool(silver_keys):
            return (
                False,
                self._details_config_missing("gold_join_columns and silver_join_columns (both required)"),
                None,
            )

        if gold_keys and len(gold_keys) != len(silver_keys):
            return False, "Invalid config: gold_join_columns and silver_join_columns must have same length", None

        # ------------------------------------------------------------------
        # Load and filter silver
        # ------------------------------------------------------------------
        gold_rows = df_gold.count()
        df_silver = self.spark.table(silver_table)

        silver_filter = cfg.get("silver_filter")
        if silver_filter:
            df_silver = df_silver.filter(f.expr(silver_filter))

        # ------------------------------------------------------------------
        # Global coverage (no keys)
        # ------------------------------------------------------------------
        if not gold_keys:
            gold_val = df_gold.selectExpr(f"{cfg['gold_expr']} AS v").first()["v"] or 0

            silver_val = df_silver.selectExpr(f"{cfg['silver_expr']} AS v").first()["v"] or 0

            if silver_val == 0 and gold_val == 0:
                delta_pct = 0.0
            elif silver_val == 0:
                delta_pct = 100.0
            else:
                delta_pct = abs(gold_val - silver_val) / silver_val * 100

            passed = delta_pct <= tol_pct

            failure_stats = None
            investigation_query = None
            if not passed:
                failure_stats = self._build_failure_stats(
                    failure_type="coverage_mismatch",
                    affected_columns=[],
                    failure_count=1,
                    total_checked=1,
                    diagnostic={
                        "gold_value": float(gold_val),
                        "silver_value": float(silver_val),
                        "delta_pct": round(delta_pct, 2),
                        "tolerance_pct": tol_pct,
                    },
                    extra={"silver_table": silver_table},
                )

                gold_where = self._get_gold_where_clause()
                silver_where = f" WHERE {silver_filter}" if silver_filter else ""
                investigation_query = {
                    "description": "Compare global coverage values",
                    "query": f"SELECT 'gold' as source, ({cfg['gold_expr']}) as value FROM {self._current_table_fq}{gold_where} UNION ALL SELECT 'silver' as source, ({cfg['silver_expr']}) as value FROM {silver_table}{silver_where}",
                }

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=gold_rows,
                table_info=self._current_table_info,
                secondary_tables=[{"table": silver_table}],
            )

            if passed:
                details = f"Coverage OK: gold={gold_val}, silver={silver_val} (delta {delta_pct:.1f}%)"
            else:
                details = self._details_fail_exceeds("Coverage delta", f"{delta_pct:.1f}%", f"{tol_pct}% limit")

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, None

        # ------------------------------------------------------------------
        # Keyed coverage
        # ------------------------------------------------------------------
        df_gold_agg = df_gold.groupBy(*gold_keys).agg(f.expr(cfg["gold_expr"]).alias("gold_value"))

        df_silver_agg = df_silver.groupBy(*silver_keys).agg(f.expr(cfg["silver_expr"]).alias("silver_value"))

        join_cond = [df_gold_agg[g] == df_silver_agg[s] for g, s in zip(gold_keys, silver_keys, strict=True)]

        df_cmp = (
            df_gold_agg.join(df_silver_agg, join_cond, join_type)
            .withColumn("gold_value", f.coalesce(f.col("gold_value"), f.lit(0)))
            .withColumn("silver_value", f.coalesce(f.col("silver_value"), f.lit(0)))
            .withColumn(
                "delta_pct",
                f.when(
                    (f.col("silver_value") == 0) & (f.col("gold_value") == 0),
                    f.lit(0.0),
                )
                .when(
                    f.col("silver_value") == 0,
                    f.lit(100.0),
                )
                .otherwise(f.abs(f.col("gold_value") - f.col("silver_value")) / f.col("silver_value") * 100),
            )
        )

        stats = df_cmp.agg(
            f.count("*").alias("total"),
            f.sum(f.when(f.col("delta_pct") > tol_pct, 1).otherwise(0)).alias("failed"),
            f.max("delta_pct").alias("max_delta"),
            f.avg(f.when(f.col("delta_pct") > tol_pct, f.col("delta_pct"))).alias("avg_failed_delta"),
        ).first()

        total = stats["total"]
        failed = stats["failed"]
        passed = failed == 0

        failure_stats = None
        investigation_query = None
        if not passed and failed > 0:
            failure_stats = self._build_failure_stats(
                failure_type="coverage_mismatch",
                affected_columns=gold_keys,
                failure_count=failed,
                total_checked=total,
                diagnostic={
                    "max_delta_pct": round(float(stats["max_delta"]), 2) if stats["max_delta"] else 0,
                    "avg_failed_delta_pct": round(float(stats["avg_failed_delta"]), 2)
                    if stats["avg_failed_delta"]
                    else 0,
                    "tolerance_pct": tol_pct,
                    "join_type": join_type,
                },
                extra={"silver_table": silver_table},
            )

            gold_keys_str = ", ".join(gold_keys)
            silver_keys_str = ", ".join(silver_keys)
            join_conditions = " AND ".join(f"g.{gk} = s.{sk}" for gk, sk in zip(gold_keys, silver_keys, strict=True))
            gold_where = self._get_gold_where_clause()
            silver_where = f" WHERE {silver_filter}" if silver_filter else ""
            investigation_query = {
                "description": "Find keys with coverage mismatch",
                "query": f"WITH gold AS (SELECT {gold_keys_str}, {cfg['gold_expr']} as gold_value FROM {self._current_table_fq}{gold_where} GROUP BY {gold_keys_str}), silver AS (SELECT {silver_keys_str}, {cfg['silver_expr']} as silver_value FROM {silver_table}{silver_where} GROUP BY {silver_keys_str}) SELECT g.*, s.silver_value, ABS(g.gold_value - s.silver_value) / NULLIF(s.silver_value, 0) * 100 as delta_pct FROM gold g {join_type.upper()} JOIN silver s ON {join_conditions} WHERE ABS(COALESCE(g.gold_value, 0) - COALESCE(s.silver_value, 0)) / NULLIF(s.silver_value, 0) * 100 > {tol_pct} ORDER BY delta_pct DESC LIMIT 100",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=gold_rows,
            table_info=self._current_table_info,
            secondary_tables=[{"table": silver_table}],
        )

        if passed:
            details = f"All {total} keys within coverage tolerance ({tol_pct}%)"
        else:
            details = self._details_fail_found(failed, f"keys exceeding {tol_pct}% tolerance", total)

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, ",".join(gold_keys) if gold_keys else None

    def _check_volume(
        self, df: DataFrame, cfg: dict[str, Any], table_ctx: dict[str, str]
    ) -> tuple[bool, str, str | None]:
        """
        Verify metric value is within expected bounds or historical deviation.

        Config:
            metric: SQL expression for volume metric (e.g., "COUNT(*)", "SUM(amount)")
            group_by: Columns to group by before aggregation (optional)
            min_value: Minimum allowed value (optional)
            max_value: Maximum allowed value (optional)
            name: Human-readable name for the check (optional)
            history: Historical comparison configuration (optional):
                table: Table containing historical DQ results
                window: Number of historical records to consider (default: 7)
                min_history: Minimum records required for comparison (default: 3)
                tolerance_percent: Allowed percentage deviation from baseline (default: 10)
        """
        start_time = datetime.now(UTC)
        metric = cfg["metric"]
        group_by = cfg.get("group_by", [])
        min_v = cfg.get("min_value")
        max_v = cfg.get("max_value")

        total_rows = df.count()
        current_df = (
            df.groupBy(*group_by).agg(f.expr(metric).alias("metric_value"))
            if group_by
            else df.selectExpr(f"{metric} as metric_value")
        )

        current_value = current_df.select(f.sum("metric_value")).first()[0] or 0

        history_cfg = cfg.get("history")
        if history_cfg:
            history_table = history_cfg["table"]
            history_df = (
                self.spark.table(history_table)
                .filter(
                    (f.col("check_name") == cfg.get("name", "volume"))
                    & (f.col("catalog") == table_ctx["catalog"])
                    & (f.col("schema") == table_ctx["schema"])
                    & (f.col("table") == table_ctx["table"])
                )
                .orderBy(f.col("run_timestamp").desc())
                .limit(history_cfg.get("window", 7))
            )

            hist_cnt = history_df.count()
            min_hist = history_cfg.get("min_history", 3)

            if hist_cnt >= min_hist:
                baseline = history_df.selectExpr(
                    """
                        percentile_approx(
                        try_cast(regexp_extract(details, 'value=([0-9.]+)', 1) AS DOUBLE),
                        0.5
                        ) AS baseline
                        """
                ).first()[0]

                if baseline and baseline > 0:
                    tol = history_cfg.get("tolerance_percent", 10)
                    deviation = abs(current_value - baseline) / baseline * 100
                    passed = deviation <= tol

                    failure_stats = None
                    investigation_query = None
                    if not passed:
                        failure_stats = self._build_failure_stats(
                            failure_type="volume_deviation",
                            affected_columns=[],
                            failure_count=1,
                            total_checked=1,
                            diagnostic={
                                "current_value": float(current_value),
                                "baseline": float(baseline),
                                "deviation_pct": round(deviation, 2),
                                "tolerance_pct": tol,
                                "history_count": hist_cnt,
                            },
                        )

                        gold_where = self._get_gold_where_clause()
                        investigation_query = {
                            "description": "Compare current metric with historical baseline",
                            "query": f"SELECT '{metric}' as metric, ({metric}) as current_value, {baseline} as baseline, ABS(({metric}) - {baseline}) / {baseline} * 100 as deviation_pct FROM {self._current_table_fq}{gold_where}",
                        }

                    execution = self._build_execution_trace(
                        start_time=start_time,
                        rows_scanned=total_rows,
                        table_info=self._current_table_info,
                        secondary_tables=[{"table": history_table}],
                    )

                    if passed:
                        details = f"Volume OK: {current_value} (within {tol}% of baseline {baseline})"
                    else:
                        details = self._details_fail_deviation("Historical", deviation, tol)

                    self._last_failure_stats = failure_stats
                    self._last_investigation_query = investigation_query
                    self._last_execution = execution

                    return passed, details, None

        # Bounds-based check
        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if min_v is not None and current_value < min_v:
            failure_stats = self._build_failure_stats(
                failure_type="volume_below_minimum",
                affected_columns=[],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "current_value": float(current_value),
                    "min_threshold": float(min_v),
                    "shortfall": float(min_v - current_value),
                },
            )

            group_by_prefix = ", ".join(group_by) + ", " if group_by else ""
            group_by_clause = f" GROUP BY {', '.join(group_by)}" if group_by else ""
            gold_where = self._get_gold_where_clause()
            investigation_query = {
                "description": "Analyze volume metric breakdown",
                "query": f"SELECT {group_by_prefix}{metric} as value FROM {self._current_table_fq}{gold_where}{group_by_clause} ORDER BY value DESC",
            }

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            details = self._details_fail_below("Volume", current_value, min_v)
            return False, details, None

        if max_v is not None and current_value > max_v:
            failure_stats = self._build_failure_stats(
                failure_type="volume_above_maximum",
                affected_columns=[],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "current_value": float(current_value),
                    "max_threshold": float(max_v),
                    "excess": float(current_value - max_v),
                },
            )

            group_by_prefix = ", ".join(group_by) + ", " if group_by else ""
            group_by_clause = f" GROUP BY {', '.join(group_by)}" if group_by else ""
            gold_where = self._get_gold_where_clause()
            investigation_query = {
                "description": "Analyze volume metric breakdown",
                "query": f"SELECT {group_by_prefix}{metric} as value FROM {self._current_table_fq}{gold_where}{group_by_clause} ORDER BY value DESC",
            }

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            details = self._details_fail_above("Volume", current_value, max_v)
            return False, details, None

        self._last_failure_stats = None
        self._last_investigation_query = None
        self._last_execution = execution

        if min_v is not None or max_v is not None:
            bounds = []
            if min_v is not None:
                bounds.append(f"min={min_v}")
            if max_v is not None:
                bounds.append(f"max={max_v}")
            details = self._details_pass_bounds("Volume", current_value, ", ".join(bounds))
        else:
            details = self._details_pass_count("Volume", current_value, "")

        return True, details, None

    def _check_cross_gold(
        self,
        df: DataFrame,  # noqa: ARG002 - 'df' required so dispatch passes table_ctx (3-arg signature)
        cfg: dict[str, Any],
        table_ctx: dict[str, str],
    ) -> tuple[bool, str, str | None]:
        """
        Cross-Gold reconciliation check.

        Compares aggregated values between two tables (Gold vs Gold or Gold vs Silver).

        Config:
            left_table: Fully qualified left table name (default: current table)
            right_table: Fully qualified right table name
            left_expr: Expression for left table
            right_expr: Expression for right table
            left_filter: Optional SQL filter for left table (e.g., "type = 'rating'")
            right_filter: Optional SQL filter for right table (e.g., "type = 'rating'")
            group_by: Optional list of columns to group by
            tolerance_abs: Numeric tolerance for comparison (default 0.01)
            raw_expr: If True, use expressions as-is (for aggregate expressions like COUNT, SUM).
                      If False (default), wrap expressions in SUM().
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "right_table", "left_expr", "right_expr")
        if not ok:
            return False, err, None

        default_left = f"{table_ctx['catalog']}.{table_ctx['schema']}.{table_ctx['table']}"
        left_table = cfg.get("left_table", default_left)
        right_table = cfg["right_table"]
        df_left = self.spark.table(left_table)
        df_right = self.spark.table(right_table)

        # Apply optional filters
        left_filter = cfg.get("left_filter")
        right_filter = cfg.get("right_filter")

        if left_filter:
            df_left = df_left.filter(f.expr(left_filter))
        if right_filter:
            df_right = df_right.filter(f.expr(right_filter))

        group_by = cfg.get("group_by", [])
        tolerance = cfg.get("tolerance_abs", 0.01)
        raw_expr = cfg.get("raw_expr", False)

        if group_by:
            if raw_expr:
                left_agg = df_left.groupBy(*group_by).agg(f.expr(cfg["left_expr"]).alias("left_value"))
                right_agg = df_right.groupBy(*group_by).agg(f.expr(cfg["right_expr"]).alias("right_value"))
            else:
                left_agg = df_left.groupBy(*group_by).agg(f.sum(f.expr(cfg["left_expr"])).alias("left_value"))
                right_agg = df_right.groupBy(*group_by).agg(f.sum(f.expr(cfg["right_expr"])).alias("right_value"))

            diff_df = left_agg.join(right_agg, group_by, "outer").withColumn(
                "diff", f.abs(f.col("left_value") - f.col("right_value"))
            )

            total = diff_df.count()
            failing = diff_df.filter(f.col("diff") > tolerance).count()
            passed = failing == 0

            failure_stats = None
            investigation_query = None
            if not passed and failing > 0:
                max_diff = diff_df.filter(f.col("diff") > tolerance).agg(f.max("diff")).first()[0]

                failure_stats = self._build_failure_stats(
                    failure_type="cross_gold_mismatch",
                    affected_columns=group_by,
                    failure_count=failing,
                    total_checked=total,
                    diagnostic={
                        "max_difference": float(max_diff) if max_diff else 0,
                        "tolerance": tolerance,
                    },
                    extra={
                        "left_table": left_table,
                        "right_table": right_table,
                        "left_expr": cfg["left_expr"],
                        "right_expr": cfg["right_expr"],
                    },
                )

                group_by_str = ", ".join(group_by)
                left_agg_expr = cfg["left_expr"] if raw_expr else f"SUM({cfg['left_expr']})"
                right_agg_expr = cfg["right_expr"] if raw_expr else f"SUM({cfg['right_expr']})"
                left_where = f" WHERE {left_filter}" if left_filter else ""
                right_where = f" WHERE {right_filter}" if right_filter else ""
                query_template = (
                    f"WITH left_agg AS (SELECT {group_by_str}, {left_agg_expr} as left_value FROM {left_table}{left_where} GROUP BY {group_by_str}), "
                    f"right_agg AS (SELECT {group_by_str}, {right_agg_expr} as right_value FROM {right_table}{right_where} GROUP BY {group_by_str}) "
                    f"SELECT l.*, r.right_value, ABS(l.left_value - r.right_value) as diff FROM left_agg l FULL OUTER JOIN right_agg r USING ({group_by_str}) "
                    f"WHERE ABS(COALESCE(l.left_value, 0) - COALESCE(r.right_value, 0)) > {tolerance} ORDER BY diff DESC LIMIT 100"
                )
                investigation_query = {
                    "description": "Find groups with cross-gold mismatch",
                    "query": query_template,
                }

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
                secondary_tables=[{"table": left_table}, {"table": right_table}],
            )

            if passed:
                details = f"All {total} groups within tolerance ({tolerance})"
            else:
                details = self._details_fail_found(failing, f"groups exceeding tolerance {tolerance}", total)

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, None

        # Global comparison
        left_val = df_left.selectExpr(f"{cfg['left_expr']} as v").first()["v"] or 0
        right_val = df_right.selectExpr(f"{cfg['right_expr']} as v").first()["v"] or 0
        diff = abs(left_val - right_val)
        passed = diff <= tolerance

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="cross_gold_mismatch",
                affected_columns=[],
                failure_count=1,
                total_checked=1,
                diagnostic={
                    "left_value": float(left_val),
                    "right_value": float(right_val),
                    "difference": float(diff),
                    "tolerance": tolerance,
                },
                extra={
                    "left_table": left_table,
                    "right_table": right_table,
                },
            )

            left_where_clause = f" WHERE {left_filter}" if left_filter else ""
            right_where_clause = f" WHERE {right_filter}" if right_filter else ""
            investigation_query = {
                "description": "Compare cross-gold values",
                "query": f"SELECT 'left' as source, ({cfg['left_expr']}) as value FROM {left_table}{left_where_clause} UNION ALL SELECT 'right' as source, ({cfg['right_expr']}) as value FROM {right_table}{right_where_clause}",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=0,
            table_info=self._current_table_info,
            secondary_tables=[{"table": left_table}, {"table": right_table}],
        )

        if passed:
            details = f"Cross-gold OK: left={left_val}, right={right_val} (diff={diff})"
        else:
            details = f"Difference {diff:.4f} exceeds tolerance {tolerance}"

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, None

    def _check_completeness(self, df: DataFrame, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
        """
        Verify all expected dimension values are present in the data.

        Config:
            column: Column containing dimension values (required)
            expected_values: List of values that must be present (required)
            allow_extra: Allow values not in expected list (default: True)
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "column", "expected_values")
        if not ok:
            return False, err, None

        column = cfg["column"]
        expected_values = {str(v) for v in cfg["expected_values"]}
        allow_extra = cfg.get("allow_extra", True)

        total_rows = df.count()

        if column not in df.columns:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=0,
                table_info=self._current_table_info,
            )
            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution
            return False, self._details_not_found("Column", column), column

        # Use Spark-native operations to avoid .collect() on high-cardinality columns
        # This is critical for serverless environments with limited driver memory
        expected_list = list(expected_values)
        distinct_df = df.select(f.col(column).cast("string").alias("_val")).filter(f.col("_val").isNotNull()).distinct()

        # Count how many expected values are present (no collect needed)
        present_count = distinct_df.filter(f.col("_val").isin(expected_list)).count()
        missing_count = len(expected_values) - present_count

        # For extra values, only count if allow_extra=False
        extra_count = 0
        if not allow_extra:
            extra_count = distinct_df.filter(~f.col("_val").isin(expected_list)).count()

        actual_count = distinct_df.count()
        passed = missing_count == 0 and extra_count == 0

        failure_stats = None
        investigation_query = None
        if not passed:
            failure_stats = self._build_failure_stats(
                failure_type="completeness_violation",
                affected_columns=[column],
                failure_count=missing_count + extra_count,
                total_checked=len(expected_values),
                diagnostic={
                    "missing_count": missing_count,
                    "extra_count": extra_count,
                    "expected_count": len(expected_values),
                    "actual_count": actual_count,
                    "allow_extra": allow_extra,
                },
            )

            gold_where = self._get_gold_where_clause()
            investigation_query = {
                "description": "List distinct values in column",
                "query": f"SELECT DISTINCT {column}, COUNT(*) as cnt FROM {self._current_table_fq}{gold_where} GROUP BY {column} ORDER BY cnt DESC",
            }

        execution = self._build_execution_trace(
            start_time=start_time,
            rows_scanned=total_rows,
            table_info=self._current_table_info,
        )

        if passed:
            details = f"All {len(expected_values)} expected values present in '{column}'"
        else:
            issues = []
            if missing_count > 0:
                issues.append(f"{missing_count} missing")
            if extra_count > 0:
                issues.append(f"{extra_count} unexpected")
            details = f"Completeness issue in '{column}': {', '.join(issues)}"

        self._last_failure_stats = failure_stats
        self._last_investigation_query = investigation_query
        self._last_execution = execution

        return passed, details, column

    def _extract_metric_from_details(self, details: str) -> float | None:
        """
        Extract metric value from details string.

        Supports formats:
        - "metric=123" or "value=123" or "current=123" or "baseline=123"
        - "gold=123" or "silver=123"
        - "Volume OK: 123" or "Volume: 123" or "Count: 123"
        - "123 rows" or "123 records"
        - Numbers in parentheses: "(123)"
        """
        if not details:
            return None

        result: float | None = None

        # Try key=value patterns (most explicit)
        pattern = r"(?:metric|value|current|gold|silver|count|total)[\s:=]+([0-9]+\.?[0-9]*)"
        match = re.search(pattern, details, re.IGNORECASE)
        if match:
            with contextlib.suppress(ValueError):
                result = float(match.group(1))

        # Try "Volume OK: X" or "Count: X" style
        if result is None:
            match = re.search(r"(?:Volume|Count|Rows?)[^0-9]*([0-9]+\.?[0-9]*)", details, re.IGNORECASE)
            if match:
                with contextlib.suppress(ValueError):
                    result = float(match.group(1))

        # Try "X rows" or "X records" style
        if result is None:
            match = re.search(r"([0-9]+\.?[0-9]*)\s*(?:rows?|records?)", details, re.IGNORECASE)
            if match:
                with contextlib.suppress(ValueError):
                    result = float(match.group(1))

        # Last resort: first number in the string (avoiding percentages)
        if result is None:
            match = re.search(r"([0-9]+\.?[0-9]*)", details)
            if match:
                # A regex-matched numeric token should always parse; suppress
                # the unlikely ValueError and fall through to return None.
                with contextlib.suppress(ValueError):
                    val = float(match.group(1))
                    # Avoid extracting percentages as the metric
                    pct_check = re.search(r"([0-9]+\.?[0-9]*)\s*%", details)
                    if pct_check and pct_check.group(1) == match.group(1):
                        # This is a percentage, try to find another number
                        remaining = details[match.end() :]
                        next_match = re.search(r"([0-9]+\.?[0-9]*)", remaining)
                        if next_match:
                            result = float(next_match.group(1))
                    else:
                        result = val

        return result

    def _check_trend(  # noqa: PLR0911
        self, df: DataFrame, cfg: dict[str, Any], table_ctx: dict[str, str]
    ) -> tuple[bool, str, str | None]:
        """
        Verify metric trend compared to historical values.

        Config:
            metric: SQL expression for the metric (e.g., "COUNT(*)", "SUM(amount)")
            history: Historical-results configuration:
                table: Table containing historical DQ results (required)
                window: Number of historical records to consider (default: 7)
                min_history: Minimum records required for comparison (default: 3)
                tolerance_percent: Allowed percentage deviation from median (default: 20)
            name: Check name to lookup in history (required)
            direction: Expected trend direction - "stable", "increasing", "decreasing" (default: stable)
        """
        start_time = datetime.now(UTC)
        ok, err = self._require_cfg(cfg, "metric", "history", "name")
        if not ok:
            return False, err, None

        history_cfg = cfg["history"] if isinstance(cfg["history"], dict) else {}
        if not history_cfg.get("table"):
            return False, self._details_config_missing("history.table"), None

        metric_expr = cfg["metric"]
        history_table = history_cfg["table"]
        check_name = cfg["name"]
        window = int(history_cfg.get("window", 7))
        min_history = int(history_cfg.get("min_history", 3))
        tolerance_pct = float(history_cfg.get("tolerance_percent", 20))
        direction = cfg.get("direction", "stable")

        total_rows = df.count()
        current_value = df.selectExpr(f"{metric_expr} as v").first()["v"] or 0

        try:
            history_df = (
                self.spark.table(history_table)
                .filter(
                    (f.col("check_name") == check_name)
                    & (f.col("catalog") == table_ctx["catalog"])
                    & (f.col("schema") == table_ctx["schema"])
                    & (f.col("table") == table_ctx["table"])
                )
                .orderBy(f.col("run_timestamp").desc())
                .limit(window)
            )

            hist_count = history_df.count()
            if hist_count < min_history:
                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=total_rows,
                    table_info=self._current_table_info,
                    secondary_tables=[{"table": history_table}],
                )

                self._last_failure_stats = None
                self._last_investigation_query = None
                self._last_execution = execution

                return (
                    True,
                    f"{self._details_insufficient('history', hist_count, min_history)}, current={current_value}",
                    None,
                )

            # Extract metric values from details
            hist_values = [
                v
                for row in history_df.collect()
                if (v := self._extract_metric_from_details(row["details"] or "")) is not None
            ]

            if len(hist_values) < min_history:
                execution = self._build_execution_trace(
                    start_time=start_time,
                    rows_scanned=total_rows,
                    table_info=self._current_table_info,
                    secondary_tables=[{"table": history_table}],
                )

                self._last_failure_stats = None
                self._last_investigation_query = None
                self._last_execution = execution

                return (
                    True,
                    f"{self._details_insufficient('parseable history', len(hist_values), min_history)}, current={current_value}",
                    None,
                )

            # Calculate baseline (median)
            sorted_vals = sorted(hist_values)
            mid = len(sorted_vals) // 2
            baseline = sorted_vals[mid] if len(sorted_vals) % 2 == 1 else (sorted_vals[mid - 1] + sorted_vals[mid]) / 2

            # Check deviation
            deviation_pct = (
                100.0
                if baseline == 0 and current_value != 0
                else (0.0 if baseline == 0 else abs(current_value - baseline) / baseline * 100)
            )

            # Check direction
            direction_checks = {
                "stable": deviation_pct <= tolerance_pct,
                "increasing": current_value >= baseline * (1 - tolerance_pct / 100),
                "decreasing": current_value <= baseline * (1 + tolerance_pct / 100),
            }
            if direction not in direction_checks:
                return False, f"Invalid config: direction '{direction}' not supported", None

            passed = direction_checks[direction]

            failure_stats = None
            investigation_query = None
            if not passed:
                failure_stats = self._build_failure_stats(
                    failure_type="trend_violation",
                    affected_columns=[],
                    failure_count=1,
                    total_checked=1,
                    diagnostic={
                        "current_value": float(current_value),
                        "baseline": float(baseline),
                        "deviation_pct": round(deviation_pct, 2),
                        "tolerance_pct": tolerance_pct,
                        "direction": direction,
                        "history_count": len(hist_values),
                    },
                )

                investigation_query = {
                    "description": "Compare current metric with historical trend",
                    "query": f"SELECT run_timestamp, details, regexp_extract(details, 'value=([0-9.]+)', 1) as metric_value FROM {history_table} WHERE check_name = '{check_name}' AND catalog = '{table_ctx['catalog']}' AND schema = '{table_ctx['schema']}' AND table = '{table_ctx['table']}' ORDER BY run_timestamp DESC LIMIT {window}",
                }

            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
                secondary_tables=[{"table": history_table}],
            )

            if passed:
                details = f"Trend OK ({direction}): current={current_value}, baseline={baseline} (deviation {deviation_pct:.1f}%)"
            else:
                details = f"Trend deviation ({direction}) {deviation_pct:.1f}% exceeds {tolerance_pct}% limit"

            self._last_failure_stats = failure_stats
            self._last_investigation_query = investigation_query
            self._last_execution = execution

            return passed, details, None

        except Exception as e:
            execution = self._build_execution_trace(
                start_time=start_time,
                rows_scanned=total_rows,
                table_info=self._current_table_info,
            )

            self._last_failure_stats = None
            self._last_investigation_query = None
            self._last_execution = execution

            return False, self._details_error(f"trend check: {e}"), None
