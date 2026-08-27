# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from datetime import datetime, UTC
from typing import Any

import great_expectations as gx
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.types.base import ProgressBarsConfig
from pyspark.sql import SparkSession

from neco_dq.test_suites.base_test_suite import DQTestSuite
from neco_dq.core.io.result_writer import DQResultWriter
from neco_dq.test_suites.ge.ge_suite_builder import build_suite_from_schema
from neco_dq.test_suites.ge.ge_result_normalizer import normalize_ge_results

logger = logging.getLogger("neco_dq")


class GEDQTestSuite(DQTestSuite):
    """
    Great Expectations DQ Test Suite.
    Uses DQResultWriter to store results in configured destinations.
    """

    # ------------------------------------------------------------------
    # Suite identity (required by base class)
    # ------------------------------------------------------------------

    @property
    def _suite_name(self) -> str:
        return "great_expectations"

    @property
    def _suite_config_key(self) -> str:
        return "great_expectations"

    # ------------------------------------------------------------------

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        self.spark = spark
        self.cfg = config
        self.ge_cfg = config.get("great_expectations", {})
        self.data_to_save = self.ge_cfg.get("data_to_save", [])
        self.defaults = self.ge_cfg.get("defaults", {})

        self.rows_by_destination: dict[str, list[dict[str, Any]]] = {}
        self.alerts_by_destination: dict[str, list[dict[str, Any]]] = {}
        self.global_alerts_table = self.ge_cfg.get("alerts_table")
        self.writer = DQResultWriter(self.spark)

        self._configure_logging()

    def _configure_logging(self):
        # Reduce GE logging to warnings only
        logging.getLogger("great_expectations").setLevel(logging.WARNING)
        logging.getLogger("great_expectations.expectations").setLevel(logging.WARNING)
        logging.getLogger("great_expectations.data_context.types.base").setLevel(logging.WARNING)

    # ------------------------------------------------------------------
    # Data Filtering (row_filter, partition_filter)
    # ------------------------------------------------------------------

    def _resolve_effective_filters(self, table_cfg: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
        """
        Resolve effective row_filter and partition_filter with inheritance.

        Precedence: Table-level > Suite-level defaults

        Returns:
            tuple of (row_filter_sql, partition_filter_dict)
        """
        # Row filter: table overrides suite default
        row_filter = table_cfg["row_filter"] if "row_filter" in table_cfg else self.defaults.get("row_filter")

        # Partition filter: table overrides suite default
        partition_filter = (
            table_cfg["partition_filter"] if "partition_filter" in table_cfg else self.defaults.get("partition_filter")
        )

        return row_filter, partition_filter

    def _apply_filters(
        self,
        df: Any,
        table_cfg: dict[str, Any],
        table_fq: str,
        run_ts: datetime,
    ) -> tuple[Any, list[dict[str, Any]], int, str | None]:
        """
        Apply row and partition filters to DataFrame.

        Returns:
            tuple of (filtered_df, filter_check_results, rows_after, where_clause)
        """
        row_filter, partition_filter = self._resolve_effective_filters(table_cfg)

        if not row_filter and not partition_filter:
            return df, [], -1, None  # -1 indicates no filtering applied

        rows_before = df.count()

        # Apply partition filter first (for pruning optimization)
        partition_filter_sql = None
        if partition_filter:
            partition_filter_sql = self._build_partition_filter(partition_filter)
            df = df.filter(partition_filter_sql)

        # Apply row filter
        if row_filter:
            df = df.filter(row_filter)

        rows_after = df.count()
        filter_ratio = f"{(rows_after / rows_before * 100):.2f}%" if rows_before > 0 else "N/A"

        # Build combined where_clause for investigation queries
        if row_filter and partition_filter_sql:
            where_clause = f"({partition_filter_sql}) AND ({row_filter})"
        elif partition_filter_sql:
            where_clause = partition_filter_sql
        else:
            where_clause = row_filter

        # Build data_filter check result
        filter_check = {
            "test_suite": "great_expectations",
            "catalog": table_cfg.get("catalog"),
            "schema": table_cfg.get("schema"),
            "table": table_fq,
            "table_fullname": table_fq,
            "run_ts": run_ts,
            "passed": True,
            "check_name": "data_filter",
            "column": None,
            "severity": "info",
            "blocking": False,
            "extra_metadata": {},
            "record_count": rows_after,
            "failed_count": 0,
            "details": {
                "status": "ok",
                "row_filter": row_filter or "none",
                "partition_filter": partition_filter_sql or "none",
                "rows_before": rows_before,
                "rows_after": rows_after,
                "filter_ratio": filter_ratio,
            },
        }

        # Handle empty result after filtering
        if rows_after == 0:
            filter_check["severity"] = "warning"
            filter_check["details"]["status"] = "no_rows_after_filter"

        return df, [filter_check], rows_after, where_clause

    # ------------------------------------------------------------------
    # Tag-based filtering for GE expectations
    # ------------------------------------------------------------------

    def _filter_checks_by_tags(self, checks: dict[str, Any]) -> dict[str, Any]:
        """
        Filter GE expectations by tags.

        GE checks have a nested structure:
        {
            "column_name": {
                "not_null": {"enabled": true, "tags": ["smoke"]},
                "unique": {"enabled": true}
            },
            "_dataset": {
                "min_rows": {"enabled": true, "tags": ["daily"]}
            }
        }

        Logic:
        - No filter set -> return all checks
        - Filter set + rule has no tags -> skip (strict filtering)
        - Filter set + rule has tags -> include if ANY tag matches

        Args:
            checks: Dict of column/dataset -> rules configurations

        Returns:
            Filtered dict with only matching rules
        """
        if not self._tags_filter:
            return checks

        filtered: dict[str, Any] = {}
        # Use cached frozenset directly (no conversion needed)
        filter_set = self._tags_filter

        for column_or_key, rules in checks.items():
            if not isinstance(rules, dict):
                # Skip non-dict entries
                continue

            filtered_rules: dict[str, Any] = {}
            for rule_name, rule_cfg in rules.items():
                if not isinstance(rule_cfg, dict):
                    continue

                rule_tags = rule_cfg.get("tags", [])
                if not rule_tags:
                    continue

                if set(rule_tags) & filter_set:
                    filtered_rules[rule_name] = rule_cfg

            if filtered_rules:
                filtered[column_or_key] = filtered_rules

        return filtered

    def _setup_ge_context(self, table: str):
        """
        Set up GE ephemeral context with datasource, asset, and batch definition.

        Returns:
            tuple of (context, batch_def)
        """
        context = gx.get_context(mode="ephemeral")
        # Show progress bars only in verbose mode
        show_progress = os.environ.get("NECO_DQ_VERBOSE", "0") == "1"
        context.config.progress_bars = ProgressBarsConfig(globally=show_progress, metric_calculations=show_progress)

        try:
            datasource = context.data_sources.get("batch")
        except KeyError:
            datasource = context.data_sources.add_pandas("batch")
        try:
            asset = datasource.get_asset(f"{table.lower()}_asset")
        except LookupError:
            asset = datasource.add_dataframe_asset(f"{table.lower()}_asset")
        try:
            batch_def = asset.get_batch_definition("default")
        except KeyError:
            batch_def = asset.add_batch_definition_whole_dataframe("default")

        return context, batch_def

    def _setup_validation(self, context, batch_def, table_cfg: dict[str, Any], schema_def: dict):
        """
        Set up GE suite, validation definition, and checkpoint.

        Returns:
            checkpoint
        """
        catalog = table_cfg["catalog"]
        schema = table_cfg["schema"]
        table = table_cfg["table"]
        table_fq = f"{catalog}.{schema}.{table}"

        suite_name = f"{catalog}_{schema}_{table}_GE"
        suite = build_suite_from_schema(schema_def, suite_name)
        context.suites.add_or_update(suite)

        vd_name = f"{table_fq}_vd"
        try:
            vd = context.validation_definitions.get(vd_name)
        except gx.exceptions.DataContextError:
            vd = gx.ValidationDefinition(name=vd_name, data=batch_def, suite=suite)
            context.validation_definitions.add(vd)

        checkpoint_name = f"{catalog}.{schema}.{table} Checkpoint"
        try:
            checkpoint = context.checkpoints.get(checkpoint_name)
        except gx.exceptions.DataContextError:
            checkpoint = gx.Checkpoint(name=checkpoint_name, validation_definitions=[vd])
            context.checkpoints.add(checkpoint)

        return checkpoint

    def _enrich_results(self, normalized: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add GE element_count / unexpected_count to normalized results from failure_stats."""
        for r in normalized:
            # Extract counts from failure_stats (v1.1.0 format)
            failure_stats = r.get("failure_stats") or {}
            record_count = failure_stats.get("total_rows_checked", 0)
            failed_count = failure_stats.get("failure_count", 0)

            # For passed checks without failure_stats, try execution trace
            if not record_count:
                execution = r.get("execution") or {}
                record_count = execution.get("rows_scanned", 0)

            r["record_count"] = record_count
            r["failed_count"] = failed_count

        return normalized

    def validate_table(self, table_cfg: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Validate a single table using GE and return normalized results.

        Creates a fresh ephemeral context per table to avoid conflicts
        between different suites (e.g., silver vs gold).

        Supports data filtering via row_filter and partition_filter:
        - Configured at suite level (defaults) or table level
        - Applied to Spark DataFrame before conversion to Pandas
        - Uses Spark SQL syntax (e.g., current_date(), INTERVAL)
        """
        catalog = table_cfg["catalog"]
        schema = table_cfg["schema"]
        table = table_cfg["table"]
        raw_checks = table_cfg["checks"]
        table_fq = f"{catalog}.{schema}.{table}"
        run_ts = datetime.now(UTC)

        # Filter checks by tags if filter is set
        schema_def = self._filter_checks_by_tags(raw_checks)
        if not schema_def:
            logger.info(f"No checks match tags filter for {table_fq}, skipping")
            return []

        # Load Spark DataFrame and apply filters before toPandas()
        df = self.spark.table(table_fq)
        df, filter_results, rows_after, where_clause = self._apply_filters(df, table_cfg, table_fq, run_ts)

        # Skip GE validation if no rows after filtering (rows_after == -1 means no filters applied)
        if rows_after == 0:
            logger.warning(f"No rows after filtering for {table_fq}, skipping GE validation")
            return filter_results

        # Convert filtered Spark DataFrame to Pandas for GE
        pdf = df.toPandas()

        # Set up GE context and validation
        context, batch_def = self._setup_ge_context(table)
        checkpoint = self._setup_validation(context, batch_def, table_cfg, schema_def)

        # Run checkpoint
        run_id = RunIdentifier(
            run_name=f"run_{table_fq}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}",
            run_time=datetime.now(UTC),
        )
        result = checkpoint.run(batch_parameters={"dataframe": pdf}, run_id=run_id)
        validation_result = result.run_results[next(iter(result.run_results.keys()))]

        # Normalize and enrich results
        normalized = normalize_ge_results(
            validation_result=validation_result,
            table_fq=table_fq,
            run_ts=run_ts,
            run_id=run_id,
            default_severity=table_cfg.get("severity", "error"),
            where_clause=where_clause,
            profile_name=self._profile_name,
            profile_target_catalog=self._profile_target_catalog,
        )
        normalized = self._enrich_results(normalized)

        return filter_results + normalized

    def run(self) -> list[dict[str, Any]]:
        """
        Execute GE tests for all configured tables and write results using DQResultWriter.
        """
        all_results: list[dict[str, Any]] = []
        all_rows: list[dict[str, Any]] = []
        failed_rows: list[dict[str, Any]] = []

        for table_cfg in self.ge_cfg.get("tables", []):
            try:
                table_results = self.validate_table(table_cfg)
            except Exception as e:
                table_fq = f"{table_cfg.get('catalog')}.{table_cfg.get('schema')}.{table_cfg.get('table')}"
                table_results = [
                    {
                        "test_suite": "great_expectations",
                        "catalog": table_cfg.get("catalog"),
                        "schema": table_cfg.get("schema"),
                        "table": table_fq,
                        "table_fullname": table_fq,
                        "run_timestamp": datetime.now(UTC),
                        "passed": False,
                        "check_name": "engine_crash",
                        "severity": "error",
                        "extra_metadata": {},
                        "record_count": 0,
                        "failed_count": 0,
                        "blocking": False,
                        "details": {"error_message": str(e)},
                    }
                ]

            all_results.extend(table_results)

            # Build canonical rows for writer
            for r in table_results:
                run_id_obj = r.get("run_id")
                run_id = getattr(run_id_obj, "run_name", str(run_id_obj))
                check_id = f"{table_cfg['table']}_{r.get('check_name', '')}_{r.get('column') or ''}"

                # Extract expected/actual from failure_details (v1.1.0 format)
                failure_details = r.get("failure_details") or {}
                expected_value = failure_details.get("expected") if failure_details else None
                actual_value = failure_details.get("actual") if failure_details else None

                row = {
                    "test_suite": "great_expectations",
                    "catalog": table_cfg["catalog"],
                    "schema": table_cfg["schema"],
                    "table": table_cfg["table"],
                    "check_id": check_id,
                    "table_fullname": f"{table_cfg['catalog']}.{table_cfg['schema']}.{table_cfg['table']}",
                    "column": r.get("column") or "_dataset",
                    "check_name": r.get("check_name", ""),
                    "status": "PASS" if r.get("passed") else "FAIL",
                    "passed": r.get("passed"),
                    "severity": r.get("severity"),
                    "blocking": r.get("blocking"),
                    "run_id": run_id,
                    "run_timestamp": r.get("run_ts"),
                    "record_count": r.get("record_count", 0),
                    "failed_count": r.get("failed_count", 0),
                    "expected_value": expected_value,
                    "actual_value": actual_value,
                    "details": r.get("details"),  # Now a human-readable string
                    "extra_metadata": r.get("extra_metadata") or {},
                    "alert_description": failure_details,
                    "rule_description": r.get("rule_description"),
                    # Debug fields (v1.1.0)
                    "failure_stats": r.get("failure_stats"),
                    "investigation_query": r.get("investigation_query"),
                    "execution": r.get("execution"),
                }

                all_rows.append(row)
                if not r.get("passed"):
                    failed_rows.append(row)

        if self.data_to_save:
            self.writer.write(all_rows=all_rows, failed_rows=failed_rows, data_to_save=self.data_to_save)

        return all_results
