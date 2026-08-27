# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import types as t

from neco_dq.test_suites.profiling.constants import _json_serializer


class ProfilingModesMixin:
    """Stats and suggest mode orchestration and output writers."""

    def _run_stats_mode(self, table_cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Run in pure stats mode - outputs statistics without pass/fail checks.

        Returns:
            dict with table metadata, schema info, and column statistics.
        """
        catalog = table_cfg["catalog"]
        schema = table_cfg["schema"]
        table = table_cfg["table"]
        fq_table = f"{catalog}.{schema}.{table}"
        run_timestamp = datetime.now(UTC)

        result: dict[str, Any] = {
            "test_suite": self._suite_name,
            "mode": "stats",
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "run_timestamp": run_timestamp,
        }

        try:
            df = self._load_table(catalog, schema, table)
        except Exception as exc:
            result["error"] = str(exc)
            return result

        # Get row count
        total_rows = df.count()
        result["row_count"] = total_rows

        # Apply sampling if needed
        sampled_df = self._get_sample(df, total_rows)
        sampled = sampled_df is not df
        if sampled:
            result["sampled"] = True
            result["sample_row_count"] = sampled_df.count()

        # Extract schema information
        result["schema_info"] = self._extract_schema_info(df, fq_table)

        # Compute column statistics (on sampled data if applicable)
        analysis_df = sampled_df if sampled else df
        analysis_row_count = result.get("sample_row_count", total_rows)
        result["column_stats"] = self._compute_column_stats(analysis_df, analysis_row_count)

        return result

    def _write_stats_output(self, stats_results: list[dict[str, Any]]) -> None:
        """
        Write stats results to configured output table.

        Config:
            stats_output:
                table: catalog.schema.profiling_stats
                write_mode: overwrite  # or append
        """
        stats_output_cfg = self._suite_cfg.get("stats_output", {})
        if not stats_output_cfg:
            return

        output_table = stats_output_cfg.get("table")
        if not output_table:
            return

        write_mode = stats_output_cfg.get("write_mode", "overwrite")

        # Flatten stats for DataFrame creation
        rows = []
        for result in stats_results:
            if "error" in result:
                continue

            base_info = {
                "catalog": result["catalog"],
                "schema": result["schema"],
                "table": result["table"],
                "run_timestamp": result["run_timestamp"],
                "row_count": result.get("row_count"),
                "sampled": result.get("sampled", False),
            }

            # Add schema info as JSON string
            base_info["schema_info_json"] = json.dumps(result.get("schema_info", {}), default=_json_serializer)

            # Create one row per column with its stats
            column_stats = result.get("column_stats", {})
            for col_name, col_stats in column_stats.items():
                row = {**base_info, "column_name": col_name}
                row["column_stats_json"] = json.dumps(col_stats, default=_json_serializer)
                rows.append(row)

        if rows:
            stats_df = self.spark.createDataFrame(rows)
            stats_df.write.mode(write_mode).saveAsTable(output_table)

    def _generate_suite_suggestions(
        self,
        table_cfg: dict[str, Any],
        column_stats: dict[str, dict[str, Any]],
        row_count: int,
        target_suite: str | None = None,
        cross_table_ctx: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Generate DQ suite configuration suggestions based on profiling stats.

        Args:
            table_cfg: Table configuration (catalog, schema, table)
            column_stats: Column statistics from profiling
            row_count: Total row count
            target_suite: Target suite to suggest for (bronze, silver, gold).
                         If None, suggests for all suites.
                         GE is suggested only for silver/gold tables.
            cross_table_ctx: Cross-table context with detected relationships:
                - fk_relationships: list of FK relationships
                - layer_pairs: list of bronze/silver/gold pairs
                - header_detail_pairs: list of header/detail pairs
                - tables_info: dict of table info

        Returns:
            dict with suggestions for the appropriate suites
        """
        catalog, schema, table = table_cfg["catalog"], table_cfg["schema"], table_cfg["table"]
        table_base = {"catalog": catalog, "schema": schema, "table": table}
        table_fq = f"{catalog}.{schema}.{table}"

        # Get primary key from Unity Catalog constraints (if defined)
        pk_columns = self._get_primary_key_columns(catalog, schema, table)

        # Gather base suggestions from profiling stats
        not_null_cols = self._suggest_not_null_columns(column_stats)
        unique_cols_from_stats = self._suggest_unique_columns(column_stats, row_count)

        # Merge PK columns with detected unique columns (PK takes priority)
        # PK columns are the definitive unique columns if defined
        if pk_columns:
            # Use PK as primary source, add other detected unique cols that aren't part of PK
            unique_cols = pk_columns + [c for c in unique_cols_from_stats if c not in pk_columns]
        else:
            unique_cols = unique_cols_from_stats
        allowed_values = self._suggest_allowed_values(column_stats, row_count)
        range_checks = self._suggest_range_checks(column_stats)
        json_cols = self._suggest_json_schema_columns(column_stats)

        # Derived data
        allowed_values_by_col = {av["column"]: av["values"] for av in allowed_values}
        range_by_col = {rc["column"]: rc for rc in range_checks}
        date_cols = [col for col, stats in column_stats.items() if stats.get("type") == "date"]

        suggestions: dict[str, Any] = {
            "table": table_fq,
            "row_count": row_count,
            "column_count": len(column_stats),
        }

        # Determine which suites to suggest based on target_suite
        # GE is only suggested for silver/gold (not bronze)
        suggest_ge = target_suite in ("silver", "gold", None)
        suggest_bronze = target_suite in ("bronze", None)
        suggest_silver = target_suite in ("silver", None)
        suggest_gold = target_suite in ("gold", None)

        # Build length_by_col for string columns (mirrors range_by_col pattern)
        length_by_col = {
            col: {"min_length": stats["min_length"], "max_length": stats["max_length"]}
            for col, stats in column_stats.items()
            if "min_length" in stats and "max_length" in stats
        }

        # Build date_format_by_col for string columns holding dates (mirrors length_by_col)
        date_format_by_col = {
            col: stats["date_format"] for col, stats in column_stats.items() if stats.get("date_format")
        }

        # Build suggestions for applicable suites
        if suggest_ge:
            ge_checks = self._build_ge_suggestions(
                column_stats,
                not_null_cols,
                unique_cols,
                allowed_values_by_col,
                range_by_col,
                row_count,
                length_data=length_by_col,
                pk_columns=pk_columns,
                date_format_by_col=date_format_by_col,
            )
            if ge_checks:
                suggestions["great_expectations"] = {**table_base, "checks": ge_checks}

        if suggest_bronze:
            bronze_checks = self._build_bronze_suggestions(column_stats, not_null_cols, date_cols)
            if bronze_checks:
                suggestions["bronze"] = {**table_base, "checks": bronze_checks}

        if suggest_silver:
            silver_checks = self._build_silver_suggestions(
                unique_cols, date_cols, column_stats, row_count, json_cols, pk_columns
            )
            # Add cross-table checks if context available
            if cross_table_ctx:
                cross_silver = self._build_cross_table_silver_suggestions(
                    table_fq,
                    cross_table_ctx.get("fk_relationships", []),
                    cross_table_ctx.get("layer_pairs", []),
                    cross_table_ctx.get("header_detail_pairs", []),
                    cross_table_ctx.get("tables_info", {}),
                    cross_table_ctx.get("cross_layer_relationships", []),
                )
                silver_checks.update(cross_silver)
            if silver_checks:
                suggestions["silver"] = {**table_base, "checks": silver_checks}

        if suggest_gold:
            gold_checks = self._build_gold_suggestions(
                unique_cols, date_cols, row_count, column_stats, json_cols, pk_columns=pk_columns
            )
            # Add cross-table checks if context available
            if cross_table_ctx:
                cross_gold = self._build_cross_table_gold_suggestions(
                    table_fq,
                    cross_table_ctx.get("fk_relationships", []),
                    cross_table_ctx.get("layer_pairs", []),
                    cross_table_ctx.get("cross_layer_relationships", []),
                )
                gold_checks.update(cross_gold)
            if gold_checks:
                suggestions["gold"] = {**table_base, "checks": gold_checks}

        return suggestions

    def _run_suggest_mode(
        self,
        table_cfg: dict[str, Any],
        cross_table_ctx: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Run in suggest mode - generates DQ configuration suggestions.

        Config options:
            suite: Target suite (bronze, silver, gold) - per table or global
                   GE is suggested only for silver/gold tables.

        Args:
            table_cfg: Table configuration (catalog, schema, table)
            cross_table_ctx: Optional cross-table context with detected relationships

        Returns:
            dict with table info and suggested configurations per suite.
        """
        catalog = table_cfg["catalog"]
        schema = table_cfg["schema"]
        table = table_cfg["table"]
        run_timestamp = datetime.now(UTC)

        # Get target suite from table config or global config
        target_suite = table_cfg.get("suite") or self._suite_cfg.get("suite")

        result: dict[str, Any] = {
            "test_suite": self._suite_name,
            "mode": "suggest",
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "run_timestamp": run_timestamp,
        }

        try:
            df = self._load_table(catalog, schema, table)
        except Exception as exc:
            result["error"] = str(exc)
            return result

        # Get row count
        row_count = df.count()
        result["row_count"] = row_count

        # Apply sampling if needed
        sampled_df = self._get_sample(df, row_count)
        sampled = sampled_df is not df
        if sampled:
            result["sampled"] = True

        # Compute column statistics
        analysis_df = sampled_df if sampled else df
        analysis_row_count = analysis_df.count() if sampled else row_count
        column_stats = self._compute_column_stats(analysis_df, analysis_row_count)

        # Generate suggestions for target suite (with cross-table context if available)
        result["suggestions"] = self._generate_suite_suggestions(
            table_cfg, column_stats, analysis_row_count, target_suite, cross_table_ctx
        )

        # Also include raw column stats for reference
        result["column_stats"] = column_stats

        return result

    def _format_yaml_suggestions(self, suggestions: dict[str, Any], *, blank_between_checks: bool = False) -> str:
        """
        Format suggestions as YAML string for human readability.

        Args:
            suggestions: the structure to serialize.
            blank_between_checks: when True, insert a blank line before each check entry
                inside a `checks:` block so the generated config is easier to scan.
        """

        # Clean up internal comments for YAML output
        def clean_for_yaml(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {
                    k: clean_for_yaml(v)
                    for k, v in obj.items()
                    if not k.startswith("_")  # Remove internal fields
                }
            if isinstance(obj, list):
                return [clean_for_yaml(item) for item in obj]
            return obj

        cleaned = clean_for_yaml(suggestions)
        text = yaml.dump(cleaned, default_flow_style=False, sort_keys=False, allow_unicode=True)
        if blank_between_checks:
            text = self._space_between_checks(text)
        return text

    @staticmethod
    def _space_between_checks(text: str) -> str:
        """Insert a blank line before each check key inside a `checks:` block.

        Keeps the YAML valid (blank lines in a mapping are ignored on parse) while making
        the per-check sections easier to read. List items (`- ...`) inside a check are left
        untouched so a single check's body stays together.
        """
        lines = text.split("\n")
        out: list[str] = []
        checks_indent: int | None = None
        seen_first = False
        for line in lines:
            stripped = line.lstrip(" ")
            indent = len(line) - len(stripped)
            if checks_indent is not None:
                # Still inside the checks block while indented deeper than `checks:`.
                if stripped and indent <= checks_indent:
                    checks_indent = None  # block ended
                elif indent == checks_indent + 2 and not stripped.startswith("-"):
                    # A new check key at the first level under `checks:`.
                    if seen_first:
                        out.append("")
                    seen_first = True
            if stripped.rstrip() == "checks:" or stripped.startswith("checks:"):
                checks_indent = indent
                seen_first = False
            out.append(line)
        return "\n".join(out)

    def _write_suggest_output(self, suggest_results: list[dict[str, Any]]) -> None:
        """
        Write suggestions to configured output (file or table).

        Config:
            suggest_output:
                path: "/path/to/suggestions/"  # Write YAML files
                # OR
                table: catalog.schema.config_suggestions  # Write to table
        """
        suggest_output_cfg = self._suite_cfg.get("suggest_output", {})
        if not suggest_output_cfg:
            return

        output_path = suggest_output_cfg.get("path")
        output_table = suggest_output_cfg.get("table")

        if output_path:
            self._write_suggest_to_files(suggest_results, output_path)

        if output_table:
            self._write_suggest_to_table(suggest_results, output_table)

        modular_path = suggest_output_cfg.get("modular_config_path")
        if modular_path:
            options = {
                "include_profiles": suggest_output_cfg.get("include_profiles", False),
                "include_policy": suggest_output_cfg.get("include_policy", True),
                "parameterize_catalog": suggest_output_cfg.get("parameterize_catalog", False),
                "overwrite": suggest_output_cfg.get("overwrite", False),
                "dry_run": suggest_output_cfg.get("dry_run", False),
            }
            self._write_suggest_to_modular_config(suggest_results, modular_path, options)

        summary_path = suggest_output_cfg.get("summary_report_path")
        if summary_path:
            self._write_suggest_summary(suggest_results, summary_path)

    def _write_text_file(self, file_path: str, content: str) -> None:
        """Write text to a path, using dbutils on Databricks or the local filesystem otherwise."""
        try:
            dbutils = self.spark._jvm.com.databricks.service.DBUtils(self.spark._jsc)
            dbutils.fs().put(file_path, content, True)
        except Exception:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)

    def _write_suggest_to_files(self, suggest_results: list[dict[str, Any]], output_path: str) -> None:
        """Write suggestions as YAML files - one file per suite per table.

        Environment support:
        - Databricks: Uses dbutils.fs.put() for DBFS/Unity Catalog volumes
        - Local Spark: Falls back to driver-local filesystem (Path.write_text)

        Note: In cluster mode, the fallback writes to the driver node's local filesystem,
        which may not be shared with executors. For cluster deployments, prefer using
        output_table or ensure output_path points to a shared filesystem.
        """
        output_dir = Path(output_path)

        # All supported suites
        suites = ["great_expectations", "bronze", "silver", "gold"]

        for result in suggest_results:
            if "error" in result:
                continue

            suggestions = result.get("suggestions", {})
            table_name = suggestions.get("table", "unknown")
            safe_name = table_name.replace(".", "_")

            # Write per-suite config files
            for suite in suites:
                if suite not in suggestions:
                    continue

                suite_cfg = suggestions[suite]
                yaml_content = self._format_yaml_suggestions(
                    {suite: {"tables": [suite_cfg]}}, blank_between_checks=True
                )

                file_path = output_dir / f"{safe_name}_{suite}.yaml"

                self._write_text_file(str(file_path), yaml_content)

    def _write_suggest_to_table(self, suggest_results: list[dict[str, Any]], output_table: str) -> None:
        """
        Write suggestions to a Delta table with normalized structure.

        Schema: one row per (table, test_suite) combination:
            catalog, schema, table, run_timestamp, row_count, column_count,
            test_suite, config_yaml, column_stats_json
        """
        rows = []
        suites = ["great_expectations", "bronze", "silver", "gold"]

        for result in suggest_results:
            if "error" in result:
                continue

            suggestions = result.get("suggestions", {})
            base_row = {
                "catalog": str(result["catalog"]),
                "schema": str(result["schema"]),
                "table": str(result["table"]),
                "run_timestamp": result["run_timestamp"],
                "row_count": int(result.get("row_count") or 0),
                "column_count": int(suggestions.get("column_count") or 0),
                "column_stats_json": json.dumps(result.get("column_stats", {}), default=_json_serializer),
            }

            # One row per suite
            for suite in suites:
                suite_cfg = suggestions.get(suite, {})
                if not suite_cfg:
                    continue

                # Format config as YAML for readability
                config_yaml = self._format_yaml_suggestions(suite_cfg)

                row = {
                    **base_row,
                    "test_suite": suite,
                    "config_yaml": config_yaml,
                }
                rows.append(row)

        if rows:
            # Define explicit schema to avoid type inference issues
            schema = t.StructType(
                [
                    t.StructField("catalog", t.StringType(), True),
                    t.StructField("schema", t.StringType(), True),
                    t.StructField("table", t.StringType(), True),
                    t.StructField("run_timestamp", t.TimestampType(), True),
                    t.StructField("row_count", t.LongType(), True),
                    t.StructField("column_count", t.IntegerType(), True),
                    t.StructField("column_stats_json", t.StringType(), True),
                    t.StructField("test_suite", t.StringType(), True),
                    t.StructField("config_yaml", t.StringType(), True),
                ]
            )
            suggest_df = self.spark.createDataFrame(rows, schema=schema)
            suggest_df.write.mode("overwrite").saveAsTable(output_table)
