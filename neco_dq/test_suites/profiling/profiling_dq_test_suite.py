# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from pyspark.sql import SparkSession

from neco_dq.core.io.result_writer import DQResultWriter
from neco_dq.test_suites.base_test_suite import DQTestSuite
from neco_dq.test_suites.profiling.checks import ProfilingChecksMixin
from neco_dq.test_suites.profiling.column_stats import ColumnStatsMixin
from neco_dq.test_suites.profiling.cross_table import CrossTableMixin
from neco_dq.test_suites.profiling.discovery import TableDiscoveryMixin
from neco_dq.test_suites.profiling.json_detection import JsonDetectionMixin
from neco_dq.test_suites.profiling.modes import ProfilingModesMixin
from neco_dq.test_suites.profiling.modular_output import ModularConfigOutputMixin
from neco_dq.test_suites.profiling.suggestions_ge import GeSuggestionsMixin
from neco_dq.test_suites.profiling.suggestions_generic import GenericSuggestionsMixin
from neco_dq.test_suites.profiling.suggestions_suites import SuiteSuggestionsMixin
from neco_dq.test_suites.profiling.summary import ProfilingSummaryMixin


class SparkProfilingDQTestSuite(
    TableDiscoveryMixin,
    JsonDetectionMixin,
    ColumnStatsMixin,
    ProfilingChecksMixin,
    GenericSuggestionsMixin,
    GeSuggestionsMixin,
    SuiteSuggestionsMixin,
    CrossTableMixin,
    ProfilingModesMixin,
    ModularConfigOutputMixin,
    ProfilingSummaryMixin,
    DQTestSuite,
):
    """
    Non-blocking, exploratory profiling test suite.
    """

    # ------------------------------------------------------------------
    # Suite identity (required by base class)
    # ------------------------------------------------------------------

    @property
    def _suite_name(self) -> str:
        return "profiling"

    @property
    def _suite_config_key(self) -> str:
        return "profiling"

    # ------------------------------------------------------------------
    # Hook overrides
    # ------------------------------------------------------------------

    def _build_row_from_check(self, result: dict, check: dict, run_timestamp) -> dict:
        """Profiling always has blocking=False."""
        row = super()._build_row_from_check(result, check, run_timestamp)
        row["blocking"] = False
        return row

    def _build_failed_row(self, row: dict, _result: dict) -> dict:
        """Profiling doesn't add alert_description to failed rows."""
        return row

    # ------------------------------------------------------------------

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        self.spark = spark
        self.cfg = config
        self.checks_cfg = self._suite_cfg.get("checks", {})
        self.writer = DQResultWriter(spark)

    def _enabled(self, name: str) -> bool:
        return self.checks_cfg.get(name, {}).get("enabled", True)

    def _severity(self, name: str) -> str:
        return self.checks_cfg.get(name, {}).get("severity", "warning")

    def _register(
        self,
        result: dict,
        name: str,
        passed: bool,
        severity: str,
        details: str,
        column: str | None = None,
    ):
        result["checks"].append(
            {
                "check_name": name,
                "passed": passed,
                "status": "PASS" if passed else "WARN",
                "severity": severity,
                "blocking": False,
                "details": details,
                "column": column,
            }
        )

    def _load_table(self, catalog: str, schema: str, table: str):
        """Load table using fully-qualified name."""
        return self.spark.table(f"{catalog}.{schema}.{table}")

    def run(self) -> list[dict[str, Any]]:
        """
        Run profiling suite with tri-mode support.

        Config:
            mode: "checks"  # default, "stats", or "suggest"

        Returns:
            - checks mode: list of check result rows (existing behavior)
            - stats mode: list of stats dicts
            - suggest mode: list of config suggestion dicts
        """
        mode = self._suite_cfg.get("mode", "checks")
        tables = self._get_tables_config()

        if mode == "stats":
            # Stats mode: pure statistics output
            stats_results = []
            for table_cfg in tables:
                result = self._run_stats_mode(table_cfg)
                stats_results.append(result)

            # Write stats to output table if configured
            self._write_stats_output(stats_results)

            return stats_results

        if mode == "suggest":
            # Suggest mode: generate DQ config suggestions
            suggest_results = []

            # For multiple tables, detect cross-table relationships
            # Design note: Two-pass approach is intentional:
            #   1. First pass: collect schema info from ALL tables (needed for relationship detection)
            #   2. Second pass: generate suggestions with cross-table context
            # This cannot be combined into a single pass because relationship detection
            # (FK, layer pairs, etc.) requires knowledge of all tables beforehand.
            cross_table_ctx = None
            if len(tables) > 1:
                # First pass: collect table info (columns, suite) for relationship detection
                tables_info: dict[str, dict[str, Any]] = {}
                for table_cfg in tables:
                    catalog = table_cfg["catalog"]
                    schema = table_cfg["schema"]
                    table = table_cfg["table"]
                    fq_name = f"{catalog}.{schema}.{table}"

                    # Determine suite from config or infer from schema name
                    suite = table_cfg.get("suite")
                    if not suite:
                        schema_lower = schema.lower()
                        if "gold" in schema_lower:
                            suite = "gold"
                        elif "silver" in schema_lower:
                            suite = "silver"
                        elif "bronze" in schema_lower:
                            suite = "bronze"

                    try:
                        df = self._load_table(catalog, schema, table)
                        tables_info[fq_name] = {
                            "columns": [
                                {"name": field.name, "type": str(field.dataType)} for field in df.schema.fields
                            ],
                            "suite": suite,
                            "catalog": catalog,
                            "schema": schema,
                            "table": table,
                        }
                    except Exception:
                        # Skip tables that can't be loaded
                        continue

                # Detect cross-table relationships
                if tables_info:
                    fk_relationships = self._detect_fk_relationships(tables_info)
                    layer_pairs = self._detect_layer_pairs(tables_info)
                    header_detail_pairs = self._detect_header_detail_pairs(tables_info)
                    cross_layer_relationships = self._detect_cross_layer_relationships(tables_info)

                    cross_table_ctx = {
                        "fk_relationships": fk_relationships,
                        "layer_pairs": layer_pairs,
                        "header_detail_pairs": header_detail_pairs,
                        "cross_layer_relationships": cross_layer_relationships,
                        "tables_info": tables_info,
                    }

            # Generate suggestions for each table
            for table_cfg in tables:
                result = self._run_suggest_mode(table_cfg, cross_table_ctx)
                suggest_results.append(result)

            # Write suggestions to output if configured
            self._write_suggest_output(suggest_results)

            return suggest_results

        # Default: checks mode (existing behavior)
        all_rows: list[dict[str, Any]] = []
        failed_rows: list[dict[str, Any]] = []

        for table_cfg in tables:
            res = self.validate_table(table_cfg)

            for c in res["checks"]:
                row = self._build_row_from_check(res, c, res["run_timestamp"])
                all_rows.append(row)

                if not c["passed"]:
                    failed_rows.append(self._build_failed_row(row, res))

        if self._data_to_save:
            self.writer.write(all_rows=all_rows, failed_rows=failed_rows, data_to_save=self._data_to_save)

        return all_rows
