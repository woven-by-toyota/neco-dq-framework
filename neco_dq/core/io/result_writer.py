# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import logging
import json
from datetime import datetime, UTC
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger("neco_dq")


class DQResultWriter:
    """
    Engine-aware, schema-safe DQ writer.

    Principles:
    - NO schema evolution by default
    - mergeSchema ONLY if explicitly enabled per destination
    - Writes ONLY requested columns (columns_to_save)
    - Engine-agnostic
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write(
        self,
        all_rows: list[dict[str, Any]],
        failed_rows: list[dict[str, Any]],
        data_to_save: list[dict[str, Any]],
    ):
        if not data_to_save:
            logger.info("No data_to_save configured, skipping write.")
            return

        for dest in data_to_save:
            table = dest["table"]
            write_mode = dest.get("write_mode", "append")
            is_alert = dest.get("alerts", False)
            merge_schema = dest.get("merge_schema", False)
            columns_to_save = dest.get("columns_to_save")

            rows = failed_rows if is_alert else all_rows

            if is_alert and not failed_rows:
                logger.info(f"No failed rows, skipping alerts for {table}")
                continue

            if not rows:
                logger.info(f"No rows to write for {table}")
                continue

            self._write_to_table(
                table=table,
                rows=rows,
                write_mode=write_mode,
                row_type="alerts" if is_alert else "rows",
                columns_to_save=columns_to_save,
                merge_schema=merge_schema,
            )

    def _write_to_table(
        self,
        table: str,
        rows: list[dict[str, Any]],
        write_mode: str,
        row_type: str,
        columns_to_save: list[str] | None,
        merge_schema: bool,
    ):
        table_df = self.spark.table(table)
        table_schema = table_df.schema
        table_columns = {f.name for f in table_schema.fields}

        if columns_to_save:
            allowed_columns = [c for c in columns_to_save if c in table_columns]

            dropped = set(columns_to_save) - set(allowed_columns)
            if dropped:
                logger.warning(f"{table}: columns dropped (not in schema): {sorted(dropped)}")
        else:
            allowed_columns = list(table_columns)

        if not allowed_columns:
            logger.warning(f"No writable columns resolved for {table}, skipping write.")
            return

        projected_rows = [self._normalize_and_project(r, allowed_columns) for r in rows]

        if not projected_rows:
            logger.info(f"No rows to write for {table}")
            return

        df = self.spark.createDataFrame(projected_rows, schema=table_schema)

        writer = df.write.format("delta").mode(write_mode)

        if merge_schema:
            writer = writer.option("mergeSchema", "true")

        writer.saveAsTable(table)

        logger.debug(
            f"Wrote {len(projected_rows)} {row_type} to {table} (mode={write_mode}, mergeSchema={merge_schema})"
        )

    def _normalize_and_project(
        self,
        row: dict[str, Any],
        allowed_columns: list[str],
    ) -> dict[str, Any]:
        out = {}

        for col in allowed_columns:
            v = row.get(col)

            if isinstance(v, str):
                try:
                    parsed = json.loads(v)
                    out[col] = parsed if isinstance(parsed, (dict, list)) else v
                except Exception:
                    out[col] = v

            elif isinstance(v, (dict, list)):
                out[col] = v

            elif isinstance(v, datetime):
                out[col] = v if v.tzinfo else v.replace(tzinfo=UTC)

            else:
                out[col] = v if v is not None else None

        return out
