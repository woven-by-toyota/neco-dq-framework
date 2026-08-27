# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import fnmatch
import re
from typing import Any


class TableDiscoveryMixin:
    """Table and schema discovery for the profiling suite."""

    def _discover_schemas(self, catalog: str, schema_pattern: str, schema_exclude_pattern: str | None) -> list[str]:
        """
        Discover schemas in a catalog using glob patterns.

        Args:
            catalog: Catalog name
            schema_pattern: Glob pattern for schema names (e.g., "category_*")
            schema_exclude_pattern: Optional exclusion pattern

        Returns:
            List of matching schema names
        """
        try:
            schemas_df = self.spark.sql(f"SHOW SCHEMAS IN {catalog}")
            # Schema name might be in 'databaseName' or 'namespace' column depending on Spark version
            schema_col = "databaseName" if "databaseName" in schemas_df.columns else "namespace"
            all_schemas = [row[schema_col] for row in schemas_df.collect()]
        except Exception:
            return []

        matched_schemas = []
        for schema_name in all_schemas:
            # Check inclusion pattern
            if not fnmatch.fnmatch(schema_name, schema_pattern):
                continue

            # Check exclusion pattern
            if schema_exclude_pattern and fnmatch.fnmatch(schema_name, schema_exclude_pattern):
                continue

            matched_schemas.append(schema_name)

        return matched_schemas

    def _discover_tables_in_schema(
        self,
        catalog: str,
        schema: str,
        table_pattern: str,
        table_exclude_pattern: str | None,
    ) -> list[dict[str, str]]:
        """
        Discover tables in a specific schema using glob patterns.

        Args:
            catalog: Catalog name
            schema: Schema name
            table_pattern: Glob pattern for table names
            table_exclude_pattern: Optional exclusion pattern

        Returns:
            List of table configs: [{catalog, schema, table}, ...]
        """
        try:
            tables_df = self.spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
            table_names = [row.tableName for row in tables_df.collect()]
        except Exception:
            return []

        matched_tables = []
        for table_name in table_names:
            # Check inclusion pattern
            if not fnmatch.fnmatch(table_name, table_pattern):
                continue

            # Check exclusion pattern
            if table_exclude_pattern and fnmatch.fnmatch(table_name, table_exclude_pattern):
                continue

            matched_tables.append(
                {
                    "catalog": catalog,
                    "schema": schema,
                    "table": table_name,
                }
            )

        return matched_tables

    def _get_layer_from_table_properties(self, catalog: str, schema: str, table: str) -> str | None:
        """Extract layer from table properties (TBLPROPERTIES).

        Looks for common property names that indicate the medallion layer:
            - layer
            - medallion_layer
            - data_layer
            - tier

        The property value must be one of: bronze, silver, gold (case-insensitive).

        Example table setup:
            ALTER TABLE catalog.schema.table SET TBLPROPERTIES ('layer' = 'silver')

        Returns:
            Layer name if found and valid, None otherwise.
        """
        layer_names = {"gold", "silver", "bronze"}
        property_names = ("layer", "medallion_layer", "data_layer", "tier")

        try:
            full_table_name = f"{catalog}.{schema}.{table}"
            props_df = self.spark.sql(f"SHOW TBLPROPERTIES {full_table_name}")

            # SHOW TBLPROPERTIES returns columns: key, value
            props = {row["key"].lower(): row["value"] for row in props_df.collect()}

            for prop_name in property_names:
                if prop_name in props:
                    prop_value = props[prop_name].lower().strip()
                    if prop_value in layer_names:
                        return prop_value

        except Exception:  # nosec B110 - intentional: optional metadata lookup
            # Table doesn't exist, no permissions, or other error - skip silently.
            # This is an optional hint for suite inference, not critical functionality.
            pass

        return None

    def _get_primary_key_columns(self, catalog: str, schema: str, table: str) -> list[str]:
        """Extract primary key columns from Unity Catalog table constraints.

        Parses the output of DESCRIBE TABLE EXTENDED to find PRIMARY KEY constraints.
        Supports both single-column and composite primary keys.

        Example constraint format:
            pk_silver_questions    PRIMARY KEY (`response_id`, `question_id`)

        Returns:
            List of column names that form the primary key, empty list if no PK defined.
        """
        try:
            full_table_name = f"{catalog}.{schema}.{table}"
            desc_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")

            # DESCRIBE TABLE EXTENDED returns columns: col_name, data_type, comment
            # Constraints appear in rows where col_name contains constraint name
            # and data_type contains "PRIMARY KEY (`col1`, `col2`)"
            for row in desc_df.collect():
                data_type = str(row["data_type"] or "")
                if "PRIMARY KEY" in data_type.upper():
                    # Extract column names from PRIMARY KEY (`col1`, `col2`, ...)
                    match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", data_type, re.IGNORECASE)
                    if match:
                        cols_str = match.group(1)
                        # Parse column names, handling backticks and whitespace
                        columns = []
                        for col in cols_str.split(","):
                            col_clean = col.strip().strip("`").strip('"').strip("'")
                            if col_clean:
                                columns.append(col_clean)
                        return columns

        except Exception:  # nosec B110 - intentional: optional constraint lookup
            # Table doesn't exist, no constraints, or other error - skip silently.
            # This is an optional hint for idempotency checks, not critical functionality.
            pass

        return []

    def _infer_suite_from_table_path(self, catalog: str, schema: str, table: str) -> str | None:
        """Infer suite type from table path components (catalog.schema.table).

        The inference is intentionally conservative to reduce false positives.
        Priority order (first match wins):
            1. Schema name - exact match or suffix (most common convention)
            2. Table name - suffix only (more specific to the table)
            3. Catalog name - suffix only (broadest scope)
            4. Table properties - TBLPROPERTIES with layer/medallion_layer/data_layer/tier

        Matching rules:
            - Exact matches: "gold", "silver", "bronze" (schema only, case-insensitive)
            - Suffix matches: names ending with "_gold", "_silver", "_bronze"
            - Table properties: value must be bronze/silver/gold

        Examples:
            - catalog="prod", schema="silver", table="customers" -> "silver" (schema exact)
            - catalog="prod", schema="dw_gold", table="facts" -> "gold" (schema suffix)
            - catalog="prod", schema="data", table="orders_bronze" -> "bronze" (table suffix)
            - catalog="bronze_raw", schema="data", table="events" -> "bronze" (catalog suffix)
            - TBLPROPERTIES('layer'='gold') -> "gold" (table property)
        """
        layer_names = ("gold", "silver", "bronze")

        # 1. Check schema (exact match)
        schema_lower = schema.lower()
        if schema_lower in layer_names:
            return schema_lower

        # 2. Check schema (suffix match)
        for layer in layer_names:
            if schema_lower.endswith(f"_{layer}"):
                return layer

        # 3. Check table name (suffix match only - avoid false positives like "goldmine")
        table_lower = table.lower()
        for layer in layer_names:
            if table_lower.endswith(f"_{layer}"):
                return layer

        # 4. Check catalog name (suffix match only)
        catalog_lower = catalog.lower()
        for layer in layer_names:
            if catalog_lower.endswith(f"_{layer}"):
                return layer

        # 5. Check table properties (fallback - requires SQL query)
        return self._get_layer_from_table_properties(catalog, schema, table)

    def _discover_tables(self) -> list[dict[str, str]]:
        """
        Discover tables in a catalog using glob patterns for both schemas and tables.

        Config (simple - single schema):
            discover_tables:
                catalog: my_catalog
                schema: my_schema
                pattern: "*_silver"      # glob pattern (default: "*")
                exclude_pattern: "_tmp*" # optional exclusion pattern

        Config (advanced - multiple schemas):
            discover_tables:
                catalog: my_catalog
                schema_pattern: "category_*"        # glob pattern for schemas
                schema_exclude_pattern: "drop_*"  # optional exclusion
                table_pattern: "*"                # glob pattern for tables (default: "*")
                table_exclude_pattern: "_tmp*"    # optional exclusion

        Returns:
            List of table configs: [{catalog, schema, table, suite?}, ...]
        """
        discovery_cfg = self._suite_cfg.get("discover_tables", {})
        if not discovery_cfg:
            return []

        catalog = discovery_cfg.get("catalog")
        if not catalog:
            return []

        # Get schema(s) to search
        schema = discovery_cfg.get("schema")
        schema_pattern = discovery_cfg.get("schema_pattern")
        schema_exclude_pattern = discovery_cfg.get("schema_exclude_pattern")

        # Get table patterns (support both old 'pattern' and new 'table_pattern')
        table_pattern = discovery_cfg.get("table_pattern") or discovery_cfg.get("pattern", "*")
        table_exclude_pattern = discovery_cfg.get("table_exclude_pattern") or discovery_cfg.get("exclude_pattern")

        # Determine which schemas to search
        schemas_to_search: list[str] = []

        if schema:
            # Single schema specified (backwards compatible)
            schemas_to_search = [schema]
        elif schema_pattern:
            # Multi-schema discovery
            schemas_to_search = self._discover_schemas(catalog, schema_pattern, schema_exclude_pattern)
        else:
            # No schema specified
            return []

        # Discover tables in each schema
        matched_tables = []
        for schema_name in schemas_to_search:
            tables = self._discover_tables_in_schema(catalog, schema_name, table_pattern, table_exclude_pattern)

            for table_cfg in tables:
                # Auto-infer suite from table path (catalog.schema.table)
                inferred_suite = self._infer_suite_from_table_path(catalog, schema_name, table_cfg["table"])
                if inferred_suite:
                    table_cfg["suite"] = inferred_suite
                matched_tables.append(table_cfg)

        return matched_tables

    def _get_tables_config(self) -> list[dict[str, Any]]:
        """
        Get tables configuration, with explicit tables taking precedence over discovery.

        Priority:
            1. Explicit tables config (existing behavior)
            2. Auto-discovered tables

        Returns:
            List of table configurations to process.
        """
        # Check for explicit tables config first (existing behavior)
        explicit_tables = self._suite_cfg.get("tables", [])
        if explicit_tables:
            return explicit_tables

        # Fall back to auto-discovery
        return self._discover_tables()
