# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from neco_dq.test_suites.profiling.constants import (
    _FQ_CATALOG_IDX,
    _FQ_SCHEMA_IDX,
    _FQ_TABLE_IDX,
)


class CrossTableMixin:
    """Cross-table relationship detection and suggestion methods."""

    def _detect_fk_relationships(
        self,
        tables_info: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Detect foreign key relationships between tables.

        Looks for patterns like:
        - Column named `<table>_id` in one table matching `id` in another table
        - Column named `<table>_id` matching `<table>_id` in another table

        Args:
            tables_info: Dict mapping table FQ name to {columns: [...], ...}

        Returns:
            List of FK relationships: [{source_table, source_column, target_table, target_column}, ...]
        """
        relationships = []

        # Build lookup: table_name -> {fq_name, columns}
        # Note: Assumes Unity Catalog format (catalog.schema.table) where table names
        # don't contain periods. This is standard for Databricks/Unity Catalog.
        table_lookup: dict[str, dict] = {}
        for fq_name, info in tables_info.items():
            parts = fq_name.split(".")
            simple_name = parts[-1] if parts else fq_name
            table_lookup[simple_name] = {"fq_name": fq_name, "columns": info.get("columns", [])}

        # For each table, look for FK columns
        for fq_name, info in tables_info.items():
            columns = info.get("columns", [])

            for col in columns:
                col_name = col if isinstance(col, str) else col.get("name", "")
                col_lower = col_name.lower()

                # Pattern: <other_table>_id -> look for table named <other_table>
                if col_lower.endswith("_id") and col_lower != "id":
                    potential_table = col_lower[:-3]  # Remove _id suffix

                    # Check if a table with that name exists
                    for target_name, target_info in table_lookup.items():
                        target_lower = target_name.lower()

                        # Match: orders_id -> orders table
                        if target_lower == potential_table:
                            target_cols = target_info["columns"]
                            target_col_names = [
                                (c if isinstance(c, str) else c.get("name", "")).lower() for c in target_cols
                            ]

                            # Look for 'id' or matching column in target
                            if "id" in target_col_names:
                                relationships.append(
                                    {
                                        "source_table": fq_name,
                                        "source_column": col_name,
                                        "target_table": target_info["fq_name"],
                                        "target_column": "id",
                                    }
                                )
                            elif col_lower in target_col_names:
                                relationships.append(
                                    {
                                        "source_table": fq_name,
                                        "source_column": col_name,
                                        "target_table": target_info["fq_name"],
                                        "target_column": col_name,
                                    }
                                )

        return relationships

    def _detect_layer_pairs(
        self,
        tables_info: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Detect Bronze/Silver/Gold layer pairs (tables with same base name but different layer suffix).

        Patterns detected:
        - orders_bronze -> orders_silver
        - orders_silver -> orders_gold
        - bronze_orders -> silver_orders

        Args:
            tables_info: Dict mapping table FQ name to info

        Returns:
            List of layer pairs: [{source_table, target_table, source_layer, target_layer}, ...]
        """
        layer_pairs = []
        layer_keywords = ["bronze", "silver", "gold"]

        # Extract table names and their layers
        table_layers: dict[str, dict] = {}  # base_name -> {layer: fq_name}

        for fq_name in tables_info:
            parts = fq_name.split(".")
            table_name = parts[-1] if parts else fq_name
            table_lower = table_name.lower()

            # Try to extract layer and base name
            for layer in layer_keywords:
                base_name = None
                if table_lower.endswith(f"_{layer}"):
                    base_name = table_lower[: -len(layer) - 1]
                elif table_lower.startswith(f"{layer}_"):
                    base_name = table_lower[len(layer) + 1 :]

                if base_name:
                    if base_name not in table_layers:
                        table_layers[base_name] = {}
                    table_layers[base_name][layer] = fq_name
                    break

        # Find pairs
        layer_order = ["bronze", "silver", "gold"]
        for base_name, layers in table_layers.items():
            for i, source_layer in enumerate(layer_order[:-1]):
                target_layer = layer_order[i + 1]
                if source_layer in layers and target_layer in layers:
                    layer_pairs.append(
                        {
                            "source_table": layers[source_layer],
                            "target_table": layers[target_layer],
                            "source_layer": source_layer,
                            "target_layer": target_layer,
                            "base_name": base_name,
                        }
                    )

        return layer_pairs

    def _detect_header_detail_pairs(
        self,
        tables_info: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Detect header/detail table pairs.

        Patterns detected:
        - orders / order_items (or orders_items, order_details, orders_detail)
        - invoice / invoice_lines

        Args:
            tables_info: Dict mapping table FQ name to info

        Returns:
            List of header/detail pairs: [{header_table, detail_table, join_key}, ...]
        """
        pairs = []
        detail_suffixes = ["_items", "_lines", "_details", "_detail", "_entries"]

        # Build simple name lookup
        simple_to_fq: dict[str, str] = {}
        for fq_name in tables_info:
            parts = fq_name.split(".")
            simple_name = parts[-1].lower() if parts else fq_name.lower()
            simple_to_fq[simple_name] = fq_name

        # Look for detail tables and find their headers
        for simple_name, fq_name in simple_to_fq.items():
            for suffix in detail_suffixes:
                if simple_name.endswith(suffix):
                    # Extract potential header name
                    base = simple_name[: -len(suffix)]
                    # Try singular form (order_items -> order, orders)
                    potential_headers = [base, base + "s", base.rstrip("s")]

                    for header_name in potential_headers:
                        if header_name in simple_to_fq and header_name != simple_name:
                            header_fq = simple_to_fq[header_name]
                            # Infer join key
                            join_key = f"{header_name}_id"

                            pairs.append(
                                {
                                    "header_table": header_fq,
                                    "detail_table": fq_name,
                                    "join_key": join_key,
                                }
                            )
                            break

        return pairs

    def _find_common_columns(
        self,
        table1_columns: list[str],
        table2_columns: list[str],
    ) -> list[str]:
        """Find columns that exist in both tables (potential join keys)."""
        set1 = {c.lower() for c in table1_columns}
        set2 = {c.lower() for c in table2_columns}
        return list(set1 & set2)

    def _detect_cross_layer_relationships(
        self,
        tables_info: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Detect relationships between tables in different layers (gold/silver/bronze)
        based on common columns.

        This is useful when tables don't follow naming conventions like *_bronze/*_silver/*_gold
        but are in different schemas (e.g., sales_silver.orders, sales_gold.summary).

        Looks for:
        - Common columns ending with _id (likely foreign keys)
        - Common columns that appear in both gold and silver tables

        Args:
            tables_info: Dict mapping table FQ name to {columns, suite, ...}

        Returns:
            List of cross-layer relationships:
            [{gold_table, silver_table, common_columns, fk_columns}, ...]
        """
        relationships = []

        # Group tables by suite
        gold_tables = {fq: info for fq, info in tables_info.items() if info.get("suite") == "gold"}
        silver_tables = {fq: info for fq, info in tables_info.items() if info.get("suite") == "silver"}
        bronze_tables = {fq: info for fq, info in tables_info.items() if info.get("suite") == "bronze"}

        # Find gold -> silver relationships
        for gold_fq, gold_info in gold_tables.items():
            gold_cols = [c["name"] for c in gold_info.get("columns", [])]
            gold_cols_lower = {c.lower(): c for c in gold_cols}

            for silver_fq, silver_info in silver_tables.items():
                silver_cols = [c["name"] for c in silver_info.get("columns", [])]
                silver_cols_lower = {c.lower(): c for c in silver_cols}

                # Find common columns
                common_lower = set(gold_cols_lower.keys()) & set(silver_cols_lower.keys())
                if not common_lower:
                    continue

                # Identify FK columns (ending with _id)
                fk_columns = [col for col in common_lower if col.endswith("_id")]

                # Use original case from gold table
                common_columns = [gold_cols_lower[c] for c in common_lower]
                fk_columns_original = [gold_cols_lower[c] for c in fk_columns]

                relationships.append(
                    {
                        "gold_table": gold_fq,
                        "gold_info": gold_info,
                        "silver_table": silver_fq,
                        "silver_info": silver_info,
                        "common_columns": common_columns,
                        "fk_columns": fk_columns_original,
                    }
                )

        # Find silver -> bronze relationships
        for silver_fq, silver_info in silver_tables.items():
            silver_cols = [c["name"] for c in silver_info.get("columns", [])]
            silver_cols_lower = {c.lower(): c for c in silver_cols}

            for bronze_fq, bronze_info in bronze_tables.items():
                bronze_cols = [c["name"] for c in bronze_info.get("columns", [])]
                bronze_cols_lower = {c.lower(): c for c in bronze_cols}

                # Find common columns
                common_lower = set(silver_cols_lower.keys()) & set(bronze_cols_lower.keys())
                if not common_lower:
                    continue

                # Identify FK columns
                fk_columns = [col for col in common_lower if col.endswith("_id")]
                common_columns = [silver_cols_lower[c] for c in common_lower]
                fk_columns_original = [silver_cols_lower[c] for c in fk_columns]

                relationships.append(
                    {
                        "silver_table": silver_fq,
                        "silver_info": silver_info,
                        "bronze_table": bronze_fq,
                        "bronze_info": bronze_info,
                        "common_columns": common_columns,
                        "fk_columns": fk_columns_original,
                    }
                )

        return relationships

    def _infer_layer_from_schema(self, schema: str) -> str | None:
        """
        Infer the DQ layer (bronze/silver/gold) from a schema name.

        Performs exact match first, then substring match.
        Gold is checked before silver to avoid false substring matches
        (e.g. a schema named "silverware_gold" returns "gold").

        Args:
            schema: Schema name to inspect.

        Returns:
            "bronze", "silver", "gold", or None if no layer keyword found.
        """
        schema_lower = schema.lower()
        if schema_lower in ("bronze", "silver", "gold"):
            return schema_lower
        for layer in ("gold", "silver", "bronze"):
            if layer in schema_lower:
                return layer
        return None

    def _suggest_cross_table_equality_checks(
        self,
        table_fq: str,
        tables_info: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Suggest cross_table_equality checks for a silver table.

        For each other silver table in the same schema that shares at least one
        ``*_id`` column, emit a disabled suggestion with ``_confidence: "low"``.

        Args:
            table_fq: Fully-qualified name of the source table.
            tables_info: Standard tables_info mapping.

        Returns:
            List of suggestion dicts (may be empty).
        """
        suggestions: list[dict[str, Any]] = []

        current_info = tables_info.get(table_fq, {})
        current_schema = current_info.get("schema", "")
        current_cols = {
            (c if isinstance(c, str) else c.get("name", "")).lower() for c in current_info.get("columns", [])
        }

        for other_fq, other_info in tables_info.items():
            if other_fq == table_fq:
                continue
            if other_info.get("suite") != "silver":
                continue
            if other_info.get("schema", "") != current_schema:
                continue

            other_cols = {
                (c if isinstance(c, str) else c.get("name", "")).lower() for c in other_info.get("columns", [])
            }
            common = current_cols & other_cols
            common_ids = [c for c in sorted(common) if c.endswith("_id")]
            if not common_ids:
                continue

            other_table_name = other_info.get("table", other_fq.split(".")[-1])
            suggestions.append(
                {
                    "enabled": False,
                    "target_table": other_fq,
                    "join_columns": common_ids[:2],
                    "compare_columns": ["<column_to_compare>"],
                    "severity": "error",
                    "_confidence": "low",
                    "_reason": f"Common columns with {other_table_name}: {common_ids}",
                }
            )

        return suggestions

    def _build_cross_table_silver_suggestions(
        self,
        table_fq: str,
        fk_relationships: list[dict],
        layer_pairs: list[dict],
        header_detail_pairs: list[dict],
        tables_info: dict[str, dict[str, Any]],
        cross_layer_relationships: list[dict] | None = None,
    ) -> dict[str, Any]:
        """
        Build Silver cross-table check suggestions for a specific table.

        Generates:
        - referential_integrity: for FK relationships
        - flow_completeness: for bronze->silver pairs
        - totals_consistency: for header/detail pairs
        """
        checks: dict[str, Any] = {}

        # Parse table FQ name
        parts = table_fq.split(".")
        catalog = parts[_FQ_CATALOG_IDX] if len(parts) > _FQ_CATALOG_IDX else ""
        schema = parts[_FQ_SCHEMA_IDX] if len(parts) > _FQ_SCHEMA_IDX else ""
        table = parts[_FQ_TABLE_IDX] if len(parts) > _FQ_TABLE_IDX else ""

        # Referential integrity: where this table has FK to another
        ri_checks = []
        for rel in fk_relationships:
            if rel["source_table"] == table_fq:
                target_parts = rel["target_table"].split(".")
                ri_checks.append(
                    {
                        "enabled": True,
                        "severity": "error",
                        "blocking": True,
                        "schema": schema,
                        "table": table,
                        "source_column": rel["source_column"],
                        "target_schema": (
                            target_parts[_FQ_SCHEMA_IDX] if len(target_parts) > _FQ_SCHEMA_IDX else schema
                        ),
                        "target_table": (
                            target_parts[_FQ_TABLE_IDX] if len(target_parts) > _FQ_TABLE_IDX else target_parts[-1]
                        ),
                        "target_column": rel["target_column"],
                    }
                )
        if ri_checks:
            checks["referential_integrity"] = ri_checks

        # Flow completeness: where this table is the silver target of a bronze source
        # 1. Try from layer_pairs (naming convention based)
        for pair in layer_pairs:
            if pair["target_table"] == table_fq and pair["source_layer"] == "bronze":
                source_parts = pair["source_table"].split(".")
                # Try to find common columns for join key
                source_cols = [
                    c if isinstance(c, str) else c.get("name", "")
                    for c in tables_info.get(pair["source_table"], {}).get("columns", [])
                ]
                target_cols = [
                    c if isinstance(c, str) else c.get("name", "")
                    for c in tables_info.get(table_fq, {}).get("columns", [])
                ]
                common = self._find_common_columns(source_cols, target_cols)
                # Prefer id columns
                join_key = next((c for c in common if "id" in c.lower()), common[0] if common else "id")

                checks["flow_completeness"] = {
                    "enabled": True,
                    "severity": "error",
                    "blocking": True,
                    "source_catalog": (
                        source_parts[_FQ_CATALOG_IDX] if len(source_parts) > _FQ_CATALOG_IDX else catalog
                    ),
                    "source_schema": source_parts[_FQ_SCHEMA_IDX] if len(source_parts) > _FQ_SCHEMA_IDX else schema,
                    "source_table": (
                        source_parts[_FQ_TABLE_IDX] if len(source_parts) > _FQ_TABLE_IDX else source_parts[-1]
                    ),
                    "join_key": join_key,
                    "tolerance_pct": 0.0,
                }
                break

        # 2. If no layer_pairs found, try from cross_layer_relationships (common columns based)
        if "flow_completeness" not in checks and cross_layer_relationships:
            for rel in cross_layer_relationships:
                if rel.get("silver_table") == table_fq and rel.get("bronze_table"):
                    bronze_info = rel.get("bronze_info", {})
                    fk_columns = rel.get("fk_columns", [])
                    common_columns = rel.get("common_columns", [])

                    # Prefer FK columns for join key, otherwise use first common column
                    join_key = fk_columns[0] if fk_columns else (common_columns[0] if common_columns else "id")

                    checks["flow_completeness"] = {
                        "enabled": True,
                        "severity": "error",
                        "blocking": True,
                        "source_catalog": bronze_info.get("catalog", catalog),
                        "source_schema": bronze_info.get("schema", ""),
                        "source_table": bronze_info.get("table", ""),
                        "join_key": join_key,
                        "tolerance_pct": 0.0,
                    }
                    break

        # Cross-table equality: same-schema silver tables sharing *_id columns
        cte = self._suggest_cross_table_equality_checks(table_fq, tables_info)
        if cte:
            checks["cross_table_equality"] = cte

        # Totals consistency: where this table is a header with detail table
        for pair in header_detail_pairs:
            if pair["header_table"] == table_fq:
                detail_parts = pair["detail_table"].split(".")
                checks["totals_consistency"] = {
                    "enabled": False,  # Disabled by default - needs manual config
                    "severity": "error",
                    "detail_schema": (detail_parts[_FQ_SCHEMA_IDX] if len(detail_parts) > _FQ_SCHEMA_IDX else schema),
                    "detail_table": (
                        detail_parts[_FQ_TABLE_IDX] if len(detail_parts) > _FQ_TABLE_IDX else detail_parts[-1]
                    ),
                    "join_key": pair["join_key"],
                    "detail_amount_column": "<amount_column>",  # Placeholder
                    "header_amount_column": "<total_column>",  # Placeholder
                    "tolerance": 0.01,
                }
                break

        return checks

    def _build_ri_from_fk_relationships(self, table_fq: str, fk_relationships: list[dict], catalog: str) -> list[dict]:
        """Build referential integrity checks from FK detection (table_id -> table)."""
        ri_checks = []
        for rel in fk_relationships:
            if rel["source_table"] == table_fq:
                target_lower = rel["target_table"].lower()
                if "silver" in target_lower:
                    target_parts = rel["target_table"].split(".")
                    ri_checks.append(
                        {
                            "enabled": True,
                            "severity": "critical",
                            "blocking": True,
                            "name": f"fk_{rel['source_column']}_exists",
                            "gold_column": rel["source_column"],
                            "silver": {
                                "catalog": (
                                    target_parts[_FQ_CATALOG_IDX] if len(target_parts) > _FQ_CATALOG_IDX else catalog
                                ),
                                "schema": (target_parts[_FQ_SCHEMA_IDX] if len(target_parts) > _FQ_SCHEMA_IDX else ""),
                                "table": (
                                    target_parts[_FQ_TABLE_IDX]
                                    if len(target_parts) > _FQ_TABLE_IDX
                                    else target_parts[-1]
                                ),
                                "column": rel["target_column"],
                            },
                        }
                    )
        return ri_checks

    def _build_ri_from_cross_layer(
        self, table_fq: str, cross_layer_relationships: list[dict], existing_ri: list[dict]
    ) -> list[dict]:
        """Build referential integrity checks from cross-layer common columns."""
        ri_checks = []
        for rel in cross_layer_relationships:
            if rel.get("gold_table") == table_fq and rel.get("fk_columns"):
                silver_info = rel.get("silver_info", {})
                for fk_col in rel["fk_columns"]:
                    # Avoid duplicates with existing RI checks
                    if any(r.get("gold_column") == fk_col for r in existing_ri):
                        continue
                    ri_checks.append(
                        {
                            "enabled": True,
                            "severity": "critical",
                            "blocking": True,
                            "name": f"fk_{fk_col}_exists",
                            "gold_column": fk_col,
                            "silver": {
                                "catalog": silver_info.get("catalog", ""),
                                "schema": silver_info.get("schema", ""),
                                "table": silver_info.get("table", ""),
                                "column": fk_col,
                            },
                        }
                    )
        return ri_checks

    def _build_reconciliation_from_layer_pairs(self, table_fq: str, layer_pairs: list[dict]) -> dict[str, Any]:
        """Build reconciliation and coverage checks from layer pairs."""
        checks: dict[str, Any] = {}
        for pair in layer_pairs:
            if pair["target_table"] == table_fq and pair["source_layer"] == "silver":
                checks["reconciliation_expr"] = [
                    {
                        "enabled": True,
                        "severity": "warning",
                        "blocking": False,
                        "name": f"{pair['base_name']}_row_count_reconciliation",
                        "source_silver": pair["source_table"],
                        "gold": "COUNT(*)",
                        "silver": "COUNT(*)",
                        "tolerance": 5.0,
                    }
                ]
                checks["coverage"] = [
                    {
                        "enabled": True,
                        "severity": "warning",
                        "blocking": False,
                        "silver_table": pair["source_table"],
                        "gold_expr": "COUNT(*)",
                        "silver_expr": "COUNT(*)",
                        "tolerance_pct": 5.0,
                    }
                ]
                break
        return checks

    def _build_reconciliation_from_cross_layer(
        self, table_fq: str, cross_layer_relationships: list[dict]
    ) -> dict[str, Any]:
        """Build reconciliation and coverage checks from cross-layer relationships."""
        checks: dict[str, Any] = {}
        for rel in cross_layer_relationships:
            if rel.get("gold_table") == table_fq:
                silver_fq = rel.get("silver_table", "")
                common_cols = rel.get("common_columns", [])
                group_by_cols = [c for c in common_cols if c.endswith("_id")][:2]

                checks["reconciliation_expr"] = [
                    {
                        "enabled": True,
                        "severity": "warning",
                        "blocking": False,
                        "name": "row_count_reconciliation",
                        "source_silver": silver_fq,
                        "gold": "COUNT(*)",
                        "silver": "COUNT(*)",
                        "group_by": group_by_cols if group_by_cols else None,
                        "tolerance": 5.0,
                    }
                ]
                checks["coverage"] = [
                    {
                        "enabled": True,
                        "severity": "warning",
                        "blocking": False,
                        "silver_table": silver_fq,
                        "gold_expr": "COUNT(*)",
                        "silver_expr": "COUNT(*)",
                        "tolerance_pct": 5.0,
                    }
                ]
                break
        return checks

    def _build_cross_table_gold_suggestions(
        self,
        table_fq: str,
        fk_relationships: list[dict],
        layer_pairs: list[dict],
        cross_layer_relationships: list[dict] | None = None,
    ) -> dict[str, Any]:
        """
        Build Gold cross-table check suggestions for a specific table.

        Generates:
        - referential_integrity: for gold->silver FK (from FK detection or cross-layer common columns)
        - reconciliation_expr: for silver->gold aggregation checks
        - coverage: for gold vs silver coverage
        """
        checks: dict[str, Any] = {}

        # Parse table FQ name for catalog
        parts = table_fq.split(".")
        catalog = parts[_FQ_CATALOG_IDX] if len(parts) > _FQ_CATALOG_IDX else ""

        # Build referential integrity checks
        ri_checks = self._build_ri_from_fk_relationships(table_fq, fk_relationships, catalog)
        if cross_layer_relationships:
            ri_checks.extend(self._build_ri_from_cross_layer(table_fq, cross_layer_relationships, ri_checks))
        if ri_checks:
            checks["referential_integrity"] = ri_checks

        # Build reconciliation and coverage checks
        recon_checks = self._build_reconciliation_from_layer_pairs(table_fq, layer_pairs)
        if not recon_checks and cross_layer_relationships:
            recon_checks = self._build_reconciliation_from_cross_layer(table_fq, cross_layer_relationships)
        checks.update(recon_checks)

        return checks
