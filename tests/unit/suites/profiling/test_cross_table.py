# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


def _tables_info() -> dict:
    """Minimal deterministic tables_info that exercises all four detectors.

    Layout:
    - cat.silver.orders / cat.silver.customer: FK (customer_id -> customer.id)
    - cat.lake.orders_bronze / cat.lake.orders_silver: layer pair (bronze -> silver)
    - cat.silver.orders / cat.silver.order_items: header/detail pair
    - cat.gold.summary + silver tables share customer_id: cross-layer relationships
    """
    return {
        "cat.silver.orders": {
            "columns": [
                {"name": "order_id", "type": "StringType()"},
                {"name": "customer_id", "type": "StringType()"},
                {"name": "amount", "type": "DoubleType()"},
            ],
            "suite": "silver",
            "catalog": "cat",
            "schema": "silver",
            "table": "orders",
        },
        "cat.silver.customer": {
            "columns": [
                {"name": "id", "type": "StringType()"},
                {"name": "name", "type": "StringType()"},
            ],
            "suite": "silver",
            "catalog": "cat",
            "schema": "silver",
            "table": "customer",
        },
        "cat.lake.orders_bronze": {
            "columns": [
                {"name": "order_id", "type": "StringType()"},
                {"name": "customer_id", "type": "StringType()"},
            ],
            "suite": "bronze",
            "catalog": "cat",
            "schema": "lake",
            "table": "orders_bronze",
        },
        "cat.lake.orders_silver": {
            "columns": [
                {"name": "order_id", "type": "StringType()"},
                {"name": "customer_id", "type": "StringType()"},
                {"name": "amount", "type": "DoubleType()"},
            ],
            "suite": "silver",
            "catalog": "cat",
            "schema": "lake",
            "table": "orders_silver",
        },
        "cat.silver.order_items": {
            "columns": [
                {"name": "item_id", "type": "StringType()"},
                {"name": "order_id", "type": "StringType()"},
                {"name": "product_id", "type": "StringType()"},
            ],
            "suite": "silver",
            "catalog": "cat",
            "schema": "silver",
            "table": "order_items",
        },
        "cat.gold.summary": {
            "columns": [
                {"name": "customer_id", "type": "StringType()"},
                {"name": "total_amount", "type": "DoubleType()"},
            ],
            "suite": "gold",
            "catalog": "cat",
            "schema": "gold",
            "table": "summary",
        },
    }


def _find_relationship(results: list[dict], **match_fields) -> dict | None:
    """Return the first relationship dict whose keys all match the given values."""
    for rel in results:
        if all(rel.get(k) == v for k, v in match_fields.items()):
            return rel
    return None


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_CROSS_TABLE)
class TestCrossTableDetection:
    @allure.title("Detect FK relationships: exactly 4 relationships found in reference tables_info")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_fk_relationships_count(self, profiling_suite):
        """Given the reference tables_info fixture, when the FK detector runs,
        then it returns exactly 4 relationships."""
        with allure.step("Run FK detector on tables_info"):
            result = profiling_suite._detect_fk_relationships(_tables_info())

        with allure.step("Verify: result is a list with exactly 4 FK relationships"):
            assert isinstance(result, list)
            assert len(result) == 4

    @allure.title("Detect FK relationships: customer_id from silver.orders links to customer.id")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_fk_relationships_customer_id_to_customer(self, profiling_suite):
        """Given the reference tables_info, when FK detection runs,
        then a relationship from silver.orders.customer_id to silver.customer.id is present."""
        with allure.step("Run FK detector on tables_info"):
            result = profiling_suite._detect_fk_relationships(_tables_info())

        with allure.step("Verify: FK from cat.silver.orders.customer_id -> cat.silver.customer.id exists"):
            rel = _find_relationship(
                result,
                source_table="cat.silver.orders",
                source_column="customer_id",
                target_table="cat.silver.customer",
                target_column="id",
            )
            assert rel is not None, f"Expected FK not found in: {result}"

    @allure.title("Detect FK relationships: customer_id from orders_bronze links to customer.id")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_fk_relationships_bronze_orders_to_customer(self, profiling_suite):
        """Given the reference tables_info, when FK detection runs,
        then a relationship from lake.orders_bronze.customer_id to silver.customer.id is present."""
        with allure.step("Run FK detector on tables_info"):
            result = profiling_suite._detect_fk_relationships(_tables_info())

        with allure.step("Verify: FK from cat.lake.orders_bronze.customer_id -> cat.silver.customer.id exists"):
            rel = _find_relationship(
                result,
                source_table="cat.lake.orders_bronze",
                source_column="customer_id",
                target_table="cat.silver.customer",
                target_column="id",
            )
            assert rel is not None, f"Expected FK not found in: {result}"

    @allure.title("Detect FK relationships: result dict has required keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_fk_relationships_dict_shape(self, profiling_suite):
        """Given FK detection results, when each entry is inspected,
        then every dict contains exactly the four required keys."""
        with allure.step("Run FK detector on tables_info"):
            result = profiling_suite._detect_fk_relationships(_tables_info())

        with allure.step("Verify: every relationship dict has the four expected keys"):
            for rel in result:
                assert set(rel.keys()) == {"source_table", "source_column", "target_table", "target_column"}

    @allure.title("Detect FK relationships: no false positives for non-id columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_fk_relationships_no_spurious_entries(self, profiling_suite):
        """Given the reference tables_info, when FK detection runs,
        then non-id columns like 'amount' and 'name' never appear as source columns."""
        with allure.step("Run FK detector on tables_info"):
            result = profiling_suite._detect_fk_relationships(_tables_info())

        with allure.step("Verify: no relationship involves 'amount' or 'name' columns"):
            for rel in result:
                assert rel["source_column"] not in {"amount", "name", "total_amount"}

    @allure.title("Detect layer pairs: orders_bronze / orders_silver surfaces as a pair")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_layer_pairs_bronze_silver(self, profiling_suite):
        """Given tables_info with orders_bronze and orders_silver, when layer-pairs detection runs,
        then exactly one pair linking them is returned."""
        with allure.step("Run layer-pairs detector on tables_info"):
            result = profiling_suite._detect_layer_pairs(_tables_info())

        with allure.step("Verify: exactly one layer pair detected"):
            assert len(result) == 1

        with allure.step("Verify: the pair links orders_bronze to orders_silver"):
            pair = result[0]
            assert pair["source_table"] == "cat.lake.orders_bronze"
            assert pair["target_table"] == "cat.lake.orders_silver"
            assert pair["source_layer"] == "bronze"
            assert pair["target_layer"] == "silver"
            assert pair["base_name"] == "orders"

    @allure.title("Detect layer pairs: result dict has required keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_layer_pairs_dict_shape(self, profiling_suite):
        """Given layer-pairs detection results, when each entry is inspected,
        then every dict contains exactly the five required keys."""
        with allure.step("Run layer-pairs detector on tables_info"):
            result = profiling_suite._detect_layer_pairs(_tables_info())

        with allure.step("Verify: every pair dict has the five expected keys"):
            for pair in result:
                assert set(pair.keys()) == {"source_table", "target_table", "source_layer", "target_layer", "base_name"}

    @allure.title("Detect layer pairs: tables not following layer-name convention return empty")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_detect_layer_pairs_empty_for_plain_tables(self, profiling_suite):
        """Given tables_info where no table names carry a layer suffix, when layer-pairs detection runs,
        then an empty list is returned."""
        with allure.step("Build tables_info with no layer-suffixed table names"):
            no_layer_tables = {
                "cat.silver.orders": {
                    "columns": [{"name": "id", "type": "StringType()"}],
                    "suite": "silver",
                    "catalog": "cat",
                    "schema": "silver",
                    "table": "orders",
                },
                "cat.bronze.orders": {
                    "columns": [{"name": "id", "type": "StringType()"}],
                    "suite": "bronze",
                    "catalog": "cat",
                    "schema": "bronze",
                    "table": "orders",
                },
            }
        with allure.step("Run layer-pairs detector"):
            result = profiling_suite._detect_layer_pairs(no_layer_tables)

        with allure.step("Verify: no layer pairs detected (schema-only distinction is not a layer pair)"):
            assert result == []

    @allure.title("Detect header/detail pairs: orders + order_items yields one pair")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_header_detail_pairs_orders_order_items(self, profiling_suite):
        """Given tables_info with orders and order_items, when header/detail detection runs,
        then exactly one pair linking them via orders_id is returned."""
        with allure.step("Run header/detail detector on tables_info"):
            result = profiling_suite._detect_header_detail_pairs(_tables_info())

        with allure.step("Verify: exactly one header/detail pair detected"):
            assert len(result) == 1

        with allure.step("Verify: pair links cat.silver.orders (header) to cat.silver.order_items (detail)"):
            pair = result[0]
            assert pair["header_table"] == "cat.silver.orders"
            assert pair["detail_table"] == "cat.silver.order_items"
            assert pair["join_key"] == "orders_id"

    @allure.title("Detect header/detail pairs: result dict has required keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_header_detail_pairs_dict_shape(self, profiling_suite):
        """Given header/detail detection results, when each entry is inspected,
        then every dict contains exactly the three required keys."""
        with allure.step("Run header/detail detector on tables_info"):
            result = profiling_suite._detect_header_detail_pairs(_tables_info())

        with allure.step("Verify: every pair dict has the three expected keys"):
            for pair in result:
                assert set(pair.keys()) == {"header_table", "detail_table", "join_key"}

    @allure.title("Detect header/detail pairs: no pairs when no detail-suffix tables present")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_detect_header_detail_pairs_empty_when_no_detail_suffix(self, profiling_suite):
        """Given tables_info with no detail-suffix table names, when header/detail detection runs,
        then an empty list is returned."""
        with allure.step("Build tables_info with no detail-suffix table names"):
            plain_tables = {
                "cat.silver.orders": {
                    "columns": [{"name": "id", "type": "StringType()"}],
                    "suite": "silver",
                    "catalog": "cat",
                    "schema": "silver",
                    "table": "orders",
                },
                "cat.silver.products": {
                    "columns": [{"name": "id", "type": "StringType()"}],
                    "suite": "silver",
                    "catalog": "cat",
                    "schema": "silver",
                    "table": "products",
                },
            }
        with allure.step("Run header/detail detector"):
            result = profiling_suite._detect_header_detail_pairs(plain_tables)

        with allure.step("Verify: no header/detail pairs detected"):
            assert result == []

    @allure.title("Detect cross-layer relationships: gold.summary links to silver tables via customer_id")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_cross_layer_relationships_gold_silver(self, profiling_suite):
        """Given tables_info with gold and silver tables sharing customer_id, when cross-layer detection runs,
        then exactly 2 gold-to-silver relationships for gold.summary are returned."""
        with allure.step("Run cross-layer detector on tables_info"):
            result = profiling_suite._detect_cross_layer_relationships(_tables_info())

        with allure.step("Verify: exactly 2 gold->silver relationships involving cat.gold.summary exist"):
            gold_silver_rels = [r for r in result if r.get("gold_table") == "cat.gold.summary"]
            assert len(gold_silver_rels) == 2

        with allure.step("Verify: gold->silver to cat.silver.orders has customer_id as common and fk column"):
            rel = _find_relationship(result, gold_table="cat.gold.summary", silver_table="cat.silver.orders")
            assert rel is not None, f"Expected gold->silver relation not found in: {result}"
            assert set(rel["common_columns"]) == {"customer_id"}
            assert set(rel["fk_columns"]) == {"customer_id"}

    @allure.title("Detect cross-layer relationships: silver.orders links to bronze.orders_bronze via shared columns")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_cross_layer_relationships_silver_bronze(self, profiling_suite):
        """Given tables_info with silver and bronze tables sharing order_id and customer_id,
        when cross-layer detection runs, then the silver-to-bronze relationship lists both columns."""
        with allure.step("Run cross-layer detector on tables_info"):
            result = profiling_suite._detect_cross_layer_relationships(_tables_info())

        with allure.step("Verify: silver->bronze rel between cat.silver.orders and cat.lake.orders_bronze exists"):
            rel = _find_relationship(
                result,
                silver_table="cat.silver.orders",
                bronze_table="cat.lake.orders_bronze",
            )
            assert rel is not None, f"Expected silver->bronze relation not found in: {result}"
            assert set(rel["common_columns"]) == {"order_id", "customer_id"}
            assert set(rel["fk_columns"]) == {"order_id", "customer_id"}

    @allure.title("Detect cross-layer relationships: gold/silver result dict has required keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_cross_layer_relationships_gold_silver_dict_shape(self, profiling_suite):
        """Given cross-layer detection results, when gold-to-silver entries are inspected,
        then every dict contains exactly the six required keys."""
        with allure.step("Run cross-layer detector on tables_info"):
            result = profiling_suite._detect_cross_layer_relationships(_tables_info())

        with allure.step("Verify: gold->silver relationship dicts have the six expected keys"):
            gold_silver_rels = [r for r in result if "gold_table" in r]
            assert len(gold_silver_rels) == 2
            gold_rel_keys = {"gold_table", "gold_info", "silver_table", "silver_info", "common_columns", "fk_columns"}
            for rel in gold_silver_rels:
                assert set(rel.keys()) == gold_rel_keys

    @allure.title("Detect cross-layer relationships: silver/bronze result dict has required keys")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_detect_cross_layer_relationships_silver_bronze_dict_shape(self, profiling_suite):
        """Given cross-layer detection results, when silver-to-bronze entries are inspected,
        then every dict contains exactly the six required keys."""
        with allure.step("Run cross-layer detector on tables_info"):
            result = profiling_suite._detect_cross_layer_relationships(_tables_info())

        with allure.step("Verify: silver->bronze relationship dicts have the six expected keys"):
            silver_bronze_rels = [r for r in result if "bronze_table" in r]
            assert len(silver_bronze_rels) == 3
            silver_rel_keys = {
                "silver_table",
                "silver_info",
                "bronze_table",
                "bronze_info",
                "common_columns",
                "fk_columns",
            }
            for rel in silver_bronze_rels:
                assert set(rel.keys()) == silver_rel_keys

    @allure.title("Detect cross-layer relationships: empty when only one layer present")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_detect_cross_layer_relationships_empty_when_single_layer(self, profiling_suite):
        """Given tables_info containing only silver tables, when cross-layer detection runs,
        then an empty list is returned."""
        with allure.step("Build tables_info with only silver tables"):
            silver_only = {
                "cat.silver.orders": {
                    "columns": [{"name": "order_id", "type": "StringType()"}],
                    "suite": "silver",
                    "catalog": "cat",
                    "schema": "silver",
                    "table": "orders",
                },
            }
        with allure.step("Run cross-layer detector"):
            result = profiling_suite._detect_cross_layer_relationships(silver_only)

        with allure.step("Verify: no cross-layer relationships detected"):
            assert result == []

    @allure.title("Detect cross-layer relationships: tables with no common columns produce no relationship")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_detect_cross_layer_relationships_no_common_columns(self, profiling_suite):
        """Given gold and silver tables with completely disjoint column sets, when cross-layer detection runs,
        then no relationships are returned."""
        with allure.step("Build gold+silver tables with disjoint column sets"):
            disjoint = {
                "cat.gold.agg": {
                    "columns": [{"name": "total", "type": "DoubleType()"}],
                    "suite": "gold",
                    "catalog": "cat",
                    "schema": "gold",
                    "table": "agg",
                },
                "cat.silver.events": {
                    "columns": [{"name": "event_name", "type": "StringType()"}],
                    "suite": "silver",
                    "catalog": "cat",
                    "schema": "silver",
                    "table": "events",
                },
            }
        with allure.step("Run cross-layer detector"):
            result = profiling_suite._detect_cross_layer_relationships(disjoint)

        with allure.step("Verify: no relationships detected (no overlapping columns)"):
            assert result == []


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_CROSS_TABLE)
class TestInferLayerFromSchema:
    @allure.title("Infer layer from schema: exact match 'silver' returns 'silver'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_infer_layer_exact_silver(self, profiling_suite):
        """Given the schema name 'silver', when _infer_layer_from_schema is called, then it returns 'silver'."""
        with allure.step("Call _infer_layer_from_schema with 'silver'"):
            result = profiling_suite._infer_layer_from_schema("silver")

        with allure.step("Verify: returns 'silver'"):
            assert result == "silver"

    @allure.title("Infer layer from schema: 'analytics_gold' contains 'gold' -> returns 'gold'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_infer_layer_contains_gold(self, profiling_suite):
        """Given the schema name 'analytics_gold', when _infer_layer_from_schema is called, then it returns 'gold'."""
        with allure.step("Call _infer_layer_from_schema with 'analytics_gold'"):
            result = profiling_suite._infer_layer_from_schema("analytics_gold")

        with allure.step("Verify: returns 'gold'"):
            assert result == "gold"

    @allure.title("Infer layer from schema: 'raw' has no layer keyword -> returns None")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE, AllureTag.SPARK)
    def test_infer_layer_no_match_returns_none(self, profiling_suite):
        """Given the schema name 'raw' containing no layer keyword, when _infer_layer_from_schema is called,
        then it returns None."""
        with allure.step("Call _infer_layer_from_schema with 'raw'"):
            result = profiling_suite._infer_layer_from_schema("raw")

        with allure.step("Verify: returns None"):
            assert result is None

    @allure.title("Infer layer from schema: 'bronze_staging' contains 'bronze' -> returns 'bronze'")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_infer_layer_contains_bronze(self, profiling_suite):
        """Given the schema name 'bronze_staging', when _infer_layer_from_schema is called, then it returns 'bronze'."""
        with allure.step("Call _infer_layer_from_schema with 'bronze_staging'"):
            result = profiling_suite._infer_layer_from_schema("bronze_staging")

        with allure.step("Verify: returns 'bronze'"):
            assert result == "bronze"


def _two_silver_tables_info() -> dict:
    """Two silver tables in the same schema sharing customer_id."""
    return {
        "cat.silver.orders": {
            "columns": [
                {"name": "order_id", "type": "StringType()"},
                {"name": "customer_id", "type": "StringType()"},
                {"name": "amount", "type": "DoubleType()"},
            ],
            "suite": "silver",
            "catalog": "cat",
            "schema": "silver",
            "table": "orders",
        },
        "cat.silver.payments": {
            "columns": [
                {"name": "payment_id", "type": "StringType()"},
                {"name": "customer_id", "type": "StringType()"},
                {"name": "total", "type": "DoubleType()"},
            ],
            "suite": "silver",
            "catalog": "cat",
            "schema": "silver",
            "table": "payments",
        },
    }


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_CROSS_TABLE)
class TestCrossTableEquality:
    @allure.title("cross_table_equality: two silver tables in same schema sharing customer_id emits suggestion")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_cross_table_equality_emitted_for_shared_id_column(self, profiling_suite):
        """Given two silver tables sharing customer_id, when cross-table silver suggestions are built,
        then a disabled cross_table_equality entry pointing to the peer table is emitted."""
        tables_info = _two_silver_tables_info()

        with allure.step("Call _build_cross_table_silver_suggestions for cat.silver.orders"):
            result = profiling_suite._build_cross_table_silver_suggestions(
                "cat.silver.orders",
                fk_relationships=[],
                layer_pairs=[],
                header_detail_pairs=[],
                tables_info=tables_info,
                cross_layer_relationships=[],
            )

        with allure.step("Verify: result contains 'cross_table_equality' key"):
            assert "cross_table_equality" in result, f"Expected 'cross_table_equality' in {list(result.keys())}"

        with allure.step("Verify: cross_table_equality is a non-empty list"):
            cte = result["cross_table_equality"]
            assert isinstance(cte, list)
            assert len(cte) >= 1

        with allure.step("Verify: entry has enabled=False"):
            entry = cte[0]
            assert entry["enabled"] is False

        with allure.step("Verify: entry has target_table pointing to cat.silver.payments"):
            assert entry["target_table"] == "cat.silver.payments"

        with allure.step("Verify: join_columns contains 'customer_id'"):
            assert "customer_id" in entry["join_columns"]

        with allure.step("Verify: _confidence is 'low'"):
            assert entry["_confidence"] == "low"
