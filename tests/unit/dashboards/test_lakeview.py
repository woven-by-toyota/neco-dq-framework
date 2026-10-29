# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for Lakeview dashboard template hydration.

These tests validate the pure template-hydration logic (no Databricks I/O):
placeholder substitution, layer/expected-table SQL generation, round-trip
fidelity against the bundled template, and query extraction.

The template is deployment-neutral: the "reference" values below live only in
this test (they reproduce the dashboard the template was extracted from) and are
NOT shipped in the module — the framework carries no project-specific names.
"""

import json
from types import SimpleNamespace

import allure
import pytest

from neco_dq.dashboards.lakeview import (
    _build_expected_filter,
    _build_expected_tables,
    _build_ge_layer_case,
    _build_scope_filter,
    _dashboard_url,
    _discover_warehouse,
    _normalize_expected,
    _normalize_layers,
    _print_confirmation,
    _quoted_csv,
    hydrate_template,
    iter_dataset_queries,
    load_template,
)
from tests.constants import AllureFeature, AllureSeverity, AllureTag


def _fake_client(warehouses):
    """A minimal stand-in for WorkspaceClient exposing .warehouses.list()."""
    return SimpleNamespace(
        warehouses=SimpleNamespace(list=lambda: warehouses),
        config=SimpleNamespace(host="https://example.cloud.databricks.com"),
    )


def _wh(id_, state):
    return SimpleNamespace(id=id_, name=f"wh-{id_}", state=SimpleNamespace(value=state))


# Synthetic hydration values — the round-trip test only asserts that the number
# of substitutions matches the template's placeholder count (so a hardcoded value
# would be caught), which needs no real deployment coordinates. Real catalog /
# warehouse / schema names are deliberately NOT present anywhere in this repo.
REF_HISTORY = "cat_ref.dq.history_ref"
REF_CATALOG = "cat_ref"
REF_WAREHOUSE = "wh_ref"
REF_LAYERS = {
    "bronze": ["bronze_schema"],
    "silver": ["silver_schema"],
    "gold": ["gold_schema"],
}
REF_EXPECTED = [
    ("gold_schema", "orders"),
    ("gold_schema", "customers"),
    ("silver_schema", "events"),
    ("bronze_schema", "ingestion"),
]

# A distinct deployment to prove neutrality across schema layouts.
CUSTOM_LAYERS = {"bronze": ["raw"], "silver": ["clean"], "gold": ["mart", "marts"]}
CUSTOM_EXPECTED = [("mart", "orders"), ("clean", "events")]
# CUSTOM_LAYERS normalized against the "mycat" global catalog (see _hydrate).
CUSTOM_BY_LAYER = {
    "bronze": ("mycat", ["raw"]),
    "silver": ("mycat", ["clean"]),
    "gold": ("mycat", ["mart", "marts"]),
}
# Resolved (catalog, schema, table) triples for CUSTOM_EXPECTED under CUSTOM_BY_LAYER.
CUSTOM_EXPECTED_TRIPLES = [("mycat", "mart", "orders"), ("mycat", "clean", "events")]


def _hydrate(**overrides):
    """Hydrate with custom-deployment defaults, overridable per test."""
    kwargs = {
        "history_table": "cat.dq.hist",
        "catalog": "mycat",
        "warehouse_id": "wh123",
        "layers": CUSTOM_LAYERS,
        "expected_tables": CUSTOM_EXPECTED,
    }
    kwargs.update(overrides)
    return hydrate_template(**kwargs)


@allure.feature(AllureFeature.CORE)
@allure.story("Dashboard — SQL fragment builders")
class TestFragmentBuilders:
    @allure.tag(AllureTag.POSITIVE)
    def test_quoted_csv(self):
        with allure.step("Values render as SQL quoted CSV"):
            assert _quoted_csv(["a", "b"]) == "'a', 'b'"
            assert _quoted_csv(["only"]) == "'only'"

    @allure.tag(AllureTag.POSITIVE)
    def test_ge_layer_case_covers_all_tiers(self):
        with allure.step("GE-folding CASE references every tier and its (catalog, schema) pairs"):
            sql = _build_ge_layer_case(CUSTOM_BY_LAYER)
            assert "THEN 'bronze'" in sql and "THEN 'silver'" in sql and "THEN 'gold'" in sql
            assert "'mycat.mart', 'mycat.marts'" in sql
            assert "CONCAT(catalog, '.', schema) IN" in sql
            assert "ELSE test_suite" in sql

    @allure.tag(AllureTag.POSITIVE)
    def test_expected_tables_cte(self):
        with allure.step("Expected-tables CTE is a UNION ALL over the resolved triples"):
            sql = _build_expected_tables(CUSTOM_EXPECTED_TRIPLES)
            assert sql.startswith("expected_tables AS (")
            assert sql.count("UNION ALL") == len(CUSTOM_EXPECTED_TRIPLES) - 1
            assert "'mart'" in sql and "'orders'" in sql and "'mycat'" in sql

    @allure.tag(AllureTag.POSITIVE)
    def test_expected_filter_predicate(self):
        with allure.step("Expected-filter is a catalog.schema.table IN predicate"):
            sql = _build_expected_filter(CUSTOM_EXPECTED_TRIPLES)
            assert sql.startswith(" AND CONCAT(catalog, '.', schema, '.', table) IN (")
            assert "'mycat.mart.orders'" in sql and "'mycat.clean.events'" in sql

    @allure.tag(AllureTag.NEGATIVE)
    def test_normalize_expected_empty_raises(self):
        with allure.step("Empty expected-tables is rejected"):
            with pytest.raises(ValueError, match="non-empty"):
                _normalize_expected([], CUSTOM_BY_LAYER)

    @allure.tag(AllureTag.NEGATIVE)
    def test_quote_injection_rejected(self):
        with allure.step("A table name containing a quote is rejected"):
            with pytest.raises(ValueError, match="invalid"):
                _normalize_expected([("ok", "b'ad")], {"bronze": ("cat", ["ok"])})

    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_expected_pair_resolves_catalog(self):
        with allure.step("A (schema, table) pair resolves its catalog from layers"):
            out = _normalize_expected([("raw", "t")], {"bronze": ("cb", ["raw"])})
            assert out == [("cb", "raw", "t")]

    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_expected_triple_passthrough(self):
        with allure.step("A (catalog, schema, table) triple keeps its explicit catalog"):
            by_layer = {"bronze": ("cat_a", ["raw"]), "silver": ("cat_b", ["raw"])}
            out = _normalize_expected([("cat_a", "raw", "t1"), ("cat_b", "raw", "t2")], by_layer)
            assert out == [("cat_a", "raw", "t1"), ("cat_b", "raw", "t2")]

    @allure.tag(AllureTag.NEGATIVE)
    def test_normalize_expected_ambiguous_pair_rejected(self):
        with allure.step("A 2-tuple whose schema lives in two catalogs is rejected"):
            by_layer = {"bronze": ("cat_a", ["raw"]), "silver": ("cat_b", ["raw"])}
            with pytest.raises(ValueError, match="ambiguous"):
                _normalize_expected([("raw", "t")], by_layer)

    @allure.tag(AllureTag.NEGATIVE)
    def test_normalize_expected_triple_not_in_layers_rejected(self):
        with allure.step("A triple whose (catalog, schema) is not in layers is rejected"):
            with pytest.raises(ValueError, match="not declared in 'layers'"):
                _normalize_expected([("other_cat", "raw", "t")], {"bronze": ("cb", ["raw"])})

    @allure.tag(AllureTag.NEGATIVE)
    def test_normalize_expected_schema_not_declared_rejected(self):
        with allure.step("A 2-tuple schema absent from every layer is rejected"):
            with pytest.raises(ValueError, match="not declared in any layer"):
                _normalize_expected([("missing", "t")], {"bronze": ("cb", ["raw"])})

    @allure.tag(AllureTag.NEGATIVE)
    def test_normalize_expected_bad_arity_rejected(self):
        with allure.step("An entry that is neither a pair nor a triple is rejected"):
            with pytest.raises(ValueError, match="\\(schema, table\\) or"):
                _normalize_expected([("a", "b", "c", "d")], {"bronze": ("cb", ["b"])})

    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_layers_list_form(self):
        with allure.step("List-form tiers inherit the global catalog, in medallion order"):
            by_layer = _normalize_layers({"gold": ["m"], "bronze": ["r"]}, "glob")
            assert list(by_layer) == ["bronze", "gold"]  # medallion order, present only
            assert by_layer["bronze"] == ("glob", ["r"])
            assert by_layer["gold"] == ("glob", ["m"])

    @allure.tag(AllureTag.POSITIVE)
    def test_normalize_layers_dict_form(self):
        with allure.step("Dict-form tiers carry their own catalog"):
            by_layer = _normalize_layers({"bronze": {"catalog": "cb", "schemas": ["r"]}}, None)
            assert by_layer == {"bronze": ("cb", ["r"])}

    @allure.tag(AllureTag.POSITIVE)
    def test_build_scope_filter_pairs(self):
        with allure.step("Scope filter is a CONCAT(catalog, '.', schema) IN over all pairs"):
            sql = _build_scope_filter({"bronze": ("cb", ["r"]), "silver": ("cs", ["c1", "c2"])})
            assert sql == "CONCAT(catalog, '.', schema) IN ('cb.r', 'cs.c1', 'cs.c2')"


@allure.feature(AllureFeature.CORE)
@allure.story("Dashboard — template loading")
class TestLoadTemplate:
    @allure.tag(AllureTag.POSITIVE)
    def test_template_loads(self):
        with allure.step("Bundled template loads and has expected shape"):
            tpl = load_template("reference")
            assert "serialized_dashboard" in tpl
            assert tpl["display_name"] == "DQ Dashboard"

    @allure.tag(AllureTag.POSITIVE)
    def test_template_is_deployment_neutral(self):
        """The shipped template must carry no project-specific names."""
        with allure.step("Template has no survey_/catalog leakage, only placeholders"):
            sd = load_template("reference")["serialized_dashboard"]
            assert "survey_" not in sd
            assert "weaver_feedback_analytics" not in sd
            assert "qualityassurance_wigqa" not in sd
            assert "{{history_table}}" in sd and "{{expected_tables}}" in sd

    @allure.tag(AllureTag.NEGATIVE)
    def test_missing_template_raises(self):
        with allure.step("Unknown template name raises FileNotFoundError"):
            with pytest.raises(FileNotFoundError):
                load_template("does-not-exist.json")


@allure.feature(AllureFeature.CORE)
@allure.story("Dashboard — hydration")
class TestHydrateTemplate:
    @allure.tag(AllureTag.POSITIVE)
    def test_hydrates_all_placeholders(self):
        with allure.step("Hydration substitutes every framework placeholder"):
            sd = _hydrate()["serialized_dashboard"]
            # None of our framework placeholders survive. (Lakeview's own native
            # "{{ @ }}" / "{{ @formatted }}" display tokens legitimately remain.)
            for tok in (
                "{{history_table}}",
                "{{scope_filter}}",
                "{{schemas}}",
                "{{warehouse_id}}",
                "{{ge_layer_case}}",
                "{{schema_layer_case}}",
                "{{schema_layer_case_bt}}",
                "{{expected_tables}}",
                "{{expected_filter}}",
                # Removed per-tier tokens must never reappear.
                "{{catalog}}",
                "{{schemas_bronze}}",
                "{{schemas_silver}}",
                "{{schemas_gold}}",
            ):
                assert tok not in sd
            assert "cat.dq.hist" in sd
            assert "mycat" in sd

    @allure.tag(AllureTag.POSITIVE)
    def test_output_is_valid_json(self):
        with allure.step("Hydrated serialized_dashboard parses as JSON"):
            sd = json.loads(_hydrate()["serialized_dashboard"])
            assert "datasets" in sd
            assert "pages" in sd

    @allure.tag(AllureTag.EDGE_CASE)
    def test_expected_filter_scopes_every_dataset(self):
        """Every dataset's base scan is restricted to the expected tables."""
        with allure.step("The catalog.schema.table IN predicate appears in all 8 datasets"):
            sd = _hydrate()["serialized_dashboard"]
            # One injected predicate per dataset (8).
            assert sd.count("CONCAT(catalog, '.', schema, '.', table) IN") == 8
            # And it carries the custom keys, not any reference table.
            assert "'mycat.mart.orders'" in sd

    @allure.tag(AllureTag.EDGE_CASE)
    def test_scope_filter_replaces_single_catalog_equality(self):
        """The per-pair scope filter replaces the old single catalog = '...' equality."""
        with allure.step("No single-catalog equality remains; (catalog, schema) pairs are present"):
            sd = _hydrate()["serialized_dashboard"]
            # The old 'catalog = '<value>'' equality filter must be gone entirely.
            assert "catalog = '" not in sd
            # Every declared (catalog, schema) pair is present in the scope predicate.
            assert "'mycat.raw'" in sd and "'mycat.clean'" in sd and "'mycat.mart'" in sd

    @allure.tag(AllureTag.POSITIVE)
    def test_custom_deployment_has_no_reference_leakage(self):
        """A team with different schemas gets ZERO reference-deployment strings."""
        with allure.step("Custom deployment leaves no survey_* / reference tables"):
            sd = _hydrate()["serialized_dashboard"]
            assert "survey_" not in sd
            assert "unified_survey_data" not in sd
            assert "questions_meta" not in sd
            assert "'mart'" in sd and "'orders'" in sd

    @allure.tag(AllureTag.POSITIVE)
    def test_display_name_override(self):
        with allure.step("Explicit display_name is used"):
            assert _hydrate(display_name="My Custom Dashboard")["display_name"] == "My Custom Dashboard"

    @allure.tag(AllureTag.POSITIVE)
    def test_default_display_name(self):
        with allure.step("Falls back to the template display_name"):
            assert _hydrate()["display_name"] == "DQ Dashboard"

    @allure.tag(AllureTag.POSITIVE)
    def test_warehouse_id_propagates(self):
        with allure.step("warehouse_id is returned on the payload"):
            assert _hydrate(warehouse_id="wh-xyz")["warehouse_id"] == "wh-xyz"

    @allure.tag(AllureTag.NEGATIVE)
    def test_unexpected_layer_rejected(self):
        with allure.step("A layer outside bronze/silver/gold is rejected"):
            with pytest.raises(ValueError, match="unexpected layer"):
                _hydrate(layers={"bronze": ["b"], "platinum": ["p"]})

    @allure.tag(AllureTag.POSITIVE)
    def test_missing_tier_allowed(self):
        with allure.step("A subset of tiers (no gold) hydrates successfully"):
            # Tiers are optional; the absent gold tier contributes no scope pairs.
            sd = _hydrate(
                layers={"bronze": ["b"], "silver": ["s"]},
                expected_tables=[("b", "t1"), ("s", "t2")],
            )["serialized_dashboard"]
            assert "'mycat.b'" in sd and "'mycat.s'" in sd
            assert "THEN 'gold'" not in sd

    @allure.tag(AllureTag.POSITIVE)
    def test_bronze_only(self):
        with allure.step("A bronze-only deployment hydrates with just its pairs"):
            sd = _hydrate(
                layers={"bronze": ["raw"]},
                expected_tables=[("raw", "ingestion")],
            )["serialized_dashboard"]
            assert "'mycat.raw'" in sd
            assert "THEN 'silver'" not in sd and "THEN 'gold'" not in sd
            assert json.loads(sd)  # still valid JSON

    @allure.tag(AllureTag.NEGATIVE)
    def test_empty_layers_rejected(self):
        with allure.step("An empty layers mapping is rejected"):
            with pytest.raises(ValueError, match="at least one medallion tier"):
                _hydrate(layers={})

    @allure.tag(AllureTag.NEGATIVE)
    def test_empty_tier_rejected(self):
        with allure.step("A present tier with no schemas is rejected"):
            with pytest.raises(ValueError, match="non-empty schema list"):
                _hydrate(layers={"bronze": []})

    @allure.tag(AllureTag.POSITIVE)
    def test_per_layer_catalog(self):
        with allure.step("Each tier can live in its own catalog"):
            sd = _hydrate(
                catalog=None,
                layers={
                    "bronze": {"catalog": "cat_b", "schemas": ["raw"]},
                    "silver": {"catalog": "cat_s", "schemas": ["clean"]},
                },
                expected_tables=[("raw", "ingestion"), ("clean", "events")],
            )["serialized_dashboard"]
            # Scope pairs carry each tier's own catalog.
            assert "'cat_b.raw'" in sd and "'cat_s.clean'" in sd
            # Expected filter resolves each schema's catalog.
            assert "'cat_b.raw.ingestion'" in sd and "'cat_s.clean.events'" in sd

    @allure.tag(AllureTag.POSITIVE)
    def test_mixed_global_and_per_layer_catalog(self):
        with allure.step("List-form tiers use the global catalog; dict-form tiers their own"):
            sd = _hydrate(
                catalog="cat_global",
                layers={
                    "bronze": ["raw"],  # uses cat_global
                    "silver": {"catalog": "cat_s", "schemas": ["clean"]},
                },
                expected_tables=[("raw", "ingestion"), ("clean", "events")],
            )["serialized_dashboard"]
            assert "'cat_global.raw'" in sd and "'cat_s.clean'" in sd

    @allure.tag(AllureTag.NEGATIVE)
    def test_list_form_without_global_catalog_rejected(self):
        with allure.step("A list-form tier without a global catalog is rejected"):
            with pytest.raises(ValueError, match="global catalog"):
                _hydrate(catalog=None, layers={"bronze": ["raw"]})

    @allure.tag(AllureTag.NEGATIVE)
    def test_cross_catalog_schema_pair_rejected(self):
        with allure.step("A 2-tuple for a schema shared across catalogs is ambiguous"):
            with pytest.raises(ValueError, match="ambiguous"):
                _hydrate(
                    catalog=None,
                    layers={
                        "bronze": {"catalog": "cat_a", "schemas": ["shared"]},
                        "silver": {"catalog": "cat_b", "schemas": ["shared"]},
                    },
                    expected_tables=[("shared", "t")],
                )

    @allure.tag(AllureTag.POSITIVE)
    def test_triple_disambiguates_cross_catalog_schema(self):
        with allure.step("Triples let a schema name shared across catalogs be scoped per-catalog"):
            sd = _hydrate(
                catalog=None,
                layers={
                    "bronze": {"catalog": "cat_a", "schemas": ["shared"]},
                    "silver": {"catalog": "cat_b", "schemas": ["shared"]},
                },
                expected_tables=[("cat_a", "shared", "t1"), ("cat_b", "shared", "t2")],
            )["serialized_dashboard"]
            # Both catalogs are in scope and each expected table keeps its own catalog.
            assert "'cat_a.shared'" in sd and "'cat_b.shared'" in sd
            assert "'cat_a.shared.t1'" in sd and "'cat_b.shared.t2'" in sd

    @allure.tag(AllureTag.POSITIVE)
    def test_mixed_pair_and_triple_expected_tables(self):
        with allure.step("The same list accepts (schema, table) pairs and (catalog, schema, table) triples"):
            sd = _hydrate(
                expected_tables=[("mart", "orders"), ("mycat", "clean", "events")],
            )["serialized_dashboard"]
            assert "'mycat.mart.orders'" in sd and "'mycat.clean.events'" in sd

    @allure.tag(AllureTag.NEGATIVE)
    def test_invalid_history_table_rejected(self):
        with allure.step("A history_table containing a quote is rejected"):
            with pytest.raises(ValueError, match="invalid history_table"):
                _hydrate(history_table="cat.dq.h'ist")

    @allure.tag(AllureTag.NEGATIVE)
    def test_invalid_warehouse_id_rejected(self):
        with allure.step("A warehouse_id containing whitespace is rejected"):
            with pytest.raises(ValueError, match="invalid warehouse_id"):
                _hydrate(warehouse_id="wh 123")

    @allure.tag(AllureTag.EDGE_CASE)
    def test_preserves_native_lakeview_tokens(self):
        """Lakeview's own ``{{ @ }}`` display tokens must survive hydration."""
        with allure.step("Native Lakeview value tokens are left intact"):
            assert "{{ @ }}" in _hydrate()["serialized_dashboard"]

    @allure.tag(AllureTag.EDGE_CASE)
    def test_substitution_is_complete_and_loss_free(self):
        """Every FROM/catalog/schema slot is a placeholder, not a baked-in value.

        Hydrating with synthetic values and asserting the substitution counts
        proves the template is fully tokenized: if any coordinate were hardcoded
        in the template instead of a placeholder, these counts would not match.
        """
        with allure.step("Substituted values appear the expected number of times"):
            payload = hydrate_template(
                history_table=REF_HISTORY,
                catalog=REF_CATALOG,
                warehouse_id=REF_WAREHOUSE,
                layers=REF_LAYERS,
                expected_tables=REF_EXPECTED,
            )
            sd = payload["serialized_dashboard"]
            # Every dataset FROM is hydrated from the single history-table token.
            assert sd.count(REF_HISTORY) == 12
            # The medallion schema union (the harmless superset pre-filter) is rendered.
            assert "'bronze_schema', 'silver_schema', 'gold_schema'" in sd
            # The expected-tables CTE + filter are rendered from REF_EXPECTED.
            assert "'orders'" in sd and "'customers'" in sd
            # expected_filter now uses catalog.schema.table keys.
            assert "cat_ref.gold_schema.orders" in sd


@allure.feature(AllureFeature.CORE)
@allure.story("Dashboard — query extraction")
class TestIterDatasetQueries:
    @allure.tag(AllureTag.POSITIVE)
    def test_extracts_all_datasets(self):
        with allure.step("All 8 datasets are extracted with queries"):
            queries = iter_dataset_queries(_hydrate()["serialized_dashboard"])
            assert len(queries) == 8
            for name, query in queries:
                assert name
                assert query.strip()
                assert not query.rstrip().endswith(";")

    @allure.tag(AllureTag.EDGE_CASE)
    def test_most_datasets_use_sel_date_parameter(self):
        """Most datasets reference :sel_date (drives validation binding).

        The one exception is the Run Date options dataset, which populates the
        dropdown itself and must not be parameterized.
        """
        with allure.step("All but the options dataset reference :sel_date"):
            queries = iter_dataset_queries(_hydrate()["serialized_dashboard"])
            parameterized = [n for n, q in queries if ":sel_date" in q]
            assert len(parameterized) == 7


@allure.feature(AllureFeature.CORE)
@allure.story("Dashboard — warehouse discovery")
class TestDiscoverWarehouse:
    @allure.tag(AllureTag.POSITIVE)
    def test_prefers_running_warehouse(self):
        with allure.step("A RUNNING warehouse is preferred over others"):
            client = _fake_client([_wh("a", "STOPPED"), _wh("b", "RUNNING"), _wh("c", "RUNNING")])
            assert _discover_warehouse(client) == "b"

    @allure.tag(AllureTag.POSITIVE)
    def test_falls_back_to_first_when_none_running(self):
        with allure.step("With no RUNNING warehouse, the first is used"):
            client = _fake_client([_wh("a", "STOPPED"), _wh("b", "STOPPED")])
            assert _discover_warehouse(client) == "a"

    @allure.tag(AllureTag.NEGATIVE)
    def test_no_warehouse_raises(self):
        with allure.step("An empty workspace raises a clear error"):
            with pytest.raises(RuntimeError, match="no SQL warehouse"):
                _discover_warehouse(_fake_client([]))


@allure.feature(AllureFeature.CORE)
@allure.story("Dashboard — terminal confirmation")
class TestPrintConfirmation:
    @allure.tag(AllureTag.POSITIVE)
    def test_prints_link_and_id(self, capsys):
        with allure.step("Confirmation shows id, action and a workspace link"):
            client = _fake_client([])
            url = _dashboard_url(client, "01fabc")
            _print_confirmation(
                "DQ Dashboard",
                "01fabc",
                url,
                created=True,
                published=True,
            )
            out = capsys.readouterr().out
            assert "generated successfully" in out
            assert "01fabc" in out
            assert "Created + published" in out
            assert "example.cloud.databricks.com" in out

    @allure.tag(AllureTag.EDGE_CASE)
    def test_draft_not_published_wording(self, capsys):
        with allure.step("Draft (publish=False) is worded as not published"):
            _print_confirmation(
                "DQ Dashboard",
                "01fabc",
                None,
                created=False,
                published=False,
            )
            out = capsys.readouterr().out
            assert "Updated (draft, not published)" in out

    @allure.tag(AllureTag.POSITIVE)
    def test_dashboard_url_builds_published_link(self):
        with allure.step("URL combines the workspace host and dashboard id"):
            url = _dashboard_url(_fake_client([]), "01fabc")
            assert url is not None
            assert "example.cloud.databricks.com" in url
            assert url.endswith("/dashboardsv3/01fabc/published")

    @allure.tag(AllureTag.EDGE_CASE)
    def test_dashboard_url_none_without_id(self):
        with allure.step("No dashboard id yields no URL"):
            assert _dashboard_url(_fake_client([]), None) is None

    @allure.tag(AllureTag.EDGE_CASE)
    def test_dashboard_url_none_without_host(self):
        with allure.step("Unknown workspace host yields no URL (not the raw id)"):
            client = SimpleNamespace(config=SimpleNamespace(host=""))
            assert _dashboard_url(client, "01fabc") is None


@allure.feature(AllureFeature.CORE)
@allure.story("Dashboard — regression datasets")
class TestRegressionDatasetsAcrossCatalogs:
    """The same schema.table in two catalogs must not merge in regression datasets."""

    @staticmethod
    def _queries(view: str) -> dict[str, str]:
        sd = json.loads(
            _hydrate(
                history_table="cat.dq.hist",
                catalog=None,
                layers={
                    "bronze": {"catalog": "cat_a", "schemas": ["shared"]},
                    "silver": {"catalog": "cat_b", "schemas": ["shared"]},
                },
                expected_tables=[("cat_a", "shared", "t"), ("cat_b", "shared", "t")],
            )["serialized_dashboard"]
        )
        return {
            ds["name"]: "".join(ds["queryLines"]).replace("cat.dq.hist", view)
            for ds in sd["datasets"]
            if ds["name"] in ("regression_summary", "regression_detail")
        }

    @pytest.fixture
    def history_view(self, spark):
        from datetime import datetime

        r1, r2 = datetime(2026, 1, 1), datetime(2026, 1, 2)
        rows = [
            # cat_a: freshness newly broken in the latest run
            ("silver", "cat_a", "shared", "t", None, "freshness", True, r1),
            ("silver", "cat_a", "shared", "t", None, "freshness", False, r2),
            # cat_b: same schema.table and check, passing in both runs
            ("silver", "cat_b", "shared", "t", None, "freshness", True, r1),
            ("silver", "cat_b", "shared", "t", None, "freshness", True, r2),
        ]
        spark.createDataFrame(
            rows,
            "test_suite string, catalog string, schema string, table string, column string, "
            "check_name string, passed boolean, run_timestamp timestamp",
        ).createOrReplaceTempView("regression_hist")
        return "regression_hist"

    @pytest.mark.spark
    @allure.title("Regression summary counts a new failure hidden by another catalog")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_regression_summary_keeps_catalogs_apart(self, spark, history_view):
        with allure.step("Run the hydrated regression_summary query"):
            row = spark.sql(self._queries(history_view)["regression_summary"], args={"sel_date": "All"}).first()

        with allure.step("Verify: the cat_a failure is counted"):
            assert row["newly_broken"] == 1
            assert row["recovered"] == 0

    @pytest.mark.spark
    @allure.title("Regression detail lists the newly failing check with its catalog")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_regression_detail_keeps_catalogs_apart(self, spark, history_view):
        with allure.step("Run the hydrated regression_detail query"):
            rows = spark.sql(self._queries(history_view)["regression_detail"], args={"sel_date": "All"}).collect()

        with allure.step("Verify: one NEW FAILURES row identified by catalog.schema.table"):
            assert [(r["table"], r["check_name"], r["change_type"]) for r in rows] == [
                ("cat_a.shared.t", "freshness", "NEW FAILURES")
            ]
