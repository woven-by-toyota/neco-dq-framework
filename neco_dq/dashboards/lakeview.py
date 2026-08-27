# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Generate Databricks AI/BI (Lakeview) dashboards from a versioned template.

A dashboard's ``serialized_dashboard`` is stored as a template
(``templates/dq_dashboard.lvdash.json``) with placeholders for the only values
that vary between deployments. A thin function fills the template and
creates/publishes it via the Lakeview REST API — no SQL is authored here, the
proven queries are reused verbatim.

Everything deployment-specific is a placeholder and every value is supplied by
the caller, so the template and this module carry no project-, catalog- or
schema-specific names:

    {{history_table}}       full 3-part name used in every dataset FROM
    {{catalog}}             the ``WHERE catalog = '...'`` filter value
    {{schemas}}             the ``schema IN (...)`` list (all tiers, quoted CSV)
    {{schemas_bronze|silver|gold}}  per-tier schema lists for the coverage pies
    {{ge_layer_case}}       GE-folding schema→layer CASE (query side)
    {{schema_layer_case}}   Coverage schema→layer CASE (query side)
    {{schema_layer_case_bt}}  widget-side computed-column CASE (backtick-quoted)
    {{expected_tables}}     the Coverage CTE of expected (schema, table) pairs
    {{expected_filter}}     WHERE predicate scoping EVERY dataset to those pairs
    {{warehouse_id}}        default warehouse for the published dashboard

The three medallion tiers (bronze/silver/gold) are the one fixed concept — the
dashboard has a coverage pie and a layer axis per tier — but which schema(s)
map to each tier is entirely caller-supplied via ``layers``.
"""

from __future__ import annotations

import json
import logging
from importlib import resources
from typing import Any

logger = logging.getLogger("neco_dq")

_TEMPLATE_PACKAGE = "neco_dq.dashboards.templates"
_DEFAULT_TEMPLATE = "dq_dashboard.lvdash.json"

# The three medallion tiers are the dashboard's fixed structure (three coverage
# pies, a layer axis). Only the *schemas* mapped to each tier vary per team.
_MEDALLION_LAYERS = ("bronze", "silver", "gold")

# Placeholder tokens as they appear in the template artifact.
_TOK_HISTORY = "{{history_table}}"
_TOK_CATALOG = "{{catalog}}"
_TOK_SCHEMAS = "{{schemas}}"
_TOK_WAREHOUSE = "{{warehouse_id}}"
_TOK_GE_CASE = "{{ge_layer_case}}"
_TOK_SCHEMA_CASE = "{{schema_layer_case}}"
_TOK_SCHEMA_CASE_BT = "{{schema_layer_case_bt}}"
_TOK_EXPECTED = "{{expected_tables}}"
_TOK_SCHEMAS_BRONZE = "{{schemas_bronze}}"
_TOK_SCHEMAS_SILVER = "{{schemas_silver}}"
_TOK_SCHEMAS_GOLD = "{{schemas_gold}}"
_TOK_EXPECTED_FILTER = "{{expected_filter}}"


def _check_ident(name: str, kind: str) -> None:
    """Reject empty / non-string / statement-breaking identifiers before SQL embed."""
    if not isinstance(name, str) or not name or any(c in name for c in ("'", "`", ";", "\n", "\r", "\t", " ")):
        raise ValueError(f"invalid {kind}: {name!r}")


def _quoted_csv(values: list[str]) -> str:
    """Render values as a SQL quoted CSV, e.g. ``'a', 'b', 'c'``."""
    return ", ".join(f"'{v}'" for v in values)


def _validate_layers(layers: dict[str, list[str]]) -> None:
    extra = set(layers) - set(_MEDALLION_LAYERS)
    if extra:
        raise ValueError(f"unexpected layer(s): {sorted(extra)}; allowed: {list(_MEDALLION_LAYERS)}")
    for layer in _MEDALLION_LAYERS:
        schemas = layers.get(layer)
        if not schemas:
            raise ValueError(f"layer {layer!r} must map to a non-empty schema list")
        for s in schemas:
            _check_ident(s, "schema name")


def _all_schemas(layers: dict[str, list[str]]) -> list[str]:
    """Flatten every schema across tiers, in medallion order."""
    out: list[str] = []
    for layer in _MEDALLION_LAYERS:
        out.extend(layers[layer])
    return out


def _build_ge_layer_case(layers: dict[str, list[str]]) -> str:
    """GE-folding CASE: map a GE row's schema to its medallion tier."""
    whens = "\n".join(
        f"      WHEN test_suite = 'great_expectations' AND schema IN ({_quoted_csv(layers[layer])}) THEN '{layer}'"
        for layer in _MEDALLION_LAYERS
    )
    return f"CASE\n{whens}\n      ELSE test_suite\n    END AS layer"


def _build_schema_layer_case(layers: dict[str, list[str]]) -> str:
    """Coverage CASE on ``e.schema`` (expected-table alias)."""
    whens = "\n".join(
        f"    WHEN e.schema IN ({_quoted_csv(layers[layer])}) THEN '{layer}'" for layer in _MEDALLION_LAYERS
    )
    return f"CASE\n{whens}\n  END AS layer"


def _build_schema_layer_case_bt(layers: dict[str, list[str]]) -> str:
    """Widget-side computed column CASE (backtick-quoted, falls back to schema)."""
    whens = " ".join(f"WHEN `schema` IN ({_quoted_csv(layers[layer])}) THEN '{layer}'" for layer in _MEDALLION_LAYERS)
    return f"CASE {whens} ELSE `schema` END"


def _build_expected_tables(catalog: str, expected_tables: list[tuple[str, str]]) -> str:
    """Build the ``expected_tables`` CTE from (schema, table) pairs."""
    if not expected_tables:
        raise ValueError("expected_tables must be a non-empty list")
    rows = []
    for i, pair in enumerate(expected_tables):
        schema, table = pair
        _check_ident(schema, "expected-table schema")
        _check_ident(table, "expected-table name")
        if i == 0:
            rows.append(f"  SELECT '{catalog}' AS catalog, '{schema}' AS schema, '{table}' AS table")
        else:
            rows.append(f"  UNION ALL SELECT '{catalog}', '{schema}', '{table}'")
    body = "\n".join(rows)
    return f"expected_tables AS (\n{body}\n)"


def _build_expected_filter(expected_tables: list[tuple[str, str]]) -> str:
    """Build a WHERE predicate restricting a base scan to the expected tables.

    Injected into every dataset's base scan (before the window functions) so the
    whole dashboard — not just the Coverage widget — reports on exactly the
    declared ``(schema, table)`` set. Matches on ``schema.table`` so it is
    independent of how each query aliases its columns.
    """
    if not expected_tables:
        raise ValueError("expected_tables must be a non-empty list")
    keys = []
    for schema, table in expected_tables:
        _check_ident(schema, "expected-table schema")
        _check_ident(table, "expected-table name")
        keys.append(f"'{schema}.{table}'")
    return f" AND CONCAT(schema, '.', table) IN ({', '.join(keys)})"


def load_template(template: str = "reference") -> dict[str, Any]:
    """Load a bundled dashboard template artifact.

    ``template="reference"`` maps to the packaged reference dashboard. Any other
    value is treated as a resource filename within the templates package.
    """
    resource = _DEFAULT_TEMPLATE if template == "reference" else template
    try:
        raw = resources.files(_TEMPLATE_PACKAGE).joinpath(resource).read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError) as exc:
        raise FileNotFoundError(f"dashboard template not found: {resource}") from exc
    return json.loads(raw)


def hydrate_template(
    *,
    history_table: str,
    catalog: str,
    warehouse_id: str,
    layers: dict[str, list[str]],
    expected_tables: list[tuple[str, str]],
    display_name: str | None = None,
    template: str = "reference",
) -> dict[str, Any]:
    """Fill a dashboard template into a ready-to-POST create/patch payload.

    Pure function — no I/O beyond reading the bundled template.

    Args:
        history_table: Full 3-part history table (every dataset ``FROM``).
        catalog: Value for the ``WHERE catalog = '...'`` filter.
        warehouse_id: SQL warehouse for the published dashboard.
        layers: Maps each medallion tier (``bronze``/``silver``/``gold``) to the
            schema(s) that belong to it. Drives the layer ``CASE`` maps, the
            ``schema IN (...)`` filters and the per-tier coverage pies, e.g.
            ``{"bronze": ["raw"], "silver": ["clean"], "gold": ["mart"]}``.
        expected_tables: ``(schema, table)`` pairs the Coverage widget treats as
            "should exist", e.g. ``[("mart", "orders"), ("clean", "events")]``.
        display_name: Dashboard title (falls back to the template's).
        template: Which bundled template to hydrate.

    Returns a dict with ``display_name``, ``warehouse_id`` and a fully-substituted
    ``serialized_dashboard``. Raises ``ValueError`` on invalid input or if any
    placeholder survives substitution.
    """
    _validate_layers(layers)
    _check_ident(history_table, "history_table")
    _check_ident(catalog, "catalog")
    _check_ident(warehouse_id, "warehouse_id")

    tpl = load_template(template)
    serialized = tpl["serialized_dashboard"]

    replacements = {
        _TOK_HISTORY: history_table,
        _TOK_CATALOG: catalog,
        _TOK_SCHEMAS: _quoted_csv(_all_schemas(layers)),
        _TOK_WAREHOUSE: warehouse_id,
        _TOK_GE_CASE: _build_ge_layer_case(layers),
        _TOK_SCHEMA_CASE: _build_schema_layer_case(layers),
        _TOK_SCHEMA_CASE_BT: _build_schema_layer_case_bt(layers),
        _TOK_EXPECTED: _build_expected_tables(catalog, expected_tables),
        _TOK_EXPECTED_FILTER: _build_expected_filter(expected_tables),
        _TOK_SCHEMAS_BRONZE: _quoted_csv(layers["bronze"]),
        _TOK_SCHEMAS_SILVER: _quoted_csv(layers["silver"]),
        _TOK_SCHEMAS_GOLD: _quoted_csv(layers["gold"]),
    }
    # The placeholders live inside JSON string values (queryLines, widget
    # expressions), so replacement values must be JSON-escaped — the multi-line
    # CASE / CTE fragments contain newlines that would otherwise break the JSON.
    # json.dumps(...) yields a quoted, escaped string; strip the outer quotes.
    for token, value in replacements.items():
        escaped = json.dumps(value)[1:-1]
        serialized = serialized.replace(token, escaped)

    # Fail loud if a placeholder leaked through — a partially-hydrated dashboard
    # would fail opaquely at query time on the warehouse instead.
    leaked = [t for t in replacements if t in serialized]
    if leaked:
        raise ValueError(f"template placeholders not substituted: {leaked}")

    # Validate the result is still parseable JSON (substitution can't break the
    # envelope, but a malformed value would surface here rather than at the API).
    json.loads(serialized)

    return {
        "display_name": display_name or tpl.get("display_name") or "DQ Dashboard",
        "warehouse_id": warehouse_id,
        "serialized_dashboard": serialized,
    }


def iter_dataset_queries(serialized_dashboard: str) -> list[tuple[str, str]]:
    """Yield ``(dataset_name, query_text)`` for every dataset in a dashboard.

    Used to validate each query against the warehouse before publishing.
    """
    sd = json.loads(serialized_dashboard)
    out: list[tuple[str, str]] = []
    for ds in sd.get("datasets", []):
        query = "".join(ds.get("queryLines", [])).rstrip().rstrip(";")
        out.append((ds.get("displayName") or ds.get("name", "?"), query))
    return out


def _workspace_client():
    """Instantiate a Databricks WorkspaceClient (lazy import; optional dep)."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:  # pragma: no cover - exercised only without the dep
        raise ImportError(
            "generate_dashboard requires the 'databricks-sdk' package. Install it with: pip install databricks-sdk"
        ) from exc
    # No-arg construction picks up DATABRICKS_HOST / client-id / secret from the
    # environment, matching how the rest of the framework authenticates.
    return WorkspaceClient()


def _discover_warehouse(client) -> str:
    """Return a warehouse id from the workspace, preferring a RUNNING one.

    Used when the caller omits ``warehouse_id``. Not deterministic — it picks the
    first RUNNING warehouse (or the first of any state); pass ``warehouse_id``
    explicitly when the target must be predictable.
    """
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("no SQL warehouse found in the workspace; pass warehouse_id explicitly")
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    chosen = running[0] if running else warehouses[0]
    logger.info("Auto-discovered warehouse %s (%s)", chosen.id, chosen.name)
    return chosen.id


def _validate_queries(client, warehouse_id: str, serialized_dashboard: str) -> None:
    """Run every dataset query on the warehouse; raise on the first failure.

    Most datasets reference the ``:sel_date`` Run Date parameter that the
    dashboard supplies at runtime. Run each query with ``sel_date='All'`` (the
    "unfiltered" sentinel the queries fall back on) so a standalone validation
    doesn't fail with UNBOUND_SQL_PARAMETER — the query would be valid in the
    dashboard but error here without the binding.
    """
    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

    params = [StatementParameterListItem(name="sel_date", value="All", type="STRING")]

    for name, query in iter_dataset_queries(serialized_dashboard):
        if not query.strip():
            continue
        # Only bind sel_date for queries that actually use it — passing an unused
        # parameter is harmless, but this keeps the intent explicit.
        stmt_params = params if ":sel_date" in query else None
        resp = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="50s",
            parameters=stmt_params,
        )
        state = resp.status.state if resp.status else None
        if state != StatementState.SUCCEEDED:
            msg = ""
            if resp.status and resp.status.error:
                msg = resp.status.error.message or ""
            raise RuntimeError(f"dataset query failed [{name}]: {state} {msg[:200]}")
        logger.debug("dataset query OK: %s", name)


def generate_dashboard(
    *,
    history_table: str,
    catalog: str,
    parent_path: str,
    layers: dict[str, list[str]],
    expected_tables: list[tuple[str, str]],
    warehouse_id: str | None = None,
    display_name: str = "DQ Dashboard",
    dashboard_id: str | None = None,
    mode: str = "create",
    template: str = "reference",
    validate_queries: bool = True,
    publish: bool = True,
) -> dict[str, Any]:
    """Create or update a Lakeview dashboard from the bundled template.

    Args:
        history_table: Full 3-part history table name (dataset ``FROM`` source).
        catalog: Value for the ``WHERE catalog = '...'`` filter.
        parent_path: Workspace folder to create the dashboard in (create mode).
        layers: Medallion tier → schema(s) map (see :func:`hydrate_template`),
            e.g. ``{"bronze": ["raw"], "silver": ["clean"], "gold": ["mart"]}``.
        expected_tables: ``(schema, table)`` pairs for the Coverage widget.
        warehouse_id: SQL warehouse backing the published dashboard. If omitted,
            the first RUNNING warehouse in the workspace is used (non-deterministic
            — pass it explicitly when the target must be predictable).
        display_name: Dashboard title.
        dashboard_id: Existing dashboard to patch (required for ``mode="update"``).
        mode: ``"create"`` (POST a new dashboard — fails with ``AlreadyExists`` if
            one with the same name exists in ``parent_path``) or ``"update"``
            (PATCH the dashboard given by ``dashboard_id``).
        template: Which bundled template to hydrate ("reference").
        validate_queries: Run each dataset query on the warehouse before publish.
        publish: Publish after create/patch so viewers see the change.

    Returns a curated summary dict with ``dashboard_id``, ``display_name``,
    ``url`` (published-dashboard link, or ``None`` if the host is unknown),
    ``action`` (``"created"`` / ``"updated"``) and ``published``.
    """
    if mode not in ("create", "update"):
        raise ValueError(f"invalid mode: {mode!r}")
    if mode == "update" and not dashboard_id:
        raise ValueError("mode='update' requires dashboard_id")

    client = _workspace_client()
    if warehouse_id is None:
        warehouse_id = _discover_warehouse(client)

    payload = hydrate_template(
        history_table=history_table,
        catalog=catalog,
        warehouse_id=warehouse_id,
        layers=layers,
        expected_tables=expected_tables,
        display_name=display_name,
        template=template,
    )

    if validate_queries:
        logger.info("Validating dataset queries on warehouse %s", warehouse_id)
        _validate_queries(client, warehouse_id, payload["serialized_dashboard"])

    do_create = mode == "create"

    if do_create:
        logger.info("Creating dashboard %r under %s", display_name, parent_path)
        result = client.api_client.do(
            "POST",
            "/api/2.0/lakeview/dashboards",
            body={
                "display_name": payload["display_name"],
                "warehouse_id": warehouse_id,
                "parent_path": parent_path,
                "serialized_dashboard": payload["serialized_dashboard"],
            },
        )
        dashboard_id = result.get("dashboard_id")
    else:
        # Patch requires the CURRENT etag — always GET fresh right before.
        current = client.api_client.do("GET", f"/api/2.0/lakeview/dashboards/{dashboard_id}")
        logger.info("Patching dashboard %s", dashboard_id)
        result = client.api_client.do(
            "PATCH",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}",
            body={
                "serialized_dashboard": payload["serialized_dashboard"],
                "etag": current.get("etag"),
                "warehouse_id": warehouse_id,
                "display_name": payload["display_name"],
            },
        )

    if publish and dashboard_id:
        logger.info("Publishing dashboard %s", dashboard_id)
        client.api_client.do(
            "POST",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
            body={"warehouse_id": warehouse_id},
        )

    # On update the PATCH response may omit dashboard_id, but the caller gave us
    # one; prefer the known id and only fall back to the response field.
    effective_id = dashboard_id or result.get("dashboard_id")
    url = _dashboard_url(client, effective_id)
    _print_confirmation(payload["display_name"], effective_id, url, do_create, published=publish)

    # Return a small, curated summary rather than the raw API response: the raw
    # response embeds the full serialized dashboard JSON, so a plain print() of it
    # dumps an unreadable wall of text. These are the fields a caller actually
    # wants (the raw response is rarely useful once the dashboard exists).
    return {
        "dashboard_id": effective_id,
        "display_name": payload["display_name"],
        "url": url,
        "action": "created" if do_create else "updated",
        "published": bool(publish),
    }


def _dashboard_url(client, dashboard_id: str | None) -> str | None:
    """Build the published-dashboard URL, or None if the id/host is unknown."""
    if not dashboard_id:
        return None
    host = (getattr(client.config, "host", "") or "").rstrip("/")
    return f"{host}/dashboardsv3/{dashboard_id}/published" if host else None


def _print_confirmation(
    display_name: str, dashboard_id: str | None, url: str | None, created: bool, published: bool
) -> None:
    """Print a terminal confirmation with the dashboard link after generation."""
    action = "Created" if created else "Updated"

    lines = [
        "",
        "DQ Dashboard generated successfully",
        f"Name:      {display_name}",
        f"ID:        {dashboard_id or '?'}",
        f"Action:    {action}{' + published' if published else ' (draft, not published)'}",
        f"Open:      {url or dashboard_id or '?'}",
        "",
    ]
    # Intentional user-facing terminal output (not logging) — the caller runs
    # this as a one-off script and wants a visible confirmation + link.
    print("\n".join(lines))  # noqa: T201
