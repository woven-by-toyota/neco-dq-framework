# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0
"""Declarative parameter schema for data-quality checks.

Single source of truth for the configuration surface of every check, used for:

1. **Alias resolution** — map deprecated parameter keys to their canonical
   names (Phase 1 renames) so existing configs keep working.
2. **Unknown-key validation** — warn (or, under an opt-in gate, reject) config
   keys that no check understands, catching typos early.
3. **Documentation / tests** — a machine-readable inventory cross-checked
   against the suites' ``_require_cfg`` calls.

Design notes
------------
- Schema entries use **canonical** (post-rename) key names in ``required`` /
  ``optional``. The ``aliases`` map translates ``old -> new``.
- Aliases may be *dotted* (``"lateness.column": "lateness.timestamp_column"``)
  to rename a key nested inside a dict value. The resolver walks the path.
- ``UNIVERSAL_KEYS`` are accepted on every check and never flagged as unknown;
  they are intentionally omitted from per-check ``required`` / ``optional``.
- Checks absent from the schema are skipped by validation (no false warnings)
  — the schema is additive and does not need to be exhaustive to be safe.
- The three shared checks (``distinct_count``, ``json_schema``, ``freshness``,
  ``business_rules``, ``compare_metric``) are defined once in ``_SHARED_CHECKS``
  and composed into the suites that mix in ``SharedChecksMixin``. Bronze gets
  only ``json_schema`` and ``business_rules`` (Option A split-mixin).

Phase 1 (this module) covers pure key renames. Phase 2 shape-changing
transforms (value rescales, dict nesting) are layered on later via callable
aliases; the resolver already tolerates callables for forward compatibility.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

# Keys accepted on any check instance; never reported as unknown.
UNIVERSAL_KEYS: frozenset[str] = frozenset(
    {
        "enabled",
        "severity",
        "blocking",
        "tags",
        "threshold",
        "name",
    }
)

# An alias target is one of:
#   - a canonical key name (str): a pure Phase 1 key rename;
#   - a ``(label, fn)`` tuple: a Phase 2 shape transform. ``fn(cfg) -> cfg``
#     rewrites the whole dict (rescale a value, nest flat keys into a block);
#     ``label`` is the human-readable "new" name shown in deprecation reports.
# A bare callable is accepted too (labelled ``<transform>``) for forward compat.
ShapeTransform = Callable[[dict], dict]
AliasTarget = str | ShapeTransform | tuple[str, ShapeTransform]


# ---------------------------------------------------------------------------
# Phase 2 shape transforms (old config shape -> new shape).
#
# Each is a pure, idempotent ``cfg -> cfg`` function applied by
# ``resolve_aliases`` when its trigger key is present. Transforms return a new
# dict and never mutate the input; a transform that finds the config already in
# the new shape is a no-op (and so emits no deprecation, see ``resolve_aliases``).
# ---------------------------------------------------------------------------


_FQ_PARTS_FULL = 3  # catalog.schema.table
_FQ_PARTS_SCHEMA_TABLE = 2  # schema.table


def _split_fq(fq: Any) -> dict:
    """Split ``'catalog.schema.table'`` (or ``'schema.table'``) into a block."""
    parts = str(fq).split(".")
    if len(parts) == _FQ_PARTS_FULL:
        return {"catalog": parts[0], "schema": parts[1], "table": parts[2]}
    if len(parts) == _FQ_PARTS_SCHEMA_TABLE:
        return {"schema": parts[0], "table": parts[1]}
    return {"table": str(fq)}


def _nest_fq(src_key: str, dst_key: str) -> ShapeTransform:
    """Transform: fully-qualified string at ``src_key`` -> nested block ``dst_key``."""

    def _fn(cfg: dict) -> dict:
        if src_key not in cfg:
            return cfg
        cfg = dict(cfg)
        value = cfg.pop(src_key)
        if dst_key not in cfg:
            cfg[dst_key] = _split_fq(value)
        return cfg

    return _fn


def _split_dict(src_key: str, mapping: dict[str, str]) -> ShapeTransform:
    """Transform: a ``{sub: val}`` dict at ``src_key`` -> flat ``mapping[sub]`` keys."""

    def _fn(cfg: dict) -> dict:
        if src_key not in cfg:
            return cfg
        cfg = dict(cfg)
        block = cfg.pop(src_key)
        if isinstance(block, dict):
            for sub, dst in mapping.items():
                if sub in block:
                    cfg.setdefault(dst, block[sub])
        return cfg

    return _fn


def _collect_into(dst_key: str, mapping: dict[str, str]) -> ShapeTransform:
    """Transform: gather flat keys (``mapping`` old->new) into nested ``dst_key``."""

    def _fn(cfg: dict) -> dict:
        if not any(flat in cfg for flat in mapping):
            return cfg
        cfg = dict(cfg)
        block = dict(cfg.get(dst_key) or {})
        for flat, nested in mapping.items():
            if flat in cfg:
                block.setdefault(nested, cfg.pop(flat))
        if block:
            cfg[dst_key] = block
        return cfg

    return _fn


def _rename_if_str(src_key: str, dst_key: str) -> ShapeTransform:
    """Transform: rename ``src_key`` -> ``dst_key`` only when its value is a string.

    Disambiguates a key that is reused as a canonical nested block: the legacy
    string form is migrated, the new dict form is left untouched (a no-op).
    """

    def _fn(cfg: dict) -> dict:
        if not isinstance(cfg.get(src_key), str):
            return cfg
        cfg = dict(cfg)
        value = cfg.pop(src_key)
        cfg.setdefault(dst_key, value)
        return cfg

    return _fn


def _rescale_ratio_to_percent(src_key: str, dst_key: str) -> ShapeTransform:
    """Transform: value at ``src_key`` (0-1) -> ``dst_key`` (0-100), rescaled x100."""

    def _fn(cfg: dict) -> dict:
        if src_key not in cfg:
            return cfg
        cfg = dict(cfg)
        value = cfg.pop(src_key)
        if dst_key not in cfg and value is not None:
            cfg[dst_key] = value * 100
        return cfg

    return _fn


# Named Phase 2 transforms referenced by the schema entries below.
# silver.referential_integrity:
_REFINT_TARGET_NEST = _collect_into(
    "target",
    {
        "target_catalog": "catalog",
        "target_schema": "schema",
        "target_table": "table",
        "target_column": "column",
    },
)
_MIN_VALID_RESCALE = _rescale_ratio_to_percent("min_valid_ratio", "min_valid_percent")
# gold.reconciliation_expr:
_RECON_SILVER_EXPR = _rename_if_str("silver", "silver_expr")
_RECON_SOURCE_SILVER = _nest_fq("source_silver", "silver")
_RECON_FILTER = _split_dict("filter", {"gold": "gold_filter", "silver": "silver_filter"})
_RECON_JOIN_FILTER = _split_dict(
    "join_filter", {"gold_column": "gold_join_columns", "silver_column": "silver_join_columns"}
)
# gold.coverage:
_COVERAGE_SILVER_NEST = _nest_fq("silver_table", "silver")
# gold.trend:
_TREND_HISTORY_NEST = _collect_into(
    "history",
    {
        "history_table": "table",
        "window": "window",
        "min_history": "min_history",
        "tolerance_pct": "tolerance_percent",
    },
)


# ---------------------------------------------------------------------------
# Shared checks (SharedChecksMixin) — composed into silver, gold, and (subset)
# bronze. Canonical names reflect Phase 1 renames.
# ---------------------------------------------------------------------------
_SHARED_CHECKS: dict[str, dict[str, Any]] = {
    "distinct_count": {
        "required": ["column"],
        "optional": ["expected", "min_distinct_count", "max_distinct_count"],
        "aliases": {"min": "min_distinct_count", "max": "max_distinct_count"},
    },
    "json_schema": {
        "required": ["column"],
        "optional": [
            "required_fields",
            "field_types",
            "allowed_values",
            "array_element",
            "sample_size",
            "max_parse_failure_percent",
        ],
        "aliases": {"max_parse_failure_pct": "max_parse_failure_percent"},
    },
    "freshness": {
        "required": ["timestamp_column"],
        "optional": ["max_delay_hours"],
        "aliases": {"column": "timestamp_column"},
    },
    "business_rules": {
        "required": ["predicate"],
        "optional": [],
        "aliases": {"check": "predicate"},
    },
    "compare_metric": {
        # expected / expected_from / metric / observed are nested dicts passed through as-is;
        # ``value`` is the static expected value (resolve_expected dispatches to the static
        # resolver whenever ``value`` is present).
        "required": [],
        "optional": [
            "metric",
            "observed",
            "expected",
            "expected_from",
            "value",
            "operator",
            "tolerance_abs",
            "missing_expected_value",
            "include_details",
        ],
        "aliases": {"tolerance": "tolerance_abs"},
    },
}


_BRONZE_CHECKS: dict[str, dict[str, Any]] = {
    "record_count": {"required": [], "optional": [], "aliases": {}},
    "record_count_anomaly": {
        "required": [],
        "optional": ["history_table", "max_deviation_percent"],
        "aliases": {"allowed_deviation_percent": "max_deviation_percent"},
    },
    # Bronze-native freshness already uses the canonical timestamp_column.
    "freshness": {
        "required": ["timestamp_column"],
        "optional": ["max_delay_hours"],
        "aliases": {},
    },
    "table_freshness": {
        "required": [],
        "optional": ["max_staleness_days"],
        "aliases": {"max_days_stale": "max_staleness_days"},
    },
    "schema_drift": {"required": [], "optional": ["expected_columns"], "aliases": {}},
    "mandatory_fields": {"required": [], "optional": ["columns"], "aliases": {}},
    "binary_not_null": {"required": [], "optional": ["column"], "aliases": {}},
    "binary_non_empty": {"required": [], "optional": ["column"], "aliases": {}},
    "binary_size_guard": {
        "required": [],
        "optional": ["column", "min_bytes", "max_bytes"],
        "aliases": {},
    },
    "binary_type": {"required": [], "optional": ["column"], "aliases": {}},
    "binary_decodable": {
        "required": [],
        "optional": ["column", "format"],
        "aliases": {},
    },
    "binary_version_header_check": {
        "required": [],
        "optional": ["column", "magic_byte", "header_size_bytes"],
        "aliases": {},
    },
    "partition_exists": {
        "required": [],
        "optional": ["partitions", "partition_column", "expected_values"],
        "aliases": {},
    },
    "binary_schema_fingerprint": {
        "required": [],
        "optional": ["column", "allowed_fingerprints", "offset", "length_bytes"],
        "aliases": {},
    },
    # Shared checks extended to bronze (Option A split-mixin).
    "json_schema": _SHARED_CHECKS["json_schema"],
    "business_rules": _SHARED_CHECKS["business_rules"],
}


_SILVER_CHECKS: dict[str, dict[str, Any]] = {
    "idempotency": {
        "required": ["key_columns"],
        "optional": [],
        "aliases": {"key_column": "key_columns"},
    },
    "status_transition": {
        "required": ["key_columns", "status_column", "timestamp_column"],
        "optional": ["allowed_flow"],
        "aliases": {"key_column": "key_columns"},
    },
    "referential_integrity": {
        # rule-11: source_* optional, default to table_ctx (current table).
        # Phase 2: target is a nested {catalog, schema, table, column} block and
        # the valid threshold is a percent (0-100), not a 0-1 ratio.
        "required": ["source_column", "target"],
        "optional": [
            "source_catalog",
            "source_schema",
            "source_table",
            "min_valid_percent",
        ],
        "aliases": {
            "schema": "source_schema",
            "table": "source_table",
            "catalog": "source_catalog",
            # Phase 2 shape transforms.
            "target_schema": ("target: {catalog, schema, table, column}", _REFINT_TARGET_NEST),
            "min_valid_ratio": ("min_valid_percent", _MIN_VALID_RESCALE),
        },
    },
    "cardinality": {
        "required": [],
        "optional": ["group_by", "min_count"],
        "aliases": {"min": "min_count"},
    },
    "totals_consistency": {
        "required": [
            "detail_schema",
            "detail_table",
            "join_columns",
            "detail_amount_column",
            "header_amount_column",
        ],
        "optional": ["detail_catalog", "tolerance_abs"],
        "aliases": {
            "join_key": "join_columns",
            "catalog": "detail_catalog",
            "tolerance": "tolerance_abs",
        },
    },
    "cross_table_equality": {
        # rule-11: left_* optional, default to table_ctx (current table).
        "required": ["right_schema", "right_table", "join_columns", "left_column", "right_column"],
        "optional": ["left_schema", "left_table", "catalog", "tolerance_abs"],
        "aliases": {"join_key": "join_columns", "tolerance": "tolerance_abs"},
    },
    "temporal_consistency": {
        # rule-11: left_* optional, default to table_ctx (current table).
        "required": [
            "right_schema",
            "right_table",
            "join_columns",
            "left_timestamp_column",
            "right_timestamp_column",
        ],
        "optional": ["left_schema", "left_table", "catalog"],
        "aliases": {
            "join_key": "join_columns",
            "left_timestamp": "left_timestamp_column",
            "right_timestamp": "right_timestamp_column",
        },
    },
    "row_temporal_consistency": {
        "required": ["left_timestamp_column", "right_timestamp_column"],
        "optional": [],
        "aliases": {
            "left_timestamp": "left_timestamp_column",
            "right_timestamp": "right_timestamp_column",
        },
    },
    "flow_completeness": {
        "required": ["source_schema", "source_table", "join_columns"],
        "optional": [
            "tolerance_percent",
            "source_catalog",
            "source_filter",
            "target_filter",
            "lateness",
        ],
        # target_* keys are forbidden (current table is the implicit target).
        "forbidden": ["target_catalog", "target_schema", "target_table"],
        "aliases": {
            "join_key": "join_columns",
            "tolerance_pct": "tolerance_percent",
            "lateness.column": "lateness.timestamp_column",
        },
    },
    # Shared checks (SharedChecksMixin).
    **_SHARED_CHECKS,
}


_GOLD_CHECKS: dict[str, dict[str, Any]] = {
    "reconciliation_expr": {
        # Phase 2: source_silver string -> nested silver block; filter/join_filter
        # dicts -> flat <role>_filter / <role>_join_columns keys.
        "required": ["silver", "gold_expr", "silver_expr"],
        "optional": [
            "tolerance_percent",
            "group_by",
            "gold_filter",
            "silver_filter",
            "gold_join_columns",
            "silver_join_columns",
        ],
        "aliases": {
            # Order matters: free the `silver` name (expr -> silver_expr) before
            # source_silver populates `silver` with the nested table block.
            "gold": "gold_expr",
            "silver": ("silver_expr", _RECON_SILVER_EXPR),
            "source_silver": ("silver: {catalog, schema, table}", _RECON_SOURCE_SILVER),
            "tolerance": "tolerance_percent",
            "filter": ("gold_filter / silver_filter", _RECON_FILTER),
            "join_filter": ("gold_join_columns / silver_join_columns", _RECON_JOIN_FILTER),
        },
    },
    "referential_integrity": {
        # Gold variant: gold_column + nested silver block (already rule-11 shape).
        "required": ["gold_column", "silver"],
        "optional": [],
        "aliases": {},
    },
    "uniqueness": {"required": ["columns"], "optional": [], "aliases": {}},
    "coverage": {
        # Phase 2: silver reference is a nested {catalog, schema, table} block.
        "required": ["silver", "gold_expr", "silver_expr"],
        "optional": [
            "tolerance_percent",
            "join_type",
            "join_columns",
            "gold_join_columns",
            "silver_join_columns",
            "silver_filter",
        ],
        "aliases": {
            "tolerance_pct": "tolerance_percent",
            "join_keys": "join_columns",
            "gold_join_keys": "gold_join_columns",
            "silver_join_keys": "silver_join_columns",
            "silver_table": ("silver: {catalog, schema, table}", _COVERAGE_SILVER_NEST),
        },
    },
    "volume": {
        "required": ["metric"],
        "optional": ["group_by", "min_value", "max_value", "history"],
        "aliases": {
            "min": "min_value",
            "max": "max_value",
            "history.tolerance_pct": "history.tolerance_percent",
        },
    },
    "cross_gold": {
        # rule-11: left_table optional, defaults to table_ctx (current table).
        "required": ["right_table", "left_expr", "right_expr"],
        "optional": [
            "left_table",
            "left_filter",
            "right_filter",
            "group_by",
            "tolerance_abs",
            "raw_expr",
        ],
        "aliases": {"tolerance": "tolerance_abs"},
    },
    "completeness": {
        "required": ["column", "expected_values"],
        "optional": ["allow_extra"],
        "aliases": {},
    },
    "trend": {
        # `name` is a universal key but is also required by this check.
        "required": ["metric", "history"],
        "optional": ["direction"],
        "aliases": {
            "history_table": ("history: {table, window, min_history, tolerance_percent}", _TREND_HISTORY_NEST),
            "window": ("history: {table, window, min_history, tolerance_percent}", _TREND_HISTORY_NEST),
            "min_history": ("history: {table, window, min_history, tolerance_percent}", _TREND_HISTORY_NEST),
            "tolerance_pct": ("history: {table, window, min_history, tolerance_percent}", _TREND_HISTORY_NEST),
        },
    },
    # Shared checks (SharedChecksMixin).
    **_SHARED_CHECKS,
}


CHECK_PARAM_SCHEMA: dict[str, dict[str, dict[str, Any]]] = {
    "bronze": _BRONZE_CHECKS,
    "silver": _SILVER_CHECKS,
    "gold": _GOLD_CHECKS,
}


# ---------------------------------------------------------------------------
# Accessors / helpers (pure functions; no I/O, no logging).
# ---------------------------------------------------------------------------
def get_check_schema(suite: str, check_name: str) -> dict[str, Any] | None:
    """Return the schema entry for ``suite``/``check_name``, or ``None``."""
    return CHECK_PARAM_SCHEMA.get(suite, {}).get(check_name)


def allowed_keys(suite: str, check_name: str) -> set[str] | None:
    """Return the set of accepted *top-level* keys after alias resolution.

    Returns ``None`` when the check is not in the schema (validation skipped).
    """
    entry = get_check_schema(suite, check_name)
    if entry is None:
        return None
    keys: set[str] = set(UNIVERSAL_KEYS)
    keys.update(entry.get("required", []))
    keys.update(entry.get("optional", []))
    # Top-level portion of each alias target (dotted targets contribute the head).
    for target in entry.get("aliases", {}).values():
        if isinstance(target, str):
            keys.add(target.split(".", 1)[0])
    return keys


def _get_nested(cfg: dict, dotted: str) -> tuple[bool, Any]:
    """Return ``(present, value)`` for a possibly dotted key path."""
    cur: Any = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur


def _pop_nested(cfg: dict, dotted: str) -> Any:
    """Remove and return the value at a dotted key path (assumes present)."""
    parts = dotted.split(".")
    cur: Any = cfg
    for part in parts[:-1]:
        cur = cur[part]
    return cur.pop(parts[-1])


def _set_nested(cfg: dict, dotted: str, value: Any) -> None:
    """Set a value at a dotted key path, creating intermediate dicts."""
    parts = dotted.split(".")
    cur: dict = cfg
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


class CheckConfigError(ValueError):
    """Invalid check parameters under ``unknown_check_params: error``.

    Raised at config validation time; fatal for the whole run (not a check
    failure), so the orchestrator re-raises it instead of summarizing a crash.
    """


def resolve_aliases(suite: str, check_name: str, cfg: dict) -> tuple[dict, list[dict[str, str]]]:
    """Return a deep cfg copy with deprecated keys renamed, plus deprecation records.

    Each deprecation record is ``{"check", "old", "new"}``. If the canonical
    key is already present, the deprecated key is dropped (canonical wins) and
    a record is still emitted so the user learns to remove it.
    """
    entry = get_check_schema(suite, check_name)
    if entry is None or not entry.get("aliases"):
        return cfg, []

    new_cfg = copy.deepcopy(cfg)
    deprecations: list[dict[str, str]] = []
    for old, target in entry["aliases"].items():
        present, value = _get_nested(new_cfg, old)
        if not present:
            continue
        # Phase 2 shape transform: ``(label, fn)`` or a bare callable.
        label, fn = (target[0], target[1]) if isinstance(target, tuple) else ("<transform>", target)
        if callable(fn):
            transformed = fn(new_cfg)
            # A transform may be a no-op when the config is already in the new
            # shape (e.g. a key whose canonical name is reused). Only flag a
            # deprecation when it actually rewrote something.
            if transformed != new_cfg:
                deprecations.append({"check": check_name, "old": old, "new": label})
            new_cfg = transformed
            continue
        # Phase 1 pure key rename.
        _pop_nested(new_cfg, old)
        canonical_present, _ = _get_nested(new_cfg, target)
        if not canonical_present:
            _set_nested(new_cfg, target, value)
        deprecations.append({"check": check_name, "old": old, "new": target})
    return new_cfg, deprecations


def unknown_keys(suite: str, check_name: str, cfg: dict) -> list[str]:
    """Return top-level cfg keys not understood by the check (post-alias).

    Empty when the check is not in the schema (validation skipped). Keys
    starting with ``_`` are annotations (e.g. ``_reason`` / ``_confidence`` in
    profiling suggestions), not parameters, and are never reported.
    """
    allowed = allowed_keys(suite, check_name)
    if allowed is None:
        return []
    return sorted(k for k in cfg if k not in allowed and not str(k).startswith("_"))


def forbidden_keys_present(suite: str, check_name: str, cfg: dict) -> list[str]:
    """Return any forbidden keys present in ``cfg`` (e.g. flow_completeness)."""
    entry = get_check_schema(suite, check_name)
    if entry is None:
        return []
    return sorted(k for k in entry.get("forbidden", []) if k in cfg)
