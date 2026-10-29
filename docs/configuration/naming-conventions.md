# Parameter Naming Conventions

Every check reads its settings from a YAML block. Across ~35 checks and three
suites, those settings follow a small set of naming conventions so that a key
means the same thing wherever it appears. This page is the reference for those
conventions, for the deprecated keys that were renamed to follow them, and for
how the framework treats unknown keys.

!!! info "Scope"
    These conventions describe **check parameter keys**: the settings inside a
    check block (`timestamp_column`, `tolerance_abs`, …). They do not change the
    `threshold` block, which is documented in [Thresholds](thresholds.md), nor the
    suite/table structure (`catalog`, `schema`, `table`) of a config file.

---

## The conventions

1. **Columns end in `_column` or `_columns`**, with a role prefix when there is
   more than one. One column is `<role>_column` (e.g. `timestamp_column`,
   `status_column`, `source_column`); a list is `<role>_columns` (e.g.
   `key_columns`, `join_columns`, `gold_join_columns`).

2. **Numeric limits use `min_` / `max_` with a noun.** A bound is never a bare
   `min` / `max`; it names what it bounds: `min_count`, `min_value`, `max_value`,
   `min_distinct_count`, `max_distinct_count`, `min_bytes`, `max_bytes`.

3. **Percentages use `_percent` and are expressed on a 0–100 scale.** Keys such
   as `tolerance_percent`, `max_deviation_percent`, `max_parse_failure_percent`,
   `min_valid_percent` all take `5.0` to mean "5%". A genuine 0–1 fraction keeps
   the `_ratio` suffix instead; the suffix tells you the scale.

4. **Tolerance is always qualified.** A bare `tolerance` is forbidden because its
   unit is ambiguous. Use `tolerance_abs` for an absolute difference in the
   metric's own units, or `tolerance_percent` for a relative deviation (0–100).

5. **Time-unit limits are `max_<quantity>_<unit>`.** The unit is part of the key,
   so there is no ambiguity about seconds vs hours vs days: `max_delay_hours`,
   `max_staleness_days`.

6. **Referenced tables use a nested block with a role prefix.** A table other
   than the one under validation is given as a nested
   `{catalog, schema, table, column?}` block (e.g. the gold `referential_integrity`
   `silver:` block), or as role-prefixed flat keys (`source_schema`,
   `detail_table`, `right_schema`).

7. **Role prefixes are directional or symmetric.** Directional comparisons use
   `source_` / `target_` or `gold_` / `silver_` (the data flows one way).
   Symmetric comparisons use `left_` / `right_` (neither side is privileged).

8. **Filters are `<role>_filter`; a row assertion is `predicate`.** Row-scoping
   SQL is `source_filter`, `target_filter`, `left_filter`, `right_filter`. The
   boolean expression a row must satisfy (in `business_rules`) is `predicate`.

9. **Aggregate expressions are `<role>_expr`.** The SQL evaluated on each side of
   a comparison is `gold_expr`, `silver_expr`, `left_expr`, `right_expr`.

10. **Collections are `expected_*` / `allowed_*`.** Sets the data is checked
    against are named for their role: `expected_values`, `expected_columns`,
    `allowed_values`, `allowed_fingerprints`, `allowed_flow`.

11. **The current table is implicit.** The table a check runs on comes from its
    place in the config (its `table_ctx`); you do not re-declare it. Keys that
    could point back at it (`left_table`, `source_schema`, …) are therefore
    **optional** and default to the current table; supply them only to point at
    a *different* table. For the same reason, keys that would redefine the current
    table as the "other" side are **forbidden** (e.g. `target_*` on
    `flow_completeness`, whose implicit target is the current table).

---

## Deprecated keys and their canonical names

Keys that pre-dated these conventions still work: the framework accepts the old
name, rewrites it to the canonical one, and records a deprecation. The old names
are listed here so existing configs can be migrated.

### Shared checks (Bronze / Silver / Gold)

| Check | Deprecated key | Canonical key |
|-------|----------------|---------------|
| `distinct_count` | `min`, `max` | `min_distinct_count`, `max_distinct_count` |
| `freshness` | `column` | `timestamp_column` |
| `business_rules` | `check` | `predicate` |
| `json_schema` | `max_parse_failure_pct` | `max_parse_failure_percent` |
| `compare_metric` | `tolerance` | `tolerance_abs` |

### Bronze

| Check | Deprecated key | Canonical key |
|-------|----------------|---------------|
| `record_count_anomaly` | `allowed_deviation_percent` | `max_deviation_percent` |
| `table_freshness` | `max_days_stale` | `max_staleness_days` |

### Silver

| Check | Deprecated key | Canonical key |
|-------|----------------|---------------|
| `idempotency`, `status_transition` | `key_column` | `key_columns` |
| `cardinality` | `min` | `min_count` |
| `totals_consistency` | `join_key`, `catalog`, `tolerance` | `join_columns`, `detail_catalog`, `tolerance_abs` |
| `cross_table_equality` | `join_key`, `tolerance` | `join_columns`, `tolerance_abs` |
| `temporal_consistency`, `row_temporal_consistency` | `join_key`, `left_timestamp`, `right_timestamp` | `join_columns`, `left_timestamp_column`, `right_timestamp_column` |
| `flow_completeness` | `join_key`, `tolerance_pct` | `join_columns`, `tolerance_percent` |
| `referential_integrity` | `catalog`, `schema`, `table` | `source_catalog`, `source_schema`, `source_table` |

### Gold

| Check | Deprecated key | Canonical key |
|-------|----------------|---------------|
| `reconciliation_expr` | `gold`, `silver`, `tolerance` | `gold_expr`, `silver_expr`, `tolerance_percent` |
| `coverage` | `tolerance_pct`, `join_keys`, `gold_join_keys`, `silver_join_keys` | `tolerance_percent`, `join_columns`, `gold_join_columns`, `silver_join_columns` |
| `volume` | `min`, `max`, `history.tolerance_pct` | `min_value`, `max_value`, `history.tolerance_percent` |
| `cross_gold` | `tolerance` | `tolerance_abs` |

!!! note "Shape changes beyond renaming"
    A few parameters changed *shape*, not just name; their migration is handled
    automatically but is more than a key swap. The gold `coverage` `silver_table`
    string becomes a nested `silver: {catalog, schema, table}` block, and the
    silver `referential_integrity` `target_*` keys collapse into a nested `target`
    block while `min_valid_ratio` (0–1) becomes `min_valid_percent` (0–100). The
    per-check pages under [Test Suites & Checks](../test-suites/index.md) show the
    current shape.

!!! warning "`trend` keeps `tolerance_pct`"
    The gold `trend` check still uses `tolerance_pct` as its canonical key; it is
    not deprecated. Do not rename it to `tolerance_percent`.

---

## Deprecation timeline

| Release | Behaviour of deprecated keys |
|---------|------------------------------|
| **This release** | New keys are canonical. Old keys are still accepted and silently rewritten, but each one is reported (once per deprecated key, with the number of tables it was seen on) in the verbose run summary and in the programmatic `deprecations` output. |
| **Next major** | Aliases are removed. Legacy keys are no longer understood and are treated as unknown parameters (see below). |

Migrate on your own schedule before the next major: switch each config to the
canonical keys and confirm the deprecation report comes back empty.

!!! note "Check IDs of multi-instance checks"
    When a check has several instances, each result gets an ID built from the
    config **as written**, e.g. `idempotency[key_column=id]`. Upgrading without
    touching the config keeps the IDs you already have in history and
    dashboards. Renaming a key during migration (`key_column` → `key_columns`)
    changes the ID to `idempotency[key_columns=id]`, so history for that
    instance starts over under the new name.

---

## Unknown parameters

Once aliases are resolved, any remaining key a check does not understand is an
*unknown parameter*: usually a typo (`kye_columns`) or a leftover key. How the
framework reacts is controlled per run by the failure policy:

```yaml
dq_policy:
  unknown_check_params: warn   # warn (default) | error | ignore
```

| Mode | Effect |
|------|--------|
| `warn` *(default)* | Log a warning naming the unknown key (and suggesting the closest valid key, e.g. *did you mean 'key_columns'?*); the key is left in the config and the check still runs. |
| `error` | Raise a `CheckConfigError` (a `ValueError`) and fail the whole run, even if other suites succeeded; useful in CI to catch typos before they ship. |
| `ignore` | Accept unknown keys silently: no warning, no error. |

An invalid value for `unknown_check_params` falls back to `warn`. Checks that are
not described in the parameter schema are skipped by this validation, so a custom
or newly added check never produces false warnings.

The same policy governs **forbidden** keys (keys that are structurally disallowed
for a check, such as `target_*` on `flow_completeness`): `warn` logs that the key
is not allowed, `error` fails the run, `ignore` says nothing.
