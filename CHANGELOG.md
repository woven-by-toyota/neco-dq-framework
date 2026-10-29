# Changelog

---

## [1.4.1] - 2026-10-29

Fixes from the review of the 1.4.0 check-parameter standardization.

### Fixed

- `dq_policy.unknown_check_params` is now honored in normal `run_dq()` runs: the orchestrator passes `dq_policy` to every suite, so `error` and `ignore` no longer behave as `warn`.
- With `unknown_check_params: error`, invalid check parameters now fail the run (`CheckConfigError`) instead of being summarized as a suite crash that evaluated to a warning.
- Resolving deprecated nested keys (`history.tolerance_pct`, `lateness.column`) no longer mutates shared `default_checks`, so every table using them is counted in the deprecation report.
- Check IDs of multi-instance checks stay as in 1.3.0 for unchanged configs using deprecated keys (e.g. `idempotency[key_column=id]`); IDs are built from the config as written.
- List-valued join columns now work as documented: `gold_join_columns` / `silver_join_columns` in `reconciliation_expr` (with a length check), and `join_columns` in `cross_table_equality` and `temporal_consistency` join on every listed column.
- `flow_completeness` with list-valued `join_columns` no longer crashes when reporting missing records (failure stats listed the key list as a single nested item).
- Gold `coverage` accepts a single join column given as a string (`gold_join_columns: id`); it was split into characters.
- Investigation queries of the join-based silver checks (`totals_consistency`, `cross_table_equality`, `temporal_consistency`, `flow_completeness`) are now fully rendered, with one join condition per key column; previously they returned the raw template with unresolved placeholders.
- Profiling suggestions are valid configs for the checks they target: `_reason` / `_confidence` / `_comment` annotations are no longer reported as unknown parameters (they failed the run under `unknown_check_params: error`); the `trend` suggestion now uses the `name` + `history: {table, window, tolerance_percent}` shape; bronze `table_freshness` and `record_count_anomaly` suggestions use `max_staleness_days` and `max_deviation_percent`.
- `trend`, and `volume` with a `name`, now find their own history: the lookup uses the persisted check ID (`trend`, or `trend[name=...]` with several instances) instead of `name`, which never matched, so these checks always passed as "insufficient history".
- `reconciliation_expr` rejects `gold_join_columns` without `silver_join_columns` (or vice versa) instead of silently comparing against all Silver rows.
- Lakeview dashboard: the regression datasets (`regression_summary`, `regression_detail`) keep checks of the same `schema.table` in different catalogs (and suites) apart; the detail table shows `catalog.schema.table`. Regenerate dashboards to pick this up.

### Documentation

- All configuration examples use canonical check parameters (no deprecated keys, no parameters a check does not accept); the deprecated-keys tables in *Naming conventions* are complete, and the incorrect note saying `trend` keeps `tolerance_pct` was removed. `cross_table_equality` / `temporal_consistency` docs no longer advertise unsupported `left_catalog` / `right_catalog` keys.

---

## [1.4.0] - 2026-10-14

Check-parameter standardization: a single naming convention across every check, with a non-breaking deprecation path for the old keys.

### Added

#### Declarative check-parameter schema (`neco_dq/core/registry/check_param_schema.py`)

A single source of truth for every check's configuration surface: required keys, optional keys, deprecated-key aliases, and forbidden keys. It drives three things that used to drift independently: alias resolution, unknown-key validation, and the machine-readable inventory cross-checked against the suites' `_require_cfg` calls (new consistency test).

#### Config-parameter validation (`dq_policy.unknown_check_params`)

Unknown top-level check parameters (typically typos) are now surfaced, with a suggested correction via edit-distance (e.g. `kye_columns` → "did you mean 'key_columns'?"). Strictness is configurable:

| Setting | Behavior |
|---------|----------|
| `warn` (default) | Logs a warning and continues (honors the non-breaking policy) |
| `error` | Fails the run with a clear message naming the check and key |
| `ignore` | Restores the pre-validation behavior |

Forbidden keys (parameters that no longer apply because the current table is implicit, e.g. `target_*` on `flow_completeness`) are surfaced the same way.

#### Deprecation reporting

Deprecated-key usage is collected during a run (deduplicated per `(check, old_key)`, with the count of distinct tables that used it) and is available programmatically in the run summary under `deprecations`. A dedicated summary section renders it in **verbose runs only** (`run_dq(verbose=True)`), so normal runs stay clean. Nothing is printed mid-execution.

#### `json_schema` supports `BINARY` columns

`json_schema` now accepts `BINARY` columns (raw UTF-8 JSON bytes, as stored for raw Kafka payloads in Bronze): the column is decoded to STRING before `from_json`, so field/type/allowed-value validation works without a separate decode step. STRING and `STRUCT` / `ARRAY<STRUCT>` columns are unchanged. Investigation-query templates decode the column too, so they remain runnable against the physical table.

### Changed

#### Parameter renames (old keys still accepted, now deprecated)

All renames below are **non-breaking**: the old key keeps working via an alias and is flagged as deprecated. Templates, docs, and generated configs now emit the new names. The old keys are scheduled for removal in the next major release, at which point they become validation errors.

**Bronze**

| Check | Old key | New key |
|-------|---------|---------|
| `record_count_anomaly` | `allowed_deviation_percent` | `max_deviation_percent` |
| `table_freshness` | `max_days_stale` | `max_staleness_days` |

**Silver**

| Check | Old key | New key |
|-------|---------|---------|
| `idempotency`, `status_transition` | `key_column` | `key_columns` |
| `cardinality` | `min` | `min_count` |
| `totals_consistency` | `join_key`, `catalog`, `tolerance` | `join_columns`, `detail_catalog`, `tolerance_abs` |
| `cross_table_equality` | `join_key`, `tolerance` | `join_columns`, `tolerance_abs` |
| `temporal_consistency` | `join_key`, `left_timestamp`, `right_timestamp` | `join_columns`, `left_timestamp_column`, `right_timestamp_column` |
| `row_temporal_consistency` | `left_timestamp`, `right_timestamp` | `left_timestamp_column`, `right_timestamp_column` |
| `flow_completeness` | `join_key`, `tolerance_pct` | `join_columns`, `tolerance_percent` |
| `lateness` | `lateness.column` | `lateness.timestamp_column` |

**Gold**

| Check | Old key | New key |
|-------|---------|---------|
| `reconciliation_expr` | `gold`, `silver` (expr), `tolerance` | `gold_expr`, `silver_expr`, `tolerance_percent` |
| `coverage` | `tolerance_pct`, `join_keys`, `gold_join_keys`, `silver_join_keys` | `tolerance_percent`, `join_columns`, `gold_join_columns`, `silver_join_columns` |
| `volume` | `min`, `max`, `history.tolerance_pct` | `min_value`, `max_value`, `history.tolerance_percent` |
| `cross_gold` | `tolerance` | `tolerance_abs` |

**Shared checks** (apply wherever the check is exposed)

| Check | Old key | New key |
|-------|---------|---------|
| `distinct_count` | `min`, `max` | `min_distinct_count`, `max_distinct_count` |
| `freshness` | `column` | `timestamp_column` |
| `json_schema` | `max_parse_failure_pct` | `max_parse_failure_percent` |
| `business_rules` | `check` | `predicate` |
| `compare_metric` | `tolerance` | `tolerance_abs` |

#### Current table is now implicit (rule 11)

Checks that re-declared the table under validation no longer require it; the table always comes from `table_ctx`, and override keys are optional defaults:

- `referential_integrity`: `source_schema`/`source_table`/`source_catalog` are now **optional**, defaulting to the validated table (previously the required `schema`/`table` were reloaded by name).
- `cross_table_equality`, `temporal_consistency`: `left_schema`/`left_table`/`left_catalog` are now **optional**, defaulting to the validated table; only the `right` side is required.
- `cross_gold`: `left_table` is now **optional**, defaulting to the validated table; the signature is widened to `(df, cfg, table_ctx)`.

#### Dashboard generation: per-layer catalogs and optional tiers

`generate_dashboard()` / `hydrate_template()` (`neco_dq/dashboards/lakeview.py`) now support medallion tiers that live in **different catalogs** and dashboards that cover only **some** tiers.

- **`layers`** accepts, per tier, either the existing `list[str]` of schemas (which live in the global `catalog`) **or** a dict `{"catalog": ..., "schemas": [...]}` for a tier in its own catalog. Mixed forms are allowed.
- **Tiers are optional**: at least one is required; a bronze-only dashboard is valid. Absent tiers render empty widgets rather than being removed.
- **`catalog`** is now optional (required only if a tier uses the list form). The previous single-catalog call shape is unchanged and fully backward compatible.
- Every dataset is now scoped by a `CONCAT(catalog, '.', schema) IN (...)` pair predicate instead of a single `catalog = '...'` equality, so tiers that share a schema name across catalogs stay correctly separated. `expected_tables` entries resolve their catalog from `layers` and are matched on `catalog.schema.table`.
- **`expected_tables`** accepts, per entry, either the existing `(schema, table)` pair (catalog resolved from `layers`) **or** an explicit `(catalog, schema, table)` triple; the two forms may be mixed. The triple lets a schema name that lives in more than one catalog be referenced unambiguously — a bare pair for such a schema is now rejected as *ambiguous* and the triple names the catalog. A triple's `(catalog, schema)` must be declared in `layers`.

### Deprecated

- All old parameter keys listed above. They are accepted with a deprecation record this release and will be **removed in the next major release**, after which unknown legacy keys become errors (see `docs/configuration/naming-conventions.md`).

### Fixed

#### `binary_decodable` crash on malformed base64 under ANSI/Photon

The `base64_json` format used `unbase64()`, which under ANSI / Photon strict mode raises `CONVERSION_INVALID_INPUT` on the first non-base64 row (e.g. a column that already holds decoded JSON text), aborting the entire check instead of counting the row as a failure. It now uses `try_to_binary(col, 'BASE64')` (NULL on malformed input) and validates JSON structure via the first decoded byte, so bad rows are counted and evaluated against the configured `threshold`.

---

## [1.3.0] - 2026-07-31

Dashboard provisioning: generate a ready-to-use Databricks AI/BI (Lakeview) dashboard on top of the DQ history table.

### Added

#### DQ Dashboard Generation (Lakeview)

A new `neco_dq.dashboards` module builds a Databricks AI/BI (Lakeview) dashboard from a bundled, versioned template hydrated with your deployment's coordinates. No SQL or widgets are authored by hand: the proven queries are reused verbatim and only deployment-specific values are filled in.

Two public functions:

- `generate_dashboard(...)`: creates or updates a dashboard in the workspace, optionally validating every dataset query on a SQL warehouse and publishing the result. Returns a curated summary dict (`dashboard_id`, `display_name`, `url`, `action`, `published`).
- `hydrate_template(...)`: the pure function behind it. Returns the create/patch payload (fully-substituted `serialized_dashboard`) without any workspace I/O, useful for inspection and testing.

The dashboard is a single page with 8 datasets feeding Status, Coverage (per bronze/silver/gold tier), DQ Dimensions, Trends, KPIs, Regression, and global Filters. The bundled `.lvdash.json` template ships with the wheel.

Key inputs:

| Parameter | Purpose |
|-----------|---------|
| `history_table` | 3-part history table used in every dataset `FROM` |
| `catalog` | Value for the `WHERE catalog = '...'` filter |
| `layers` | Medallion tier → schema(s) map; drives the layer `CASE` maps, `schema IN (...)` filters and coverage pies |
| `expected_tables` | `(schema, table)` pairs that scope the **entire** dashboard, not just coverage: a table you omit is invisible in every widget |
| `warehouse_id` | SQL warehouse (optional; auto-discovers the first RUNNING one if omitted) |
| `mode` | `"create"` (default) or `"update"` (requires `dashboard_id`) |

Design notes:

- `create` is intentionally not idempotent and there is no lookup-by-name: the Lakeview list API is eventually consistent, so a just-created dashboard may not appear yet. Save the `dashboard_id` from creation and call again with `mode="update"` to patch.
- Query validation binds `sel_date='All'` (the "unfiltered" sentinel) so the seven datasets that reference the `:sel_date` Run Date parameter don't fail standalone with `UNBOUND_SQL_PARAMETER`.
- This is standalone by design, not wired into `run_dq()`: dashboard provisioning is infrequent and needs workspace credentials a normal DQ run does not.
- Inside a Databricks notebook nothing extra is needed. `databricks-sdk` ships with the runtime and `WorkspaceClient()` authenticates from the notebook context. Only external runs (local/CI) need `pip install databricks-sdk` and `DATABRICKS_*` env vars.

---

## [1.2.2] - 2026-06-30

Profiling suite improvements: enhanced suggestions, modular-config output, and a refactor of the suite.

### Added

#### Modular-Config Output for `suggest` Mode

`suggest` mode can write its suggestions directly as a modular-config directory tree that `run_dq` reads, instead of only flat per-suite YAML files or a Delta table. Set `suggest_output.modular_config_path` and it generates per-suite `suite.yaml`, one `test_groups/<table>.yaml` per table, GE configs under the owning suite's `great_expectations/`, and a default `global/policy.yaml`.

Options under `suggest_output`:

| Option | Default | Effect |
|--------|---------|--------|
| `modular_config_path` | (none) | Directory to write the modular tree into |
| `include_policy` | `true` | Write `global/policy.yaml` (required by the resolver) |
| `include_profiles` | `false` | Write a `global/profiles.yml` template |
| `parameterize_catalog` | `false` | Replace the catalog with `{{ profile.target_catalog }}` |
| `overwrite` | `false` | Replace existing files (skips them on local fs otherwise) |
| `dry_run` | `false` | Return the would-write paths without writing |

With `parameterize_catalog` and `include_profiles`, one generated config can target dev/stg/prod by switching profile. Files are named after the table only (catalog and schema are implied by the directory layout). Implemented as a new `ModularConfigOutputMixin`; suggestion-generation logic is unchanged.

#### New Check Suggestions

The suggest builders now propose additional checks:

- `distinct_count` (Silver): moderate-cardinality columns
- `trend` and `completeness` (Gold)
- `cross_table_equality` (Silver): tables sharing `*_id` columns
- Great Expectations: `length` (narrow-range string columns, backed by a new `ExpectColumnValueLengthsToBeBetween` handler), `date_format` (string columns holding dates in a consistent strftime format), and a `min_rows`/`max_rows` volume band

`date_format` detection samples non-null string values and proposes the first strftime format (e.g. `%Y-%m-%d`) that at least 90% of the sample matches. Only string columns are checked; real date/timestamp columns are typed and validated through `dtype`.

Cross-table layer detection gained `_infer_layer_from_schema`, which reads bronze/silver/gold from the schema name. Checks that need manual setup (`trend`, `completeness`, `cross_table_equality`, GE `length`, GE `date_format`, and `volume`) default to `enabled: false`.

#### New Great Expectations Rules

The GE suite builder gained four column/dataset rules, each backed by a real GE expectation so they validate when enabled (previously the suite only had not_null, unique, dtype, between, values_set, regex, length):

- `date_format` (`ExpectColumnValuesToMatchStrftimeFormat`): string values match a strftime date format. Fills the gap where dates stored as strings could only be checked with a fragile `regex`.
- `not_in_set` (`ExpectColumnValuesToNotBeInSet`): blocklist for forbidden/dirty values (e.g. `"N/A"`, `"unknown"`, `"-1"`).
- `distinct_values_in_set` (`ExpectColumnDistinctValuesToBeInSet`): the column's distinct value set must be a subset of the allowed set. Set-aware enum validation, complementary to `values_set` which works row-by-row with a `mostly` tolerance.
- `multicolumn_sum` (`ExpectMulticolumnSumToEqual`, dataset-level): per-row arithmetic invariant across columns (e.g. `subtotal + tax == total`).

Only `date_format` is also proposed by the profiling suggest builders; the other three are not reliably inferable from a snapshot and stay manual-only.

#### Confidence Metadata

Each suggestion carries `_confidence` (high/medium/low) and `_reason` so a reviewer can see why it was proposed. These `_`-prefixed fields are stripped from the emitted YAML by the formatter, so they appear only in the Delta-table output and in-memory results.

#### Summary Report

`suggest_output.summary_report_path` writes a `PROFILING_SUMMARY.md` with:

- Overview (tables profiled, total rows, total checks)
- Tables grouped by layer (bronze/silver/gold)
- Checks by suite with enabled/disabled tallies
- Checks requiring manual configuration (every `enabled: false` check)
- Schema highlights (JSON/date/PK-candidate/enum column counts)
- Data quality observations (high null ratio, single-value columns, name-based PII hints)
- A copy-paste Quick Start and a review checklist

All sections are derived from data already in the suggest results; no metrics are fabricated.

### Changed

#### Profiling Suite Split into Mixins

The `profiling_dq_test_suite.py` monolith (3119 lines, 89 methods) is now a package of nine responsibility-scoped mixins (`discovery`, `column_stats`, `json_detection`, `checks`, `suggestions_generic`, `suggestions_ge`, `suggestions_suites`, `cross_table`, `modes`) plus a shared `constants.py`. `SparkProfilingDQTestSuite` composes them via inheritance, with `DQTestSuite` last in the MRO so `super()` in the hook overrides still resolves. The main file drops to 214 lines (suite identity, hook overrides, `__init__`, four shared helpers, and `run()`).

The refactor is behavior-preserving: every method signature and the public import path are unchanged, the registry still resolves the suite, and the characterization tests stay green. The design-doc's builder/dataclass/Protocol rewrite is intentionally deferred.

#### Generated Config Readability

Generated `test_groups` and GE YAML now place a blank line before each check inside a `checks:` block, so the config is easier to scan. The spacing is valid YAML and round-trips through the resolver.

#### `volume` Disabled by Default

The Gold `volume` check now defaults to `enabled: false` because its `history.table` is a placeholder (`<catalog.schema.dq_history_table>`) that must be set before the check can run, matching how `record_count_anomaly` already behaves.

### Fixed

#### `date_format` Check Skipped for `%z`/`%Z` Directives

`_handle_date_format` in `ge_suite_builder.py` now skips the expectation entirely when the format string contains `%z` or `%Z`, emitting a `logging.warning` instead of raising. GE validates the format at construction using a naive `datetime.now()`, so `%z` is rejected before any data is checked. Even stripping `%z`, `strptime` fails on values like `2026-05-26T01:32:37.098+00:00` because the `+00:00` suffix is not consumed. The fix is to skip rather than silently produce a broken expectation. Use the `regex` check to validate timezone-aware timestamp formats.

#### `nan` String Coerced to `float('nan')` in Config

`_convert_string_value` (config resolver) called `float()` on every string, so a config value of `"nan"` became a real `float('nan')`. Downstream this crashed the GE suite: a `nan` inside a `value_set` makes `nan != nan`, so GE decides the suite changed under it and raises. The float branch now rejects non-finite forms (`nan`, `inf`, `-inf`, `infinity`) and keeps them as strings.

> Why: profiling samples NULLs as the literal string `"nan"` in `top_values`, so any generated config carrying that value broke GE at runtime. This is also why the bug hit `nan` and not `None`: `float("None")` already raised and stayed a string, `float("nan")` silently succeeded.

#### Suggested Checks Aligned to Real Check Handlers

An audit of the suggest builders against the checks each suite actually implements found suggestions that mapped to no handler, plus suggestions enabled as blocking on evidence too weak to support it. The profiling samples are statistical (`approx_count_distinct`, a small JSON sample), so a snapshot cannot prove uniqueness or a closed value set.

- GE `match_like` and a separate `_dataset.row_count` key were emitted but had no handler in `ge_suite_builder.py`, so enabling them did nothing (silent no-op). `match_like` is removed (redundant with the existing `regex` rule); the volume suggestion now emits `min_rows`/`max_rows`, the keys the builder actually handles.
- `unique` (GE), `uniqueness` (Gold), and `idempotency` (Silver) were suggested `enabled: true` and blocking from an approximate distinct ratio. They are now blocking only when the column(s) come from a Unity Catalog primary key; otherwise suggested `enabled: false` with a review comment, so a guess never blocks a pipeline.
- `mandatory_fields` (Bronze) dropped from `error/blocking` to a non-blocking `warning`. 0% nulls in a snapshot is an observation, not a contract; an optional field can be null later.
- `json_schema` (Silver/Gold) defaults to `enabled: false` when `required_fields`/`field_types` were inferred from a sample, because an optional field absent from the sample would make the check fail on first run.
- `distinct_count` (Silver) is no longer suggested for date/timestamp or id-like columns: their distinct count grows over time, so a fixed `max` guarantees false positives.
- `completeness` (Gold) is no longer suggested for free-text columns. It is now gated on `_is_enum_column`, matching the gate the GE `values_set` suggestion already used.

#### GE Check Names in the History Table

`GE_CHECK_NAME_MAP` in the result normalizer was missing entries for `length` and the four new GE rules, so those checks were written to the history table under their raw GE type (e.g. `expect_column_value_lengths_to_be_between`) instead of the framework name. They now map to `length`, `date_format`, `not_in_set`, `distinct_values_in_set`, and `multicolumn_sum`, matching the names used elsewhere. Investigation-query templates were added for `length`, `not_in_set`, and `distinct_values_in_set`.

> Why: the writer is schema-driven and never dropped the rows, so the bug was silent. Reports and the results viewer just showed an inconsistent raw name for these checks.

### Documentation

Aligned `neco_dq/docs/test-suites/great-expectations.md` and `profiling.md` with the current behavior:

- GE doc: documented every column and dataset rule the builder actually handles, including the ones that were missing (`nullable`, `length`, `date_format`, `not_in_set`, `distinct_values_in_set`, `integrity.pair_gt`, `integrity.pair_eq`, `multicolumn_sum`). Added `enabled: true` to the rule examples that were missing it; without it a GE rule is skipped silently, so the old examples were copy-paste traps. Added a timezone limitation note to `date_format`: formats with `%z`/`%Z` are not supported (see Fixed section); the `regex` check is the documented alternative.
- Profiling doc: corrected `mandatory_fields` to non-blocking `warning`, documented the primary-key-vs-candidate rule for uniqueness checks (enabled+blocking only with a UC primary key), added the `distinct_count` date/id carve-out, the gold `completeness`/`trend` suggestions, the GE `date_format` suggestion, and the `min_rows`/`max_rows` volume band. Added documentation for the previously undocumented output options: `modular_config_path` (with all sub-options: `include_profiles`, `include_policy`, `parameterize_catalog`, `overwrite`, `dry_run`) and `summary_report_path`. Delta table section updated to note that the table is created automatically and each run overwrites it.

---

## [1.2.1] - 2026-06-22

### Fixed

- `block_on_threshold` now evaluates the global failure rate across all suites, not per-suite
  > Why: The terminal logger shows a global success rate (total passed / total checks across all engines), but the policy was comparing the threshold against each engine individually. A suite with few checks but many failures (e.g., 4 fails on 5 checks = 80%) could trigger blocking even when the global rate was well below the configured threshold (e.g., 1.6% across 255 total checks). The fix aggregates `total_checks` and `failed` across all engines before comparing against `failure_threshold`, making the policy consistent with what the summary displays. Two regression tests added.

---

## [1.2.0] - 2026-06-18

Multi-environment configuration support via profiles.

### Added

#### Profiles System

New `profiles.yaml` file enables environment-specific configuration (dev/stg/prod):

```yaml
# profiles.yml
profiles:
  dev:
    target_catalog: analytics_dev
    variables:
      freshness_hours: 48
      min_record_count: 100

  prod:
    target_catalog: analytics_prod
    variables:
      freshness_hours: 12
      min_record_count: 10000

default_profile: dev
```

#### Jinja2 Variables in Config

Use `{{ profile.* }}` and `{{ var.* }}` in any YAML config:

| Variable | Description |
|----------|-------------|
| `{{ profile.name }}` | Active profile name (e.g., "dev", "prod") |
| `{{ profile.target_catalog }}` | Catalog from profile |
| `{{ var.* }}` | Any custom variable defined in `variables:` |

```yaml
tables:
  - catalog: "{{ profile.target_catalog }}"
    schema: bronze
    table: orders
    checks:
      freshness:
        enabled: true
        max_delay_hours: "{{ var.freshness_hours }}"
      mandatory_fields:
        enabled: true
        columns: "{{ var.mandatory_columns }}"
```

All types are auto-converted: strings, numbers, booleans, and lists.

#### New `profile` Parameter

```python
from neco_dq.runner import run_dq

# Explicit profile
run_dq("/path/to/config", profile="prod")

# From environment variable (NECO_DQ_PROFILE=prod)
run_dq("/path/to/config")
```

#### Profile Policy Overrides

Override DQ policy per environment:

```yaml
profiles:
  dev:
    target_catalog: analytics_dev
    policy:
      failure_threshold: 0.5   # block only if >50% fail
      block_on_threshold: false
      block_test_suites:
        bronze: false          # don't block on bronze in dev

  prod:
    target_catalog: analytics_prod
    policy:
      failure_threshold: 0.05  # block if >5% fail
      block_on_threshold: true # FAIL if threshold exceeded (even non-blocking)
```

Profile policy values override `global/policy.yaml`. `block_test_suites` is deep-merged.

#### Profile Inheritance

Profiles can extend other profiles using `extends`:

```yaml
profiles:
  base:
    variables:
      freshness_hours: 24
      min_record_count: 1000

  dev:
    extends: base
    target_catalog: analytics_dev
    variables:
      freshness_hours: 48  # override base

  prod:
    extends: base
    target_catalog: analytics_prod
```

- Child values override parent values
- Variables are deep-merged (child adds/overrides, parent values preserved)
- Multi-level inheritance supported (child → parent → grandparent)
- Circular inheritance detected and raises `CircularInheritanceError`

#### Profile Resolution Order

1. Explicit `profile` parameter (highest priority)
2. `NECO_DQ_PROFILE` environment variable
3. `default_profile` from profiles.yml
4. `"default"` fallback

#### Profile Context in Execution Trace

When profiles are used, check results include profile info in the `execution` field:

```yaml
execution:
  duration_ms: 1234
  rows_scanned: 50000
  profile:
    name: "prod"
    target_catalog: "analytics_prod"
```

This enables:
- **Traceability**: Know which profile was used for each check result
- **Debugging**: "This failed in prod but not in dev", check `execution.profile`
- **Audit**: Historical results show the environment context

The `profile` field is only present when profiles are actively used (not when using single-file configs without profiles).

#### Enhanced Slack Alerts

Redesigned Slack notifications with consolidated summary format:

```
*Data Quality Summary*
`qa` • 2026-06-12 15:30 UTC

*Status:* *FAIL* (3 blocking)
*Success Rate:* 90.2% (55/61 checks)  •  Threshold: 10%

bronze         🟨    10/11
silver         🟩    15/15
gold           🟥    30/35

*Top Failures:*
• `freshness` → gold.aggregated_metrics (summary_created_at)
• `referential_integrity` → gold.aggregated_metrics (record_id)
```

**New Slack options:**

| Option | Default | Description |
|--------|---------|-------------|
| `send_always` | `false` | Send on PASS and WARN (not just FAIL) |
| `mention_on_fail` | `false` | Add `@channel` mention on FAIL |
| `include_job_link` | `true` | Include Databricks job URL |
| `max_failures_shown` | `5` | Max failures in Top Failures list |
| `compact_mode` | `false` | Single-line summary format |

```yaml
alerts:
  slack:
    enabled: true
    send_always: true
    mention_on_fail: true
```

**Other improvements:**
- Profile name shown in Slack message (if not "default")
- Environment auto-detected from Databricks workspace URL (hidden if "unknown")
- Colored squares for suite status: 🟥 FAIL, 🟨 WARN, 🟩 PASS
- Job link only shown when running on Databricks

### New Modules

| Module | Purpose |
|--------|---------|
| `neco_dq/config/context.py` | `DQContext` dataclass for runtime profile data |
| `neco_dq/config/profile_loader.py` | Load and resolve profiles from profiles.yml |

### Changed

- `resolve_config()` now accepts optional `context: DQContext` parameter
- `run_dq()` now accepts optional `profile: str` parameter
- All internal config functions propagate context for Jinja2 rendering
- `_build_execution_trace()` now includes `profile` when profile context is set
- `failure_threshold` is now informational only: shown in summary and policy message when exceeded
- Blocking failures always cause FAIL regardless of threshold
- Non-blocking failures always cause WARN (threshold doesn't suppress warnings)
- Summary now shows threshold when configured: `Success Rate: 85.7%  │  Threshold: 10%  │  Status: WARN`
- Policy message includes threshold detail when exceeded: `DQ completed with WARNINGS (failure rate exceeds 10% threshold)`
- New `block_on_threshold` policy option: when `true`, non-blocking failures exceeding threshold cause FAIL instead of WARN

### Fixed

#### Check Logic Improvements

**Silver Suite:**
- `_check_status_transition`: Replaced `.collect()` with Spark window functions to prevent OOM on large datasets
- `_check_totals_consistency`: Now correctly detects headers with non-zero amounts but no detail rows (previously silent pass)

**Gold Suite:**
- `_check_reconciliation_expr`: Fixed join filter bug when gold and silver use different column names
- `_check_coverage`: Replaced `fillna()` with `coalesce()` for proper NULL handling in outer joins
- `_check_completeness`: Replaced `.collect()` with Spark-native operations for serverless compatibility
- `_extract_metric_from_details`: Improved robustness with multiple extraction patterns for historical trend analysis

**Shared Checks:**
- `_check_json_schema`: Empty data now returns warning message instead of silent pass
- `_check_freshness`: Properly handles timezones (converts all timestamps to UTC for comparison)

**Bronze Suite:**
- `_check_binary_version_header_check`: NULL values now correctly fail instead of silently passing

#### Error Messages

- **Misleading error on invalid config path**: When a non-existent path was passed to `run_dq()`, the error incorrectly suggested it was a missing template tag. Now shows a clear message: `Config path not found: 'path/'. Ensure the path exists and points to a valid YAML file or modular config directory.`

### Backward Compatibility

Profiles are 100% optional. Existing configurations work without any changes:
- No `profiles.yaml` file required
- Hardcoded values in configs still work
- Use `{{ var.x | default(y) }}` for gradual migration

---

## [1.1.0] - 2026-06-08

Enhanced debugging capabilities with statistics-only failure analysis. **No PII exposure.**

### Added

#### Four-Layer Result Structure

Every check result now includes 4 layers of information:

| Layer | Purpose | When Present |
|-------|---------|--------------|
| `details` | Human-readable message (~60 chars) | Always |
| `failure_stats` | Aggregate statistics (counts, distributions) | Only on FAIL |
| `investigation_query` | SQL template for debugging | Only on FAIL |
| `execution` | Performance context (duration, row counts) | Always |

#### New Helper Methods (`base_test_suite.py`)

**PASS helpers:** `_details_pass_count()`, `_details_pass_bounds()`, `_details_pass_all()`, `_details_pass_comparison()`, `_details_pass_freshness()`, `_details_pass_reconciliation()`

**FAIL helpers:** `_details_fail_found()`, `_details_fail_missing()`, `_details_fail_exceeds()`, `_details_fail_below()`, `_details_fail_above()`, `_details_fail_mismatch()`, `_details_fail_deviation()`

**CONFIG/ERROR helpers:** `_details_config_missing()`, `_details_not_found()`, `_details_error()`, `_details_no_data()`, `_details_insufficient()`

**Debug builders:** `_build_failure_stats()`, `_build_investigation_query()`, `_build_execution_trace()`

#### All Suites Updated

Bronze, Silver, Gold, Shared, and GE suites now include:
- Narrative `details` messages
- `failure_stats` with distributions and diagnostics
- `investigation_query` SQL templates (16 check-specific templates)
- `execution` trace with timing and row counts

### Changed

#### Standardized Detail Messages

All messages now start with a word (never a number):

| Type | Pattern | Example |
|------|---------|---------|
| PASS | `{Metric} OK: {value}` | `"Record count OK: 5000 rows"` |
| PASS | `All {n} {items} {condition}` | `"All 5000 keys unique"` |
| FAIL | `Found {n} {items} ({pct}%)` | `"Found 50 duplicate keys (5.0%)"` |
| FAIL | `{Metric} {value} exceeds {limit}` | `"Data age 48h exceeds 24h limit"` |
| CONFIG | `Missing config: {keys}` | `"Missing config: key_column"` |

#### Simplified System Checks

- `data_filter`: Now shows only `status=ok` (filter details moved to `execution`)
- `table_read`: Now shows only `status=ok` (table info in `execution`)

### Removed (Breaking Changes)

| Removed | Migration |
|---------|-----------|
| `_safe_sample()` method | Use `failure_stats` for statistics |
| `include_sample` config | No replacement needed |
| `partial_unexpected_list` (GE) | Use `failure_stats` instead |

### Security

- **NEVER stores**: row values, primary keys, sample data
- **Only stores**: counts, percentages, column names, query templates
- Security review: PASSED (2026-05-28)

### Fixed

- **History parsing backward compatibility**: Checks that read from history tables now support both old and new detail message formats
  - `record_count_anomaly` (Bronze): Parses count from `"record_count=X"` (old) and `"Record count OK: X rows"` (new)
  - `_extract_metric_from_details` (Gold): Parses metric from `"metric=X"`, `"value=X"` (old), `"Volume OK: X"`, `"current=X"` (new)
- **Summary unique tables count**: TOTALS row now shows correct unique table count (passed from loading phase instead of recalculating)
- **Removed unused `tables_list`** from summary engine (was computed but never consumed)

---

## [1.0.0] - 2026-05-19

The version bump to 1.0.0 is intentional. After extensive testing and usage, we've validated that the framework is stable and ready for a 1.0 release.
### Added
- **Enhanced Terminal Summary**: Completely redesigned DQ summary output for better clarity
  - Cleaner column headers: `Checks` instead of `Total`, removed `Col Issues`
  - Suite abbreviations: `great_expectations@silver` → `ge@silver`
  - Unique tables count in TOTALS row (not sum across suites)
  - Legend explaining status codes: `PASS = all ok | WARN = non-blocking failures | FAIL = blocking failures`
  - All failed tables shown (no truncation)
  - Loading summary shows unique table count: `Loaded 5 test suites (16 tables, 9 unique)`
  - "Tables with failures" shows suite + short table name: `gold  aggregated_metrics  3 failures`

- **Consistent Table Naming**: All suites now use fully qualified table names (`catalog.schema.table`)
  - Native suites (bronze, silver, gold) aligned with GE suite naming
  - Enables accurate unique table counting across all suites
  - Summary correctly deduplicates tables shared between native and GE suites

- **GE Check Name Mapping**: Short, consistent names for Great Expectations checks
  - `expect_column_values_to_not_be_null` → `not_null`
  - `expect_column_values_to_be_unique` → `unique`
  - `expect_column_values_to_be_between` → `between`
  - `expect_column_values_to_be_in_set` → `values_set`
  - `expect_column_values_to_match_regex` → `regex`
  - `expect_table_row_count_to_be_between` → `row_count`
  - And 5 more mappings for common GE expectations
  - Original GE type preserved in `extra_metadata["ge_expectation_type"]`

- **GE Progress Bar Control**: Configure progress bar visibility via `verbose` parameter
  - `verbose=False` (default): GE progress bars hidden
  - `verbose=True`: GE progress bars visible
  - Uses `ProgressBarsConfig` in GE DataContextConfig

### Changed
- **Failed checks grouped by type**: Issues now grouped by check name across all suites
  ```
  Failed checks by type:
    freshness (10):
        gold             summary_created_at         x4
        bronze           batch_ingestion_at         x3
        silver           created_at                 x3
    not_null (5):
        ge@silver        user_id                    x3
        ge@gold          order_id                   x2
  ```

- **Status resolution simplified**:
  - `FAIL` = any blocking failure
  - `WARN` = non-blocking failures only
  - `PASS` = no failures
  - Removed complex severity-based logic

- **Logging improvements**:
  - "Starting DQ" → "Starting Neco DQ"
  - Result writer logs moved to DEBUG level
  - Logger explicitly set to INFO when `verbose=False`
  - Removed redundant WARNING log for WARN status

### Example Output
```
[INFO] neco_dq: Loading test suites...
[INFO] neco_dq:   → bronze: 2 tables
[INFO] neco_dq:   → silver: 2 tables
[INFO] neco_dq:   → gold: 5 tables
[INFO] neco_dq:   → great_expectations@silver: 2 tables
[INFO] neco_dq:   → great_expectations@gold: 5 tables
[INFO] neco_dq: Loaded 5 test suites (16 tables, 9 unique)
[INFO] neco_dq: Running checks...
[INFO] neco_dq:   → bronze
[INFO] neco_dq:     ✓ 11 checks (10 passed, 1 failed)
...
─────────────────────────────────────────────────────────────────────────
 DATA QUALITY SUMMARY
─────────────────────────────────────────────────────────────────────────
Suite                        Status   Checks   Pass     Fail     Tables
─────────────────────────────────────────────────────────────────────────
bronze                       WARN     11       10       1        2
gold                         FAIL     35       30       5        5
ge@gold                      PASS     50       50       0        5
silver                       WARN     17       15       2        2
ge@silver                    PASS     110      110      0        2
─────────────────────────────────────────────────────────────────────────
TOTALS                                223      215      8        9 unique

Success Rate: 96.4%  │  Status: FAIL (3 blocking)

Failed checks by type:
  freshness (5):
      silver           started_at                 x2
      bronze           batch_ingestion_at         x1
      gold             event_timestamp..          x1
      gold             summary_created_at         x1
  referential_integrity (3):
      gold             record_id                  x3

Tables with failures (6):
      gold             aggregated_metrics             3 failures
      gold             summary_changes                2 failures
      bronze           ingestion_events               1 failure
      silver           latest_records                 1 failure
      silver           cleaned_events                 1 failure

Legend: PASS=all ok │ WARN=non-blocking failures │ FAIL=blocking
─────────────────────────────────────────────────────────────────────────
```

---

## [0.7.6] - 2026-04-09

### Added
- **Tag-Based Check Selector**: Run only checks with specific tags across all suites
  - New `tags` parameter in `run_dq()`: `run_dq(config, tags=["critical", "daily"])`
  - Configure tags at check level: `tags: [critical, daily, smoke]`
  - **Strict filtering**: checks without tags are skipped when filter is active
  - OR logic: check runs if ANY of its tags match ANY filter tag
  - Full support for Bronze, Silver, Gold, and Great Expectations suites
  - **Smart pre-filtering**: only tables with tagged checks are loaded
  - Suites without any tagged tables are skipped entirely
  - GE suite: tags supported at rule level (e.g., `column.not_null.tags: [smoke]`)

### Filtering Behavior
| Scenario | Behavior |
|----------|----------|
| No `tags` parameter | All checks run (normal behavior) |
| `tags` set + check has matching tag | ✅ Check runs |
| `tags` set + check has non-matching tag | ❌ Check skipped |
| `tags` set + check has no tags | ❌ Check skipped |
| `tags` set + table has no tagged checks | ❌ Table not loaded |
| `tags` set + suite has no tagged tables | ❌ Suite skipped |

### Configuration Example (Bronze/Silver/Gold)
```yaml
silver:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      checks:
        idempotency:
          enabled: true
          tags: [critical, daily]

        freshness:
          enabled: true
          tags: [daily, smoke]

        record_count:
          enabled: true
          tags: [hourly]
```

### Configuration Example (Great Expectations)
```yaml
great_expectations:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      checks:
        order_id:
          not_null:
            enabled: true
            tags: [critical, smoke]   # Tag at rule level
          unique:
            enabled: true
            # No tags = skipped when filtering
        amount:
          between:
            enabled: true
            min: 0
            max: 10000
            tags: [daily]
```

### Usage
```python
from neco_dq.runner import run_dq

# Run only critical checks
results = run_dq("/Workspace/config/", tags=["critical"])

# Run daily checks across specific suites
results = run_dq(
    "/Workspace/config/",
    run_suites=["silver", "gold"],
    tags=["daily"]
)

# Run smoke tests
results = run_dq("/Workspace/config/", tags=["smoke"])
```

### Suggested Tag Conventions
| Tag | Description |
|-----|-------------|
| `critical` | Must pass for pipeline to proceed |
| `smoke` | Quick sanity checks |
| `daily` | Run once per day |
| `hourly` | Run every hour |
| `expensive` | Long-running checks |
| `regression` | Post-deployment validation |

---

## [0.7.5] - 2026-04-02

### Added
- **Data Filtering for Great Expectations Suite**: Full filtering support for GE suite, consistent with Bronze/Silver/Gold
  - `row_filter`: Suite/table-level filtering using **Spark SQL** syntax (supports `current_date()`, `INTERVAL`)
  - `partition_filter`: Partition-based filtering with dict syntax
  - `row_condition`: Check-level filtering using **Pandas query** syntax
  - `defaults` section support for inherited filters across all tables
  - Automatic `data_filter` check in results showing rows before/after filtering

- **Check-level row_condition**: Filter rows for individual GE checks
  - Supported checks: `not_null`, `nullable`, `unique` (single column), `between`, `values_set`, `regex`
  - Uses Pandas query syntax (e.g., `"status == 'ACTIVE'"`)
  - Useful for conditional validation (e.g., validate `shipping_date` only when `status == 'SHIPPED'`)

### Configuration Example
```yaml
great_expectations:
  defaults:
    row_filter: "batch_ingestion_at >= current_date() - INTERVAL 7 DAYS"  # Spark SQL

  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      checks:
        order_id:
          not_null:
            enabled: true
            severity: critical

        shipping_date:
          not_null:
            enabled: true
            row_condition: "status == 'SHIPPED'"  # Pandas query
            severity: warning
```

### Important Notes
- **`row_filter`** uses Spark SQL → dynamic dates work (`current_date() - INTERVAL 7 DAYS`)
- **`row_condition`** uses Pandas query → static values only, no SQL functions
- For date filtering, prefer `row_filter` over `row_condition`

---

## [0.7.4] - 2026-03-12

### Added
- **Kendall W Label Validation**: Enhanced `kendall_w` observed metric with consensus label validation
  - New `label` parameter: Static expected label (e.g., `"strong"`, `"moderate"`, `"low"`)
  - New `label_column` parameter: Column name containing expected label per row/group
  - New `thresholds` parameter: Custom thresholds dict for label classification
  - Validates observed consensus level against expected label
  - Returns detailed mismatch errors when label validation fails

- **Configurable Thresholds**: `DEFAULT_KENDALL_W_THRESHOLDS` now includes `"low": 0.2` label
  - Default thresholds: `strong` (≥0.9), `moderate` (≥0.7), `fair` (≥0.5), `weak` (≥0.3), `low` (≥0.2), `none` (<0.2)
  - Custom thresholds can be provided via `thresholds` config parameter

- **Configurable Minimum Raters**: New `min_raters` parameter for Kendall W calculation
  - `min_raters: 2` (default): Requires at least 2 raters, statistically correct
  - `min_raters: 1`: Allows single-rater calculations for compatibility with source systems

### Improved
- **Better Error Reporting**: Label mismatch errors now include totals for better visibility
  - Format: `kendall_w_label_mismatch: 93/150 failed (57 passed); group=(...): observed=weak, expected=moderate`
  - Shows failed count, total groups, and passed count at a glance

### Changed
- Refactored `_kendall_w` function into smaller helper functions for better maintainability:
  - `_build_expected_labels_map()`: Builds group → label mapping from DataFrame column
  - `_format_label_mismatch_error()`: Formats error messages for label mismatches
  - `_prepare_kendall_w_dataframe()`: Prepares DataFrame for Kendall W calculation
  - `_kendall_w_grouped()`: Handles grouped calculation with label validation
  - `_kendall_w_ungrouped()`: Handles ungrouped calculation with label validation

### Example Configuration
```yaml
compare_metric:
  metric:
    type: kendall_w
    column: items
    rank_sum_field: mean_rank
    appearances_field: item_appearances_n
    group_by: [record_id, question_id]
    label_column: consensus_level    # Expected label from column
    min_raters: 1                    # Allow single-rater calculations
    thresholds:                      # Custom thresholds
      strong: 0.9
      moderate: 0.3
      low: 0.2
  expected_from:
    type: column_value
    column: kendall_w
    group_by: [record_id, question_id]
  operator: "~"
  tolerance: 0.01
```

---

## [0.7.3] - 2026-03-11

### Added
- **Verbose Mode**: New `verbose` parameter in `run_dq()` to control output verbosity
  - `verbose=False` (default): Clean logs, progress bars hidden
  - `verbose=True`: Debug logging level, progress bars visible
  - Hides Great Expectations tqdm progress bars by default (`TQDM_DISABLE=1`)

### Improved
- **Logging System**: Streamlined and unified terminal logs for better readability
  - Unified logger name: `neco_dq` across all components (previously `DQ Runner`, `Orchestrator`, `ConfigResolver`, `DQResultWriter`, `GEDQTestSuite`, `DQAlerts`)
  - Compact log format without timestamp: `[INFO] neco_dq: message`
  - Removed redundant messages (e.g., duplicate "Starting" messages)
  - Added visual hierarchy with `→` indentation for suite loading and execution
  - Suite loading now sorted by layer order: bronze → silver → gold
  - Per-suite completion message: `✓ 12 checks (11 passed, 1 failed)`
  - Summary line shows total tables: `Loaded 5 test suites (16 tables)`
  - Removed internal details (e.g., `DQOrchestrator initialized`)
  - ConfigResolver logs moved to DEBUG level (hidden by default)
  - `force=True` in logging.basicConfig for Databricks compatibility

### Example Output (Before → After)
```
# Before (13 lines, 2 loggers)
INFO:DQ Runner:Initializing Spark session...
INFO:DQ Runner:Resolving DQ config: config/
INFO:DQ Runner:Starting DQ run | version=0.7.2 | config=config/
INFO:Orchestrator:DQOrchestrator initialized | run_suites=None | exclude_suites=set()
INFO:DQ Runner:Instantiating test suites from config...
INFO:Orchestrator:Test suite 'bronze' loaded (2 tables)
INFO:Orchestrator:Test suite 'gold' loaded (5 tables)
...
INFO:Orchestrator:Total test suite(s) registered: 5 → [...]
INFO:DQ Runner:Running DQ test suites...
INFO:Orchestrator:Starting DQ orchestrator with 5 test suite(s)...

# After (clean, compact)
[INFO] neco_dq: Resolving config: config/
[INFO] neco_dq: Starting DQ v0.7.3
[INFO] neco_dq: Loading test suites...
[INFO] neco_dq:   → bronze: 2 tables
[INFO] neco_dq:   → silver: 2 tables
[INFO] neco_dq:   → gold: 5 tables
[INFO] neco_dq: Loaded 5 test suites (16 tables)
[INFO] neco_dq: Running checks...
[INFO] neco_dq:   → bronze
[INFO] neco_dq:     ✓ 12 checks (11 passed, 1 failed)
[INFO] neco_dq:   → silver
[INFO] neco_dq:     ✓ 15 checks (15 passed, 0 failed)
[INFO] neco_dq:   → gold
[INFO] neco_dq:     ✓ 33 checks (30 passed, 3 failed)
```

---

## [0.7.2] - 2026-03-10

### Improved
- **Terminal Summary**: Enhanced DQ summary output with better readability and more information
  - Wider `Suite` column (28 chars) to accommodate `great_expectations@*` names
  - Added `TOTALS` row with aggregated counts across all suites
  - Added `Success Rate` percentage (e.g., `Success Rate: 96.9%`)
  - Improved header labels (`Total`, `Passed`, `Failed`, `Tables`, `Col Issues`)

- **Issues by Type**: Better formatting for failure details
  - Changed from dict format `{'columns': '...', 'count': N}` to human-readable `column_name (N failures)`
  - Column names truncated to 25 chars with `..` suffix to prevent line overflow
  - Reduced max columns display from 4 to 3 for better readability

- **Tables with Failures**: New section showing affected tables
  - Lists tables sorted by failure count (descending)
  - Shows max 5 tables, then `... and N more`
  - Aggregates failures across all suites (same table in multiple suites = summed count)

### Example Output
```
────────────────────────────────────────────────────────────
 DATA QUALITY SUMMARY
────────────────────────────────────────────────────────────
Suite                        Status   Total   Passed   Failed   Tables   Col Issues
-------------------------------------------------------------------------------------------
bronze                       WARN     13      12       1        2        0
silver                       WARN     17      14       3        2        2
gold                         WARN     33      30       3        5        1
great_expectations@silver    PASS     110     110      0        2        0
great_expectations@gold      PASS     50      50       0        5        0
-------------------------------------------------------------------------------------------
TOTALS                                223     216      7        16       3
Success Rate: 96.9%
Issues by type:
- bronze
    • table_freshness: table (1 failure)
- silver
    • freshness: started_at (2 failures)
    • idempotency: unique_response_id (1 failure)
- gold
    • compare_metric: table (2 failures)
    • freshness: event_timestamp.. (1 failure)
Tables with failures (4):
    • daily_summary: 3 failures
    • sessions: 2 failures
    • raw_events: 1 failure
    • kpi_dashboard: 1 failure
────────────────────────────────────────────────────────────
```

---

## [0.7.1] - 2026-03-10

### Added
- **Failure Threshold**: Optional `failure_threshold` parameter in `dq_policy`
  - Blocks pipeline only when failure rate exceeds the configured threshold
  - Example: `failure_threshold: 0.1` blocks only if >10% of checks fail
  - Backward compatible: no threshold = current behavior (any blocking failure blocks)
  - Per-suite evaluation: each suite's failure rate is calculated independently

### Changed
- Removed unused `fail_on_error` parameter from `DQFailurePolicy`
- **Severity-based warnings**: Non-blocking failures with `critical` or `error` severity now trigger `DQ_WARN` (exit code 10)
  - Previously only `severity: warning` triggered warnings
  - `severity: low` or `info` with `blocking: false` still returns `SUCCESS`

### Exit Code Hierarchy (Updated)
| Condition | Exit Code |
|-----------|-----------|
| `blocking: true` + failed | DQ_ERROR (20) |
| `blocking: false` + `severity: critical/error/warning` + failed | DQ_WARN (10) |
| `blocking: false` + `severity: low/info` + failed | SUCCESS (0) |

### Usage Example
```yaml
dq_policy:
  failure_threshold: 0.1  # Block only if >10% of checks fail
  block_test_suites:
    bronze: true
    silver: true
```

---

## [0.7.0] - 2026-03-03

### Added
- **Custom Test Suites**: Load custom test suites from external Python files
  - New `custom_suite_files` parameter in `run_dq()` function
  - Supports single path or list of paths for multiple custom suites
  - Dynamic loading of `DQTestSuite` subclasses from any Python file
  - Automatic suite name derivation from class name (CamelCase → snake_case)
  - Support for local paths and `/Workspace/` paths (Databricks)

- **Registry Functions**: New functions in `test_suite_registry.py`
  - `load_custom_suite_from_file(file_path)` - Load suite class from Python file
  - `register_custom_suite(name, class)` - Register custom suite in registry
  - `_derive_suite_config_key(class_name, path)` - Derive config key from class name

- **Documentation**: Complete guide for custom test suites
  - `docs/test-suites/custom.md` - Usage examples and best practices
  - Check method signature documentation
  - Helper methods reference

### Usage Example
```python
# my_custom_suite.py
class MyCustomSuite(DQTestSuite):
    @property
    def _suite_name(self) -> str:
        return "my_custom"

    @property
    def _suite_config_key(self) -> str:
        return "my_custom"

    def _check_my_validation(self, df, cfg):
        # Custom check logic
        return True, "ok", None

# Run with single custom suite
from neco_dq.runner import run_dq

run_dq(
    config_ref="/Workspace/dq-config/",
    custom_suite_files="/Workspace/my_custom_suite.py"
)

# Run with multiple custom suites
run_dq(
    config_ref="/Workspace/dq-config/",
    custom_suite_files=[
        "/Workspace/order_checks.py",
        "/Workspace/customer_checks.py",
    ]
)
```

### Configuration Example
```yaml
# dq-config/my_custom.yaml
my_custom:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: my_table
      checks:
        my_validation:
          enabled: true
          severity: error
```

---

## [0.6.0] - 2026-03-09

### Added
- **Data Filtering System**: Filter data before running DQ checks
  - `row_filter`: SQL WHERE expression applied to table data
  - `partition_filter`: Dict-based partition filtering with automatic SQL conversion
  - Suite-level `defaults` section for inherited filters
  - Table-level overrides for granular control
  - Filter metadata included in results (`filters_applied`)
  - Empty result handling with `no_rows_after_filter` warning

- **Partition Auto-Discovery**: Automatically detect and filter on table partitions
  - `partition_filter: {auto: true}` - filter on latest partition value
  - `partition_filter: {auto_latest: N}` - filter on latest N partition values
  - Reads partition metadata via `DESCRIBE DETAIL`
  - Validates partition columns and warns if non-partition columns used
  - Works with single and multi-column partitions

- **Extended Filter Metadata**: Rich details in `data_filter` check for traceability
  - `rows_before` / `rows_after`: Row counts before and after filtering
  - `filter_ratio`: Percentage of rows remaining after filter
  - `row_filter` / `partition_filter`: Actual SQL expressions applied
  - `auto_discovery`: Partition auto-resolution details

### Configuration Example
```yaml
silver:
  defaults:
    row_filter: "status != 'DELETED'"
    # Auto-discover partitions (recommended)
    partition_filter:
      auto: true
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      # Explicit partition values (overrides auto)
      partition_filter:
        date: "2024-01-15"
```

---

## [0.5.1] - 2026-03-04

### Fixed
- **Pandas version pin**: Added `pandas>=2.0,<3.0` to dependencies to prevent Great Expectations from upgrading pandas to 3.0.x, which causes PySpark compatibility warnings

---

## [0.5.0] - 2026-03-04

### Added
- **Pytest infrastructure**: Complete test setup with isolated virtualenv
  - `Makefile` with targets: `test`, `test-html`, `test-cov`, `test-allure`
  - Isolated `.venv-test/` with pyspark (not databricks-connect)
  - `pyproject.toml` with dev dependencies and ruff configuration

- **Test fixtures and utilities**:
  - `tests/conftest.py` - Pytest configuration with dynamic fixture loading
  - `tests/constants.py` - AllureSeverity, AllureTag, AllureFeature, AllureStory (StrEnum)
  - `tests/fixtures/` - Spark session, DataFrames, suite-specific fixtures
  - `tests/utils/assertions.py` - DQ assertion helpers

- **Unit tests**: 411 tests total (388 pass, 23 skipped for integration)
  - **Metrics** (217 tests, 99% coverage):
    - `test_observed.py`: 111 tests - all metric functions
    - `test_expected.py`: 72 tests - all resolver functions
    - `test_operators.py`: 34 tests - all comparison operators
  - **Suites** (103 tests):
    - `test_shared_checks.py`: 41 tests, 87% coverage
    - `test_bronze_checks.py`: 29 tests, 55% coverage
    - `test_silver_checks.py`: 22 tests, 43% coverage (10 skipped - require Unity Catalog)
    - `test_gold_checks.py`: 34 tests, 72% coverage (2 skipped - require Unity Catalog)
    - `test_ge_suite_builder.py`: 17 tests, 83% coverage
  - **Core components** (51 tests):
    - `test_suite_registry.py`: 9 tests, 87% coverage
    - `test_failure_policy.py`: 9 tests - DQFailurePolicy.evaluate()
    - `test_summary_engine.py`: 13 tests - BaseSummaryEngine methods
    - `test_orchestrator.py`: 13 tests (9 unit + 4 integration placeholders)
    - `test_runner.py`: 7 tests (integration placeholders)

- **Allure TestOps integration**:
  - Gherkin steps format (Given/When/Then)
  - Tags: lowercase (`positive`, `negative`, `spark`)
  - Name format: `Feature > Story > Title`

### Technical Notes
- Suite coverage varies because many checks require `spark.table()` for cross-table operations
- Silver checks (referential_integrity, totals_consistency, cross_table_equality, temporal_consistency, flow_completeness) require Unity Catalog
- Gold referential_integrity requires catalog.schema.table format
- These are marked as integration tests and skipped in unit test runs

---

## [0.4.1] - 2026-02-27

### Added
- **SharedChecksMixin**: Centralized shared checks between Silver and Gold suites
  - `_check_distinct_count` - distinct value count validation
  - `_check_json_schema` - JSON structure validation with field type inference
  - `_check_freshness` - data freshness verification
  - `_check_business_rules` - SQL expression validation
  - `_check_compare_metric` - generic metric comparison
- **Nested path validation for `json_schema` check**: Support for dot-notation to validate nested JSON structures
  - `required_fields: [rows.row_code, cells.cell_count]` - validate fields inside nested arrays
  - `field_types: {rows.row_order: int, cells.cell_row_pct: double}` - type validation for nested fields
  - `allowed_values: {cells.status: [active, inactive]}` - allowed values for nested fields
  - Automatically handles array explosion for nested paths

### Changed
- Profiling suggestions now include:
  - `cardinality` check for Silver (FK pattern detection)
  - `status_transition` check for Silver (workflow detection)
  - `record_count_anomaly` check for Bronze (requires history table)

### Improved
- JSON schema validation with configurable `max_parse_failure_pct` tolerance
- Borda ranking uses `rank()` instead of `row_number()` for tied values
- Better documentation for `_dispatch_check` introspection pattern
- **Profiling suite now detects nested JSON structures** and suggests `json_schema` checks with dot-notation paths (e.g., `rows.row_code`, `cells.cell_count`)

---

## [0.4.0] - 2026-02-24

### Added
- **Profiling Suite** (`profiling_dq_test_suite.py`): Auto-generates DQ configuration suggestions
  - `mode: profile` - Analyze table structure and statistics
  - `mode: suggest` - Generate ready-to-use YAML configurations
  - Cross-table relationship detection (FK, layer pairs, header-detail)
  - JSON column detection with field type inference
  - Automatic suite inference based on schema naming conventions

### Features
- Suggestions for Bronze, Silver, Gold, and Great Expectations suites
- Column statistics: null ratio, distinct count, top values, min/max
- Smart detection of:
  - Unique columns (idempotency candidates)
  - Low cardinality columns (allowed_values candidates)
  - Date columns (freshness candidates)
  - JSON columns (schema validation candidates)

---

## [0.3.1] - 2026-02-18

### Added
- **Documentation** (`neco_dq/docs/`): Comprehensive framework documentation
  - `index.md` - Overview and quick start
  - `getting-started.md` - Installation and first run guide
  - `architecture/` - System design and components
  - `configuration/` - Configuration modes and examples
  - `test-suites/` - Per-suite documentation (Bronze, Silver, Gold, GE)

---

## [0.3.0] - 2026-02-13

### Added
- **Modular Configuration**: Directory-based configuration structure
  ```
  config/
  ├── global/policy.yaml
  └── suites/
      ├── bronze/
      │   ├── suite.yaml
      │   ├── test_groups/
      │   └── great_expectations/
      ├── silver/
      └── gold/
  ```

### Features
- Separation of global policy from suite-specific configurations
- Per-table configuration files in `test_groups/`
- Great Expectations integration per suite
- `data_to_save` configuration for result persistence
- Default checks at suite level with table-level overrides

### Changed
- ConfigResolver supports both legacy single-file and modular directory modes
- Template-based configuration with Jinja2 support

---

## [0.2.0] - 2026-01-30

### Added
- **compare_metric Check**: Generic metric comparison framework
  - Flexible observed metrics (`count`, `aggregate`, `sql`, `array_stats`, etc.)
  - Multiple expected resolvers (`static`, `sql`, `column_value`, `array_field_value`)
  - Comparison operators (`==`, `!=`, `>`, `<`, `>=`, `<=`, `~`, `within_pct`)
  - Tolerance support for approximate comparisons
  - Group-by support for per-group validation

### Features
- `core/metrics/observed.py` - 20+ metric types
- `core/metrics/expected.py` - Expected value resolvers
- `core/metrics/operators.py` - Comparison operators
- Array statistics: mean, median, std, confidence intervals
- Ranking metrics: `kendall_w`, `borda_count`

### Changed
- Silver and Gold suites now use unified `_check_compare_metric`
- Replaced hardcoded checks with configurable metric comparisons

---

## [0.1.0] - 2026-01-08

### Added
- **Initial Release**: Core Data Quality validation platform

### Architecture
- `DQTestSuite` abstract base class with template method pattern
- Registry-based suite loading (`test_suite_registry.py`)
- Orchestrator for coordinated execution (`dq_orchestrator.py`)
- Policy engine for failure handling (`dq_failure_policy.py`)
- Summary engine for result aggregation (`summary_engine.py`)
- Slack alerts integration (`slack_alerts.py`)

### Test Suites
- **Bronze Suite**: Raw ingestion validation
  - `record_count`, `mandatory_fields`, `schema_drift`
  - `freshness`, `table_freshness`, `partition_exists`
  - Binary validation checks (Avro/Protobuf)

- **Silver Suite**: Transformation correctness
  - `idempotency`, `referential_integrity`, `cardinality`
  - `flow_completeness`, `temporal_consistency`
  - `status_transition`, `totals_consistency`

- **Gold Suite**: Aggregation validation
  - `uniqueness`, `completeness`, `coverage`
  - `reconciliation_expr`, `cross_gold`, `trend`

- **Great Expectations Suite**: GE integration
  - Dataset-level and column-level checks
  - Result normalization to standard format

### Core Features
- Configuration-driven validation (YAML)
- Standardized result contract across all suites
- Multi-instance check support (list-based configurations)
- Severity levels: `critical`, `error`, `warning`
- Blocking checks for pipeline control

---

## Version Numbering

- **Major (X.0.0)**: Breaking changes
- **Minor (0.X.0)**: New features, backward compatible
- **Patch (0.0.X)**: Bug fixes, documentation, minor improvements
