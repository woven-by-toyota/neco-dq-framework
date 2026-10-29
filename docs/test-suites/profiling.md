# Profiling Test Suite

The Profiling test suite is an **exploratory, non-blocking** suite designed to analyze data and **automatically suggest** DQ configurations.

## Characteristics

The Profiling suite is:

- **Non-blocking**: Typically configured with `blocking: false`
- **Exploratory**: Designed for data discovery and quality assessment
- **Multi-mode**: Supports `checks`, `stats`, and `suggest` modes
- **Auto-discovery**: Can discover tables using glob patterns

## Purpose

Profiling serves two main purposes:

1. **Data Exploration**: Understand data characteristics (nullability, cardinality, value distributions)
2. **Configuration Generation**: Automatically suggest DQ checks for Bronze, Silver, Gold, and GE suites

!!! tip "Debugging failures"
    When checks fail, see [Debug Experience](../architecture/debug-experience.md) for understanding `failure_stats`, `investigation_query`, and how to investigate.

---

## Suggest Mode

The `suggest` mode analyzes table statistics and generates ready-to-use DQ configurations.

### How It Works

1. **Profile tables**: Compute statistics for all columns (nulls, distinct counts, types, value distributions)
2. **Detect patterns**: Identify columns suitable for specific checks (unique keys, enums, dates, JSON)
3. **Detect relationships**: Find FK relationships, layer pairs, header/detail patterns
4. **Generate configs**: Output YAML configurations for Bronze, Silver, Gold, and GE suites

### Configuration

```yaml
profiling:
  mode: suggest  # Enable suggest mode

  # Option 1: Explicit table list
  tables:
    - catalog: my_catalog
      schema: my_schema_silver
      table: orders
      suite: silver  # Optional: target suite (bronze, silver, gold)

  # Option 2: Auto-discovery with patterns
  discover_tables:
    catalog: my_catalog
    schema_pattern: "sales_*"        # Glob pattern for schemas
    schema_exclude_pattern: "drop_*"  # Optional exclusion
    table_pattern: "*"                # Glob pattern for tables
    table_exclude_pattern: "_tmp*"    # Optional exclusion

  # Output configuration
  suggest_output:
    path: "/Workspace/Users/me/dq_configs/"  # Write YAML files
    # OR
    table: catalog.schema.config_suggestions  # Write to Delta table
```

### Suite Auto-Detection

When using `discover_tables`, the profiling suite automatically infers the target suite based on:

1. **Schema name**: exact match or suffix (`*_silver`, `*_gold`, `*_bronze`)
2. **Table name**: suffix match (`orders_silver`)
3. **Catalog name**: suffix match (`bronze_raw`)
4. **Table properties**: `TBLPROPERTIES('layer' = 'silver')`

---

## Statistics Computed

### Common Statistics (All Columns)

| Statistic | Description |
|-----------|-------------|
| `null_count` | Number of null values |
| `null_pct` | Percentage of null values |
| `distinct_count` | Approximate distinct count |
| `distinct_ratio` | distinct_count / row_count |

### Numeric Columns

| Statistic | Description |
|-----------|-------------|
| `min` | Minimum value |
| `max` | Maximum value |
| `mean` | Average value |
| `stddev` | Standard deviation |
| `p25`, `p50`, `p75`, `p95` | Percentiles |

### String Columns

| Statistic | Description |
|-----------|-------------|
| `min_length` | Minimum string length |
| `max_length` | Maximum string length |
| `avg_length` | Average string length |
| `top_values` | Top 10 most frequent values with counts |
| `is_json` | Whether column contains JSON data |
| `json_is_array` | Whether JSON is array-type |
| `json_sample_fields` | Detected JSON field names |
| `field_types` | Inferred types for JSON fields |
| `date_format` | Detected strftime format when the column holds dates as strings |

### Date/Timestamp Columns

| Statistic | Description |
|-----------|-------------|
| `min_date` | Earliest date |
| `max_date` | Latest date |
| `date_range_days` | Range in days |

---

## Suggested Checks by Suite

!!! note "Uniqueness needs a primary key to be enabled"
    Distinct counts are computed with `approx_count_distinct`, so a column that looks
    unique in a profiling snapshot is only a candidate. Uniqueness-based checks
    (`unique`, `uniqueness`, `idempotency`, `no_duplicate`) are suggested
    `enabled: true` and blocking **only** when the column(s) come from a Unity Catalog
    primary key. Otherwise they are suggested `enabled: false` with a review comment,
    so a guess never blocks a pipeline. See [Primary Key Integration](#primary-key-integration).

### Bronze Suite Suggestions

| Check | Condition | Description |
|-------|-----------|-------------|
| `record_count` | Always | Verify table is not empty |
| `mandatory_fields` | Columns with 0% nulls | Ensure required columns are non-null (suggested as non-blocking `warning`) |
| `schema_drift` | Always | Compare schema against expected structure |
| `freshness` | Date column with ingestion keywords | Validate data recency |
| `table_freshness` | Always | Verify Delta metadata freshness |
| `record_count_anomaly` | Disabled by default | Requires `history_table` configuration |

**Example output:**

```yaml
bronze:
  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: orders
      checks:
        record_count:
          enabled: true
          severity: critical
          blocking: true

        mandatory_fields:
          enabled: true
          columns:
            - transaction_id
            - created_at
          severity: warning

        schema_drift:
          enabled: true
          expected_columns:
            transaction_id: string
            created_at: timestamp
            value: long
          severity: warning

        table_freshness:
          enabled: true
          max_staleness_days: 2
          severity: warning
```

---

### Silver Suite Suggestions

| Check | Condition | Description |
|-------|-----------|-------------|
| `idempotency` | Primary key detected (enabled) or unique candidate (disabled) | Verify no duplicate records |
| `freshness` | Date column with timestamp keywords | Validate data freshness |
| `row_temporal_consistency` | Multiple date columns | Verify date ordering (updated >= created) |
| `cardinality` | FK pattern columns | Minimum rows per group |
| `distinct_count` | Moderate-cardinality columns, except date/timestamp and id-like | Verify distinct value count stays in range |
| `status_transition` | Status/state column detected | Workflow transition validation |
| `json_schema` | JSON columns detected | Validate JSON structure |
| `referential_integrity` | FK relationships detected | Cross-table relationship validation |
| `flow_completeness` | Bronze-to-Silver pair detected | Row completeness from source |

**Example output:**

```yaml
silver:
  tables:
    - catalog: my_catalog
      schema: my_schema_silver
      table: orders
      checks:
        # No primary key on this table, so idempotency is a disabled candidate.
        # With a Unity Catalog PK it would be enabled: true + blocking: true.
        idempotency:
          enabled: false
          key_columns: [transaction_id]
          severity: error
          _reason: "Candidate key (from approximate profiling). Review, then enable."

        freshness:
          enabled: true
          timestamp_column: created_at
          max_delay_hours: 72
          severity: warning

        # required_fields/field_types were inferred from a sample, so the check is
        # suggested disabled: an optional field absent from the sample would fail it.
        json_schema:
          - enabled: false
            column: metadata
            array_element: true
            required_fields:
              - key
              - value
            field_types:
              key: string
              value: int
            severity: error
```

---

### Gold Suite Suggestions

| Check | Condition | Description |
|-------|-----------|-------------|
| `uniqueness` | Primary key detected (enabled) or unique candidate (disabled) | Verify no duplicate rows |
| `volume` | Always (when row_count > 0), disabled by default | Row count with historical tolerance |
| `freshness` | Date column with timestamp keywords | Validate data freshness |
| `trend` | Date column + numeric metric column, disabled by default | Metric trend vs recent history |
| `completeness` | Enum-like column with 2-10 distinct values, disabled by default | Verify all expected values are present |
| `json_schema` | JSON columns detected | Validate JSON structure |
| `referential_integrity` | FK to Silver detected | Cross-table validation |
| `reconciliation_expr` | Silver-to-Gold pair detected | Aggregation consistency |
| `coverage` | Silver-to-Gold pair detected | Coverage validation |

**Example output:**

```yaml
gold:
  tables:
    - catalog: my_catalog
      schema: my_schema_gold
      table: summary
      checks:
        # summary_id is a unique candidate but not a declared primary key, so
        # uniqueness is suggested disabled. A Unity Catalog PK would make it
        # enabled: true + blocking: true.
        uniqueness:
          enabled: false
          columns:
            - summary_id
          severity: critical
          _comment: "Candidate unique column(s) (from approximate profiling). Review, then enable."

        volume:
          enabled: true
          metric: "COUNT(*)"
          history:
            table: "<catalog.schema.dq_history_table>"
            tolerance_percent: 50
          severity: warning

        freshness:
          enabled: true
          timestamp_column: run_timestamp
          max_delay_hours: 24
          severity: warning
```

---

### Great Expectations Suggestions

GE suggestions are generated for Silver and Gold tables only (not Bronze).

| Rule | Condition | Description |
|------|-----------|-------------|
| `dtype` | Always | Validate column data type |
| `not_null` | Columns with 0% nulls | Verify non-null values (warning) |
| `unique` | Primary key (enabled) or high distinct ratio candidate (disabled) | Verify uniqueness |
| `values_set` | Low cardinality + enum pattern | Allowed values validation |
| `between` | Numeric columns, disabled by default | Min/max range validation |
| `regex` | String columns with pattern keywords, disabled by default | Format validation |
| `length` | String columns with narrow length range, disabled by default | String length validation |
| `date_format` | String columns holding dates in a consistent format, disabled by default | Date string format validation |
| `min_rows` / `max_rows` | Always (when row_count > 0) | Dataset row count band (warning) |
| `no_duplicate` | Primary key (enabled) or multiple unique candidates (disabled) | Composite key validation |
| `integrity.pair_gte` | Date pairs (updated/created) | Date ordering validation |

**Example output:**

```yaml
great_expectations:
  tables:
    - catalog: my_catalog
      schema: my_schema_silver
      table: orders
      checks:
        transaction_id:
          not_null:
            enabled: true
            severity: warning
          # No primary key declared, so unique is a disabled candidate.
          # A Unity Catalog PK would make it enabled: true + blocking: true.
          unique:
            enabled: false
            severity: error
            _comment: "Candidate unique column (from approximate profiling). Review, then enable."

        status:
          values_set:
            enabled: true
            values:
              - draft
              - submitted
              - approved
            severity: warning

        score:
          between:
            enabled: false
            min: 0
            max: 100
            severity: warning

        created_date:
          date_format:
            enabled: false
            format: "%Y-%m-%d"
            severity: warning

        _dataset:
          min_rows: 800
          max_rows: 1200
          severity: warning
```

---

## Cross-Table Relationship Detection

When profiling multiple tables, the suite automatically detects relationships.

### FK Relationships

Detects patterns like:
- `orders_id` column in table A -> `id` column in `orders` table
- `<table>_id` -> `<table>.id`

### Layer Pairs

Detects Bronze/Silver/Gold table pairs:
- `orders_bronze` -> `orders_silver` -> `orders_gold`
- `bronze_orders` -> `silver_orders`

### Header/Detail Pairs

Detects parent/child table patterns:
- `orders` + `order_items`
- `invoice` + `invoice_lines`

### Cross-Layer Relationships

Detects relationships based on common columns between layers:
- Gold tables with FK columns matching Silver tables
- Silver tables with FK columns matching Bronze tables

---

## Output Options

### YAML Files

Write per-suite YAML files to a directory:

```yaml
suggest_output:
  path: "/Workspace/Users/me/dq_configs/"
```

Creates files like:
- `catalog_schema_table_bronze.yaml`
- `catalog_schema_table_silver.yaml`
- `catalog_schema_table_gold.yaml`
- `catalog_schema_table_great_expectations.yaml`

### Delta table

Write suggestions to a Delta table:

```yaml
suggest_output:
  table: catalog.schema.config_suggestions
```

The table is created automatically on the first run (no DDL required). Each run overwrites the entire table.

One row is written per (table, suite) combination. Schema:

| Column | Type | Description |
|--------|------|-------------|
| `catalog` | string | Source table catalog |
| `schema` | string | Source table schema |
| `table` | string | Source table name |
| `run_timestamp` | timestamp | When profiling ran |
| `row_count` | long | Row count of the profiled table |
| `column_count` | integer | Column count of the profiled table |
| `test_suite` | string | Suite name: bronze, silver, gold, great_expectations |
| `config_yaml` | string | Suggested YAML config for this suite |
| `column_stats_json` | string | Raw column statistics as JSON |

### Modular config tree

Write profiling output as a modular config directory tree that `run_dq()` can read directly:

```yaml
suggest_output:
  modular_config_path: "/Workspace/Users/me/dq_config/"
```

Directory structure produced:

```
dq_config/
  global/
    policy.yaml              # blocking policy for each suite
    profiles.yml             # only written when include_profiles: true
  suites/
    bronze/
      suite.yaml
      test_groups/
        orders.yaml          # {"tables": [{catalog, schema, table, checks}]}
      great_expectations/
        orders.yaml          # bare check dict (no tables wrapper)
    silver/
      suite.yaml
      test_groups/
        orders.yaml
      great_expectations/
        orders.yaml
    gold/
      suite.yaml
      test_groups/
        orders.yaml
```

GE checks are placed under the suite that owns the table (silver by default, falling back to the first present suite).

#### Options

```yaml
suggest_output:
  modular_config_path: "/Workspace/Users/me/dq_config/"
  include_profiles: false        # write global/profiles.yml (default: false)
  include_policy: true           # write global/policy.yaml (default: true)
  parameterize_catalog: false    # replace catalog with {{ profile.target_catalog }} (default: false)
  overwrite: false               # skip files that already exist (default: false)
  dry_run: false                 # compute paths but write nothing (default: false)
```

`include_profiles`: when `true`, writes `global/profiles.yml` with a `dev` profile. The `target_catalog` is taken from the first profiled table. Set this when you intend to run the config across multiple environments using profiles.

`include_policy`: when `false`, skips writing `global/policy.yaml`. Only do this when merging into an existing config tree that already has a policy file. `run_dq()` requires `policy.yaml` to be present.

`parameterize_catalog`: when `true`, replaces the catalog field in every table entry with `{{ profile.target_catalog }}`. Pair with `include_profiles: true` for a config that works across catalogs.

`overwrite`: when `false` (the default), any file that already exists on the local filesystem is skipped. On Databricks, `dbutils.fs.put` always overwrites regardless of this flag.

`dry_run`: when `true`, returns the list of paths that would be written without writing anything.

### Summary report

Writes `PROFILING_SUMMARY.md` to the given directory:

```yaml
suggest_output:
  summary_report_path: "/Workspace/Users/me/dq_config/"
```

The report covers:

- overview: total tables, rows scanned, checks suggested (enabled vs. disabled)
- tables by layer: bronze / silver / gold breakdown with row and column counts
- checks by suite: how many checks each suite generated and how many are enabled
- disabled checks to review: checks that profiling left disabled pending manual validation
- data quality observations: high null ratios, possible PII columns (name-based), single-value columns
- quick start: a `run_dq()` snippet pointing at the output path
- review checklist: common post-profiling tasks (idempotency keys, freshness thresholds, between ranges)

When profiling a large schema for the first time, this file is a good place to start: it shows what needs manual attention before switching DQ checks to blocking mode.

---

## Sampling

For large tables, enable sampling to reduce computation time:

```yaml
profiling:
  mode: suggest

  # Explicit sampling
  sampling:
    enabled: true
    fraction: 0.1        # Sample 10% of rows
    max_rows: 1000000    # Cap at 1M rows
    seed: 42             # Reproducibility

  # OR auto-sampling based on row count
  auto_sample_threshold: 10000000  # Auto-sample if > 10M rows
```

---

## Primary Key Integration

The suggest mode integrates with Unity Catalog constraints to detect primary keys:

```sql
-- Define PK in Unity Catalog
ALTER TABLE catalog.schema.table ADD CONSTRAINT pk_name PRIMARY KEY (col1, col2);
```

When a PK constraint is defined:
- The PK columns are used as the definitive unique columns
- `idempotency` check uses the PK as `key_columns`
- Other detected unique columns are still included but PK takes priority

The PK constraint also decides whether uniqueness checks are enabled. A primary key
is authoritative, so `unique`, `uniqueness`, `idempotency`, and `no_duplicate` are
suggested `enabled: true` and blocking when backed by a PK. Without a PK, the same
checks are suggested `enabled: false` with a review comment, because a distinct ratio
from `approx_count_distinct` cannot prove uniqueness on its own.

---

## Complete Example

```yaml
profiling:
  mode: suggest

  # Discover all tables in sales_* schemas
  discover_tables:
    catalog: my_catalog
    schema_pattern: "sales_*"
    schema_exclude_pattern: "*_staging"
    table_pattern: "*"
    table_exclude_pattern: "_*"

  # Sampling for large tables
  auto_sample_threshold: 5000000
  sampling:
    fraction: 0.1
    max_rows: 500000
    seed: 42

  # Output to both files and table
  suggest_output:
    path: "/Workspace/Users/me/dq_configs/sales/"
    table: my_dq_catalog.dq_admin.config_suggestions
```

This configuration will:
1. Discover all tables in schemas matching `sales_*`
2. Auto-sample tables with >5M rows
3. Generate Bronze, Silver, Gold, and GE suggestions
4. Output YAML files and store in Delta table

---

## Workflow Recommendation

1. **Run profiling with suggest mode** on your target tables
2. **Review generated configurations** - check enabled/disabled checks
3. **Customize thresholds** - adjust based on data characteristics
4. **Enable disabled checks** - some checks require manual configuration
5. **Add cross-table checks** - referential_integrity, compare_metric, etc.
6. **Save configurations** in your project's YAML config directory

---

## See Also

- [Bronze Suite](bronze.md) - Ingestion-level validation
- [Silver Suite](silver.md) - Data quality validation
- [Gold Suite](gold.md) - Business metrics validation
- [Configuration Guide](../configuration/modular-config.md) - How to structure configs
