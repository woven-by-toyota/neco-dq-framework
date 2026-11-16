# Modular Configuration

Modular configuration is the **recommended approach** for production use. It organizes validations into a directory structure that's organized, scalable, and version-control friendly.

## Overview

Modular configuration separates concerns into focused files:
- **Global settings**: Shared policy, alerts, metadata
- **Suite directories**: One per validation layer (bronze, silver, gold)
- **Test groups**: Logical groupings of related tests
- **Great Expectations**: GE-specific configurations

## Directory Structure

The root folder name is up to your team (`config/`, `dq-config/`, `quality-checks/`, etc.). The framework only requires the internal structure: `global/` and `suites/` directories.

```
config/                          # Any name you choose
├── global/
│   ├── policy.yaml              # Failure handling policy
└── suites/
    ├── bronze/
    │   ├── suite.yaml       # Bronze suite configuration
    │   ├── test_groups/
    │   │   ├── schema.yaml
    │   │   ├── nulls.yaml
    │   │   ├── primary_keys.yaml
    │   │   └── completeness.yaml
    │   └── great_expectations/
    │       └── expectations.json
    ├── silver/
    │   ├── suite.yaml
    │   ├── test_groups/
    │   │   ├── transformations.yaml
    │   │   ├── business_rules.yaml
    │   │   └── data_quality.yaml
    │   └── great_expectations/
    │       └── expectations.json
    └── gold/
        ├── suite.yaml
        ├── test_groups/
        │   ├── aggregations.yaml
        │   ├── metrics.yaml
        │   └── completeness.yaml
        └── great_expectations/
            └── expectations.json
```

## Global Configuration

### global/policy.yaml

Defines failure handling behavior:

```yaml
dq_policy:
  # Optional: Informational threshold (0.0 to 1.0)
  # Shown in summary when exceeded, but doesn't change pass/fail behavior by default
  failure_threshold: 0.1

  # Optional: When true, non-blocking failures exceeding threshold cause FAIL instead of WARN
  block_on_threshold: false

  # Control which test suites can block the pipeline
  block_test_suites:
    profiling: false
    bronze: true
    silver: true
    gold: true
    great_expectations: true

  alerts:
    slack:
      enabled: true
```

**Policy Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `failure_threshold` | float | None | Informational threshold (0.0-1.0). Shown in summary when exceeded. |
| `block_on_threshold` | bool | false | When `true`, non-blocking failures exceeding threshold cause FAIL instead of WARN. |
| `block_test_suites.<suite>` | bool | true | Whether blocking failures in this suite cause pipeline to fail. |

**Exit Code Behavior:**

- **Blocking failures**: Always cause FAIL (exit code 20) regardless of threshold
- **Non-blocking failures**: Cause WARN (exit code 10) by default
- **`block_on_threshold: true`**: Non-blocking failures exceeding threshold escalate to FAIL

## Suite Configuration

Each suite has its own directory under `suites/`.

### suites/bronze/suite.yaml

Bronze suite configuration:

```yaml
suite: bronze

default_checks:
  record_count:
    enabled: true
    severity: critical
    blocking: true

  table_freshness:
    enabled: true
    severity: warning
    blocking: false

data_to_save:
  - table: my_dq_catalog.dq_metadata.dq_test_history
    write_mode: append
    alerts: false
    columns_to_save:
      - test_suite
      - catalog
      - schema
      - table
      - column
      - run_timestamp
      - passed
      - status
      - check_name
      - severity
      - details
      - blocking
```

### Test Groups

Test groups organize related validations into separate files for better organization.

#### suites/bronze/test_groups/raw_events.yaml

```yaml
  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: raw_events

      checks:
        freshness:
          enabled: true
          timestamp_column: batch_raw_events_at
          max_delay_hours: 24

        schema_drift:
          enabled: true
          expected_columns:
            batch_raw_events_at: timestamp
            created_at: string
            updated_at: string
            item_created_at: string
            completed_at: string
            batch_id: string
            project_id: string
            order_id: string
            channel_id: string
            owner_group: string
            owner_id: string
            owner_email: string
            transaction_id: string
            customer_id: string
            customer_email: string
            is_duplicate: string
            channel_type: string
            status: string
            template_id: string
            content: string
            event_data: string

        mandatory_fields:
          enabled: true
          columns:
            - batch_raw_events_at
            - batch_id
            - project_id
            - order_id
            - transaction_id

        record_count_anomaly:
          enabled: true
          history_table: my_dq_catalog.dq_metadata.dq_test_history
          max_deviation_percent: 40
```

## How Test Groups Work

Test groups are automatically merged into the suite configuration:

```
suite.yaml
    ↓
ConfigResolver reads
    ↓
Merges test_groups/*.yaml
    ↓
Produces unified config
    ↓
Passed to suite
```

### Advantages

**Organized**: Related tests grouped logically

**Reusable**: Test groups can be shared across suites

**Maintainable**: Easier to find and update tests

**Collaborative**: Team members can work on different groups

## Default Checks Inheritance

The `default_checks` section in `suite.yaml` defines checks that are **automatically applied to all tables** in that suite. It's how you keep validation consistent across your data without repeating checks per table.

### How Inheritance Works

```
suite.yaml (default_checks)
         │
         ▼
┌────────────────────────┐
│  All tables inherit    │
│  default_checks        │
└────────────────────────┘
         │
         ▼
┌────────────────────────┐
│  Table-specific checks │
│  can OVERRIDE or ADD   │
└────────────────────────┘
```

### Example: suite.yaml with default_checks

```yaml
# suites/bronze/suite.yaml
suite: bronze

default_checks:
  # These checks apply to ALL tables in this suite
  record_count:
    enabled: true
    severity: warning
    blocking: false

  table_freshness:
    enabled: true
    severity: warning
    blocking: false
    max_staleness_days: 2

  mandatory_fields:
    enabled: true
    severity: critical
    blocking: true
    columns:
      - batch_id
      - created_at
```

### Example: Table inheriting and overriding

```yaml
# suites/bronze/test_groups/orders.yaml
tables:
  - catalog: my_catalog
    schema: my_schema
    table: orders

    checks:
      # This OVERRIDES the default mandatory_fields with different columns
      mandatory_fields:
        enabled: true
        severity: critical
        blocking: true
        columns:
          - order_id
          - customer_id
          - order_date

      # This ADDS a new check specific to this table
      freshness:
        enabled: true
        timestamp_column: order_date
        max_delay_hours: 24

      # record_count and table_freshness are INHERITED automatically
```

**Result:** The `orders` table will have:

| Check | Source |
|-------|--------|
| `record_count` | Inherited from default_checks |
| `table_freshness` | Inherited from default_checks |
| `mandatory_fields` | Overridden with table-specific columns |
| `freshness` | Added specifically for this table |

### Disabling an Inherited Check

To disable a default check for a specific table:

```yaml
tables:
  - catalog: my_catalog
    schema: my_schema
    table: legacy_table

    checks:
      # Disable the inherited record_count check
      record_count:
        enabled: false
```

---

## Running DQ checks

Once the config tree is in place, run it with `run_dq()`:

```python
from neco_dq.runner import run_dq

# Run all suites in the config directory
results = run_dq("/Workspace/config/")
```

`run_dq()` also takes `run_suites` / `exclude_suites`, a runtime `selector` to
pick tables, `tags`, `profile` and `verbose`. The full parameter table, the
filtering model and end-to-end examples live on the
[`run_dq()` API Reference](run-dq-api.md).

## Multi-environment (profiles)

Profiles let one config target dev / staging / prod without duplicating files, via
Jinja2 templates (`{{ profile.target_catalog }}`, `{{ var.* }}`) resolved at load
time. Add a `profiles.yml` to the config directory and pass `profile="prod"` (or
set `NECO_DQ_PROFILE`). See [Multi-Environment (Profiles)](profiles.md) for the
full setup.

---

## Complete Example

Here's a complete modular configuration for a production project:

### Directory Structure

```
/Workspace/config/sales_dq/
├── global/
│   └── policy.yaml
└── suites/
    ├── bronze/
    │   ├── suite.yaml              # Default checks for all bronze tables
    │   ├── test_groups/
    │   │   ├── raw_events.yaml      # Ingestion layer tables
    │   │   ├── events.yaml         # Event tables
    │   │   └── metadata.yaml       # Metadata tables
    │   └── great_expectations/
    │       └── column_checks.yaml
    ├── silver/
    │   ├── suite.yaml              # Default checks for all silver tables
    │   ├── test_groups/
    │   │   ├── core_entities.yaml  # Core business entities
    │   │   ├── lookups.yaml        # Lookup/dimension tables
    │   │   └── relationships.yaml  # Tables with FK relationships
    │   └── great_expectations/
    │       └── data_quality.yaml
    └── gold/
        ├── suite.yaml              # Default checks for all gold tables
        ├── test_groups/
        │   ├── aggregations.yaml   # Aggregated metrics
        │   ├── reports.yaml        # Report tables
        │   └── kpis.yaml           # KPI tables
        └── great_expectations/
            └── business_rules.yaml
```

### global/policy.yaml

```yaml
dq_policy:
  failure_threshold: 0.1  # Informational threshold

  block_test_suites:
    profiling: false
    bronze: true
    silver: true
    gold: true
    great_expectations: true

  alerts:
    slack:
      enabled: true
```

### suites/silver/suite.yaml

```yaml
suite: silver

# Default checks applied to ALL silver tables
default_checks:
  idempotency:
    enabled: true
    severity: critical
    blocking: true
    key_columns: [id]

  freshness:
    enabled: true
    severity: warning
    blocking: false
    timestamp_column: updated_at
    max_delay_hours: 24

data_to_save:
  - table: my_catalog.dq_metadata.dq_test_history
    write_mode: append
```

### suites/silver/test_groups/core_entities.yaml

```yaml
tables:
  # This table uses default idempotency (with id column) and freshness
  - catalog: my_catalog
    schema: silver
    table: customers
    checks:
      # Add table-specific check
      mandatory_fields:
        enabled: true
        columns: [customer_id, email, created_at]

  # This table overrides the default idempotency key
  - catalog: my_catalog
    schema: silver
    table: orders
    checks:
      # Override: use order_id instead of id
      idempotency:
        enabled: true
        severity: critical
        blocking: true
        key_columns: [order_id]

      # Add referential integrity check
      referential_integrity:
        enabled: true
        source_schema: silver
        source_table: orders
        source_column: customer_id
        target:
          schema: silver
          table: customers
          column: customer_id
```

### Running the Configuration

```python
from neco_dq.runner import run_dq

# Full validation
results = run_dq("/Workspace/config/sales_dq/")

# Quick validation: only silver core entities
results = run_dq(
    "/Workspace/config/sales_dq/",
    run_suites=["silver"],
    selector={"silver": ["customers", "orders"]}
)
```

---

## Best Practices

### Organizing Test Groups

| Strategy | When to Use |
|----------|-------------|
| **By data domain** | `orders.yaml`, `customers.yaml`, `products.yaml` |
| **By source system** | `salesforce.yaml`, `stripe.yaml`, `hubspot.yaml` |
| **By check type** | `schema_checks.yaml`, `relationships.yaml`, `metrics.yaml` |
| **By criticality** | `critical_tables.yaml`, `secondary_tables.yaml` |

### Using default_checks Effectively

| Do | Don't |
|----|-------|
| Define common checks in `suite.yaml` | Repeat the same check in every test_group file |
| Override only when necessary | Copy all defaults to each table |
| Use `enabled: false` to disable inherited checks | Delete inherited checks manually |
| Keep default_checks minimal and universal | Add table-specific checks to defaults |

---

## Next Steps

- **Profiles**: Multi-environment configuration with [Profiles](profiles.md)
- **Test Suites**: Learn about [Bronze Suite](../test-suites/bronze.md), [Silver Suite](../test-suites/silver.md), [Gold Suite](../test-suites/gold.md)
- **Compare Metric**: Deep dive into [Compare Metric](compare-metric.md) for complex metric validation
- **Data Filtering**: Learn about [partition_filter and row_filter](data-filtering.md)

---
