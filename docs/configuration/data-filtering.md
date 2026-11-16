# Data Filtering

Data filtering allows you to run DQ checks on a **subset of your data**. This is useful for:

- Validating only the most recent partitions
- Testing specific data segments
- Reducing compute costs on large tables
- Incremental data quality validation

## Overview

The framework supports two types of filters:

| Filter Type | Purpose | Scope |
|-------------|---------|-------|
| `partition_filter` | Filter on partition columns (optimized) | Partition pruning |
| `row_filter` | SQL WHERE expression for any column | Row-level filtering |

Both filters can be configured at **suite level** (defaults) or **table level** (overrides).

---

## Partition Filter

Partition filters use Delta table partition pruning to keep scans fast.

### Basic Usage

```yaml
tables:
  - catalog: my_catalog
    schema: my_schema
    table: events
    partition_filter:
      date: "2024-01-15"
```

### Multiple Values (IN clause)

```yaml
partition_filter:
  date: ["2024-01-14", "2024-01-15", "2024-01-16"]
```

Generated SQL: `date IN ('2024-01-14', '2024-01-15', '2024-01-16')`

### Multiple Columns

```yaml
partition_filter:
  year: 2024
  month: 1
```

Generated SQL: `year = 2024 AND month = 1`

### Auto-Discovery (Recommended)

The framework can automatically discover partition columns and filter on the latest values.

#### Latest Single Partition

```yaml
partition_filter:
  auto: true
```

This will:
1. Query table metadata for partition columns
2. Find the maximum value for each partition column
3. Filter data to only include the latest partition

#### Latest N Partitions

```yaml
partition_filter:
  auto_latest: 7  # Last 7 partitions
```

Useful for validating a rolling window of recent data.

### Validation

The framework validates that `partition_filter` columns are actual partition columns:

- If a non-partition column is used, a **warning** is included in the check details
- The filter still executes, but without partition pruning optimization

---

## Row Filter

Row filters apply SQL WHERE expressions to filter rows after partition pruning.

### Basic Usage

```yaml
tables:
  - catalog: my_catalog
    schema: my_schema
    table: orders
    row_filter: "status = 'COMPLETED'"
```

### Complex Expressions

```yaml
row_filter: "amount > 0 AND created_at >= '2024-01-01'"
```

### Combined with Partition Filter

```yaml
tables:
  - catalog: my_catalog
    schema: my_schema
    table: events
    partition_filter:
      date: "2024-01-15"
    row_filter: "event_type = 'purchase'"
```

**Execution order:**
1. Partition filter applied first (partition pruning)
2. Row filter applied second (row-level filtering)

---

## Suite-Level Defaults

Define filters once at suite level and apply to all tables.

### Configuration

```yaml
silver:
  defaults:
    partition_filter:
      auto: true  # All tables use latest partition
    row_filter: "is_valid = true"

  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      # Inherits: partition_filter.auto=true, row_filter="is_valid = true"

    - catalog: my_catalog
      schema: my_schema
      table: payments
      partition_filter:
        date: "2024-01-15"  # Override suite default
      # Still inherits: row_filter="is_valid = true"
```

### Inheritance Rules

| Level | Priority |
|-------|----------|
| Table-level filter | Highest (overrides suite default) |
| Suite-level default | Applied if no table-level filter |
| No filter | All data processed |

**Note:** Table-level filters completely replace suite defaults (no merging).

---

## Filter Check Output

When filters are applied, a `data_filter` check is automatically added to results.
Its `details` field is a short status string:

```
check_name: data_filter
passed: true
details: status=ok
```

The before/after row counts are not in `details`; they are recorded in the check's
`execution.filters_applied` trace (`rows_before`, `rows_after`), alongside the rest
of the [execution trace](../architecture/debug-experience.md).

### Empty Result Handling

If filtering results in zero rows:

```
check_name: data_filter
passed: true
severity: warning
details: status=no_rows_after_filter
```

The check passes with a warning, and no further checks are executed for that table.

---

## Complete Examples

### Example 1: Production Daily Validation

Validate only today's data across all tables:

```yaml
silver:
  defaults:
    partition_filter:
      auto: true  # Latest partition for all tables

  tables:
    - catalog: prod_catalog
      schema: sales
      table: orders
      checks:
        idempotency:
          enabled: true
          severity: critical
          blocking: true
          key_columns: [order_id]

    - catalog: prod_catalog
      schema: sales
      table: payments
      checks:
        referential_integrity:
          enabled: true
          source_schema: sales
          source_table: payments
          source_column: order_id
          target:
            schema: sales
            table: orders
            column: order_id
```

### Example 2: Rolling Window Validation

Validate last 7 days of data:

```yaml
gold:
  defaults:
    partition_filter:
      auto_latest: 7

  tables:
    - catalog: analytics
      schema: metrics
      table: daily_revenue
      checks:
        volume:
          enabled: true
          metric: "SUM(revenue)"
          min_value: 1
```

### Example 3: Segment-Specific Validation

Validate only completed orders in a specific region:

```yaml
silver:
  tables:
    - catalog: prod_catalog
      schema: sales
      table: orders
      partition_filter:
        region: "EU"
        date: ["2024-01-14", "2024-01-15"]
      row_filter: "status = 'COMPLETED' AND amount > 0"
      checks:
        business_rules:
          enabled: true
          name: valid_completed_orders
          predicate: "shipping_date IS NOT NULL"
```

### Example 4: Mixed Filtering Strategies

Different filters for different tables:

```yaml
bronze:
  defaults:
    partition_filter:
      auto: true  # Default: latest partition

  tables:
    # Uses default (latest partition)
    - catalog: raw
      schema: events
      table: clicks

    # Override: specific date range
    - catalog: raw
      schema: events
      table: purchases
      partition_filter:
        date: ["2024-01-01", "2024-01-02", "2024-01-03"]

    # Override: no partition filter, row filter only
    - catalog: raw
      schema: events
      table: errors
      partition_filter: null  # Disable partition filter
      row_filter: "error_code IS NOT NULL"
```

---

## Best Practices

### Performance

1. **Always use partition filters for large tables** - Partition pruning significantly reduces scan time
2. **Prefer `auto: true`** for daily validation - Automatically adapts to data
3. **Use `auto_latest: N`** for trend validation - Rolling window without manual updates

### Configuration

1. **Set suite-level defaults** - Reduces repetition and ensures consistency
2. **Override only when needed** - Table-level overrides for exceptions
3. **Document complex filters** - Add comments explaining business logic

### Validation

1. **Check filter results** - Review `data_filter` check details
2. **Monitor `filter_ratio`** - Unexpected ratios may indicate issues
3. **Handle empty results** - Decide if empty filtered data is acceptable

---

## Troubleshooting

### Filter Not Applied

**Symptom:** `partition_filter=none` in output

**Causes:**
- Table has no partition columns (for `auto` mode)
- No partition values found
- Filter configuration error

**Solution:** Check table metadata and filter syntax

### Non-Partition Column Warning

**Symptom:** `warning=non_partition_columns=[col1]` in output

**Cause:** Using a non-partition column in `partition_filter`

**Solution:** Move non-partition columns to `row_filter` for better performance

### Empty Results

**Symptom:** `status=no_rows_after_filter`

**Causes:**
- Filter too restrictive
- No data for specified partition
- Data not yet available

**Solution:** Verify filter values match actual data

---

## See Also

- [Modular Config](modular-config.md) - How to structure configuration
- [Tags Filtering](tags-filtering.md) - Filter checks by tags
- [Bronze Suite](../test-suites/bronze.md) - Bronze layer checks
- [Silver Suite](../test-suites/silver.md) - Silver layer checks
- [Gold Suite](../test-suites/gold.md) - Gold layer checks
