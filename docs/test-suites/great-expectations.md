# Great Expectations Test Suite

The Great Expectations test suite provides schema-driven, column-centric validation integrated into the neco_dq framework.

## Characteristics

The Great Expectations suite is:

- **Schema-driven**: Strong schema contracts and data contracts
- **Column-centric**: Focused on column-level validation
- **Declarative**: Readable, configuration-first approach
- **Contract-focused**: Well-suited for producer-consumer data contracts
- **Integrated**: Centralized policy enforcement, persistence, and alerting through neco_dq

## Purpose

Great Expectations validation ensures:

- Strong schema contracts are enforced
- Column-level validations are declarative and readable
- Data contracts between producers and consumers are honored
- Rules can be reused outside Spark (e.g., Pandas-based checks)
- Structural guarantees are maintained

!!! tip "Debugging failures"
    When checks fail, see [Debug Experience](../architecture/debug-experience.md) for understanding `failure_stats`, `investigation_query`, and how to investigate.

---

## When to Use

The GE suite is recommended when:

**Schema contracts**: You want strong, enforceable schema definitions

**Column-level validation**: Validations are primarily column-focused

**Declarative rules**: Rules should be readable and configuration-driven

**Cross-platform**: Same rules may be reused outside Spark

**Typical use cases:**
- Schema enforcement
- Data contracts between teams
- Ingestion-level structural guarantees
- Early Silver layer validation

---

## Configuration Examples

### 1. Minimal Example

A minimal configuration validating only column existence and non-nullability.

```yaml
great_expectations:
  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: transactions_raw
      checks:
        transaction_id:
          not_null:
            enabled: true
            severity: critical
            blocking: true
```

**Implicit behavior:**
- Every column defined in `checks` is checked for existence
- Column existence is always critical and blocking

**Executed checks:**
- `column_exists` (implicit, critical)
- `not_null`

---

### 2. Standard Example (Recommended)

A typical schema contract with type, nullability, and value constraints.

```yaml
great_expectations:
  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: transactions_raw
      checks:
        transaction_id:
          not_null:
            enabled: true
            severity: critical
            blocking: true

          unique:
            enabled: true
            severity: critical
            blocking: true

        score:
          dtype:
            enabled: true
            values: ["int", "bigint"]
            severity: warning
            blocking: false

          between:
            enabled: true
            min: 1
            max: 5
            mostly: 0.99
            severity: warning
            blocking: false
```

**Each rule produces one independent result row.**

!!! warning "Every rule needs `enabled: true`"
    All GE rules (except the implicit `column_exists`) are opt-in. A rule without
    `enabled: true` is skipped silently. The only exception is `nullable`, which runs
    when `enabled: false` to enforce a not-null policy.

---

### 3. With Dataset-Level Rules

Dataset-level rules are defined under the special `_dataset` key.

```yaml
great_expectations:
  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: transactions_raw
      checks:
        # Dataset-level rules
        _dataset:
          min_rows: 1
          max_rows: 1000000
          severity: critical
          blocking: true

          integrity:
            pair_gte:
              - column_A: updated_at
                column_B: created_at
                mostly: 1.0
                severity: critical
                blocking: true

          no_duplicate:
            enabled: true
            column_list:
              - transaction_id
              - product_id
            severity: critical
            blocking: true

        # Column-level rules
        transaction_id:
          not_null:
            enabled: true
            severity: critical
            blocking: true
```

---

## How YAML Maps to Great Expectations

| YAML Check | GE Expectation | Purpose |
|------------|----------------|---------|
| `not_null` | `ExpectColumnValuesToNotBeNull` | No null values |
| `nullable` | `ExpectColumnValuesToNotBeNull` | Null policy (run when `enabled: false`) |
| `unique` | `ExpectColumnValuesToBeUnique` | Unique values |
| `dtype` | `ExpectColumnValuesToBeInTypeList` | Data type validation |
| `between` | `ExpectColumnValuesToBeBetween` | Numeric range |
| `values_set` | `ExpectColumnValuesToBeInSet` | Categorical values |
| `not_in_set` | `ExpectColumnValuesToNotBeInSet` | Forbidden values (blocklist) |
| `distinct_values_in_set` | `ExpectColumnDistinctValuesToBeInSet` | Distinct set is subset of allowed |
| `regex` | `ExpectColumnValuesToMatchRegex` | String pattern |
| `length` | `ExpectColumnValueLengthsToBeBetween` | String length range |
| `date_format` | `ExpectColumnValuesToMatchStrftimeFormat` | Date string format |
| `_dataset.min_rows / max_rows` | `ExpectTableRowCountToBeBetween` | Row count bounds |
| `_dataset.integrity.pair_gte` | `ExpectColumnPairValuesAToBeGreaterThanB` | Cross-column A >= B |
| `_dataset.integrity.pair_gt` | `ExpectColumnPairValuesAToBeGreaterThanB` | Cross-column A > B |
| `_dataset.integrity.pair_eq` | `ExpectColumnPairValuesToBeEqual` | Cross-column A == B |
| `_dataset.no_duplicate` | `ExpectCompoundColumnsToBeUnique` | Compound key uniqueness |
| `_dataset.multicolumn_sum` | `ExpectMulticolumnSumToEqual` | Per-row column sum invariant |

---

## Column-Level Check Details

### column_exists (implicit)

**Purpose:** Ensures the column exists in the table schema.

**Behavior:** This check is automatically executed for every column defined in `checks`. It's always critical and blocking.

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Column 'transaction_id' exists` |
| FAIL | `Column 'transaction_id' not found` |

---

### not_null

**Purpose:** Ensures column contains no null values.

**Key configuration:**
- `enabled`: whether to run this check
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectColumnValuesToNotBeNull`

**Example:**

```yaml
transaction_id:
  not_null:
    enabled: true
    severity: critical
    blocking: true
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 values valid` |
| FAIL | `Found 45 null values (0.3% of 15000)` |

---

### unique

**Purpose:** Ensures column or compound key uniqueness.

**Key configuration:**
- `enabled`: whether to run this check
- `column_list` (optional): for compound keys
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:**
- `ExpectColumnValuesToBeUnique` (single column)
- `ExpectCompoundColumnsToBeUnique` (compound key)

**Example 1: Single column**

```yaml
transaction_id:
  unique:
    enabled: true
    severity: critical
    blocking: true
```

**Example 2: Compound key**

```yaml
transaction_id:
  unique:
    enabled: true
    column_list: [transaction_id, product_id]
    severity: critical
    blocking: true
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 values valid` |
| FAIL | `Found 120 duplicates (0.8% of 15000)` |

---

### dtype

**Purpose:** Validates data types.

**Key configuration:**
- `enabled`: whether to run this check
- `values`: list of allowed data types
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectColumnValuesToBeInTypeList`

**Example:**

```yaml
score:
  dtype:
    enabled: true
    values: ["int", "bigint"]
    severity: warning
    blocking: false
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 values valid` |
| FAIL | `Type mismatch: expected ['int', 'bigint']` |

---

### between

**Purpose:** Validates numeric ranges.

**Key configuration:**
- `enabled`: whether to run this check
- `min`: minimum value
- `max`: maximum value
- `mostly` (optional): percentage of values that must pass (default: 1.0)
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectColumnValuesToBeBetween`

**Example:**

```yaml
score:
  between:
    enabled: true
    min: 1
    max: 5
    mostly: 0.99
    severity: warning
    blocking: false
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 values valid` |
| FAIL | `Found 250 values out of range [1, 5] (1.7% of 15000)` |

---

### values_set

**Purpose:** Validates allowed categorical values.

**Key configuration:**
- `enabled`: whether to run this check
- `values`: list of allowed values
- `mostly` (optional): percentage of values that must pass (default: 1.0)
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectColumnValuesToBeInSet`

**Example:**

```yaml
status:
  values_set:
    enabled: true
    values: ["CREATED", "PAID", "CANCELLED"]
    mostly: 1.0
    severity: critical
    blocking: true
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 values valid` |
| FAIL | `Found 35 invalid values (0.2% of 15000)` |

---

### not_in_set

**Purpose:** Validates that values are NOT in a forbidden set. Useful as a blocklist for placeholder or dirty values (e.g. `"N/A"`, `"unknown"`, `"-1"`).

**Key configuration:**
- `enabled`: whether to run this check
- `values`: list of forbidden values
- `mostly` (optional): fraction of values that must NOT be in the set (default: 1.0)
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectColumnValuesToNotBeInSet`

**Example:**

```yaml
status:
  not_in_set:
    enabled: true
    values: ["N/A", "unknown", "-1"]
    severity: warning
    blocking: false
```

---

### distinct_values_in_set

**Purpose:** Validates that the column's set of distinct values is a subset of an allowed set. Unlike `values_set`, which checks each row with a `mostly` tolerance, this checks the distinct value set as a whole, which fits enum columns well.

**Key configuration:**
- `enabled`: whether to run this check
- `values`: list of allowed values

**Mapped to:** `ExpectColumnDistinctValuesToBeInSet`

**Example:**

```yaml
status:
  distinct_values_in_set:
    enabled: true
    values: ["CREATED", "PAID", "CANCELLED"]
    severity: warning
```

---

### regex

**Purpose:** Validates string patterns.

**Key configuration:**
- `enabled`: whether to run this check
- `pattern`: regex pattern to match
- `mostly` (optional): percentage of values that must pass (default: 1.0)
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectColumnValuesToMatchRegex`

**Example:**

```yaml
email:
  regex:
    enabled: true
    pattern: "^[^@]+@[^@]+\\.[^@]+$"
    mostly: 0.98
    severity: warning
    blocking: false
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 values valid` |
| FAIL | `Found 180 pattern mismatches (1.2% of 15000)` |

---

### length

**Purpose:** Validates that string value lengths fall within a range. Useful for fixed-width codes (e.g. a 5-digit postal code) or near-fixed-length identifiers.

**Key configuration:**
- `enabled`: whether to run this check
- `min`: minimum allowed length
- `max`: maximum allowed length
- `mostly` (optional): percentage of values that must pass (default: 1.0)
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectColumnValueLengthsToBeBetween`

**Example:**

```yaml
postal_code:
  length:
    enabled: true
    min: 5
    max: 5
    severity: warning
    blocking: false
```

---

### date_format

**Purpose:** Validates that string values match a strftime date format. Use this for dates stored as strings, where a typed date/timestamp column is not available.

**Key configuration:**
- `enabled`: whether to run this check
- `format`: strftime pattern (e.g. `%Y-%m-%d`)
- `mostly` (optional): percentage of values that must pass (default: 1.0)
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectColumnValuesToMatchStrftimeFormat`

**Supported formats**

This check uses Python `strptime` internally, which requires the format to match the value exactly, with no prefix and no suffix. The following formats work:

| Format | Example value |
|---|---|
| `%Y-%m-%d` | `2026-05-26` |
| `%d/%m/%Y` | `26/05/2026` |
| `%Y%m%d` | `20260526` |
| `%Y-%m-%dT%H:%M:%S` | `2026-05-26T01:32:37` |
| `%Y-%m-%dT%H:%M:%S.%f` | `2026-05-26T01:32:37.098000` |
| `%d-%m-%Y %H:%M:%S` | `26-05-2026 01:32:37` |

**Timezone offsets do not work**

Timestamps with a timezone offset (e.g. `2026-05-26T01:32:37.098+00:00`) do not work with this check. `strptime` must consume the entire value, and the `+00:00` suffix has no matching directive that works without breaking GE at construction time. Formats with `%z` or `%Z` are skipped automatically with a warning in the logs.

For timezone-aware timestamps, use `regex` instead:

```yaml
batch_ingestion_at:
  regex:
    enabled: true
    pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+[+-]\\d{2}:\\d{2}$"
    severity: warning
    blocking: false
```

**Example:**

```yaml
created_date:
  date_format:
    enabled: true
    format: "%Y-%m-%d"
    severity: warning
    blocking: false
```

---

## Dataset-Level Check Details

### min_rows / max_rows

**Purpose:** Validates table row count bounds.

**Key configuration:**
- `min_rows`: minimum acceptable row count
- `max_rows`: maximum acceptable row count
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectTableRowCountToBeBetween`

**Example:**

```yaml
_dataset:
  min_rows: 1
  max_rows: 1000000
  severity: critical
  blocking: true
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Row count OK: 15000` |
| FAIL | `Row count 0 outside expected range [1, 1000000]` |
| FAIL | `Row count 1500000 outside expected range [1, 1000000]` |

---

### integrity.pair_gte / pair_gt / pair_eq

**Purpose:** Cross-column integrity rules comparing two columns row by row:

- `pair_gte`: column_A >= column_B
- `pair_gt`: column_A > column_B (strictly greater)
- `pair_eq`: column_A == column_B

Each key takes a list of rules, so a table can define several pairs.

**Key configuration (per rule):**
- `column_A`: first column
- `column_B`: second column
- `mostly` (optional): percentage of rows that must pass (default: 1.0)
- `ignore_row_if` (optional): how to handle missing values (default: `either_value_is_missing`)
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:**
- `pair_gte` / `pair_gt`: `ExpectColumnPairValuesAToBeGreaterThanB` (`or_equal` true/false)
- `pair_eq`: `ExpectColumnPairValuesToBeEqual`

**Example:**

```yaml
_dataset:
  integrity:
    pair_gte:
      - column_A: updated_at
        column_B: created_at
        mostly: 1.0
        severity: critical
        blocking: true
    pair_eq:
      - column_A: total
        column_B: recomputed_total
        severity: warning
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 values valid` |
| FAIL | `Found 85 pair violations (0.6% of 15000)` |

---

### no_duplicate

**Purpose:** Validates no duplicate rows on specified columns.

**Key configuration:**
- `enabled`: whether to run this check
- `column_list`: columns to check for uniqueness
- `use_schema_columns` (optional): use all defined schema columns
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectCompoundColumnsToBeUnique`

**Example 1: Explicit column list**

```yaml
_dataset:
  no_duplicate:
    enabled: true
    column_list:
      - transaction_id
      - product_id
    severity: critical
    blocking: true
```

**Example 2: Use schema columns**

```yaml
_dataset:
  no_duplicate:
    enabled: true
    use_schema_columns: true
    severity: critical
    blocking: true
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 values valid` |
| FAIL | `Found 45 duplicates (0.3% of 15000)` |

---

### multicolumn_sum

**Purpose:** Validates a per-row arithmetic invariant across columns (e.g. `subtotal + tax == total`). Takes a list of rules, so a table can define several invariants.

**Key configuration (per rule):**
- `enabled`: whether to run this rule (default: true)
- `column_list`: columns to sum
- `sum_total`: the value the sum must equal
- `mostly` (optional): fraction of rows that must pass (default: 1.0)
- `ignore_row_if` (optional): how to handle missing values (default: `all_values_are_missing`)
- `severity`: `critical` or `warning`
- `blocking`: whether to stop pipeline on failure

**Mapped to:** `ExpectMulticolumnSumToEqual`

**Example:**

```yaml
_dataset:
  multicolumn_sum:
    - column_list: [subtotal, tax]
      sum_total: 100
      severity: warning
      blocking: false
```

---

## Metadata and Blocking Behavior

Each expectation is enriched with framework metadata:

- `severity`: `warning` | `critical`
- `blocking`: `true` | `false`
- `rule_description`: Human-readable description

**These fields are:**
- Propagated into neco_dq result rows
- Evaluated centrally by the failure policy layer
- Persisted and used for alerting

**Important:** GE itself does not control pipeline execution, neco_dq does.

---

## Result Normalization

GE validation results are:

1. **Normalized** into the standard neco_dq schema
2. **Enriched** with:
   - `record_count`
   - `failed_count`
   - `expected` vs `actual` values
3. **Written** using `DQResultWriter`

This guarantees uniform reporting across Spark and GE suites.

**Example transformation:**

```
GE Result:
{
  "success": false,
  "unexpected_count": 5,
  "element_count": 1000
}

neco_dq Result:
{
  "suite_name": "great_expectations",
  "table": "transactions_raw",
  "test_name": "not_null",
  "column": "transaction_id",
  "observed": 5,
  "expected": 0,
  "pass_fail": False,
  "severity": "critical",
  "blocking": True
}
```

---

## Complete Example

Full configuration for schema contract validation:

```yaml
great_expectations:
  # Tables with schema contracts
  tables:
    # Answers raw table
    - catalog: my_catalog
      schema: my_schema_bronze
      table: transactions_raw
      checks:
        # Dataset-level rules
        _dataset:
          # Row count bounds
          min_rows: 1
          max_rows: 1000000
          severity: critical
          blocking: true

          # Cross-column integrity
          integrity:
            pair_gte:
              - column_A: updated_at
                column_B: created_at
                mostly: 1.0
                severity: critical
                blocking: true

          # No duplicates on compound key
          no_duplicate:
            enabled: true
            column_list:
              - transaction_id
              - product_id
            severity: critical
            blocking: true

        # Primary key: transaction_id
        transaction_id:
          not_null:
            enabled: true
            severity: critical
            blocking: true

          unique:
            enabled: true
            severity: critical
            blocking: true

        # Foreign key: product_id
        product_id:
          not_null:
            enabled: true
            severity: critical
            blocking: true

        # Score validation
        score:
          dtype:
            enabled: true
            values: ["int", "bigint"]
            severity: warning
            blocking: false

          between:
            enabled: true
            min: 1
            max: 5
            mostly: 0.99
            severity: warning
            blocking: false

        # Status validation
        status:
          values_set:
            enabled: true
            values: ["CREATED", "PAID", "CANCELLED"]
            mostly: 1.0
            severity: critical
            blocking: true

        # Email validation
        email:
          regex:
            enabled: true
            pattern: "^[^@]+@[^@]+\\.[^@]+$"
            mostly: 0.98
            severity: warning
            blocking: false

    # Payments table
    - catalog: my_catalog
      schema: my_schema_bronze
      table: payments_raw
      checks:
        _dataset:
          min_rows: 1
          severity: critical
          blocking: true

        payment_id:
          not_null:
            enabled: true
            severity: critical
            blocking: true

          unique:
            enabled: true
            severity: critical
            blocking: true

        amount:
          dtype:
            enabled: true
            values: ["decimal", "double"]
            severity: critical
            blocking: true

          between:
            enabled: true
            min: 0
            max: 1000000
            mostly: 1.0
            severity: critical
            blocking: true

        currency:
          values_set:
            enabled: true
            values: ["USD", "EUR", "GBP"]
            mostly: 1.0
            severity: critical
            blocking: true
```

---

## Data Filtering

The GE suite supports two levels of data filtering:

1. **Suite/Table-level filtering** (`row_filter`, `partition_filter`): Applied to Spark DataFrame before conversion to Pandas
2. **Check-level filtering** (`row_condition`): Applied by Great Expectations on the Pandas DataFrame

### row_filter (Recommended for dates)

Filters data using **Spark SQL syntax** before converting to Pandas. Supports dynamic functions like `current_date()`.

```yaml
great_expectations:
  # Suite-level default (applies to all tables)
  defaults:
    row_filter: "batch_ingestion_at >= current_date() - INTERVAL 7 DAYS"

  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      # Table-level override
      row_filter: "created_at >= current_date() - INTERVAL 3 DAYS"
      checks:
        order_id:
          not_null:
            enabled: true
```

**Key points:**
- Uses **Spark SQL** syntax (dynamic dates work: `current_date()`, `INTERVAL`)
- Applied **before** `toPandas()` conversion (better performance on large tables)
- Produces a `data_filter` check in results showing rows before/after

### partition_filter

Filters on partition columns for optimized reads.

```yaml
great_expectations:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: events
      partition_filter:
        date: "2026-03-30"
      checks:
        event_id:
          not_null:
            enabled: true
```

**Supports:**
- Single value: `date: "2026-03-30"`
- Multiple values: `date: ["2026-03-29", "2026-03-30"]`
- Multiple columns: `{year: 2026, month: 3}`

### row_condition (Check-level)

Filters rows for a **specific check** using **Pandas query syntax**. Useful for conditional validation.

```yaml
great_expectations:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      checks:
        shipping_date:
          not_null:
            enabled: true
            # Only validate for shipped orders
            row_condition: "status == 'SHIPPED'"
            severity: critical
            blocking: true
```

**Key points:**
- Uses **Pandas query** syntax (NOT Spark SQL)
- **Does NOT support** dynamic date functions (`current_date()` won't work)
- Best for filtering on **string** or **numeric** columns

### Comparison: row_filter vs row_condition

| Feature | `row_filter` | `row_condition` |
|---------|--------------|-----------------|
| **Scope** | Suite/Table level | Single check |
| **Engine** | Spark SQL | Pandas query |
| **Dynamic dates** | `current_date()`, `INTERVAL` | Static values only |
| **Applied** | Before `toPandas()` | After `toPandas()` |
| **Performance** | Better (filters early) | N/A (already in memory) |
| **Use case** | Date ranges, partitions | Conditional logic per check |

### When to use which

| Scenario | Use |
|----------|-----|
| Filter last N days of data | `row_filter` |
| Filter on partition columns | `partition_filter` |
| Validate only specific statuses | `row_condition` |
| Different date ranges per check | `row_filter` at table level |
| Same condition for all checks | `row_filter` in `defaults` |

### Combined example

```yaml
great_expectations:
  defaults:
    # All tables: last 7 days
    row_filter: "batch_ingestion_at >= current_date() - INTERVAL 7 DAYS"

  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      checks:
        order_id:
          not_null:
            enabled: true
            severity: critical

        # Only validate shipping_date for shipped orders
        shipping_date:
          not_null:
            enabled: true
            row_condition: "status == 'SHIPPED'"
            severity: warning

        # Only validate amount > 0 for non-refunded orders
        amount:
          between:
            enabled: true
            min: 0.01
            row_condition: "order_type != 'REFUND'"
            severity: warning
```

### Filter output

When filters are applied, a `data_filter` check is added to results:

```
check_name: data_filter
passed: true
details:
  status: ok
  row_filter: "batch_ingestion_at >= current_date() - INTERVAL 7 DAYS"
  partition_filter: none
  rows_before: 1000000
  rows_after: 50000
  filter_ratio: 5.00%
```

---

## Design Guidelines

### Do

- **Use GE for schema contracts and column-level validation**: GE excels at declarative schema enforcement
- **Prefer GE in early Silver layers**: Where schema contracts are critical
- **Use dataset-level rules for structural guarantees**: Row counts, cross-column integrity
- **Set severity appropriately**: Critical for schema, warning for data quality

### Avoid

- **Avoid metric reconciliation in GE**: That's Gold suite responsibility
- **Don't use GE for complex business logic**: Use Spark-native Silver/Gold suites instead
- **Skip column existence checks**: They're automatic and critical

---

## Conceptual Positioning

```
Bronze → Silver (with GE) → Gold
```

**Bronze**: Data landed correctly (technical validation)

**Silver (with GE)**: Schema contracts enforced (structural validation)

**Silver (Spark-native)**: Data is correct and consistent (business validation)

**Gold**: Data supports business decisions (KPI validation)

GE is particularly well-suited for the Bronze → early Silver transition where schema contracts matter most.

---

## See Also

- [Bronze Suite](bronze.md) - Technical validation layer
- [Silver Suite](silver.md) - Business validation layer
- [Architecture Overview](../architecture/overview.md) - Framework design
- [Great Expectations Documentation](https://docs.greatexpectations.io/) - Official GE docs
