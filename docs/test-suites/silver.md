# Silver Test Suite

The Silver test suite validates **cleaned and transformed data** after business logic is applied.

## Characteristics

The Silver suite is:

- **Business-aware**: Understands domain constraints and business rules
- **Column-aware**: Validates specific columns and their relationships
- **Contract-focused**: Enforces correctness and consistency guarantees
- **Check-centric**: Each check produces one result row

## Purpose

Silver validation ensures:

- Data is correct and consistent
- Business rules are properly enforced
- Transformations produced expected results
- Referential integrity is maintained
- Cross-table relationships are valid

!!! tip "Debugging failures"
    When checks fail, see [Debug Experience](../architecture/debug-experience.md) for understanding `failure_stats`, `investigation_query`, and how to investigate.

---

## Configuration Examples

### 1. Minimal Example

Use this when you want to enforce only the most basic correctness guarantee, such as idempotency on a business key.

```yaml
silver:
  tables:
    - catalog: my_catalog
      schema: my_schema_silver
      table: transactions
      checks:
        idempotency:
          enabled: true
          severity: critical
          blocking: true
          key_column: transaction_id
```

**Executed checks:**
- `table_read` (implicit, always critical)
- `idempotency`

---

### 2. Standard Silver Checks (Recommended)

Covers the most common Silver-layer guarantees.

```yaml
silver:
  tables:
    - catalog: my_catalog
      schema: my_schema_silver
      table: transactions
      checks:
        idempotency:
          enabled: true
          severity: critical
          blocking: true
          key_column: transaction_id,product_id

        business_rules:
          enabled: true
          severity: critical
          blocking: true
          name: valid_amounts
          check: "amount >= 0 AND amount < 1000000"
```

**Behavior:**
- Each check validates specific business guarantees
- Typically `blocking: true` and `severity: critical`
- Unlike Bronze, Silver focuses on correctness over technical validation

---

### 3. Multiple Check Instances (Advanced)

Some Silver checks support multiple instances of the same check, each with different parameters.

```yaml
silver:
  tables:
    - catalog: my_catalog
      schema: my_schema_silver
      table: transactions
      checks:
        idempotency:
          - enabled: true
            severity: critical
            blocking: true
            key_column: transaction_id

          - enabled: true
            severity: warning
            blocking: false
            key_column: transaction_id,product_id
```

**Each instance produces one independent result row.**

---

## Threshold Configuration

Row-level checks can tolerate a number or percentage of failing rows via a
`threshold` block. The mechanics (`max_failure_percent`, `max_failure_count`, AND
logic, suite-level defaults) are the same across suites and documented once in
[Thresholds](../configuration/thresholds.md). The Silver-specific part is which
checks support it:

### Supported Silver Checks

| Check | Supports Threshold |
|-------|-------------------|
| `idempotency` | yes |
| `status_transition` | yes |
| `cardinality` | yes |
| `row_temporal_consistency` | yes |
| `business_rules` | yes (shared check) |
| `json_schema` | yes (shared check) |
| `distinct_count` | no (binary pass/fail) |
| `referential_integrity` | no (uses `min_valid_ratio`) |
| `totals_consistency` | no (uses `tolerance`) |
| `cross_table_equality` | no (uses `tolerance`) |
| `temporal_consistency` | no (binary pass/fail) |
| `flow_completeness` | no (uses `tolerance_pct`) |
| `compare_metric` | no (uses operators and value tolerance) |
| `freshness` | no (uses `max_delay_hours`) |

---

## How YAML Maps to Code

| YAML Check Name | Method Invoked | Purpose |
|-----------------|-------------------|---------|
| `idempotency` | `_check_idempotency` | Ensures uniqueness of business keys |
| `status_transition` | `_check_status_transition` | Validates status progression |
| `referential_integrity` | `_check_referential_integrity` | Foreign key validation |
| `cardinality` | `_check_cardinality` | Validates minimum cardinality per group |
| `totals_consistency` | `_check_totals_consistency` | Header totals match detail rows |
| `cross_table_equality` | `_check_cross_table_equality` | Compares values across tables |
| `compare_metric` | `_check_compare_metric` | Generic metric comparison |
| `temporal_consistency` | `_check_temporal_consistency` | Timestamp ordering across tables |
| `row_temporal_consistency` | `_check_row_temporal_consistency` | Timestamp ordering within row |
| `flow_completeness` | `_check_flow_completeness` | Upstream records exist downstream |
| `distinct_count` | `_check_distinct_count` | Validate distinct value count |
| `json_schema` | `_check_json_schema` | Validate JSON column structure |
| `business_rules` | `_check_business_rules` | Validate SQL expressions |
| `freshness` | `_check_freshness` | Validate data freshness |

**Each enabled check:**
- Produces one output row per instance
- Has its own severity and blocking behavior
- May reference multiple tables

---

## Check Details

### idempotency

**Purpose:** Ensures uniqueness of business keys.

**What it does:**
- Groups by the configured key columns
- Fails if duplicates exist

**Key configuration:**
- `key_column`: comma-separated string or list of column names
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of duplicate keys (0-100)
  - `max_failure_count`: maximum allowed number of duplicate keys

**Result details (v1.1.0+):**
- PASS: `"All {n} keys unique"`
- FAIL: `"Found {n} duplicate keys ({pct}% of {total})"`

**Typical use:**
- Prevent duplicate business records
- Enforce uniqueness constraints
- Validate deduplication logic

**Example:**

```yaml
idempotency:
  enabled: true
  severity: critical
  blocking: true
  key_column: transaction_id,product_id
```

**Example with threshold:**

```yaml
idempotency:
  enabled: true
  severity: warning
  blocking: false
  key_column: transaction_id,product_id
  threshold:
    max_failure_percent: 0.1  # Allow up to 0.1% duplicate keys
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 keys unique` |
| FAIL | `Found 45 duplicate keys (0.3% of 15000)` |

---

### status_transition

**Purpose:** Validates that status changes follow an allowed progression.

**What it does:**
- Checks status changes against allowed flow
- Validates timestamps are ordered correctly
- Detects invalid state transitions

**Key configuration:**
- `key_column`: business key (e.g., order_id)
- `status_column`: column containing status values
- `timestamp_column`: column for ordering transitions
- `allowed_flow`: ordered list of valid status values
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of invalid transitions (0-100)
  - `max_failure_count`: maximum allowed number of invalid transitions

**Result details:** `invalid_transitions=<count>; violations=[...]`

**Typical use:**
- Enforce workflow state machines
- Validate order processing flows
- Ensure status integrity

**Example:**

```yaml
status_transition:
  enabled: true
  severity: critical
  blocking: true
  key_column: order_id
  status_column: status
  timestamp_column: updated_at
  allowed_flow:
    - CREATED
    - PAID
    - SHIPPED
    - COMPLETED
```

**Example with threshold:**

```yaml
status_transition:
  enabled: true
  severity: warning
  blocking: false
  key_column: order_id
  status_column: status
  timestamp_column: updated_at
  allowed_flow:
    - CREATED
    - PAID
    - SHIPPED
    - COMPLETED
  threshold:
    max_failure_percent: 2.0  # Allow up to 2% invalid transitions
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 5000 transitions valid` |
| FAIL | `Found 23 invalid transitions (0.5% of 5000)` |

---

### referential_integrity

**Purpose:** Ensures that foreign keys exist in the target table.

**What it does:**
- Joins source and target tables
- Counts orphaned records (foreign keys without matching targets)
- Validates against minimum valid ratio threshold

**Key configuration:**
- `schema` / `table` / `source_column`: source table and FK column
- `target_schema` / `target_table` / `target_column`: target table and PK column
- `catalog` (optional): catalog for source/target tables (default: current table's catalog)
- `min_valid_ratio` (optional): minimum acceptable match ratio (default: 1.0)

**Result details:** `valid_ratio=<ratio>; min_ratio=<ratio>`

**Typical use:**
- Validate foreign key relationships
- Ensure referential integrity after joins
- Detect missing reference data

**Example:**

```yaml
referential_integrity:
  enabled: true
  severity: critical
  blocking: true
  schema: my_schema_silver
  table: transactions
  source_column: product_id
  target_schema: my_schema_gold
  target_table: products
  target_column: product_id
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 10000 references valid` |
| FAIL | `Found 150 orphan records (1.5% of 10000)` |

---

### cardinality

**Purpose:** Validates minimum cardinality per group.

**What it does:**
- Groups by specified columns
- Counts records per group
- Fails if any group has fewer than minimum required

**Key configuration:**
- `group_by`: column(s) to group by
- `min`: minimum records per group
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of violated groups (0-100)
  - `max_failure_count`: maximum allowed number of violated groups

**Result details:** `groups_below_min=<count>; min_required=<min>`

**Typical use:**
- Ensure each customer has minimum orders
- Validate each category has minimum records
- Detect incomplete data

**Example:**

```yaml
cardinality:
  enabled: true
  severity: warning
  blocking: false
  group_by: transaction_id
  min: 1
```

**Example with threshold:**

```yaml
cardinality:
  enabled: true
  severity: warning
  blocking: false
  group_by: order_id
  min: 5
  threshold:
    max_failure_percent: 5.0  # Allow up to 5% of groups below minimum
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 500 groups meet minimum` |
| FAIL | `Found 12 groups below minimum (2.4% of 500)` |

---

### totals_consistency

**Purpose:** Validates that header totals match aggregated detail rows.

**What it does:**
- Joins header and detail tables
- Aggregates detail amounts
- Compares aggregated totals to header totals
- Allows configurable tolerance for floating-point differences

**Key configuration:**
- `join_key`: key to join header and detail tables
- `detail_schema` / `detail_table`: detail table location
- `detail_amount_column`: column to aggregate in detail table
- `header_amount_column`: column in header table to compare against
- `tolerance`: acceptable difference (default: 0.0)
- `catalog` (optional): catalog for detail table (default: current table's catalog)

**Result details:** `mismatched_records=<count>; max_difference=<amount>`

**Typical use:**
- Validate order totals match line items
- Ensure invoice totals equal detail sums
- Detect calculation errors

**Example:**

```yaml
totals_consistency:
  enabled: true
  severity: critical
  blocking: true
  join_key: order_id
  detail_schema: sales_silver
  detail_table: order_items
  detail_amount_column: amount
  header_amount_column: total_amount
  tolerance: 0.01
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 2000 totals consistent` |
| FAIL | `Found 15 header/detail mismatches (0.75% of 2000)` |

---

### cross_table_equality

**Purpose:** Compares values across two tables for equality.

**What it does:**
- Joins two tables on a common key
- Compares specified columns
- Fails if values differ beyond tolerance

**Key configuration:**
- `left_schema` / `left_table` / `left_column`: left side of comparison
- `right_schema` / `right_table` / `right_column`: right side of comparison
- `join_key`: column to join on
- `tolerance`: acceptable difference (default: 0.0)
- `catalog` (optional): catalog for tables (default: current table's catalog)

**Result details (v1.1.0+):**
- PASS: `"All {n} records match"`
- FAIL: `"Found {n} mismatched records ({pct}% of {total})"`

**Typical use:**
- Compare silver to gold snapshots
- Validate consistency across layers
- Detect unexpected changes

**Example:**

```yaml
cross_table_equality:
  enabled: true
  severity: critical
  blocking: true
  left_schema: my_schema_silver
  left_table: transactions
  right_schema: my_schema_gold
  right_table: transactions_snapshot
  join_key: line_item_id
  left_column: score
  right_column: score
  tolerance: 0
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 8000 values match` |
| FAIL | `Found 34 mismatched rows (0.4% of 8000)` |

---

### compare_metric

**Purpose:** Generic metric comparison using expected vs observed values.

!!! info "Full Reference"
    For complete documentation including array statistics, Kendall's W, Borda Count, and advanced examples, see **[Compare Metric Reference](../configuration/compare-metric.md)**.

**What it does:**
- Computes an observed metric (count, sum, avg, countDistinct, etc.)
- Compares against expected value or metric
- Supports operators: `==`, `>=`, `<=`, `!=`, `<`, `>`
- Optional `group_by` for grouped comparisons

**Key configuration:**
- `metric`: observed metric definition (type, column, optional function)
- `value`: expected static value (if comparing to constant)
- `expected_from`: expected metric definition (if comparing to another metric)
- `operator`: comparison operator
- `group_by` (optional): columns to group by

**Result details:** `observed=<value>; expected=<value>; operator=<op>`

**Typical use:**
- Validate aggregations
- Compare metrics across tables
- Enforce business thresholds

**Example 1: Compare to constant**

```yaml
compare_metric:
  enabled: true
  severity: critical
  blocking: true
  metric:
    type: sum
    column: amount
  value: 100000
  operator: ">="
```

**Example 2: Compare to another metric**

```yaml
compare_metric:
  enabled: true
  severity: warning
  blocking: false
  metric:
    type: aggregate
    function: countDistinct
    column: line_item_id
  operator: ">="
  expected_from:
    type: aggregate
    function: countDistinct
    column: product_id
    table: products
    schema: my_schema_silver
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Metric 'compare_metric' OK: 150000 >= 100000` |
| PASS | `Metric 'compare_metric' OK: 10/10 groups satisfy >=` |
| FAIL | `Metric 'compare_metric' expected 100000, got 85000` |
| FAIL | `Found 3 groups failing metric 'compare_metric' (30% of 10)` |

---

### temporal_consistency

**Purpose:** Validates ordering of timestamps across tables.

**What it does:**
- Joins two tables on a common key
- Compares timestamps
- Ensures left timestamp is after (or before) right timestamp

**Key configuration:**
- `left_schema` / `left_table` / `left_timestamp`: left table and timestamp
- `right_schema` / `right_table` / `right_timestamp`: right table and timestamp
- `join_key`: column to join on
- `catalog` (optional): catalog for tables (default: current table's catalog)

**Result details (v1.1.0+):**
- PASS: `"All {n} records in valid order"`
- FAIL: `"Found {n} invalid orderings ({pct}% of {total})"`

**Typical use:**
- Validate processing timestamps after raw_events
- Ensure transformation order
- Detect timestamp anomalies

**Example:**

```yaml
temporal_consistency:
  enabled: true
  severity: critical
  blocking: true
  left_schema: my_schema_silver
  left_table: transactions
  right_schema: my_schema_bronze
  right_table: transactions_raw
  join_key: line_item_id
  left_timestamp: processed_at
  right_timestamp: raw_events_timestamp
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 10000 timestamps ordered` |
| FAIL | `Found 25 temporal violations (0.25% of 10000)` |

---

### row_temporal_consistency

**Purpose:** Validates timestamp ordering within the same row.

**What it does:**
- Compares two timestamp columns in the same table
- Ensures left timestamp is after (or before) right timestamp
- No join required

**Key configuration:**
- `left_timestamp`: first timestamp column
- `right_timestamp`: second timestamp column
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of violations (0-100)
  - `max_failure_count`: maximum allowed number of violations

**Result details (v1.1.0+):**
- PASS: `"All {n} records in valid order"`
- FAIL: `"Found {n} invalid orderings ({pct}% of {total})"`

**Typical use:**
- Validate created_at before updated_at
- Ensure start_time before end_time
- Detect timestamp logic errors

**Example:**

```yaml
row_temporal_consistency:
  enabled: true
  severity: critical
  blocking: true
  left_timestamp: updated_at
  right_timestamp: created_at
```

**Example with threshold:**

```yaml
row_temporal_consistency:
  enabled: true
  severity: warning
  blocking: false
  left_timestamp: updated_at
  right_timestamp: created_at
  threshold:
    max_failure_count: 50  # Allow up to 50 temporal violations
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 15000 row timestamps ordered` |
| FAIL | `Found 75 timestamp inconsistencies (0.5% of 15000)` |

---

### flow_completeness

**Purpose:** Ensures that all upstream records exist downstream.

**What it does:**
- Joins source and target tables
- Identifies records in source missing from target
- Optionally applies source filter

**Key configuration:**
- `source_schema` / `source_table`: upstream table
- `join_key`: column to join on
- `source_filter` (optional): SQL filter for source records

**Result details (v1.1.0+):**
- PASS: `"All {n} source records found in target"`
- FAIL: `"Missing {n} records from target ({pct}% of {total})"`

**Typical use:**
- Validate all bronze records reached silver
- Ensure no data loss in transformations
- Detect incomplete flows

**Example:**

```yaml
flow_completeness:
  enabled: true
  severity: error
  blocking: true
  source_schema: my_schema_bronze
  source_table: raw_events
  join_key: transaction_id
  source_filter: "status = 'SENT'"
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 5000 records present in target` |
| FAIL | `Missing 120 records in target (2.4% of 5000)` |

---

### distinct_count

**Purpose:** Validates that the number of distinct values in a column is within expected bounds.

**What it does:**
- Counts distinct values in a column
- Compares against expected value or min/max range
- Fails if count is outside bounds

**Key configuration:**
- `column`: Column to count distinct values (required)
- `expected`: Exact expected distinct count (optional)
- `min`: Minimum expected distinct count (optional)
- `max`: Maximum expected distinct count (optional)

**Result details:** `distinct_count=<count>; expected=<value>` or `distinct_count=<count>; min=<min>; max=<max>`

**Typical use:**
- Validate cardinality constraints
- Ensure lookup tables have expected number of entries
- Detect data anomalies

**Example 1: Exact count**

```yaml
distinct_count:
  enabled: true
  severity: warning
  blocking: false
  column: status
  expected: 5
```

**Example 2: Range**

```yaml
distinct_count:
  enabled: true
  severity: warning
  blocking: false
  column: category_id
  min: 10
  max: 100
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Distinct count OK: 5 values in 'status' (expected 5)` |
| PASS | `Distinct count OK: 25 in 'category_id' (min=10, max=100)` |
| FAIL | `Distinct count mismatch: expected 5, got 7` |
| FAIL | `Distinct count 8 below minimum 10` |
| FAIL | `Distinct count 150 above maximum 100` |

---

### json_schema

**Purpose:** Validates JSON column structure, required fields, field types, and allowed values.

**What it does:**
- Parses JSON column content
- Validates required fields exist
- Validates field types match expectations
- Validates field values are in allowed sets
- Supports nested field validation with dot-notation

**Key configuration:**
- `column`: JSON column name (required)
- `required_fields`: List of fields that must exist (optional, supports dot-notation)
- `field_types`: Dict of field -> expected type (optional)
- `allowed_values`: Dict of field -> allowed value list (optional)
- `array_element`: If true, validates array elements (default: true)
- `sample_size`: Rows to sample for validation (default: 1000, 0 = all)
- `max_parse_failure_pct`: Max percentage of rows that may fail JSON parsing before the check hard-fails (default: 0)
- `threshold`: Optional threshold configuration
  - `max_failure_percent`: Maximum allowed percentage of validation failures (0-100)
  - `max_failure_count`: Maximum allowed number of validation failures

**Supported types:** `string`, `int`, `long`, `double`, `float`, `boolean`

**Result details:** `total_elements=<count>; missing_fields={...}; type_mismatches={...}; invalid_values={...}`

**Typical use:**
- Validate JSON payloads from APIs
- Ensure configuration columns have correct structure
- Validate semi-structured data

**Example:**

```yaml
json_schema:
  enabled: true
  severity: critical
  blocking: true
  column: metadata
  required_fields:
    - id
    - type
    - rows.row_code    # Nested field validation
  field_types:
    id: string
    count: int
  allowed_values:
    type: ["A", "B", "C"]
    status: ["active", "inactive"]
```

**Example with threshold:**

```yaml
json_schema:
  enabled: true
  severity: warning
  blocking: false
  column: payload
  required_fields:
    - event_type
    - timestamp
  threshold:
    max_failure_percent: 5.0  # Allow up to 5% validation failures
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `JSON schema OK: 5000 elements validated` |
| FAIL | `JSON validation failed: 2 missing fields, 1 type mismatches in 'metadata'` |
| FAIL | `JSON parse failed: 50 rows (1.0%) exceed 0% limit` |

---

### business_rules

**Purpose:** Validates that rows satisfy business rules expressed as SQL expressions.

**What it does:**
- Applies SQL boolean expression to each row
- Counts rows that fail the expression
- Supports threshold for acceptable failure rate

**Key configuration:**
- `check`: SQL boolean expression that must be true (required)
- `name`: Human-readable name for the rule (default: "business_rule")
- `threshold`: Optional threshold configuration
  - `max_failure_percent`: Maximum allowed percentage of failing rows (0-100)
  - `max_failure_count`: Maximum allowed number of failing rows

**Result details:** `rule=<name>; failed_rows=<count>; total_rows=<count>`

**Typical use:**
- Enforce business constraints
- Validate derived fields
- Check data consistency rules

**Example:**

```yaml
business_rules:
  enabled: true
  severity: critical
  blocking: true
  name: valid_amounts
  check: "amount >= 0 AND amount < 1000000"
```

**Example with threshold:**

```yaml
business_rules:
  enabled: true
  severity: warning
  blocking: false
  name: status_consistency
  check: "status != 'COMPLETED' OR completed_at IS NOT NULL"
  threshold:
    max_failure_percent: 1.0  # Allow up to 1% rule violations
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Rule 'valid_amounts' passed (10000 rows)` |
| FAIL | `Found 150 rows violating rule 'valid_amounts' (1.5% of 10000)` |

---

### freshness

**Purpose:** Ensures data is recent based on a timestamp column.

**What it does:**
- Computes the maximum value of a configured timestamp column
- Compares it to the current time (UTC)
- Fails if the delay exceeds the allowed threshold

**Key configuration:**
- `column`: timestamp column to check (required)
- `max_delay_hours`: maximum acceptable delay (default: 24)

**Result details:** `latest=<timestamp>; age_h=<hours>; max_delay_h=<threshold>`

**Typical use:**
- Validate that Silver tables are updated regularly
- Ensure transformation pipelines are running on schedule
- Detect stale data before it propagates to Gold

**Example:**

```yaml
freshness:
  enabled: true
  severity: warning
  blocking: false
  column: updated_at
  max_delay_hours: 48
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Data fresh: 12.5h old (within 48h limit)` |
| FAIL | `Data age 72.3h exceeds 48h limit` |
| FAIL | `No data found in column 'updated_at'` |

---

## Design Guidelines

### Do

- **Enforce data correctness and contracts**: Silver checks ensure business logic is applied correctly
- **Prefer blocking for business-critical guarantees**: Use `blocking: true` for critical checks
- **Use multiple instances**: Different instances of the same check with different parameters
- **Validate cross-table relationships**: Referential integrity, consistency, completeness

### Avoid

- **Perform aggregations meant for KPIs**: KPI calculations belong in Gold
- **Validate raw technical properties**: Technical validation belongs in Bronze
- **Create overly complex checks**: Keep checks focused and understandable

---

## Conceptual Positioning

```
Bronze → Silver → Gold
```

**Bronze**: Data landed correctly (technical validation)

**Silver**: Data is correct and consistent (business validation)

**Gold**: Data supports business decisions (KPI validation)

This separation is intentional and enforced by the framework design.

---

## Complete Example

Full Silver configuration for a sales processing pipeline:

```yaml
silver:
  # Tables with specific checks
  tables:
    # Answers table
    - catalog: my_catalog
      schema: my_schema_silver
      table: transactions
      checks:
        idempotency:
          enabled: true
          severity: critical
          blocking: true
          key_column: transaction_id,product_id

        business_rules:
          enabled: true
          severity: critical
          blocking: true
          name: valid_amounts
          check: "amount >= 0 AND amount < 1000000"

        referential_integrity:
          enabled: true
          severity: critical
          blocking: true
          schema: my_schema_silver
          table: transactions
          source_column: product_id
          target_schema: my_schema_gold
          target_table: products
          target_column: product_id

        row_temporal_consistency:
          enabled: true
          severity: critical
          blocking: true
          left_timestamp: updated_at
          right_timestamp: created_at

    # Orders table
    - catalog: my_catalog
      schema: sales_silver
      table: orders
      checks:
        idempotency:
          enabled: true
          severity: critical
          blocking: true
          key_column: order_id

        status_transition:
          enabled: true
          severity: critical
          blocking: true
          key_column: order_id
          status_column: status
          timestamp_column: updated_at
          allowed_flow:
            - CREATED
            - PAID
            - SHIPPED
            - COMPLETED

        totals_consistency:
          enabled: true
          severity: critical
          blocking: true
          join_key: order_id
          detail_schema: sales_silver
          detail_table: order_items
          detail_amount_column: amount
          header_amount_column: total_amount
          tolerance: 0.01

        compare_metric:
          enabled: true
          severity: warning
          blocking: false
          metric:
            type: sum
            column: total_amount
          value: 0
          operator: ">"
```

---

## See Also

- [Bronze Suite](bronze.md) - Previous validation layer
- [Gold Suite](gold.md) - Next validation layer
- [Architecture Overview](../architecture/overview.md) - Framework design
