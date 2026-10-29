# Gold Test Suite

The Gold test suite validates **aggregated business metrics** and final outputs ready for consumption by analytics, reporting, and ML systems.

## Characteristics

The Gold suite is:

- **Business-metric focused**: Validates KPIs and business metrics
- **Reconciliation-oriented**: Ensures consistency with Silver layer
- **Aggregation-aware**: Validates computed aggregations and rollups
- **KPI- and reporting-ready**: Ensures data meets business requirements
- **Check-centric**: Each check produces one result row

## Purpose

Gold validation ensures:

- Business metrics are numerically correct
- Aggregations reconcile with Silver source data
- KPIs meet business expectations and targets
- Metrics are internally consistent
- Data is fresh and ready for consumption

!!! tip "Debugging failures"
    When checks fail, see [Debug Experience](../architecture/debug-experience.md) for understanding `failure_stats`, `investigation_query`, and how to investigate.

---

## Configuration Examples

### 1. Minimal Example

Use this when you want to enforce a single business guarantee, such as freshness or a basic metric comparison.

```yaml
gold:
  tables:
    - catalog: my_catalog
      schema: my_schema_gold
      table: payments
      checks:
        freshness:
          enabled: true
          severity: critical
          blocking: true
          timestamp_column: paid_at
          max_delay_hours: 24
```

**Executed checks:**
- `table_read` (implicit, always critical)
- `freshness`

---

### 2. Standard Gold Checks (Recommended)

Covers the most common Gold-layer guarantees: metric validation and reconciliation with Silver.

```yaml
gold:
  tables:
    - catalog: my_catalog
      schema: my_schema_gold
      table: payments
      checks:
        compare_metric:
          enabled: true
          severity: critical
          blocking: true
          metric:
            type: sum
            column: amount
          value: 100000
          operator: ">="

        reconciliation_expr:
          enabled: true
          severity: critical
          blocking: true
          source_silver: my_catalog.my_schema_silver.payments
          gold_expr: amount
          silver_expr: amount
          tolerance_percent: 0.01
```

**Behavior:**
- Validates business metrics meet thresholds
- Reconciles aggregated Gold values against Silver source
- Typically `blocking: true` and `severity: critical` for financial metrics

---

### 3. Multiple Check Instances (Advanced)

Gold checks support multiple instances of the same check, for example validating metrics at different aggregation levels.

```yaml
gold:
  tables:
    - catalog: my_catalog
      schema: my_schema_gold
      table: payments
      checks:
        compare_metric:
          - enabled: true
            severity: critical
            blocking: true
            metric:
              type: sum
              column: amount
            value: 100000
            operator: ">="

          - enabled: true
            severity: warning
            blocking: false
            metric:
              type: count
            value: 1000
            operator: ">="
```

**Each instance produces one independent result row.**

---

## Threshold Configuration

Row-level checks can tolerate a number or percentage of failing rows via a
`threshold` block. The mechanics (`max_failure_percent`, `max_failure_count`, AND
logic, suite-level defaults) are the same across suites and documented once in
[Thresholds](../configuration/thresholds.md). The Gold-specific part is which
checks support it:

### Supported Gold Checks

| Check | Supports Threshold |
|-------|-------------------|
| `uniqueness` | yes |
| `business_rules` | yes (shared check) |
| `json_schema` | yes (shared check) |
| `compare_metric` | no (uses operators and value tolerance) |
| `reconciliation_expr` | no (uses `tolerance_percent`) |
| `referential_integrity` | no (strict: fails if any FK is missing) |
| `coverage` | no (uses `tolerance_percent`) |
| `volume` | no (uses `min_value`/`max_value` bounds or history `tolerance_percent`) |
| `freshness` | no (uses `max_delay_hours`) |
| `cross_gold` | no (uses `tolerance_abs`) |
| `completeness` | no (binary pass/fail) |
| `trend` | no (uses history deviation) |
| `distinct_count` | no (binary pass/fail) |

---

## How YAML Maps to Code

| YAML Check Name | Method Invoked | Purpose |
|-----------------|----------------|---------|
| `business_rules` | `_check_business_rules` | Validates custom business expressions at row level |
| `compare_metric` | `_check_compare_metric` | Compares observed metrics with expected values |
| `reconciliation_expr` | `_check_reconciliation_expr` | Reconciles aggregated Gold values against Silver |
| `referential_integrity` | `_check_referential_integrity` | Ensures Gold foreign keys exist in Silver |
| `uniqueness` | `_check_uniqueness` | Ensures uniqueness of Gold-level keys |
| `coverage` | `_check_coverage` | Validates Gold metrics sufficiently cover Silver metrics |
| `volume` | `_check_volume` | Detects anomalous variations of volume metrics |
| `freshness` | `_check_freshness` | Ensures Gold data is up to date |
| `cross_gold` | `_check_cross_gold` | Compares metrics across two Gold tables |
| `completeness` | `_check_completeness` | Verify expected dimension values are present |
| `trend` | `_check_trend` | Verify metric trend against history |
| `distinct_count` | `_check_distinct_count` | Validate distinct value count |
| `json_schema` | `_check_json_schema` | Validate JSON column structure |

**Each enabled check:**
- Produces one output row per instance
- Has its own severity and blocking behavior
- May reference Silver or other Gold tables

---

## Check Details

### business_rules

**Purpose:** Validates custom business expressions at row level.

**What it does:**
- Applies SQL expressions to each row
- Validates business logic and constraints
- Fails if any row violates the rule

**Key configuration:**
- `name`: descriptive name for the rule
- `predicate`: SQL expression that must evaluate to true
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of failed rows (0-100)
  - `max_failure_count`: maximum allowed number of failed rows

**Result details:** `failed_rows=<count>; expression=<check>`

**Typical use:**
- Enforce business constraints
- Validate calculated fields
- Check derived metrics

**Example:**

```yaml
business_rules:
  enabled: true
  severity: critical
  blocking: true
  name: amount_and_currency_valid
  predicate: "amount >= 0 AND currency IS NOT NULL"
```

**Example with threshold:**

```yaml
business_rules:
  enabled: true
  severity: warning
  blocking: false
  name: amount_positive
  predicate: "amount >= 0"
  threshold:
    max_failure_percent: 1.0  # Allow up to 1% rule violations
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Rule 'amount_and_currency_valid' passed (5000 rows)` |
| FAIL | `Found 45 rows violating rule 'amount_positive' (0.9% of 5000)` |

---

### compare_metric

**Purpose:** Compares observed metrics with expected values.

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
- `name` (optional): descriptive name

**Result details:** `observed=<value>; expected=<value>; operator=<op>`

**Typical use:**
- Validate business thresholds
- Compare metrics across tables
- Validate KPIs

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
    column: answer_id
  operator: ">="
  expected_from:
    type: aggregate
    function: countDistinct
    column: product_id
    table: questions
    schema: my_schema_silver
```

**Example 3: Compare array size to table count**

```yaml
compare_metric:
  enabled: true
  severity: error
  blocking: true
  name: products_vs_orders_per_region

  # Observed: number of questions per order_id from JSON array
  metric:
    type: size_array
    column: metadata
    group_by:
      - order_id

  # Expected: number of orders per order_id from orders table
  expected_from:
    type: aggregate
    catalog: my_catalog
    schema: my_schema_silver
    table: orders
    column: product_id
    function: countDistinct
    group_by:
      - order_id

  operator: "=="
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Metric 'total_amount_threshold' OK: 150000 >= 100000` |
| PASS | `Metric 'products_vs_orders_per_region' OK: 50/50 groups satisfy ==` |
| FAIL | `Metric 'payment_count_threshold' expected 1000, got 850` |
| FAIL | `Found 5 groups failing metric 'products_vs_orders_per_region' (10% of 50)` |

---

### reconciliation_expr

**Purpose:** Reconciles aggregated Gold values against Silver values.

**What it does:**
- Aggregates Gold data using provided expression
- Aggregates Silver source data using provided expression
- Compares aggregated values
- Allows configurable tolerance for floating-point differences

**Key configuration:**
- `source_silver`: fully qualified Silver table name
- `gold_expr`: SQL expression to aggregate Gold data
- `silver_expr`: SQL expression to aggregate Silver data
- `group_by` (optional): columns to group by for reconciliation
- `gold_join_columns` / `silver_join_columns` (optional): restrict Silver to the
  keys present in Gold; one column or a list, paired by position (same length)
- `tolerance_percent`: acceptable percentage difference (default: 1.0)
- `name` (optional): descriptive name

**Result details:** `mismatched_groups=<count>; max_diff=<amount>`

**Typical use:**
- Validate Gold aggregations match Silver source
- Ensure no data loss in aggregation
- Detect calculation errors

**Example:**

```yaml
reconciliation_expr:
  enabled: true
  severity: warning
  blocking: false
  name: customers_reconciliation

  source_silver: my_catalog.my_schema_silver.orders

  gold_expr: "SUM(unique_customers)"
  silver_expr: "COUNT(DISTINCT customer_id)"

  group_by:
    - order_id

  tolerance_percent: 5.0
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Reconciliation OK: gold=15000, silver=15000 (diff 0.0%)` |
| PASS | `All 100 groups reconcile (within 5%)` |
| FAIL | `Reconciliation deviation 12.5% exceeds 5% limit` |
| FAIL | `Found 8 groups exceeding 5% deviation (8% of 100)` |

---

### referential_integrity

**Purpose:** Ensures Gold foreign keys exist in Silver.

**What it does:**
- Joins Gold and Silver tables
- Identifies orphaned Gold records
- Fails if any Gold foreign key is missing in Silver (strict, no ratio tolerance)

**Key configuration:**
- `gold_column`: foreign key column in Gold table
- `silver`: Silver table location and column
  - `catalog`, `schema`, `table`, `column`

**Result details:** `orphaned_records=<count>`

!!! note "Strict in Gold"
    Unlike Silver's `referential_integrity`, the Gold check has no
    `min_valid_ratio`: it passes only when zero foreign keys are missing.

**Typical use:**
- Validate Gold references to Silver
- Ensure referential integrity
- Detect missing reference data

**Example:**

```yaml
referential_integrity:
  enabled: true
  severity: critical
  blocking: true
  gold_column: payment_id
  silver:
    catalog: my_catalog
    schema: my_schema_silver
    table: payments
    column: payment_id
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 5000 FK references valid in catalog.schema.payments` |
| FAIL | `Found 45 orphaned FK values in 'payment_id' (0.9% of 5000)` |

---

### uniqueness

**Purpose:** Ensures uniqueness of Gold-level keys.

**What it does:**
- Groups by specified columns
- Identifies duplicate keys
- Fails if duplicates exist

**Key configuration:**
- `columns`: list of column names that should be unique
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of duplicate keys (0-100)
  - `max_failure_count`: maximum allowed number of duplicate keys

**Result details (v1.1.0+):**
- PASS: `"All {n} keys unique"`
- FAIL: `"Found {n} duplicate keys ({pct}% of {total})"`

**Typical use:**
- Validate business key uniqueness in aggregated data
- Ensure aggregation grain is correct
- Detect duplicate aggregations

**Example:**

```yaml
uniqueness:
  enabled: true
  severity: critical
  blocking: true
  columns: [payment_id]
```

**Example with threshold:**

```yaml
uniqueness:
  enabled: true
  severity: warning
  blocking: false
  columns: [payment_id, date]
  threshold:
    max_failure_percent: 0.5  # Allow up to 0.5% duplicate keys
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 5000 rows unique on [payment_id]` |
| FAIL | `Found 25 duplicate rows on [payment_id, date] (0.5% of 5000)` |

---

### coverage

**Purpose:** Validates that Gold metrics sufficiently cover Silver metrics.

**What it does:**
- Aggregates Gold data using provided expression
- Aggregates Silver data using provided expression
- Joins on specified keys
- Validates coverage percentage within tolerance

**Key configuration:**
- `gold_expr`: SQL expression to aggregate Gold data
- `gold_join_columns`: column, or list of columns, to join Gold data
- `silver_expr`: SQL expression to aggregate Silver data
- `silver_table`: fully qualified Silver table name
- `silver_join_columns`: column, or list of columns, to join Silver data (paired by position with `gold_join_columns`)
- `tolerance_percent`: acceptable coverage gap (default: 1.0%)
- `name` (optional): descriptive name

**Result details:** `coverage=<pct>%; tolerance=<pct>%`

**Typical use:**
- Ensure Gold aggregations cover Silver data
- Validate data completeness in aggregation
- Detect missing data in rollups

**Example:**

```yaml
coverage:
  enabled: true
  severity: warning
  blocking: false
  name: orders_n_coverage

  gold_expr: "SUM(orders_n)"
  gold_join_columns: [order_id]

  silver_expr: "COUNT(DISTINCT transaction_id)"
  silver_table: my_catalog.my_schema_silver.orders
  silver_join_columns: [order_id]

  tolerance_percent: 5.0
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Coverage OK: gold=15000, silver=15200 (delta 1.3%)` |
| PASS | `All 50 keys within coverage tolerance (5%)` |
| FAIL | `Coverage delta 8.5% exceeds 5% limit` |
| FAIL | `Found 12 keys exceeding 5% tolerance (24% of 50)` |

---

### volume

**Purpose:** Detects anomalous variations of a volume metric.

**What it does:**
- Operates in two modes:
  - **Historical mode** (recommended): compares current metric against historical baseline
  - **Stateless mode**: evaluates metric consistency within current run only
- Mode is automatically selected based on configuration
- Validates volume is within expected tolerance

**Key configuration:**
- `metric`: SQL expression to compute volume
- `min_value` (optional): minimum allowed value (stateless bounds mode)
- `max_value` (optional): maximum allowed value (stateless bounds mode)
- `group_by` (optional): columns to group by
- `name` (optional): descriptive name (also used to match history rows)
- `history` (optional): historical comparison settings
  - `table`: history table location
  - `window`: number of historical records to consider (default: 7)
  - `min_history`: minimum historical records required (default: 3)
  - `tolerance_percent`: allowed deviation from the historical baseline (default: 10)

**Result details:** `anomalies=<count>; max_deviation=<pct>%`

**Typical use:**
- Detect sudden volume drops or spikes
- Validate data pipeline completeness
- Monitor metric stability

**Example 1: Historical mode**

```yaml
volume:
  enabled: true
  severity: warning
  blocking: false

  metric: "COUNT(*)"

  history:
    table: my_dq_catalog.dq_metadata.dq_test_history
    window: 7
    min_history: 3
    tolerance_percent: 20
```

**Example 2: Stateless mode with grouping**

```yaml
volume:
  enabled: true
  severity: warning
  blocking: false

  metric: "SUM(orders_n)"
  group_by:
    - order_id

  tolerance_pct: 40
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Volume OK: 15000 (within 20% of baseline 14500)` |
| PASS | `Volume OK: 15000 (min=10000, max=20000)` |
| FAIL | `Historical deviation 35.2% exceeds 20% limit` |
| FAIL | `Volume 8500 below minimum 10000` |
| FAIL | `Volume 25000 above maximum 20000` |

---

### freshness

**Purpose:** Ensures Gold data is up to date.

**What it does:**
- Computes maximum timestamp value
- Compares to current time
- Fails if delay exceeds threshold

**Key configuration:**
- `timestamp_column`: timestamp column to check
- `max_delay_hours`: maximum acceptable delay (default: 24)

**Result details:** `latest=<timestamp>; delay_hours=<hours>`

**Typical use:**
- Validate Gold tables are updated regularly
- Ensure metrics are current
- Detect stale aggregations

**Example:**

```yaml
freshness:
  enabled: true
  severity: critical
  blocking: true
  timestamp_column: paid_at
  max_delay_hours: 24
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Data fresh: 6.5h old (within 24h limit)` |
| FAIL | `Data age 36.2h exceeds 24h limit` |
| FAIL | `No data found in column 'paid_at'` |

---

### cross_gold

**Purpose:** Compares metrics across two Gold tables.

**What it does:**
- Evaluates expressions on two Gold tables
- Compares computed values
- Allows configurable tolerance

**Key configuration:**
- `left_table` (optional): left Gold table (schema.table format); defaults to the table under validation
- `right_table`: right Gold table (schema.table format)
- `left_expr`: SQL expression for left table
- `right_expr`: SQL expression for right table
- `tolerance_abs`: acceptable absolute difference (default: 0.01)

**Result details:** `difference=<amount>; tolerance=<amount>`

**Typical use:**
- Validate consistency across Gold tables
- Compare rollups at different grains
- Ensure metric alignment

**Example:**

```yaml
cross_gold:
  enabled: true
  severity: critical
  blocking: true
  left_table: my_schema_gold.daily_revenue
  right_table: my_schema_gold.monthly_revenue
  left_expr: "SUM(amount)"
  right_expr: "SUM(amount)"
  tolerance_abs: 0.01
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Cross-gold OK: left=150000.00, right=150000.00 (diff=0.00)` |
| PASS | `All 12 groups within tolerance (0.01)` |
| FAIL | `Difference 150.5000 exceeds tolerance 0.01` |
| FAIL | `Found 3 groups exceeding tolerance 0.01 (25% of 12)` |

---

### completeness

**Purpose:** Verifies that all expected dimension values are present in the data.

**What it does:**
- Extracts distinct values from a column
- Compares against expected values list
- Identifies missing and (optionally) extra values

**Key configuration:**
- `column`: Column containing dimension values (required)
- `expected_values`: List of values that must be present (required)
- `allow_extra`: Allow values not in expected list (default: true)

**Result details:** `expected=<count>; actual=<count>; missing=[...]; extra=[...]`

**Typical use:**
- Validate all regions/countries are present
- Ensure all product categories exist
- Verify rollup completeness

**Example:**

```yaml
completeness:
  enabled: true
  severity: critical
  blocking: true
  column: region
  expected_values:
    - "EMEA"
    - "APAC"
    - "AMERICAS"
  allow_extra: false
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 3 expected values present in 'region'` |
| FAIL | `Completeness issue in 'region': 1 missing` |
| FAIL | `Completeness issue in 'region': 1 missing, 2 unexpected` |
| FAIL | `Column not found: 'region'` |

---

### trend

**Purpose:** Verifies metric trend compared to historical values.

**What it does:**
- Computes current metric value
- Retrieves historical values from DQ history table
- Calculates deviation from median baseline
- Supports stable, increasing, or decreasing trend directions

**Key configuration:**
- `metric`: SQL expression for the metric (required)
- `history_table`: Table containing historical DQ results (required)
- `name`: Check name to lookup in history (required)
- `window`: Number of historical records to consider (default: 7)
- `min_history`: Minimum records required for comparison (default: 3)
- `tolerance_pct`: Allowed percentage deviation from median (default: 20)
- `direction`: Expected trend - "stable", "increasing", "decreasing" (default: stable)

**Result details:** `current=<value>; baseline=<median>; deviation_pct=<pct>; direction=<dir>`

**Typical use:**
- Monitor KPI stability over time
- Detect unexpected metric changes
- Validate growth/decline trends

**Example 1: Stable metric**

```yaml
trend:
  enabled: true
  severity: warning
  blocking: false
  metric: "COUNT(*)"
  history_table: my_dq_catalog.dq_metadata.dq_test_history
  name: daily_record_count
  window: 7
  min_history: 3
  tolerance_pct: 20
  direction: stable
```

**Example 2: Increasing metric**

```yaml
trend:
  enabled: true
  severity: warning
  blocking: false
  metric: "SUM(revenue)"
  history_table: analytics_gold.dq.history
  name: monthly_revenue_trend
  window: 12
  min_history: 6
  tolerance_pct: 10
  direction: increasing
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Trend OK (stable): current=15000, baseline=14800 (deviation 1.4%)` |
| PASS | `Insufficient history (2 < 3), current=15000` |
| FAIL | `Trend deviation (stable) 28.5% exceeds 20% limit` |
| FAIL | `Trend deviation (increasing) 15.2% exceeds 10% limit` |

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
- `min_distinct_count`: Minimum expected distinct count (optional)
- `max_distinct_count`: Maximum expected distinct count (optional)

**Result details:** `distinct_count=<count>; expected=<value>` or `distinct_count=<count>; min=<min>; max=<max>`

**Typical use:**
- Validate cardinality constraints in aggregated data
- Ensure dimension tables have expected entries
- Detect data anomalies in rollups

**Example:**

```yaml
distinct_count:
  enabled: true
  severity: warning
  blocking: false
  column: product_category
  min_distinct_count: 5
  max_distinct_count: 50
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Distinct count OK: 25 in 'product_category' (min=5, max=50)` |
| FAIL | `Distinct count 3 below minimum 5` |
| FAIL | `Distinct count 75 above maximum 50` |

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
- `threshold`: Optional threshold configuration
  - `max_failure_percent`: Maximum allowed percentage of validation failures (0-100)
  - `max_failure_count`: Maximum allowed number of validation failures

**Supported types:** `string`, `int`, `long`, `double`, `float`, `boolean`

**Result details:** `total_elements=<count>; missing_fields={...}; type_mismatches={...}; invalid_values={...}`

**Typical use:**
- Validate aggregated JSON metadata
- Ensure configuration columns have correct structure
- Validate semi-structured Gold outputs

**Example:**

```yaml
json_schema:
  enabled: true
  severity: critical
  blocking: true
  column: summary_metadata
  required_fields:
    - aggregation_type
    - period
    - metrics.count
  field_types:
    period: string
    metrics.count: int
  allowed_values:
    aggregation_type: ["daily", "weekly", "monthly"]
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `JSON schema OK: 1000 elements validated` |
| FAIL | `JSON validation failed: 1 missing fields, 2 invalid values in 'summary_metadata'` |
| FAIL | `JSON parse failed: 15 rows (1.5%) exceed 0% limit` |

---

## Design Guidelines

### Do

- **Validate business metrics and reconciliation**: Gold ensures KPIs are correct
- **Prefer blocking for financial or KPI-critical checks**: Use `blocking: true` for critical metrics
- **Use Silver as authoritative source for reconciliation**: Always reconcile Gold aggregations against Silver
- **Validate metric consistency**: Ensure derived metrics are correctly calculated

### Avoid

- **Validate row-level correctness**: Row-level validation belongs in Silver
- **Skip reconciliation checks**: Always validate Gold matches Silver source
- **Ignore historical context**: Use volume checks to detect anomalies

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

Full Gold configuration for a sales analytics pipeline:

```yaml
gold:
  # Tables with specific checks
  tables:
    # Payments aggregation table
    - catalog: my_catalog
      schema: my_schema_gold
      table: payments
      checks:
        # Validate metric thresholds
        compare_metric:
          - enabled: true
            severity: critical
            blocking: true
            name: total_amount_threshold
            metric:
              type: sum
              column: amount
            value: 100000
            operator: ">="

          - enabled: true
            severity: warning
            blocking: false
            name: payment_count_threshold
            metric:
              type: count
            value: 1000
            operator: ">="

        # Reconcile with Silver
        reconciliation_expr:
          enabled: true
          severity: critical
          blocking: true
          name: amount_reconciliation
          source_silver: my_catalog.my_schema_silver.payments
          gold_expr: amount
          silver_expr: amount
          tolerance_percent: 0.01

        # Validate uniqueness
        uniqueness:
          enabled: true
          severity: critical
          blocking: true
          columns: [payment_id]

        # Check freshness
        freshness:
          enabled: true
          severity: critical
          blocking: true
          timestamp_column: paid_at
          max_delay_hours: 24

        # Business rules
        business_rules:
          enabled: true
          severity: critical
          blocking: true
          name: valid_amounts
          predicate: "amount >= 0 AND currency IS NOT NULL"

    # Orders aggregation
    - catalog: my_catalog
      schema: my_schema_gold
      table: sales_summary
      checks:
        # Compare array sizes to table counts
        compare_metric:
          enabled: true
          severity: error
          blocking: true
          name: products_vs_orders_per_region
          metric:
            type: size_array
            column: metadata
            group_by:
              - order_id
          expected_from:
            type: aggregate
            catalog: my_catalog
            schema: my_schema_silver
            table: orders
            column: product_id
            function: countDistinct
            group_by:
              - order_id
          operator: "=="

        # Reconcile records
        reconciliation_expr:
          enabled: true
          severity: warning
          blocking: false
          name: customers_reconciliation
          source_silver: my_catalog.my_schema_silver.orders
          gold_expr: "SUM(unique_customers)"
          silver_expr: "COUNT(DISTINCT customer_id)"
          group_by:
            - order_id
          tolerance_percent: 5.0

        # Coverage check
        coverage:
          enabled: true
          severity: warning
          blocking: false
          name: orders_coverage
          gold_expr: "SUM(orders_n)"
          gold_join_columns: [order_id]
          silver_expr: "COUNT(DISTINCT transaction_id)"
          silver_table: my_catalog.my_schema_silver.orders
          silver_join_columns: [order_id]
          tolerance_percent: 5.0

        # Volume anomaly detection
        volume:
          enabled: true
          severity: warning
          blocking: false
          metric: "SUM(orders_n)"
          group_by:
            - order_id
          tolerance_pct: 40

    # Revenue rollup table
    - catalog: my_catalog
      schema: my_schema_gold
      table: revenue_daily
      checks:
        # Cross-table consistency
        cross_gold:
          enabled: true
          severity: critical
          blocking: true
          # left_table defaults to the table under validation (revenue_daily)
          right_table: my_schema_gold.revenue_monthly
          left_expr: "SUM(amount)"
          right_expr: "SUM(amount)"
          tolerance_abs: 0.01

        # Referential integrity to Silver
        referential_integrity:
          enabled: true
          severity: critical
          blocking: true
          gold_column: payment_id
          silver:
            catalog: my_catalog
            schema: my_schema_silver
            table: payments
            column: payment_id

        # Volume monitoring with history
        volume:
          enabled: true
          severity: warning
          blocking: false
          metric: "COUNT(*)"
          history:
            table: my_dq_catalog.dq_metadata.dq_test_history
            window: 7
            min_history: 3
            tolerance_percent: 20
```

---

## See Also

- [Silver Suite](silver.md) - Previous validation layer
- [Bronze Suite](bronze.md) - First validation layer
- [Architecture Overview](../architecture/overview.md) - Framework design
