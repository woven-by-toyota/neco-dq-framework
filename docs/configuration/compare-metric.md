# Compare Metric Reference

The `compare_metric` check is a generic validation tool that compares **observed values** computed from your data against **expected values** from various sources.

!!! tip "When to Use"
    Use `compare_metric` when you need to:

    - Validate aggregations (sums, counts, averages)
    - Compare metrics across tables
    - Enforce business thresholds
    - Validate statistical calculations
    - Check array field values
    - Verify ranking algorithms (Kendall's W, Borda Count)

---

## Basic Syntax

```yaml
compare_metric:
  enabled: true
  severity: error
  blocking: true
  name: my_metric_check        # Optional: descriptive name

  observed:                     # REQUIRED: What to measure
    type: <metric_type>
    # ... metric-specific parameters

  # CHOOSE ONE: static value OR dynamic expected
  value: 100                    # Option 1: Compare to static value
  expected:                     # Option 2: Compare to dynamic value
    type: <resolver_type>
    # ... resolver-specific parameters

  operator: "=="                # Comparison operator
  tolerance_abs: 0.01               # Optional: for approximate comparisons
```

!!! info "Key Aliases"
    This reference uses `observed` and `expected`. The following legacy aliases are still accepted:

    - `observed` **or** `metric`: defines the observed value
    - `expected` **or** `expected_from`: defines the dynamic expected value

---

## Observed Metrics

The `observed` section defines what value to compute from your data.

### count

Counts rows in the DataFrame.

```yaml
observed:
  type: count
```

### aggregate

Applies a Spark aggregation function.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `column` | Yes | Column to aggregate |
| `function` | Yes | `sum`, `avg`, `count`, `countDistinct` |
| `where` | No | SQL filter expression |
| `group_by` | No | Columns for grouping |

```yaml
observed:
  type: aggregate
  column: amount
  function: sum
  where: "status = 'completed'"
  group_by:
    - order_id
```

### column_value

Extracts a single column value (or grouped values).

| Parameter | Required | Description |
|-----------|----------|-------------|
| `column` | Yes | Column to extract |
| `where` | No | SQL filter expression |
| `group_by` | No | Columns for grouping |

```yaml
observed:
  type: column_value
  column: total_responses
  group_by:
    - order_id
    - product_id
```

### sql

Executes a raw SQL query.

```yaml
observed:
  type: sql
  query: "SELECT COUNT(*) FROM {catalog}.{schema}.{table} WHERE status = 'active'"
```

!!! note "Placeholders"
    Use `{catalog}`, `{schema}`, `{table}` to reference the current table.

### size_array

Counts elements in an array column.

```yaml
observed:
  type: size_array
  column: items
  group_by:
    - order_id
```

### sum_array_field

Sums a specific field within array elements.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `column` | Yes | Array column |
| `field` | Yes | Field to sum within array |
| `path` | No | Path for nested arrays |
| `value_type` | No | Cast type (`int`, `double`) |
| `group_by` | No | Columns for grouping |

```yaml
observed:
  type: sum_array_field
  column: choices
  field: choice_pct
  value_type: double
  group_by:
    - order_id
    - product_id
```

### array_stats

Computes statistical measures from array field values.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `column` | Yes | Array column |
| `field` | Yes | Field containing numeric values |
| `stat` | Yes | Statistical measure (see below) |
| `weight` | No | Field for weighted calculations |
| `path` | No | Path for nested arrays (e.g., `ranks`) |
| `item_group_by` | No | Match array elements (e.g., `item_code\|item_label`) |
| `median_method` | No | For median: `discrete`, `lower`, `upper`, `continuous` |
| `group_by` | No | Columns for grouping |

**Supported `stat` values:**

| Stat | Description |
|------|-------------|
| `mean` | Average (weighted if `weight` specified) |
| `median` | Median value (configurable method) |
| `std` | Sample standard deviation |
| `min` | Minimum value |
| `max` | Maximum value |
| `ci_95_low` | 95% confidence interval lower bound |
| `ci_95_high` | 95% confidence interval upper bound |

**Example: Weighted Mean**

```yaml
observed:
  type: array_stats
  column: rating_levels
  field: rating_value
  weight: rating_count
  stat: mean
  group_by:
    - order_id
    - product_id
```

**Example: Median with Method**

```yaml
observed:
  type: array_stats
  column: rating_levels
  field: rating_value
  stat: median
  median_method: lower
  group_by:
    - order_id
    - product_id
```

**Example: Nested Array Stats**

```yaml
observed:
  type: array_stats
  column: items
  path: ranks                    # Navigate to nested 'ranks' array
  field: rank_position
  weight: rank_count
  item_group_by: item_code|item_label
  stat: mean
  group_by:
    - order_id
    - product_id
```

### kendall_w

Computes Kendall's W coefficient of concordance for ranking data.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `column` | Yes | Array column with ranking data |
| `rank_sum_field` | No | Field for mean rank (default: `mean_rank`) |
| `appearances_field` | No | Field for rater count (default: `item_appearances_n`) |
| `label` | No | Expected consensus label (e.g., `strong`, `moderate`) |
| `label_column` | No | Column containing expected label per group |
| `min_raters` | No | Minimum raters required (default: `2`) |
| `thresholds` | No | Custom threshold values for labels |
| `group_by` | No | Columns for grouping |

**Default Thresholds:**

| Label | Threshold |
|-------|-----------|
| `strong` | >= 0.9 |
| `moderate` | >= 0.7 |
| `fair` | >= 0.5 |
| `weak` | >= 0.3 |
| `low` | >= 0.2 |
| `none` | >= 0.0 |

**Example: Basic Kendall W**

```yaml
observed:
  type: kendall_w
  column: items
  rank_sum_field: mean_rank
  appearances_field: item_appearances_n
  group_by:
    - order_id
    - product_id
```

**Example: With Label Validation**

```yaml
observed:
  type: kendall_w
  column: items
  label_column: kendall_w_label    # Validate against expected label
  min_raters: 1                     # Allow single-rater
  thresholds:                       # Custom 3-level system
    strong: 0.5
    moderate: 0.3
    low: 0.0
  group_by:
    - order_id
    - product_id
```

### borda_count

Computes Borda Count scores for ranking data.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `column` | Yes | Array column with ranking items |
| `ranks_field` | Yes | Field containing ranks array |
| `rank_position_field` | Yes | Field for rank position within ranks |
| `rank_count_field` | Yes | Field for vote count at each rank |
| `item_id_field` | Yes | Field(s) identifying items (e.g., `item_code\|item_label`) |
| `fields` | No | Output fields to include |
| `group_by` | No | Columns for grouping |

```yaml
observed:
  type: borda_count
  column: items
  ranks_field: ranks
  rank_position_field: rank_position
  rank_count_field: rank_count
  item_id_field: "item_code|item_label"
  fields: [item_id, borda_points, borda_norm_pct]
  group_by:
    - order_id
    - product_id
```

---

## Expected Resolvers

The `expected` section defines where to get the expected value.

### static (using `value`)

Compare to a literal value.

```yaml
value: 100.00
operator: "=="
```

### aggregate

Compute aggregation from another table.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `catalog` | No | Target catalog |
| `schema` | No | Target schema |
| `table` | No | Target table |
| `column` | Yes | Column to aggregate |
| `function` | Yes | `sum`, `avg`, `count`, `countDistinct` |
| `where` | No | SQL filter expression |
| `group_by` | No | Columns for grouping |

```yaml
expected:
  type: aggregate
  catalog: my_catalog
  schema: my_schema
  table: source_table
  column: transaction_id
  function: count
  where: "type = 'single_choice'"
  group_by:
    - order_id
```

### sql

Execute a raw SQL query.

```yaml
expected:
  type: sql
  query: "SELECT COUNT(*) FROM catalog.schema.table WHERE status = 'active'"
```

### column_value

Extract column value from a table.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `catalog` | No | Target catalog |
| `schema` | No | Target schema |
| `table` | No | Target table |
| `column` | Yes | Column to extract |
| `where` | No | SQL filter expression |
| `group_by` | No | Columns for grouping |

```yaml
expected:
  type: column_value
  catalog: my_catalog
  schema: my_schema
  table: summary_table
  column: expected_mean
  group_by:
    - order_id
    - product_id
```

### array_field_value

Extract values from array field elements.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `catalog` | No | Target catalog |
| `schema` | No | Target schema |
| `table` | No | Target table |
| `column` | Yes | Array column |
| `field` | Yes* | Single field to extract |
| `fields` | Yes* | Multiple fields to extract |
| `item_group_by` | No | Match array elements by field(s) |
| `group_by` | No | Columns for grouping |

*Use either `field` or `fields`, not both.

```yaml
expected:
  type: array_field_value
  catalog: my_catalog
  schema: my_schema
  table: summary_ranking
  column: items
  fields: [borda_points, borda_norm_pct]
  item_group_by: "item_code|item_label"
  group_by:
    - order_id
    - product_id
```

---

## Operators

| Operator | Description | Tolerance Support |
|----------|-------------|-------------------|
| `==` | Exact equality | No |
| `!=` | Not equal | No |
| `>` | Greater than | No |
| `>=` | Greater or equal | No |
| `<` | Less than | No |
| `<=` | Less or equal | No |
| `~` | Approximate equality | Yes (absolute) |
| `within_pct` | Within percentage | Yes (percentage) |
| `in` | Value in collection | No |
| `not_in` | Value not in collection | No |
| `between` | Value in range [min, max] | No |

### Using Tolerance

For approximate comparisons with floating-point values:

```yaml
operator: "~"
tolerance_abs: 0.01  # Absolute tolerance: |observed - expected| <= 0.01
```

```yaml
operator: "within_pct"
tolerance_abs: 5.0   # With within_pct the value is a percentage: difference <= 5%
```

---

## Group By

Use `group_by` to perform row-level comparisons across grouped data.

**Important:** When using `group_by`:

- Both `observed` and `expected` must have matching `group_by` columns
- Group keys are compared as string tuples
- All groups must pass for the check to succeed

```yaml
compare_metric:
  enabled: true
  observed:
    type: aggregate
    column: total
    function: sum
    group_by:
      - order_id
      - product_id

  expected:
    type: column_value
    catalog: my_catalog
    schema: my_schema
    table: reference
    column: expected_total
    group_by:
      - order_id
      - product_id

  operator: "=="
```

---

## Complete Examples

### Example 1: Simple Threshold Check

Verify total amount exceeds minimum.

```yaml
compare_metric:
  enabled: true
  severity: critical
  blocking: true

  observed:
    type: aggregate
    column: amount
    function: sum

  value: 100000
  operator: ">="
```

### Example 2: Cross-Table Count Comparison

Verify row counts match between tables.

```yaml
compare_metric:
  enabled: true
  severity: error
  name: responses_count_match

  observed:
    type: count

  expected:
    type: aggregate
    catalog: my_catalog
    schema: silver
    table: source_responses
    column: "*"
    function: count
    where: "type = 'single_choice'"

  operator: "=="
```

### Example 3: Grouped Metric Validation

Verify aggregated values match reference table per group.

```yaml
compare_metric:
  enabled: true
  severity: error
  name: revenue_per_region

  observed:
    type: aggregate
    column: revenue
    function: sum
    group_by:
      - region
      - quarter

  expected:
    type: column_value
    catalog: my_catalog
    schema: gold
    table: revenue_targets
    column: target_revenue
    group_by:
      - region
      - quarter

  operator: ">="
```

### Example 4: Array Field Sum Equals 100%

Verify percentages in array sum to 100.

```yaml
compare_metric:
  enabled: true
  severity: error
  name: percentage_sum_100

  observed:
    type: sum_array_field
    column: choices
    field: choice_pct
    value_type: double
    group_by:
      - order_id
      - product_id

  value: 100.00
  operator: "~"
  tolerance_abs: 0.01
```

### Example 5: Weighted Mean from Array

Validate weighted mean calculation.

```yaml
compare_metric:
  enabled: true
  severity: error
  name: rating_mean

  observed:
    type: array_stats
    column: rating_levels
    field: rating_value
    weight: rating_count
    stat: mean
    group_by:
      - order_id
      - product_id

  expected:
    type: column_value
    catalog: my_catalog
    schema: gold
    table: summary_rating
    column: mean
    group_by:
      - order_id
      - product_id

  operator: "~"
  tolerance_abs: 0.001
```

### Example 6: Kendall W Consensus Validation

Validate ranking consensus coefficient and label.

```yaml
compare_metric:
  enabled: true
  severity: error
  name: kendall_consensus

  observed:
    type: kendall_w
    column: items
    rank_sum_field: mean_rank
    appearances_field: item_appearances_n
    label_column: kendall_w_label
    min_raters: 1
    thresholds:
      strong: 0.5
      moderate: 0.3
      low: 0.0
    group_by:
      - order_id
      - product_id

  expected:
    type: column_value
    catalog: my_catalog
    schema: gold
    table: summary_ranking
    column: kendall_w
    group_by:
      - order_id
      - product_id

  operator: "~"
  tolerance_abs: 0.01
```

### Example 7: Borda Count Validation

Validate Borda Count ranking scores.

```yaml
compare_metric:
  enabled: true
  severity: error
  name: verify_borda_count

  observed:
    type: borda_count
    column: items
    ranks_field: ranks
    rank_position_field: rank_position
    rank_count_field: rank_count
    item_id_field: "item_code|item_label"
    fields: [item_id, borda_points, borda_norm_pct]
    group_by:
      - order_id
      - product_id

  expected:
    type: array_field_value
    catalog: my_catalog
    schema: gold
    table: summary_ranking
    column: items
    fields: [borda_points, borda_norm_pct]
    item_group_by: "item_code|item_label"
    group_by:
      - order_id
      - product_id

  operator: "=="
```

### Example 8: SQL-Based Validation

Use raw SQL for complex queries.

```yaml
compare_metric:
  enabled: true
  severity: warning
  name: active_users_count

  observed:
    type: count

  expected:
    type: sql
    query: |
      SELECT COUNT(DISTINCT user_id)
      FROM my_catalog.silver.user_sessions
      WHERE session_date >= CURRENT_DATE - INTERVAL 30 DAYS
        AND status = 'active'

  operator: ">="
```

---

## Result Output

Check results include detailed information:

```
check_name: compare_metric
name: rating_mean
passed: true
details: observed=4.52; expected=4.52; operator=~; tolerance=0.001
```

For grouped comparisons with failures:

```
check_name: compare_metric
name: rating_mean
passed: false
details: Found 3/100 groups failing; group=('order_1', 'prod_5'): observed=4.2, expected=4.8; ...
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "No matching groups" | Group keys don't match | Ensure `group_by` columns exist in both sources |
| Type mismatch | Comparing incompatible types | Check `value_type` for arrays |
| Tolerance not working | Using `==` instead of `~` | Use `~` operator with `tolerance_abs` |
| Empty results | Filter too restrictive | Check `where` conditions |

### Debugging Tips

1. **Start simple**: Test with `type: count` and static `value` first
2. **Check group alignment**: Ensure `group_by` columns match exactly
3. **Use named checks**: Add `name` for easier identification in results
4. **Test tolerance**: For floating-point comparisons, always use `~` with `tolerance_abs`

---

## See Also

- [Silver Suite](../test-suites/silver.md) - Silver layer checks
- [Gold Suite](../test-suites/gold.md) - Gold layer checks
- [Data Filtering](data-filtering.md) - Filter data before validation
- [Modular Config](modular-config.md) - Configuration structure
