# Tags Filtering

Tags filtering allows you to selectively run checks based on custom tags. This is useful for:

- Running only critical checks during time-sensitive operations
- Separating smoke tests from full validation
- Scheduling different check groups (hourly, daily, weekly)
- Isolating expensive checks for off-peak execution

## Overview

Tags are configured at the **check level** and filtered at **runtime** via the `tags` parameter.

**Key behaviors:**

- **Strict filtering**: When `tags` filter is set, checks without tags are skipped
- **OR logic**: A check runs if ANY of its tags match ANY filter tag
- **Smart pre-filtering**: Only tables with tagged checks are loaded
- **Suite skipping**: Suites without any tagged tables are skipped entirely

## Quick Start

### 1. Add Tags to Checks

```yaml
silver:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      checks:
        idempotency:
          enabled: true
          severity: critical
          blocking: true
          tags: [critical, daily]
          key_column: order_id

        freshness:
          enabled: true
          severity: warning
          blocking: false
          tags: [daily, smoke]

        record_count:
          enabled: true
          severity: warning
          blocking: false
          tags: [hourly]
```

### 2. Run with Tag Filter

```python
from neco_dq.runner import run_dq

# Run only critical checks
results = run_dq("/Workspace/config/", tags=["critical"])

# Run daily checks
results = run_dq("/Workspace/config/", tags=["daily"])

# Run both smoke and critical checks
results = run_dq("/Workspace/config/", tags=["smoke", "critical"])
```

## Filtering Behavior

| Scenario | Behavior |
|----------|----------|
| No `tags` parameter | All checks run (normal behavior) |
| `tags` set + check has matching tag | Check runs |
| `tags` set + check has non-matching tag | Check skipped |
| `tags` set + check has no tags | Check skipped |
| `tags` set + table has no tagged checks | Table not loaded |
| `tags` set + suite has no tagged tables | Suite skipped entirely |

## Configuration by Suite

### Bronze / Silver / Gold Suites

Tags are added at the check configuration level:

```yaml
silver:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      checks:
        idempotency:
          enabled: true
          severity: critical
          blocking: true
          tags: [critical, daily]
          key_columns: [order_id]

        freshness:
          enabled: true
          severity: warning
          blocking: false
          tags: [daily, smoke]
          date_column: created_at
          max_age_hours: 24

        record_count:
          enabled: true
          tags: [hourly]
          min_count: 1

        # Check without tags - skipped when filtering
        schema_check:
          enabled: true
```

### Great Expectations Suite

For GE, tags are configured at the **rule level** (inside the column/dataset configuration):

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

        _dataset:
          min_rows:
            enabled: true
            value: 1
            tags: [smoke, hourly]
```

### List-style Check Configurations

For checks that support multiple instances (list syntax), tags can be added to each instance:

```yaml
silver:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: orders
      checks:
        compare_metric:
          - observed:
              metric: count
            expected: 1000
            operator: ">="
            tags: [critical]

          - observed:
              metric: aggregate
              column: amount
              function: sum
            expected: 50000
            operator: ">="
            tags: [daily, finance]
```

## Combining with Other Filters

Tags filtering works alongside other filtering options:

```python
# Run critical checks only on silver suite
results = run_dq(
    "/Workspace/config/",
    run_suites=["silver"],
    tags=["critical"]
)

# Run daily checks on specific tables
results = run_dq(
    "/Workspace/config/",
    selector={"silver": ["orders", "customers"]},
    tags=["daily"]
)

# Exclude expensive tests during smoke runs
results = run_dq(
    "/Workspace/config/",
    tags=["smoke"]  # Checks tagged "expensive" won't run
)
```

## Suggested Tag Conventions

| Tag | Description | Typical Usage |
|-----|-------------|---------------|
| `critical` | Must pass for pipeline to proceed | Pre-merge validation |
| `smoke` | Quick sanity checks | Fast feedback loops |
| `daily` | Run once per day | Scheduled batch jobs |
| `hourly` | Run every hour | Near-real-time monitoring |
| `expensive` | Long-running checks | Off-peak execution |
| `regression` | Post-deployment validation | Release validation |
| `finance` | Financial data checks | Audit compliance |
| `pii` | PII-related validations | Privacy compliance |

## Use Cases

### 1. CI/CD Pipeline Stages

```python
# PR validation - fast checks only
if is_pull_request:
    results = run_dq(config, tags=["smoke", "critical"])

# Post-merge - full validation
else:
    results = run_dq(config)  # All checks
```

### 2. Scheduled Jobs

```python
# Hourly job
results = run_dq(config, tags=["hourly"])

# Daily job - includes hourly checks
results = run_dq(config, tags=["daily", "hourly"])

# Weekly full check
results = run_dq(config, tags=["weekly", "expensive"])
```

### 3. On-Demand Validation

```python
# Finance team audit
results = run_dq(config, tags=["finance", "critical"])

# Privacy compliance check
results = run_dq(config, tags=["pii"])
```

## Performance Considerations

Tags filtering is optimized for efficiency:

1. **Pre-filtering at orchestrator level**: Tables without tagged checks are never loaded
2. **Suite skipping**: If no tables remain after filtering, the suite is skipped entirely
3. **Early check filtering**: Checks are filtered before execution, not after

This means running `tags=["smoke"]` on a large configuration will only load and process the relevant subset.

## Best Practices

### Do

- Use consistent tag names across your organization
- Document your tag conventions in your config repository
- Combine tags for flexible scheduling (e.g., `daily` implies `critical` + `daily` checks)
- Use `smoke` tag for checks that can run in under a minute

### Don't

- Add tags to every check (defeats the purpose of filtering)
- Use too many unique tags (becomes hard to manage)
- Rely solely on tags for critical checks (ensure they run in at least one scheduled job)

## Troubleshooting

### "No checks match tags filter"

This log message appears when:
- No checks in a table have any of the specified tags
- The table is skipped to save resources

**Solution**: Verify that your checks have the expected tags configured.

### Checks not running

If expected checks aren't running:

1. Verify the check has `tags` configured (not just `enabled: true`)
2. Remember: checks without tags are skipped when filtering is active
3. Check for typos in tag names (tags are case-sensitive)

### Debugging

Enable verbose logging to see filtering decisions:

```python
results = run_dq(config, tags=["critical"], verbose=True)
```

Look for log messages like:
- `Tags filter set: ['critical'] for suite silver`
- `No checks match tags filter for catalog.schema.table, skipping`
