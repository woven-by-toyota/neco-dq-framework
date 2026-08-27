# Database Setup

The Neco DQ Framework requires a metadata table to persist execution results.

## Purpose

The history table is used by the framework for:

- Historical tracking of Data Quality test executions
- Alerting and auditability
- Troubleshooting and trend analysis

This section explains how to create the required metadata table and which parts are customizable by each team.

---

## Customization Principles

The framework follows a flexible but contract-based approach:

| Component | Customizable | Notes |
|-----------|--------------|-------|
| **Schema name** | yes | Each team can use their own catalog/database |
| **Table names** | yes | Align with team naming conventions |
| **Column names** | no | Fixed by framework contract |
| **Column types** | no | Fixed by framework contract |
| **Column semantics** | no | Fixed by framework contract |

**This ensures:**
- Each team can align with its own catalog/database conventions
- The framework can reliably write and read metadata without additional configuration

---

## Required Table

### DQ Test History Table

**Purpose:** Stores the outcome of every Data Quality test execution.

**Recommendation:** **Required**

This table provides a complete audit trail of all executed checks (passed and failed), enabling:
- Historical analysis
- Trend monitoring
- Debugging and investigation
- Compliance and auditability
- Alert generation (via `alert_description` column)

---

## Reference SQL Script

Below is a reference implementation used by the framework. It can be reused as-is or adapted only in terms of schema and table names.

```sql
-- =========================================================
-- 1. CREATE SCHEMA
-- =========================================================
CREATE SCHEMA IF NOT EXISTS my_dq_catalog.dq_metadata;

-- =========================================================
-- 2. CREATE TABLE: dq_test_history
-- Stores all DQ check results
-- =========================================================
CREATE TABLE my_dq_catalog.dq_metadata.dq_test_history (
    run_timestamp TIMESTAMP,
    test_suite STRING,
    catalog STRING,
    schema STRING,
    table STRING,
    column STRING,
    passed BOOLEAN,
    status STRING,
    check_name STRING,
    severity STRING,
    details STRING,
    blocking BOOLEAN,
    alert_description STRING,           -- DEPRECATED: use details + check_name instead
    -- v1.1.0 debug experience columns
    execution STRING,
    failure_stats STRING,
    investigation_query STRING
)
USING DELTA;
```

---

## Schema Details

### dq_test_history Table

| Column | Type | Description |
|--------|------|-------------|
| `run_timestamp` | TIMESTAMP | When the check was executed |
| `test_suite` | STRING | Suite name (bronze, silver, gold, great_expectations) |
| `catalog` | STRING | Catalog of the validated table |
| `schema` | STRING | Schema of the validated table |
| `table` | STRING | Name of the validated table |
| `column` | STRING | Column name (if check is column-specific) |
| `passed` | BOOLEAN | Whether the check passed (true) or failed (false) |
| `status` | STRING | Status of the check (PASSED, FAILED, WARNING) |
| `check_name` | STRING | Name of the executed check |
| `severity` | STRING | Severity level (critical, warning) |
| `details` | STRING | Human-readable result message |
| `blocking` | BOOLEAN | Whether the check is blocking the pipeline |
| `alert_description` | STRING | **DEPRECATED** - Use `details` + `check_name` instead |
| `execution` | STRING | JSON with performance context (duration_ms, rows_scanned) - v1.1.0+ |
| `failure_stats` | STRING | JSON with failure statistics (counts, distributions) - v1.1.0+, only on FAIL |
| `investigation_query` | STRING | SQL template for debugging failures - v1.1.0+, only on FAIL |

---

## Optional: Separate Alerts Table

If you need a dedicated table for alerts only (e.g., for integration with alerting systems or lightweight monitoring), you can configure a separate destination with `alerts: true` in your suite's `data_to_save` configuration.

```yaml
# In suite.yaml (e.g., suites/bronze/suite.yaml)
data_to_save:
  # History table - all results
  - table: my_catalog.dq_metadata.dq_test_history
    write_mode: append
    alerts: false

  # Alerts table - failed checks only (optional)
  - table: my_catalog.dq_metadata.dq_test_alerts
    write_mode: append
    alerts: true
```

The alerts destination (with `alerts: true`) receives only failed checks, making it useful for:
- Integration with alerting systems (PagerDuty, Slack workflows, etc.)
- Incident tracking dashboards
- Lightweight monitoring without querying full history

---

## Per-destination options

Each entry under `data_to_save` accepts:

| Option | Type | Default | Effect |
|--------|------|---------|--------|
| `table` | str | *required* | Full 3-part destination table (`catalog.schema.table`) |
| `write_mode` | str | `append` | How to write (`append`) |
| `alerts` | bool | `false` | When `true`, write only failed checks (the alerts destination) |
| `columns_to_save` | list[str] | all columns | Restrict the written columns to this subset, in this order |
| `merge_schema` | bool | `false` | Enable Delta schema merge when the result columns evolve |

```yaml
data_to_save:
  - table: my_catalog.dq_metadata.dq_test_history
    write_mode: append
    alerts: false
    merge_schema: true
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
      - failure_stats       
      - investigation_query  
      - execution   
```

---

## What Can Be Changed

Teams may freely change:

**Catalog name**
```sql
-- Example: Use your team's catalog
my_catalog.dq_metadata.dq_test_history
```

**Schema name**
```sql
-- Example: Use your team's schema
my_dq_catalog.my_team_dq.dq_test_history
```

**Table names**
```sql
-- Example: Use your team's naming convention
my_dq_catalog.dq_metadata.dq_execution_history
```

**Configuration:**

After creating tables with custom names, update your suite's `data_to_save` configuration:

```yaml
# In suite.yaml (e.g., suites/bronze/suite.yaml)
data_to_save:
  - table: my_catalog.my_schema.my_history_table
    write_mode: append
    alerts: false
```

---

## What Must Remain Unchanged

The following **must not** be modified:

**Column names**
```sql
-- WRONG: Renaming columns
CREATE TABLE ... (
    execution_time TIMESTAMP,  -- should be run_timestamp
    suite STRING,               -- should be test_suite
    ...
)
```

**Column data types**
```sql
-- WRONG: Changing data types
CREATE TABLE ... (
    run_timestamp STRING,       -- should be TIMESTAMP
    passed INT,                 -- should be BOOLEAN
    ...
)
```

**Why this matters:**

The framework relies on this schema contract to:
- Normalize results across test suites
- Generate summaries and alerts
- Support future integrations (e.g., dashboards, Allure, BI tools)
- Enable cross-team consistency

---

## Example: Custom Setup

Here's an example of a customized setup for a team:

```sql
-- Team-specific catalog and schema
CREATE SCHEMA IF NOT EXISTS data_analytics_prod.quality_checks;

-- Custom table name following team conventions
CREATE TABLE data_analytics_prod.quality_checks.validation_history (
    run_timestamp TIMESTAMP,
    test_suite STRING,
    catalog STRING,
    schema STRING,
    table STRING,
    column STRING,
    passed BOOLEAN,
    status STRING,
    check_name STRING,
    severity STRING,
    details STRING,
    blocking BOOLEAN,
    alert_description STRING,           -- DEPRECATED: use details + check_name instead
    -- v1.1.0 debug experience columns
    execution STRING,
    failure_stats STRING,
    investigation_query STRING
)
USING DELTA;
```

**Configuration:**

```yaml
# In suite.yaml
data_to_save:
  - table: data_analytics_prod.quality_checks.validation_history
    write_mode: append
    alerts: false
```

---

## Operational Notes

### Delta Tables

Tables are created as Delta tables for:
- ACID transactions
- Time travel capabilities
- Efficient updates and deletes
- Schema evolution support

### Partitioning

**Default:** No partitioning is enforced

**Optional:** Teams may add partitioning later if needed:

```sql
CREATE TABLE my_dq_catalog.dq_metadata.dq_test_history (
    run_timestamp TIMESTAMP,
    test_suite STRING,
    catalog STRING,
    schema STRING,
    table STRING,
    column STRING,
    passed BOOLEAN,
    status STRING,
    check_name STRING,
    severity STRING,
    details STRING,
    blocking BOOLEAN,
    alert_description STRING,           -- DEPRECATED: use details + check_name instead
    -- v1.1.0 debug experience columns
    execution STRING,
    failure_stats STRING,
    investigation_query STRING
)
USING DELTA
PARTITIONED BY (DATE(run_timestamp), test_suite);
```

### Retention and Cleanup

**Responsibility:** Owning team

**Recommendation:** Implement retention policies based on your needs:

```sql
-- Example: Delete history older than 90 days
DELETE FROM my_dq_catalog.dq_metadata.dq_test_history
WHERE run_timestamp < CURRENT_DATE - INTERVAL 90 DAYS;

-- Example: Vacuum to reclaim space
VACUUM my_dq_catalog.dq_metadata.dq_test_history RETAIN 168 HOURS;
```

### Permissions

Ensure the framework has appropriate permissions:

```sql
-- Grant write permissions to the service account
GRANT INSERT, SELECT ON TABLE my_dq_catalog.dq_metadata.dq_test_history
  TO `service_account@example.com`;
```

---

## Verification

After creating the table, verify it is accessible:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Verify history table
history_df = spark.sql("""
    SELECT * FROM my_dq_catalog.dq_metadata.dq_test_history
    LIMIT 1
""")
print("History table accessible: OK")
```

---

## Summary

| Aspect | Flexibility |
|--------|-------------|
| **Catalog/Schema** | yes, customizable |
| **Table names** | yes, customizable |
| **Column contract** | no, fixed by framework |
| **Partitioning** | yes, team decision |
| **Retention** | yes, team responsibility |

**Key Takeaways:**

1. The framework **does not enforce** a central schema
2. The framework **does enforce** a strict metadata contract
3. The provided SQL is a **reference implementation**, not a hard dependency
4. This approach enables **gradual adoption** across teams without changing the overall Data Quality policy model

---

## Next Steps

After setting up the metadata table:

1. **Configure persistence** in your YAML files
2. **Run a test validation** to verify writes work
3. **Query the history table** to see results
4. **Set up retention policies** for long-term maintenance

---

## See Also

- [Getting Started](../getting-started.md) - Complete tutorial
- [Modular Config](modular-config.md) - Configure the framework
