# Debug Experience

When a check fails, you need to quickly understand what went wrong and how to investigate. The Debug Experience provides structured information at multiple layers.

---

## Result Layers

Every check result includes these layers:

| Layer | Purpose | When Present |
|-------|---------|--------------|
| `details` | Human-readable summary | Always |
| `failure_stats` | Aggregate statistics | Only on FAIL |
| `investigation_query` | SQL template for debugging | Only on FAIL |
| `execution` | Performance context | Always |

### details

A concise message (~60 chars) that tells you what happened at a glance.

**PASS examples:**
```
Record count OK: 5000 rows
All 5000 keys unique
Data age 2h within 24h limit
```

**FAIL examples:**
```
Found 50 duplicate keys (5.0%)
Data age 48h exceeds 24h limit
Missing 3 required columns
```

### failure_stats

Aggregate statistics about the failure. Never contains row values or PII.

```json
{
  "failure_type": "duplicate_key",
  "affected_columns": ["order_id"],
  "failure_count": 47,
  "total_rows_checked": 50000,
  "failure_percentage": 0.094,
  "distribution": {
    "duplicates_2": 30,
    "duplicates_3_5": 12,
    "duplicates_6_plus": 5
  },
  "diagnostic": {
    "max_duplicates_per_key": 8,
    "avg_duplicates_per_key": 2.4
  }
}
```

### investigation_query

A SQL template you can run to find the failing rows. The query is parameterized with your table and filter context.

```sql
-- Find duplicate keys
SELECT order_id, COUNT(*) as cnt
FROM catalog.schema.orders
WHERE batch_date = '2026-06-01'
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 100
```

### execution

Context about how the check ran: timing, row counts, filters applied, table info, and profile context.

```json
{
  "duration_ms": 1234,
  "started_at": "2026-06-15T10:30:00Z",
  "rows_scanned": 1000000,
  "rows_after_filter": 50000,
  "filters_applied": {
    "row_filter": "status = 'active'",
    "partition_filter": {
      "date": "2026-06-01"
    }
  },
  "table": {
    "catalog": "analytics_prod",
    "schema": "silver",
    "name": "orders"
  },
  "profile": {
    "name": "prod",
    "target_catalog": "analytics_prod"
  }
}
```

**Fields:**

| Field | Description |
|-------|-------------|
| `duration_ms` | Check execution time in milliseconds |
| `started_at` | ISO 8601 timestamp when check started |
| `rows_scanned` | Total rows in source table |
| `rows_after_filter` | Rows remaining after filters |
| `filters_applied` | Filters that were applied (row_filter, partition_filter, time_window) |
| `table` | Primary table info (catalog, schema, name) |
| `secondary_tables` | Other tables read (for cross-table checks) |
| `profile` | Profile context (only present when profiles are used) |

The `profile` field is only present when running with an active profile (not when using single-file configs without profiles).

---

## Security Guarantees

The Debug Experience is designed to never expose sensitive data:

| Data Type | Stored? | Reason |
|-----------|---------|--------|
| Counts/percentages | Yes | Aggregate statistics |
| Distribution buckets | Yes | Statistical summaries |
| Column names | Yes | Schema metadata |
| Query templates | Yes | No data embedded |
| Row values | **Never** | Direct PII risk |
| Primary keys | **Never** | Can be PII (SSN, email) |
| Sample rows | **Never** | Contains values |

---

## Using the Debug Layers

### Quick Triage

Start with `details` to understand what failed:

```
Found 50 duplicate keys (5.0%)
```

### Assess Severity

Check `failure_stats` to understand the scale:

```json
{
  "failure_count": 50,
  "total_rows_checked": 1000,
  "failure_percentage": 5.0,
  "distribution": {
    "duplicates_2": 45,
    "duplicates_3_plus": 5
  }
}
```

Most duplicates appear only twice (45 keys), with 5 keys having 3+ duplicates.

### Investigate

Run the `investigation_query` in Databricks to find the actual failing rows:

```sql
SELECT order_id, COUNT(*) as cnt
FROM catalog.schema.orders
WHERE batch_date = '2026-06-01'
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
```

### Understand Context

Check `execution` if results seem unexpected:

```json
{
  "rows_scanned": 1000000,
  "rows_after_filter": 1000,
  "filters_applied": {
    "row_filter": "status = 'pending'"
  },
  "table": {
    "catalog": "analytics_prod",
    "schema": "silver",
    "name": "orders"
  },
  "profile": {
    "name": "prod",
    "target_catalog": "analytics_prod"
  }
}
```

Only 1000 rows were checked due to the filter. The 5% failure rate is within those filtered rows. The `profile` field confirms this ran against the prod environment.

---

## Failure Stats by Check Type

### Duplicate Checks (idempotency, uniqueness)

```json
{
  "failure_type": "duplicate_key",
  "affected_columns": ["order_id"],
  "distribution": {
    "duplicates_2": 30,
    "duplicates_3_5": 12,
    "duplicates_6_plus": 5
  }
}
```

### Null Checks (mandatory_fields)

```json
{
  "failure_type": "null_value",
  "affected_columns": ["email", "phone"],
  "null_distribution": {
    "email": 100,
    "phone": 56,
    "both_null": 23
  }
}
```

### Referential Integrity

```json
{
  "failure_type": "orphan_reference",
  "affected_columns": ["product_id"],
  "reference_info": {
    "target_table": "catalog.products",
    "target_column": "id"
  },
  "diagnostic": {
    "distinct_orphan_fks": 45
  }
}
```

### Compare Metric (grouped)

```json
{
  "failure_type": "metric_mismatch",
  "group_columns": ["account_id", "date"],
  "deviation_stats": {
    "max_deviation_pct": 50.0,
    "avg_deviation_pct": 15.3
  }
}
```

---

## Querying Debug Data

Debug layers are stored in the history table. Query them for analysis:

```sql
SELECT
  check_name,
  details,
  failure_stats,
  investigation_query
FROM dq_test_history
WHERE passed = false
  AND run_timestamp > current_date() - 7
ORDER BY run_timestamp DESC
```

Parse `failure_stats` JSON for aggregations:

```sql
SELECT
  check_name,
  get_json_object(failure_stats, '$.failure_type') as failure_type,
  get_json_object(failure_stats, '$.failure_count') as failure_count,
  get_json_object(failure_stats, '$.failure_percentage') as failure_pct
FROM dq_test_history
WHERE passed = false
```
