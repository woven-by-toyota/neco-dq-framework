# Bronze Test Suite

The Bronze test suite validates **raw data** immediately after ingestion from source systems.

## Characteristics

The Bronze suite is:

- **Ingestion-level**: Validates data as soon as it lands
- **Technical**: No business logic, only technical guarantees
- **Check-centric**: Each check produces one result row
- **Cheap and deterministic**: Fast, repeatable validations

## Purpose

Bronze validation ensures:

- Data arrived from source systems
- Tables are readable and not empty
- Data is fresh enough
- Schema hasn't drifted unexpectedly
- Mandatory fields are present
- Binary payloads are valid (if applicable)

!!! tip "Debugging failures"
    When checks fail, see [Debug Experience](../architecture/debug-experience.md) for understanding `failure_stats`, `investigation_query`, and how to investigate.

---

## Configuration Examples

### 1. Minimal Example

Use this when you only want to verify that a table exists and is not empty.

```yaml
bronze:
  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: transactions

      checks:
        record_count:
          enabled: true
          severity: warning
          blocking: false
```

**Executed checks:**
- `table_read` (implicit, always critical)
- `record_count`

---

### 2. Standard Ingestion Checks (Recommended)

Covers the most common ingestion guarantees.

```yaml
bronze:
  default_checks:
    record_count:
      enabled: true
      severity: warning
      blocking: false

    table_freshness:
      enabled: true
      severity: warning
      blocking: false
      max_staleness_days: 2

  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: transactions
```

**Behavior:**
- `default_checks` apply to all tables
- Each table inherits these checks automatically

---

### 3. Table-Level Overrides

Override or extend default checks for a specific table.

```yaml
bronze:
  default_checks:
    record_count:
      enabled: true
      severity: warning
      blocking: false

  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: transactions
      checks:
        freshness:
          enabled: true
          severity: critical
          blocking: true
          timestamp_column: event_timestamp
          max_delay_hours: 6
```

**In this case:**
- `record_count` comes from `default_checks`
- `freshness` is defined only for this specific table

---

## Threshold Configuration

Row-level checks can tolerate a number or percentage of failing rows via a
`threshold` block. The mechanics (`max_failure_percent`, `max_failure_count`, AND
logic, suite-level defaults) are the same across suites and documented once in
[Thresholds](../configuration/thresholds.md). The Bronze-specific part is which
checks support it:

### Supported Bronze Checks

| Check | Supports Threshold |
|-------|-------------------|
| `mandatory_fields` | yes |
| `json_schema` | yes (shared check) |
| `business_rules` | yes (shared check) |
| `binary_not_null` | yes |
| `binary_non_empty` | yes |
| `binary_size_guard` | yes |
| `binary_decodable` | yes |
| `binary_version_header_check` | yes |
| `binary_schema_fingerprint` | yes |
| `record_count` | no (binary pass/fail) |
| `record_count_anomaly` | no (uses `max_deviation_percent`) |
| `freshness` | no (uses `max_delay_hours`) |
| `table_freshness` | no (uses `max_staleness_days`) |
| `schema_drift` | no (binary pass/fail) |
| `binary_type` | no (binary pass/fail) |

---

## Available Checks

### Check Reference Table

| YAML Check Name | Method Invoked | Purpose |
|-----------------|----------------|---------|
| `record_count` | `_check_record_count` | Verify table is not empty |
| `record_count_anomaly` | `_check_record_count_anomaly` | Detect unexpected volume changes |
| `freshness` | `_check_freshness` | Validate data recency (row-level timestamp) |
| `table_freshness` | `_check_table_freshness` | Validate table metadata freshness |
| `schema_drift` | `_check_schema_drift` | Prevent unexpected schema changes |
| `mandatory_fields` | `_check_mandatory_fields` | Ensure required columns exist and are non-null |
| `json_schema` | `_check_json_schema` | Validate JSON column structure (shared check) |
| `business_rules` | `_check_business_rules` | Validate SQL expressions (shared check) |
| `binary_not_null` | `_check_binary_not_null` | Binary column must exist and have no nulls |
| `binary_non_empty` | `_check_binary_non_empty` | Binary payloads must not be empty |
| `binary_size_guard` | `_check_binary_size_guard` | Validate binary payload size bounds |
| `binary_type` | `_check_binary_type` | Ensure column has binary data type |
| `binary_decodable` | `_check_binary_decodable` | Verify binary payload can be decoded |
| `binary_version_header_check` | `_check_binary_version_header_check` | Validate binary header structure |
| `binary_schema_fingerprint` | `_check_binary_schema_fingerprint` | Ensure binary schema conformity |
| `partition_exists` | `_check_partition_exists` | Verify expected partitions exist |

**Each enabled check:**
- Produces one output row
- Has its own severity and blocking behavior

---

## Check Details

### record_count

**Purpose:** Verifies that the table contains at least one record.

**What it does:**
- Executes a `count()` on the DataFrame
- Fails if the table is empty

**Typical use:**
- Detect missing or failed ingestions
- Ensure upstream pipelines actually produced data

**Notes:**
- Very lightweight check
- Often enabled by default in Bronze
- Can be blocking in critical ingestion paths

**Example:**

```yaml
record_count:
  enabled: true
  severity: warning
  blocking: false
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Record count OK: 15000 rows` |
| FAIL | `Table empty: 0 rows` |

---

### record_count_anomaly

**Purpose:** Detects unexpected changes in record volume compared to previous runs.

**What it does:**
- Reads the last recorded `record_count` from the configured history table
- Compares current row count with the previous value
- Fails if the deviation exceeds a configurable percentage

**Key configuration:**
- `history_table`: table used to retrieve historical values
- `max_deviation_percent` (default: 40%)

**Typical use:**
- Catch partial loads or duplicates
- Early anomaly detection in ingestion pipelines

**Notes:**
- Automatically passes if no history is available

**Example:**

```yaml
record_count_anomaly:
  enabled: true
  severity: warning
  blocking: false
  history_table: my_dq_catalog.dq_metadata.dq_test_history
  max_deviation_percent: 40
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Record count OK: 15000 (deviation 12.5% within 40% limit)` |
| PASS | `No history configured` |
| PASS | `No previous value in history` |
| FAIL | `Count deviation 65.3% exceeds 40% limit` |

---

### freshness

**Purpose:** Ensures data is recent based on a timestamp column inside the table.

**What it does:**
- Computes the maximum value of a configured timestamp column
- Compares it to the current time (UTC)
- Fails if the delay exceeds the allowed threshold

**Key configuration:**
- `timestamp_column`: column containing timestamps
- `max_delay_hours` (default: 24)

**Typical use:**
- Validate that upstream systems are still publishing data
- Enforce SLAs on event-time freshness

**Example:**

```yaml
freshness:
  enabled: true
  severity: critical
  blocking: true
  timestamp_column: updated_at
  max_delay_hours: 24
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Data fresh: 2.3h old (within 24h limit)` |
| FAIL | `Data age 48.5h exceeds 24h limit` |
| FAIL | `No data found in column 'updated_at'` |

---

### table_freshness

**Purpose:** Checks freshness based on table metadata rather than row-level timestamps.

**What it does:**
- Reads `lastModified` from `DESCRIBE DETAIL`
- Computes how many days the table has been stale
- Fails if it exceeds the configured threshold

**Key configuration:**
- `max_staleness_days` (default: 2)

**Typical use:**
- Validate batch pipelines
- Useful when tables do not contain reliable timestamp columns

**Example:**

```yaml
table_freshness:
  enabled: true
  severity: warning
  blocking: false
  max_staleness_days: 2
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Table fresh: updated 8.5h ago (within 2d limit)` |
| FAIL | `Table stale: 72.3h since update exceeds 2d limit` |

---

### schema_drift

**Purpose:** Prevents unexpected schema changes from propagating downstream.

**What it does:**
- Compares the actual table schema with an expected schema definition
- Detects:
  - Missing columns
  - Type mismatches

**Key configuration:**
- `expected_columns`: dictionary mapping column names to expected types

**Typical use:**
- Protect Silver and Gold layers
- Enforce schema contracts across teams

**Notes:**
- Frequently configured as blocking in Silver/Gold

**Example:**

```yaml
schema_drift:
  enabled: true
  severity: critical
  blocking: true
  expected_columns:
    id: string
    created_at: timestamp
    amount: double
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Schema OK: 5 columns verified` |
| FAIL | `Schema issues: 2 missing, 1 type mismatch` |

---

### mandatory_fields

**Purpose:** Ensures that required columns are present and non-null.

**What it does:**
- Verifies column existence
- Checks for null values in mandatory columns

**Key configuration:**
- `columns`: list of mandatory column names
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of null rows (0-100)
  - `max_failure_count`: maximum allowed number of null rows

**Result details:** `violated_columns=[...]`

**Typical use:**
- Enforce minimum data completeness
- Validate business-critical attributes

**Example:**

```yaml
mandatory_fields:
  enabled: true
  severity: critical
  blocking: true
  columns:
    - id
    - created_at
    - amount
```

**Example with threshold:**

```yaml
mandatory_fields:
  enabled: true
  severity: warning
  blocking: false
  columns:
    - id
    - created_at
  threshold:
    max_failure_percent: 1.0  # Allow up to 1% null rows
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 3 mandatory columns present and non-null` |
| FAIL | `Found 150 null values (1.5% of 10000)` |
| FAIL | `Missing 2 required columns` |

---

## Binary Payload Checks

These checks are specialized for validating binary columns (e.g., serialized Avro, Protobuf, encoded JSON).

### binary_not_null

**Purpose:** Ensures that the specified binary column exists and contains no nulls.

**What it does:**
- Verifies that the column exists
- Counts null values
- Returns pass/fail depending on whether any nulls are present

**Key configuration:**
- `column`: binary column to check
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of null rows (0-100)
  - `max_failure_count`: maximum allowed number of null rows

**Result details:** `null_count: <number_of_null_values>`

**Typical use:**
- Enforce minimum data completeness in binary columns
- Ensure critical binary payloads are not missing

**Example:**

```yaml
binary_not_null:
  enabled: true
  severity: critical
  blocking: true
  column: payload_bin
```

**Example with threshold:**

```yaml
binary_not_null:
  enabled: true
  severity: warning
  blocking: false
  column: payload_bin
  threshold:
    max_failure_count: 10  # Allow up to 10 null payloads
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All values non-null in 'payload_bin'` |
| FAIL | `Found 25 null values (0.5% of 5000)` |
| FAIL | `Column not found: 'payload_bin'` |

---

### binary_non_empty

**Purpose:** Ensures that binary column contains non-empty payloads.

**What it does:**
- Verifies column existence
- Counts rows with zero-length binary values
- Returns pass/fail depending on whether any empty values exist

**Key configuration:**
- `column`: binary column to check
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of empty rows (0-100)
  - `max_failure_count`: maximum allowed number of empty rows

**Result details:** `empty_binary_count: <number_of_empty_values>`

**Typical use:**
- Validate payload presence
- Detect incomplete or placeholder binary data

**Example:**

```yaml
binary_non_empty:
  enabled: true
  severity: warning
  blocking: false
  column: payload_bin
```

**Example with threshold:**

```yaml
binary_non_empty:
  enabled: true
  severity: warning
  blocking: false
  column: payload_bin
  threshold:
    max_failure_percent: 0.5  # Allow up to 0.5% empty payloads
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All values non-empty in 'payload_bin'` |
| FAIL | `Found 12 empty values (0.24% of 5000)` |

---

### binary_size_guard

**Purpose:** Ensures binary payloads fall within expected size limits.

**What it does:**
- Verifies column existence
- Checks that each payload length is between `min_bytes` and `max_bytes`

**Key configuration:**
- `column`: binary column to check
- `min_bytes` (optional): minimum allowed length
- `max_bytes` (optional): maximum allowed length
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of invalid rows (0-100)
  - `max_failure_count`: maximum allowed number of invalid rows

**Result details:** `invalid_size_count: <number_of_invalid_payloads>`

**Typical use:**
- Protect downstream consumers from undersized or oversized payloads
- Enforce schema consistency

**Example:**

```yaml
binary_size_guard:
  enabled: true
  severity: critical
  blocking: true
  column: payload_bin
  min_bytes: 5
  max_bytes: 1024
```

**Example with threshold:**

```yaml
binary_size_guard:
  enabled: true
  severity: warning
  blocking: false
  column: payload_bin
  min_bytes: 5
  max_bytes: 1024
  threshold:
    max_failure_percent: 2.0  # Allow up to 2% out-of-bounds payloads
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Binary size OK in 'payload_bin' (min=5B, max=1024B)` |
| FAIL | `Found 45 size violations (0.9% of 5000)` |

---

### binary_type

**Purpose:** Ensures the column has binary data type.

**What it does:**
- Checks if column exists
- Verifies Spark type is `BinaryType`

**Key configuration:**
- `column`: binary column to check

**Result details:** `type: <actual_spark_data_type>`

**Typical use:**
- Validate column type against expected schema
- Prevent accidental type mismatches (string, integer, etc.)

**Example:**

```yaml
binary_type:
  enabled: true
  severity: critical
  blocking: true
  column: payload_bin
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Type OK: 'payload_bin' is binary` |
| FAIL | `Type mismatch: expected binary, got string` |
| FAIL | `Column not found: 'payload_bin'` |

---

### binary_decodable

**Purpose:** Ensures binary payload can be decoded according to a specified format.

**What it does:**
- Verifies column existence
- Supports decoding formats: `json`, `base64_json`, `avro`, `protobuf`
- Counts invalid payloads (undecodable or malformed)

**Key configuration:**
- `column`: binary column to check
- `format`: decoding format (`json`, `base64_json`, `avro`, `protobuf`)
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of invalid rows (0-100)
  - `max_failure_count`: maximum allowed number of invalid rows

**Result details:** `invalid_payloads: <number_of_invalid_rows>`

**Typical use:**
- Validate payload integrity before parsing downstream
- Detect corrupt, truncated, or improperly encoded data

**Example:**

```yaml
binary_decodable:
  enabled: true
  severity: critical
  blocking: true
  column: payload_bin
  format: base64_json
```

**Example with threshold:**

```yaml
binary_decodable:
  enabled: true
  severity: warning
  blocking: false
  column: payload_bin
  format: base64_json
  threshold:
    max_failure_count: 100  # Allow up to 100 invalid payloads
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All rows decodable as base64_json` |
| FAIL | `Found 78 decode failures (base64_json) (1.6% of 5000)` |
| FAIL | `Invalid format: 'xml' not supported` |

---

### binary_version_header_check

**Purpose:** Validates binary payload header, checking magic byte and minimum size.

**What it does:**
- Verifies column existence
- Ensures payload is at least `header_size_bytes` long
- Compares first byte against `magic_byte`

**Key configuration:**
- `column`: binary column to check
- `magic_byte`: expected first byte (integer)
- `header_size_bytes`: minimum header length
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of invalid rows (0-100)
  - `max_failure_count`: maximum allowed number of invalid rows

**Result details:**
- `too_short: <number_of_short_payloads>`
- `invalid_magic: <number_of_invalid_headers>`

**Typical use:**
- Enforce versioning or signature validation in binary payloads
- Detect malformed headers or incompatible versions

**Example:**

```yaml
binary_version_header_check:
  enabled: true
  severity: critical
  blocking: true
  column: payload_bin
  magic_byte: 0
  header_size_bytes: 5
```

**Example with threshold:**

```yaml
binary_version_header_check:
  enabled: true
  severity: warning
  blocking: false
  column: payload_bin
  magic_byte: 0
  header_size_bytes: 5
  threshold:
    max_failure_percent: 1.0  # Allow up to 1% header violations
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All headers valid (magic=0, size>=5B)` |
| FAIL | `Found 34 invalid headers (0.7% of 5000)` |

---

### binary_schema_fingerprint

**Purpose:** Ensures that binary payloads conform to an expected schema fingerprint.

**What it does:**
- Computes a fingerprint from a slice of the binary column (offset + length)
- Converts the extracted bytes to hexadecimal
- Compares each fingerprint against a list of allowed fingerprints
- Collects invalid fingerprints for reporting

**Key configuration:**
- `column`: the binary column to inspect
- `allowed_fingerprints`: list of valid fingerprints (hex, case-insensitive)
- `offset` (optional, default=2): start byte (1-based) for fingerprint extraction
- `length_bytes` (optional, default=4): number of bytes to extract
- `threshold` (optional): failure tolerance
  - `max_failure_percent`: maximum allowed percentage of invalid rows (0-100)
  - `max_failure_count`: maximum allowed number of invalid rows

**Result details:** `invalid_values: <list_of_invalid_fingerprints>`

**Typical use:**
- Validate binary payloads for schema conformity
- Enforce standardization on payload structures

**Example:**

```yaml
binary_schema_fingerprint:
  enabled: true
  severity: critical
  blocking: true
  column: payload_bin
  allowed_fingerprints:
    - "0013010D"
    - "12D685AA"
    - "12D685BB"
    - "12D685CC"
    - "FFFFFFFF"
  offset: 2
  length_bytes: 4
```

**Example with threshold:**

```yaml
binary_schema_fingerprint:
  enabled: true
  severity: warning
  blocking: false
  column: payload_bin
  allowed_fingerprints:
    - "0013010D"
    - "12D685AA"
  offset: 2
  length_bytes: 4
  threshold:
    max_failure_percent: 0.1  # Allow up to 0.1% unknown fingerprints
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All fingerprints valid (5 allowed)` |
| FAIL | `Found 23 invalid fingerprints (0.5% of 5000)` |

---

### partition_exists

**Purpose:** Verifies that expected partitions exist in a Delta table.

**What it does:**
- Queries Delta table metadata for existing partitions
- Compares against expected partition specifications
- Supports both multi-column partitions and single-column checks

**Key configuration (Option 1 - Multiple partitions):**
- `partitions`: List of partition specifications (e.g., `[{year: 2024, month: 1}, {year: 2024, month: 2}]`)

**Key configuration (Option 2 - Single column):**
- `partition_column`: Single partition column name
- `expected_values`: List of expected values for that column

**Result details:** `expected=<count>; missing=<count>; missing_partitions=[...]`

**Typical use:**
- Validate that expected data partitions have been loaded
- Verify daily/monthly partition availability
- Detect missing or incomplete data loads

**Example 1: Multi-column partitions**

```yaml
partition_exists:
  enabled: true
  severity: critical
  blocking: true
  partitions:
    - year: 2024
      month: 1
    - year: 2024
      month: 2
```

**Example 2: Single column with expected values**

```yaml
partition_exists:
  enabled: true
  severity: warning
  blocking: false
  partition_column: date
  expected_values:
    - "2024-01-01"
    - "2024-01-02"
    - "2024-01-03"
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 3 expected partitions exist` |
| PASS | `All 5 expected date values exist` |
| FAIL | `Missing 2 expected partitions` |
| FAIL | `Missing 1 date partitions` |
| FAIL | `Table is not partitioned` |

---

## Shared Checks

`json_schema` and `business_rules` are shared checks (from `SharedChecksMixin`,
composed into every suite) also available at the Bronze layer. At Bronze they are
intended for **structural / technical** validation of raw payloads, not domain
logic, which belongs in Silver/Gold (see [Design Guidelines](#design-guidelines)).
The configuration is identical across suites; the full reference lives in
[Silver › json_schema](silver.md#json_schema) and
[Silver › business_rules](silver.md#business_rules).

### json_schema

**Purpose:** Validates JSON column structure, required fields, field types, and allowed values, useful for raw API/event payloads landed in Bronze.

**Key configuration:**
- `column`: JSON column name (required)
- `required_fields`: List of fields that must exist (optional, supports dot-notation)
- `field_types`: Dict of field -> expected type (optional)
- `allowed_values`: Dict of field -> allowed value list (optional)
- `array_element`: If true, validates array elements (default: true)
- `sample_size`: Rows to sample for validation (default: 1000, 0 = all)
- `max_parse_failure_percent`: Max percentage of rows that may fail JSON parsing before the check hard-fails (default: 0)
- `threshold`: Optional threshold configuration (`max_failure_percent`, `max_failure_count`)

**Example:**

```yaml
json_schema:
  enabled: true
  severity: warning
  blocking: false
  column: raw_payload
  required_fields:
    - event_id
    - event_type
    - payload.source    # Nested field validation
  field_types:
    event_id: string
    retry_count: int
  threshold:
    max_failure_percent: 5.0  # Tolerate up to 5% malformed payloads at ingestion
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `JSON schema OK: 5000 elements validated` |
| FAIL | `JSON validation failed: 2 missing fields, 1 type mismatches in 'raw_payload'` |
| FAIL | `JSON parse failed: 50 rows (1.0%) exceed 0% limit` |

---

### business_rules

**Purpose:** Validates that rows satisfy a SQL boolean expression. At Bronze keep predicates **technical** (payload sanity, format bounds, non-null invariants) rather than business logic.

**Key configuration:**
- `predicate`: SQL boolean expression that must be true for every row (required)
- `name`: Human-readable name for the rule (default: "business_rule")
- `threshold`: Optional threshold configuration (`max_failure_percent`, `max_failure_count`)

**Example:**

```yaml
business_rules:
  enabled: true
  severity: warning
  blocking: false
  name: ingested_at_present
  predicate: "ingested_at IS NOT NULL AND length(raw_payload) > 0"
  threshold:
    max_failure_percent: 1.0  # Tolerate up to 1% malformed records
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `Rule 'ingested_at_present' passed (10000 rows)` |
| FAIL | `Found 150 rows violating rule 'ingested_at_present' (1.5% of 10000)` |

---

## Design Guidelines

### Do

- **Keep checks cheap and deterministic**: Bronze checks should execute quickly and produce consistent results
- **Prefer warning over critical**: Unless ingestion must stop, use warning severity
- **Use `default_checks`**: Define common checks once and apply to all tables
- **Focus on technical validation**: Schema, volume, freshness, data types

### Avoid

- **Encode business logic in Bronze**: Business rules belong in Silver/Gold
- **Create cross-table dependencies**: Each table should validate independently
- **Over-configure blocking checks**: Too many blocking checks can halt pipelines unnecessarily

---

## Typical Evolution Pattern

```
Bronze → Silver → Gold
```

**Bronze:** Data landed correctly (technical)
**Silver:** Data is correct (quality)
**Gold:** Data makes sense (business)

This separation is intentional and enforced by the framework design.

---

## Complete Example

Full Bronze configuration for a sales ingestion pipeline:

```yaml
bronze:
  # Default checks applied to all tables
  default_checks:
    record_count:
      enabled: true
      severity: warning
      blocking: false

    table_freshness:
      enabled: true
      severity: warning
      blocking: false
      max_staleness_days: 2

    record_count_anomaly:
      enabled: true
      severity: warning
      blocking: false
      history_table: my_dq_catalog.dq_metadata.dq_test_history
      max_deviation_percent: 40

  # Tables with specific overrides
  tables:
    # Standard table
    - catalog: my_catalog
      schema: my_schema_bronze
      table: transactions

    # Table with custom freshness check
    - catalog: my_catalog
      schema: my_schema_bronze
      table: events
      checks:
        freshness:
          enabled: true
          severity: critical
          blocking: true
          timestamp_column: event_timestamp
          max_delay_hours: 6

    # Table with binary payload validation
    - catalog: my_catalog
      schema: my_schema_bronze
      table: raw_payloads
      checks:
        binary_not_null:
          enabled: true
          severity: critical
          blocking: true
          column: payload_bin

        binary_decodable:
          enabled: true
          severity: critical
          blocking: true
          column: payload_bin
          format: base64_json

        binary_schema_fingerprint:
          enabled: true
          severity: critical
          blocking: true
          column: payload_bin
          allowed_fingerprints:
            - "0013010D"
            - "12D685AA"
          offset: 2
          length_bytes: 4
```

---

## See Also

- [Silver Suite](silver.md) - Next validation layer
- [Configuration Guide](../configuration/modular-config.md) - How to structure configs
- [Architecture Overview](../architecture/overview.md) - Framework design
