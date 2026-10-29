# Getting Started

<span class="read-time">5 MIN READ</span>
<span class="page-subtitle">Get Neco DQ running in minutes</span>

---

### 1. Install

```bash
pip install neco-dq-framework
```

Clusters with internet access can install straight from PyPI with the same command.

If your Databricks environment has no outbound internet, or you want to pin an exact
version, host the wheel yourself on a Unity Catalog Volume. Download it from PyPI once:

```bash
pip download neco-dq-framework --no-deps -d ./wheelhouse
```

Upload the resulting `.whl` to a Volume your workspace can read, then install it:

```python
%pip install /Volumes/<catalog>/<schema>/wheelhouse/neco_dq_framework-X.Y.Z-py3-none-any.whl
```

### 2. Create Config

Save this as `/path/bronze_suite.yaml`:

```yaml
bronze:
  tables:
    - catalog: your_catalog
      schema: your_schema
      table: your_table
      checks:
        freshness:
          enabled: true
          timestamp_column: batch_ingestion_at
          max_delay_hours: 24

        schema_drift:
          enabled: true
          expected_columns:
            project_id: string
            record_id: string
```

### 3. Run

```python
from neco_dq.runner import run_dq

results = run_dq("/path/bronze_suite.yaml")
```

### 4. Check Results

```
─────────────────────────────────────────────────────────────────────────
 DATA QUALITY SUMMARY
─────────────────────────────────────────────────────────────────────────
Suite                        Status   Checks   Pass     Fail     Tables
─────────────────────────────────────────────────────────────────────────
bronze                       PASS     2        2        0        1
─────────────────────────────────────────────────────────────────────────
TOTALS                                2        2        0        1 unique

Success Rate: 100.0%  │  Status: PASS

Legend: PASS=all ok │ WARN=non-blocking failures │ FAIL=blocking
─────────────────────────────────────────────────────────────────────────
```

That's it! See [Installation & Deployment](installation-deployment.md) for alternative installation options.

---

## Your First Validation

### Step 1: Create Configuration

For production, use the modular configuration structure:

```
/path/config/
├── global/
│   └── policy.yaml
└── suites/
    └── bronze/
        ├── suite.yaml
        └── test_groups/
            └── ingestion.yaml
```

**policy.yaml**:

```yaml
dq_policy:
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

**suite.yaml**:

```yaml
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
```

**ingestion.yaml**:

```yaml
tables:
  - catalog: my_catalog
    schema: my_schema_bronze
    table: answers
    checks:
      mandatory_fields:
        enabled: true
        severity: critical
        blocking: true
        columns:
          - user_id
          - record_id
          - response_date

      schema_drift:
        enabled: true
        severity: critical
        blocking: true
        expected_columns:
          user_id: string
          record_id: string
          response_date: timestamp
          rating: integer
```

!!! note "Configuration options"
    For simpler use cases, you can use a single YAML file. See [Single-File Configuration](configuration/single-file.md).
    For multi-environment deployments (dev/staging/prod), see [Profiles](configuration/profiles.md).

### Step 2: Run Validation

```python
from neco_dq.runner import run_dq

results = run_dq(
    "/path/config/",
    run_suites=["bronze"]
)
```

### Step 3: Understand Results

```
─────────────────────────────────────────────────────────────────────────
 DATA QUALITY SUMMARY
─────────────────────────────────────────────────────────────────────────
Suite                        Status   Checks   Pass     Fail     Tables
─────────────────────────────────────────────────────────────────────────
bronze                       WARN     11       10       1        2
silver                       WARN     17       15       2        2
gold                         FAIL     35       30       5        5
ge@silver                    PASS     110      110      0        2
ge@gold                      PASS     50       50       0        5
─────────────────────────────────────────────────────────────────────────
TOTALS                                223      215      8        9 unique

Success Rate: 96.4%  │  Status: FAIL (3 blocking)

Failed checks by type:
  freshness (5):
      silver           started_at                 x2
      bronze           batch_ingestion_at         x1
      gold             event_timestamp..          x1
      gold             summary_created_at         x1
  referential_integrity (3):
      gold             record_id                  x3

Tables with failures (6):
      gold             aggregated_metrics             3 failures
      gold             summary_changes                2 failures
      bronze           ingestion_events               1 failure
      silver           latest_records                 1 failure
      silver           cleaned_events                 1 failure

Legend: PASS=all ok │ WARN=non-blocking failures │ FAIL=blocking
─────────────────────────────────────────────────────────────────────────
DQ POLICY RESULT → exit_code=20 | DQ completed with ERRORS
```

### Exit Codes

| Exit Code | Status | Meaning |
|-----------|--------|---------|
| **0** | PASS | All checks passed |
| **10** | WARN | Non-blocking failures only |
| **20** | FAIL | One or more blocking failures |
| **99** | SYSTEM_ERROR | Invalid configuration or system error |

---

## What if a check fails?

A failing check doesn't just print `FAIL`: it attaches statistics and an
investigation query template (never row values) so you can find the offending rows.
See [Debugging Failures](architecture/debug-experience.md) for how to read
`failure_stats` and `investigation_query`.

## Next steps by role

Pick the path that matches what you're here to do:

<div class="qa-grid">

<a class="qa-card" href="../installation-deployment/">
<span class="qa-card__title">I'm operating a pipeline</span>
<span class="qa-card__desc">Deploy on Databricks, schedule jobs, wire up exit codes and Slack alerts → Installation & Deployment</span>
</a>

<a class="qa-card" href="../test-suites/">
<span class="qa-card__title">I'm defining data checks</span>
<span class="qa-card__desc">Pick the right suite and write checks → Test Suites Overview</span>
</a>

<a class="qa-card" href="../configuration/modular-config/">
<span class="qa-card__title">I'm structuring config</span>
<span class="qa-card__desc">Production config tree, default_checks inheritance, profiles → Configuration Basics</span>
</a>

<a class="qa-card" href="../architecture/overview/">
<span class="qa-card__title">I want to understand the design</span>
<span class="qa-card__desc">Orchestrator, suites, policy engine and the result contract → Architecture</span>
</a>

</div>

!!! tip "Not sure which suite you need?"
    The [Test Suites Overview](test-suites/index.md) has a "which suite do I need?"
    comparison table. Start there before diving into a specific suite's reference.
