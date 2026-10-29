# Installation & Deployment

Installation options, terminal output, execution flow, failure policies, Databricks jobs, Declarative Pipelines, and security.

!!! tip "New to neco_dq?"
    Start with [Getting Started](getting-started.md) for a 5-minute quick start.

---

## Installation

### Prerequisites

- Access to a Databricks workspace
- Python >= 3.11

### Where to Find the Wheel

| Location | Path | Use Case |
|----------|------|----------|
| **PyPI** | `neco-dq-framework` | pip install, local development |
| **Unity Catalog Volume** | `/Volumes/<catalog>/<schema>/wheelhouse/` | Databricks jobs, notebooks, DLT pipelines |

### Option 1: From PyPI

```bash
pip install neco-dq-framework
```

### Option 2: From a Unity Catalog Volume

Hosting the wheel on a Volume is the usual choice for Databricks jobs, notebooks, and
DLT pipelines. It works when the cluster has no outbound internet, and it lets you pin an
exact version instead of resolving it from PyPI at install time.

First get the wheel. Download it from PyPI on any machine with internet access:

```bash
pip download neco-dq-framework --no-deps -d ./wheelhouse
```

This writes `neco_dq_framework-X.Y.Z-py3-none-any.whl` to `./wheelhouse`. Upload that
file to a Volume your workspace can read, then install it:

```python
%pip install /Volumes/<catalog>/<schema>/wheelhouse/neco_dq_framework-X.Y.Z-py3-none-any.whl
```

For **Databricks Jobs**, add the library in task configuration:

```yaml
libraries:
  - whl: /Volumes/<catalog>/<schema>/wheelhouse/neco_dq_framework-X.Y.Z-py3-none-any.whl
```

For **classic (non-serverless) Declarative Pipelines**, add the wheel under `libraries`:

```json
{
  "libraries": [
    {"whl": "/Volumes/<catalog>/<schema>/wheelhouse/neco_dq_framework-X.Y.Z-py3-none-any.whl"}
  ]
}
```

**Serverless pipelines reject `libraries.whl`.** Declare the wheel as an environment
dependency instead:

```json
{
  "environment": {
    "dependencies": [
      "/Volumes/<catalog>/<schema>/wheelhouse/neco_dq_framework-X.Y.Z-py3-none-any.whl"
    ]
  }
}
```

Or install from a wheel uploaded to your Workspace:

```python
%pip install --upgrade --force-reinstall \
  /Workspace/Users/your.email@example.com/libs/neco_dq_framework-X.Y.Z-py3-none-any.whl
```

### Verify Installation

```python
import neco_dq
print(f"neco_dq version: {neco_dq.__version__}")
```

!!! info "Flexibility for Teams"
    The wheel can be published to **PyPI** and/or a **Unity Catalog Volume**. Teams can:

    - **Install from PyPI** for local development or CI/CD pipelines
    - **Use directly** from a shared volume (always up-to-date)
    - **Copy to a team-dedicated volume** for version pinning or isolation
    - **Upload to team Workspace** for full control over updates

---

## Terminal Output

The framework provides clean, informative terminal output during execution.

### Standard Output

```
[INFO] neco_dq: Resolving config: /Workspace/config/
[INFO] neco_dq: Starting DQ v1.4.1
[INFO] neco_dq: Loading test suites...
[INFO] neco_dq:   → bronze: 2 tables
[INFO] neco_dq:   → silver: 2 tables
[INFO] neco_dq:   → gold: 5 tables
[INFO] neco_dq:   → great_expectations@silver: 2 tables
[INFO] neco_dq:   → great_expectations@gold: 5 tables
[INFO] neco_dq: Loaded 5 test suites (16 tables, 9 unique)
[INFO] neco_dq: Running checks...
[INFO] neco_dq:   → bronze
[INFO] neco_dq:     ✓ 11 checks (10 passed, 1 failed)
[INFO] neco_dq:   → silver
[INFO] neco_dq:     ✓ 17 checks (15 passed, 2 failed)
[INFO] neco_dq:   → gold
[INFO] neco_dq:     ✓ 35 checks (30 passed, 5 failed)
[INFO] neco_dq:   → ge@silver
[INFO] neco_dq:     ✓ 110 checks (110 passed, 0 failed)
[INFO] neco_dq:   → ge@gold
[INFO] neco_dq:     ✓ 50 checks (50 passed, 0 failed)
```

**Key features:**

- Unified logger name: `neco_dq` across all components
- Compact format without timestamps
- Visual hierarchy with `→` indentation
- Per-suite completion message with `✓` checkmarks
- Suites sorted by layer order: bronze → silver → gold
- GE suites abbreviated: `great_expectations@silver` → `ge@silver`
- Unique table count shown: `16 tables, 9 unique`

### Verbose Mode

Control output verbosity and Great Expectations progress bars with the `verbose` parameter:

```python
from neco_dq.runner import run_dq

# Default: clean output, GE progress bars hidden
results = run_dq("/Workspace/config/")

# Verbose: debug logging, GE progress bars visible
results = run_dq(
    "/Workspace/config/",
    verbose=True
)
```

| `verbose` | Log Level | GE Progress Bars |
|-----------|-----------|------------------|
| `False` (default) | INFO | Hidden |
| `True` | DEBUG | Visible |

When verbose mode is enabled:

- Log level is set to DEBUG (shows ConfigResolver details)
- Great Expectations progress bars are visible (via `ProgressBarsConfig`)
- Additional internal details are logged

---

## Understanding Execution Flow

At a high level, `run_dq()` resolves your config, runs the requested suites, applies
the failure policy, then emits a summary, alerts and stored results, returning an
exit code (0 / 10 / 20 / 99). The full step-by-step flow with diagram lives in
[Architecture → Execution Flow](architecture/overview.md#execution-flow); the exit
codes are covered under [Failure Handling](#failure-handling) below.

---

## Failure Handling

Each check has two properties that control how failures affect the pipeline:

| Property | Values | Effect |
|----------|--------|--------|
| `severity` | `warning`, `critical` | Informational only (appears in reports) |
| `blocking` | `true`, `false` | Determines exit code |

The framework has no defaults. Each team decides what should block and what should warn.

```yaml
checks:
  freshness:
    enabled: true
    severity: critical
    blocking: true      # Exit 20 on failure

  record_count:
    enabled: true
    severity: warning
    blocking: false     # Exit 10 on failure (non-blocking)
```

**Exit codes:**

| Code | When |
|------|------|
| 0 | All checks passed |
| 10 | Only `blocking: false` checks failed |
| 20 | At least one `blocking: true` check failed |

---

## Running as a Databricks Job

For production deployments, run neco_dq as a scheduled Databricks Job.

### Job Configuration

**Step 1: Reference the wheel**

Use any of these options based on your team's preference:

=== "Shared Volume (Recommended)"

    Reference the wheel directly from a shared Unity Catalog Volume:

    ```
    /Volumes/<catalog>/<schema>/wheelhouse/neco_dq_framework-X.Y.Z-py3-none-any.whl
    ```

=== "Team Volume"

    Copy the wheel to your team's dedicated volume for version pinning:

    ```
    /Volumes/<your_catalog>/<your_schema>/wheelhouse/neco_dq_framework-X.Y.Z-py3-none-any.whl
    ```

=== "Team Workspace"

    Upload the wheel to your team's Workspace folder:

    ```
    /Workspace/Users/your.email@example.com/libs/neco_dq_framework-X.Y.Z-py3-none-any.whl
    ```

**Step 2: Create entrypoint script**

Save as `/Workspace/Users/your.email@example.com/scripts/run_dq.py`:

```python
from neco_dq.runner import run_dq

def main():

    results = run_dq(
        "/feature/config/",
        run_suites=["bronze"]
    )

    print(results)

if __name__ == "__main__":
    main()
```

**Step 3: Configure Databricks Job**

Create a new Job with these settings:

| Setting | Value |
|---------|-------|
| **Task type** | Python wheel |
| **Environment** | Path to wheel (Volume or Workspace, see Step 1) |
| **Script** | Path to `run_dq.py` |
| **Cluster** | Select existing or create new |
| **Schedule** | Table Update |
| **Max retries** | 0 |
| **Timeout** | 30 minutes |
| **Email notifications** | On failure |

---

## Running Inside a Declarative Pipeline

You can run neco_dq inside a Lakeflow Declarative Pipeline (formerly DLT) so the graph
gates on data quality. The pattern is different from a Job: `run_dq()` cannot be a
free-floating call, it has to live inside a dataset so the pipeline knows about it.

### The Pattern

Wrap `run_dq()` in a `@dp.table` that returns a DataFrame of results, and gate that
dataset with a native expectation. One `@dp.table` becomes one node in the graph.

```python
import json
from pyspark import pipelines as dp
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType
from neco_dq.runner import run_dq

CONFIG_REF = "/Workspace/Users/your.email@example.com/dq-config/"

RESULT_SCHEMA = StructType([
    StructField("suite", StringType()),
    StructField("status", StringType()),
    StructField("blocking_failures", LongType()),
    StructField("summary_json", StringType()),
])


@dp.table(name="my_catalog.my_schema.dq_results_bronze")
@dp.expect_all_or_fail({"no_blocking_dq_failures": "blocking_failures = 0"})
def dq_results_bronze():
    # Read the upstream tables so DQ runs after they materialize (see below).
    spark.read.table("my_catalog.my_schema.orders")
    spark.read.table("my_catalog.my_schema.customers")

    try:
        summary = run_dq(config_ref=CONFIG_REF, run_suites=["bronze"])
        rows = [
            Row(
                suite=name,
                status=str(s.get("status")),
                blocking_failures=int(s.get("blocking_failures", 0)),
                summary_json=json.dumps(s, default=str),
            )
            for name, s in summary.items()
        ]
    except Exception as e:  # noqa: BLE001
        # Capture the policy error as a row so the expectation fails the node,
        # not the analysis phase. See "Blocking failures" below.
        rows = [Row(suite="bronze", status="FAIL", blocking_failures=1,
                    summary_json=json.dumps({"blocking_error": str(e)}))]

    return spark.createDataFrame(rows, schema=RESULT_SCHEMA)
```

The `@dp.expect_all_or_fail` decorator turns the node red when a blocking failure
lands, and green otherwise. An expectation always needs a dataset decorator
(`@dp.table` or `@dp.view`): it cannot decorate a bare function on its own.

### Graph Granularity

The number of nodes in the graph equals the number of `@dp.table` definitions. Pick the
granularity you want:

| Granularity | How | Nodes in graph |
|-------------|-----|----------------|
| Per suite | One `@dp.table` per suite, `run_suites=["bronze"]` | One per suite |
| Per table | One `@dp.table` per table, `selector={"bronze": ["orders"]}` | One per table |
| Single node | One `@dp.table` calling `run_dq()` for everything | One, layer shown in the `suite` column |

There is no way to get many graph nodes from a single table: "one table" and "many
steps" are in conflict, because the count of `@dp.table` definitions drives both.

### Blocking Failures Turn the Node Red

`run_dq()` runs at graph-analysis time, before materialization. A blocking failure
raises a `RuntimeError`, and if that propagates it aborts the whole update during
analysis: every step shows as "not run", with no red node to point at.

The fix is in the pattern above. Catch the exception inside the function, emit a row
with `blocking_failures=1`, and let `@dp.expect_all_or_fail` fail the node at
materialization. Upstream data tables stay green, and only the DQ node turns red.

### Full-Refresh Ordering

`run_dq()` reads tables by their full Unity Catalog name, which does not register a
pipeline dependency edge. On a full refresh the validation can run before the upstream
tables exist, producing false `record_count` or `freshness` failures.

Read the upstream tables inside the function (the `spark.read.table(...)` calls in the
example) so the pipeline orders the DQ node after them. Incremental runs against tables
that already hold data are not affected.

---

## Service Principal and Security Model

For production deployments, run neco_dq using a **service principal** with least-privilege access.

### Required Permissions

The service principal needs:

| Resource | Permission | Purpose |
|----------|-----------|---------|
| **Source tables** | `SELECT` | Read data for validation |
| **Metadata catalog/schema** | `USE CATALOG`, `USE SCHEMA` | Access metadata database |
| **History table** | `SELECT`, `INSERT` | Write execution results |
| **Alerts table** | `SELECT`, `INSERT` | Write alerts (if enabled) |

### Security Best Practices

- **No write permissions**: Service principal should NEVER have `INSERT`, `UPDATE`, or `DELETE` on source data
- **Dedicated metadata catalog**: Use separate catalog/schema for DQ metadata, not production catalogs
- **Audit trail**: All executions are logged with service principal identity in `dq_test_history`
- **Rotate credentials**: If using personal access tokens, rotate them regularly

**See**: [Database Setup](configuration/database-setup.md) for metadata table configuration.

---

## Result Persistence and Reporting

The framework persists validation results to Delta tables for historical tracking and reporting.

### Required Setup

Before running validations in production, you need to create metadata tables to persist execution results:

1. **dq_test_history** - Stores ALL check results (passed and failed)
2. **dq_test_alerts_log** - Stores ONLY failures and warnings (optional)

**See**: [Database Setup](configuration/database-setup.md) for complete setup instructions, table schemas, and customization options.

---

## Next Steps

- [Architecture Overview](architecture/overview.md) - System design and components
- [Modular Config](configuration/modular-config.md) - Production configuration
- [Test Suites](test-suites/bronze.md) - Available checks by layer
