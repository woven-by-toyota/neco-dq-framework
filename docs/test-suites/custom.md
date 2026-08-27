# Custom Test Suites

Create your own test suites with custom checks tailored to your specific data quality requirements.

## Overview

The `custom_suite_files` parameter in `run_dq()` allows you to load custom test suites from Python files. This enables:

- Custom validation logic specific to your domain
- Reusable checks across multiple tables
- Extension of the built-in suite functionality

!!! tip "Debugging failures"
    When checks fail, see [Debug Experience](../architecture/debug-experience.md) for understanding `failure_stats`, `investigation_query`, and how to investigate.

## Quick Start

### 1. Create the Custom Suite File

```python
# /Workspace/Users/your_user/my_custom_suite.py

from neco_dq.test_suites.base_test_suite import DQTestSuite
from neco_dq.core.io.result_writer import DQResultWriter


class MyCustomSuite(DQTestSuite):
    """Custom test suite for domain-specific validations."""

    def __init__(self, spark, cfg: dict):
        self.spark = spark
        self.cfg = cfg
        self.writer = DQResultWriter(spark)

    @property
    def _suite_name(self) -> str:
        return "my_custom"

    @property
    def _suite_config_key(self) -> str:
        return "my_custom"

    # Define your custom checks as _check_<name> methods
    def _check_custom_validation(self, df, cfg):
        """
        Custom check example.

        Args:
            df: Spark DataFrame
            cfg: Check configuration from YAML

        Returns:
            tuple of (passed: bool, details: str, column: str | None)
        """
        column = cfg.get("column")
        threshold = cfg.get("threshold", 0)

        # Your validation logic here
        null_count = df.filter(df[column].isNull()).count()
        total_count = df.count()

        passed = null_count <= threshold

        # Narrative details format (v1.1.0+)
        if passed:
            details = f"All {total_count} values valid in {column}"
        else:
            pct = null_count / total_count * 100 if total_count > 0 else 0
            details = f"Found {null_count} null values ({pct:.1f}% of {total_count})"

        return passed, details, column
```

**Example Output:**

| Status | Details |
|--------|---------|
| PASS | `All 5000 values valid in important_column` |
| FAIL | `Found 25 null values (0.5% of 5000)` |

### 2. Create the YAML Configuration

```yaml
# dq-config/my_custom.yaml

my_custom:
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: my_table
      checks:
        custom_validation:
          enabled: true
          column: important_column
          threshold: 10
          severity: error
          blocking: true
```

### 3. Run with Custom Suite

```python
from neco_dq.runner import run_dq

# Single custom suite
results = run_dq(
    config_ref="/Workspace/Users/your_user/dq-config/",
    custom_suite_files="/Workspace/Users/your_user/my_custom_suite.py"
)

# Multiple custom suites
results = run_dq(
    config_ref="/Workspace/Users/your_user/dq-config/",
    custom_suite_files=[
        "/Workspace/Users/your_user/order_checks.py",
        "/Workspace/Users/your_user/customer_checks.py",
    ]
)
```

## Suite Naming Convention

The suite name is automatically derived from your class name:

| Class Name | Suite Config Key |
|------------|------------------|
| `MyCustomSuite` | `my_custom` |
| `DataQualityChecksSuite` | `data_quality_checks` |
| `OrderValidationDQTestSuite` | `order_validation` |

The following suffixes are removed: `DQTestSuite`, `TestSuite`, `Suite`

## Check Method Signature

All check methods must follow this pattern:

```python
def _check_<name>(self, df, cfg) -> tuple[bool, str, str | None]:
    """
    Args:
        df: Spark DataFrame (filtered if filters configured)
        cfg: Dict with check configuration from YAML

    Returns:
        - passed: True if check passes
        - details: String with check details for reporting
        - column: Column name (optional, for column-specific checks)
    """
    pass
```

## Using Base Suite Features

Your custom suite inherits all features from `DQTestSuite`:

### Threshold Evaluation

```python
def _check_with_threshold(self, df, cfg):
    failed_count = df.filter("condition").count()
    total_count = df.count()

    # Uses threshold from config or suite defaults
    passed, metadata = self._evaluate_threshold(failed_count, total_count, cfg)
    details = self._build_threshold_details(f"failed={failed_count}", metadata)

    return passed, details, None
```

### Data Filtering

Filters are automatically applied before your check runs:

```yaml
my_custom:
  defaults:
    partition_filter:
      date: "2024-01-15"
    row_filter: "status = 'active'"
  tables:
    - catalog: my_catalog
      schema: my_schema
      table: my_table
      # Table-level filters override defaults
      partition_filter:
        date: ["2024-01-14", "2024-01-15"]
```

### Helper Methods

```python
# Validate required config keys
valid, error = self._require_cfg(cfg, "column", "threshold")
if not valid:
    return False, error, None

# Get severity and blocking from config
severity = self._severity(cfg)  # default: "warning"
blocking = self._blocking(cfg)  # default: False

# Build narrative details (v1.1.0+)
# PASS: "All 5000 keys unique"
# FAIL: "Found 50 duplicate keys (5.0% of 1000)"
pct = failed_count / total_count * 100 if total_count > 0 else 0
details = f"Found {failed_count} invalid rows ({pct:.1f}% of {total_count})"

# Build debug fields for failures (v1.1.0+)
failure_stats = self._build_failure_stats(
    failure_type="validation_error",
    affected_columns=[column],
    failure_count=failed_count,
    total_checked=total_count,
    diagnostic={"threshold": threshold}
)

investigation_query = self._build_investigation_query(
    check_type="generic",
    table_fq=f"{cfg['catalog']}.{cfg['schema']}.{cfg['table']}",
    affected_columns=[column],
    cfg=cfg
)

execution = self._build_execution_trace(
    start_time=start_time,
    rows_scanned=total_count
)
```

## Complete Example

### Custom Suite with Multiple Checks

```python
# my_domain_suite.py

from pyspark.sql import functions as F
from neco_dq.test_suites.base_test_suite import DQTestSuite
from neco_dq.core.io.result_writer import DQResultWriter


class MyDomainSuite(DQTestSuite):
    """Domain-specific data quality checks."""

    def __init__(self, spark, cfg: dict):
        self.spark = spark
        self.cfg = cfg
        self.writer = DQResultWriter(spark)

    @property
    def _suite_name(self) -> str:
        return "my_domain"

    @property
    def _suite_config_key(self) -> str:
        return "my_domain"

    def _check_positive_amounts(self, df, cfg):
        """Verify all amounts are positive."""
        column = cfg.get("column", "amount")

        negative_count = df.filter(F.col(column) < 0).count()
        total = df.count()

        passed, meta = self._evaluate_threshold(negative_count, total, cfg)

        # Narrative details format (v1.1.0+)
        if passed:
            details = f"All {total} amounts positive"
        else:
            pct = negative_count / total * 100 if total > 0 else 0
            details = f"Found {negative_count} negative amounts ({pct:.1f}% of {total})"

        return passed, details, column

    def _check_valid_status(self, df, cfg):
        """Verify status values are in allowed list."""
        column = cfg.get("column", "status")
        allowed = cfg.get("allowed_values", [])

        invalid = df.filter(~F.col(column).isin(allowed))
        invalid_count = invalid.count()
        total = df.count()

        passed = invalid_count == 0

        # Narrative details format (v1.1.0+)
        if passed:
            details = f"All {total} status values valid"
        else:
            details = f"Found {invalid_count} invalid status values"

        return passed, details, column

    def _check_date_sequence(self, df, cfg):
        """Verify date columns are in correct sequence."""
        start_col = cfg.get("start_column")
        end_col = cfg.get("end_column")

        valid, error = self._require_cfg(cfg, "start_column", "end_column")
        if not valid:
            return False, error, None

        invalid = df.filter(F.col(start_col) > F.col(end_col))
        invalid_count = invalid.count()
        total = df.count()

        passed = invalid_count == 0

        # Narrative details format (v1.1.0+)
        if passed:
            details = f"All {total} date sequences valid"
        else:
            details = f"Found {invalid_count} invalid date sequences ({start_col} > {end_col})"

        return passed, details, f"{start_col},{end_col}"
```

**Example Output for custom checks:**

| Check | Status | Details |
|-------|--------|---------|
| `positive_amounts` | PASS | `All 10000 amounts positive` |
| `positive_amounts` | FAIL | `Found 15 negative amounts (0.15% of 10000)` |
| `valid_status` | PASS | `All 10000 status values valid` |
| `valid_status` | FAIL | `Found 8 invalid status values` |
| `date_sequence` | PASS | `All 10000 date sequences valid` |
| `date_sequence` | FAIL | `Found 3 invalid date sequences (created_at > updated_at)` |

### Configuration

```yaml
# dq-config/my_domain.yaml

my_domain:
  data_to_save:
    - all_results
    - failed_only

  defaults:
    threshold:
      max_failure_percent: 1.0

  tables:
    - catalog: sales
      schema: transactions
      table: orders
      checks:
        positive_amounts:
          enabled: true
          column: total_amount
          severity: error
          blocking: true

        valid_status:
          enabled: true
          column: order_status
          allowed_values: ["pending", "confirmed", "shipped", "delivered"]
          severity: warning

        date_sequence:
          enabled: true
          start_column: created_at
          end_column: updated_at
          severity: error
```

### Execution

```python
from neco_dq.runner import run_dq

results = run_dq(
    config_ref="/Workspace/Users/your_user/dq-config/",
    custom_suite_files="/Workspace/Users/your_user/my_domain_suite.py",
    run_suites=["my_domain"]  # Optional: run only custom suite
)
```

## Modular Configuration

Custom suites work with the modular configuration structure. Create a directory for each custom suite under `suites/`.

### Directory Structure

```
dq-config/
├── global/
│   └── policy.yaml
└── suites/
    ├── bronze/
    │   └── ...
    ├── silver/
    │   └── ...
    ├── order_checks/              # Custom suite directory
    │   ├── suite.yaml
    │   └── test_groups/
    │       ├── validations.yaml
    │       └── business_rules.yaml
    └── customer_checks/           # Another custom suite
        ├── suite.yaml
        └── test_groups/
            └── data_quality.yaml
```

### Suite Configuration

**`suites/order_checks/suite.yaml`**:

```yaml
suite: order_checks

default_checks:
  valid_total:
    enabled: true
    severity: warning

data_to_save:
  - all_results
  - failed_only
```

### Test Groups

**`suites/order_checks/test_groups/validations.yaml`**:

```yaml
tables:
  - catalog: sales
    schema: transactions
    table: orders
    checks:
      valid_total:
        enabled: true
        column: total_amount
        severity: error
        blocking: true

      valid_status:
        enabled: true
        allowed_values: ["pending", "confirmed", "shipped"]
        severity: warning

  - catalog: sales
    schema: transactions
    table: order_items
    checks:
      positive_quantity:
        enabled: true
        column: quantity
        severity: error
```

### Running with Modular Config

```python
from neco_dq.runner import run_dq

# Run all suites (including custom)
results = run_dq(
    config_ref="/Workspace/dq-config/",
    custom_suite_files=[
        "/Workspace/order_checks.py",
        "/Workspace/customer_checks.py",
    ]
)

# Run only custom suites
results = run_dq(
    config_ref="/Workspace/dq-config/",
    custom_suite_files=[
        "/Workspace/order_checks.py",
        "/Workspace/customer_checks.py",
    ],
    run_suites=["order_checks", "customer_checks"]
)
```

### Name Mapping

The suite directory name must match the `_suite_config_key` from your Python class:

| Python Class | `_suite_config_key` | Directory Name |
|--------------|---------------------|----------------|
| `OrderChecksSuite` | `order_checks` | `suites/order_checks/` |
| `CustomerChecksSuite` | `customer_checks` | `suites/customer_checks/` |

## Best Practices

1. **Single Responsibility**: Each check should validate one specific aspect
2. **Clear Details**: Include all relevant counts and thresholds in details
3. **Use Thresholds**: Use `_evaluate_threshold()` for flexible pass/fail criteria
4. **Validate Config**: Use `_require_cfg()` to fail gracefully on missing config
5. **Column Attribution**: Return the column name for column-specific checks
6. **Reuse Helpers**: Use base class methods for consistent behavior

## Limitations

- Each file must contain exactly one `DQTestSuite` subclass
- Each suite must implement `_suite_name` and `_suite_config_key` properties
- Suite names must be unique across all loaded files
