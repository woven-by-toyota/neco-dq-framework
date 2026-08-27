# `run_dq()` API Reference

`run_dq()` is the main entry point for executing data quality checks. This page is
the reference for its parameters and the filtering model. For how to *structure* a
config, see [Modular Configuration](modular-config.md).

```python
from neco_dq.runner import run_dq

results = run_dq(
    config_ref,              # Path to config directory or file
    profile=None,            # Optional: profile name from profiles.yml
    run_suites=None,         # Optional: only run these suites
    exclude_suites=None,     # Optional: skip these suites
    selector=None,           # Optional: filter specific tables per suite
    tags=None,               # Optional: only run checks with matching tags
    custom_suite_files=None, # Optional: Python file(s) with custom suites
    verbose=False,           # Optional: show debug output and progress bars
)
```

`run_dq()` returns a `dict[str, Any]`: a summary of results keyed per test suite.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_ref` | `str` | *required* | Path to config directory or YAML file |
| `profile` | `str` | `None` | Profile name from `profiles.yml` (else `NECO_DQ_PROFILE` or `default_profile`) |
| `run_suites` | `list[str]` | `None` | Only run these test suites |
| `exclude_suites` | `list[str]` | `None` | Skip these test suites |
| `selector` | `dict[str, list[str]]` | `None` | Filter specific tables per suite |
| `tags` | `list[str]` | `None` | Only run checks with matching tags |
| `custom_suite_files` | `str \| list[str]` | `None` | Path(s) to Python file(s) with custom `DQTestSuite`(s). See [Custom Suites](../test-suites/custom.md). |
| `verbose` | `bool` | `False` | Enable debug logging and progress bars |

## Basic usage

```python
from neco_dq.runner import run_dq

# Run all suites in the config directory
results = run_dq("/Workspace/config/")
```

## Selective suite execution

Control which test suites to run:

```python
# Run only bronze suite
results = run_dq(
    "/Workspace/config/",
    run_suites=["bronze"]
)

# Run bronze and silver suites
results = run_dq(
    "/Workspace/config/",
    run_suites=["bronze", "silver"]
)

# Run all suites EXCEPT profiling
results = run_dq(
    "/Workspace/config/",
    exclude_suites=["profiling"]
)

# Run gold suite and its Great Expectations checks
results = run_dq(
    "/Workspace/config/",
    run_suites=["gold", "great_expectations@gold"]
)
```

## Runtime table selection with `selector`

The `selector` parameter filters which tables to validate **at runtime**, without
modifying your configuration files. This is useful for debugging, incremental
validation, or splitting work across parallel jobs.

### Basic usage

```python
# Run only specific tables in bronze suite
results = run_dq(
    "/Workspace/config/",
    run_suites=["bronze"],
    selector={
        "bronze": ["orders", "customers"]  # Only these tables
    }
)
```

This runs bronze checks only on `orders` and `customers` tables, skipping all
other tables defined in the configuration.

### Multiple suites

```python
# Run specific tables in multiple suites
results = run_dq(
    "/Workspace/config/",
    run_suites=["bronze", "silver"],
    selector={
        "bronze": ["orders", "customers"],
        "silver": ["orders_cleaned", "customers_enriched"]
    }
)
```

### Great Expectations suites

For GE suites, use the parent suite name:

```python
# Config has: great_expectations@silver
results = run_dq(
    "/Workspace/config/",
    selector={"silver": ["orders"]}  # Works for both silver and ge@silver
)
```

!!! tip "selector inheritance for Great Expectations"
    When you filter tables for a suite, the same filter applies to its Great
    Expectations checks. For example, `selector={"gold": ["summary"]}` also filters
    `great_expectations@gold` to only run checks on the `summary` table.

### Use cases

| Scenario | Example |
|----------|---------|
| **Debug single table** | `selector={"bronze": ["problematic_table"]}` |
| **Incremental validation** | `selector={"silver": ["newly_loaded_table"]}` |
| **CI/CD subset** | `selector={"gold": ["critical_kpis"]}` |
| **Parallel jobs** | Split tables across workers using selector |

### Combining with other filters

`selector` works with other filtering options:

```python
results = run_dq(
    "/Workspace/config/",
    run_suites=["bronze", "silver"],     # Suite-level filter
    selector={"bronze": ["orders"]},      # Table-level filter
    tags=["critical"]                     # Check-level filter
)
```

**Filter order:**

1. `run_suites` / `exclude_suites`: which suites to run
2. `selector`: which tables within those suites
3. `tags`: which checks within those tables
4. `partition_filter` / `row_filter`: which rows within those tables

See [Data Filtering](data-filtering.md) and [Tag Filtering](tags-filtering.md) for
the row- and check-level filters.

## Verbose mode

Enable detailed logging and progress bars:

```python
results = run_dq(
    "/Workspace/config/",
    verbose=True  # Shows debug output and progress bars
)
```

## Running with a profile

```python
# Explicit profile
results = run_dq("/Workspace/config/", profile="prod")

# Or environment variable
# export NECO_DQ_PROFILE=prod
results = run_dq("/Workspace/config/")
```

See [Multi-Environment (Profiles)](profiles.md) for the full profile model.

## Complete example: production usage

```python
from neco_dq.runner import run_dq

# Full validation: all suites, all tables
results = run_dq("/Workspace/config/")

# Quick validation: only bronze, specific tables
results = run_dq(
    "/Workspace/config/",
    run_suites=["bronze"],
    selector={"bronze": ["critical_table_1", "critical_table_2"]}
)

# Skip expensive checks: exclude profiling
results = run_dq(
    "/Workspace/config/",
    exclude_suites=["profiling"]
)

# Debug mode: verbose output for troubleshooting
results = run_dq(
    "/Workspace/config/",
    run_suites=["silver"],
    selector={"silver": ["problem_table"]},
    verbose=True
)
```
