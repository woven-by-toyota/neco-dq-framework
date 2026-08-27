# Code Quality & Best Practices

This guide covers coding standards, patterns, and best practices specific to the Neco DQ Framework.

---

## Architecture Principles

For architecture principles, component ownership, and design patterns, see:

- [Architecture Overview](../architecture/overview.md) - System components and execution flow
- [Governance](governance.md) - Component boundaries, anti-patterns, and design patterns

---

## Check Implementation Standards

### Standard Signature

All checks MUST follow this contract:

```python
def _check_<name>(
    self,
    df: DataFrame,
    cfg: dict[str, Any]
) -> tuple[bool, str, str | None]:
    """
    Brief description.

    Config:
        param1: type - Description
        param2: type - Description (optional)

    Returns:
        (passed, details, column)
    """
    pass
```

### Return Contract

Every check returns a 3-tuple:

| Element | Type | Description |
|---------|------|-------------|
| `passed` | `bool` | `True` if check passed |
| `details` | `str` | Human-readable narrative (~60 chars, starts with a word) |
| `column` | `str \| None` | Column name or `None` for table-level |

**Good details format (v1.1.0+):**
```python
# PASS patterns
"All 5000 keys unique"
"Record count OK: 1523 rows"
"Volume OK: 700 (within 500-1000)"

# FAIL patterns
"Found 50 duplicate keys (5.0% of 1000)"
"Missing 3 required columns"
"Data age 48h exceeds 24h limit"
```

**Bad details format:**
```python
"check failed"  # Too vague
"error"         # No context
"5"             # Never start with a number
```

---

## Generalization Requirement

**Every check MUST be generic and reusable across any dataset.**

### Naming Conventions

Names must describe behavior, not business domain:

| Bad (Business-specific) | Good (Generic) |
|-------------------------|----------------|
| `_check_row_count` | `_check_row_count` |
| `_check_payment_flow` | `_check_status_transition` |
| `_check_rating_aggregation` | `_check_compare_metric` |

### Parameter Names

Use standardized generic parameter names:

| Parameter | Type | Purpose |
|-----------|------|---------|
| `column` | `str` | Single column name |
| `columns` | `list[str]` | Multiple column names |
| `key_column` | `str \| list[str]` | Primary/composite key |
| `timestamp_column` | `str` | Timestamp field |
| `value_column` | `str` | Numeric value field |
| `group_by` | `list[str]` | Grouping columns |
| `filter` | `str` | SQL WHERE clause |
| `threshold` | `dict` | Row-level failure threshold |
| `tolerance` | `float` | Value comparison tolerance |
| `operator` | `str` | Comparison operator |

### Example: Generic vs Business-Specific

```python
# BAD - Business-specific
def _check_value_range(self, df, cfg):
    return df.filter(col("rating").between(1, 5)).count() == df.count()

# GOOD - Generic and reusable
def _check_value_in_range(self, df: DataFrame, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
    """Validates column values are within specified range."""
    column = cfg["column"]
    min_val = cfg["min_value"]
    max_val = cfg["max_value"]

    invalid_count = df.filter(~col(column).between(min_val, max_val)).count()
    passed = invalid_count == 0

    return passed, f"invalid_count={invalid_count}, range=[{min_val}, {max_val}]", column
```

---

## PySpark Engineering Rules

### Preferred Patterns

```python
# Use DataFrame API
df.filter(col("status") == "active").count()

# Explicit casts
df.withColumn("amount", col("amount").cast("decimal(10,2)"))

# Explicit null filtering
df.filter(col("email").isNotNull())

# Narrow transformations when possible
df.select("id", "name").filter(col("active"))
```

### Avoid

```python
# Implicit schema inference
spark.read.json(path)  # Use .schema() instead

# Unnecessary actions
df.count()  # Only when needed
df.collect()  # Avoid on large datasets

# Full scans when aggregations suffice
df.collect()  # Use df.agg() instead
```

### Composite Key Handling

Support both single string and list:

```python
key_cols = cfg["key_column"]

# Normalize to list
if isinstance(key_cols, str):
    key_cols = [k.strip() for k in key_cols.split(",")]

# Use normalized list
dupes = df.groupBy(*key_cols).agg(count("*").alias("cnt"))
```

---

## Error Handling

Checks should handle errors gracefully:

```python
def _check_example(self, df: DataFrame, cfg: dict[str, Any]) -> tuple[bool, str, str | None]:
    try:
        column = cfg["column"]

        if column not in df.columns:
            return False, f"column_not_found: {column}", column

        # ... perform validation ...
        return passed, details, column

    except KeyError as e:
        return False, f"missing_config_parameter: {e}", None

    except Exception as e:
        return False, f"check_error: {e!s}", cfg.get("column")
```

---

## Type Hints

### Requirements

- All public functions must have type hints
- Use `from __future__ import annotations` for forward references
- Use `dict[str, Any]` for configuration dictionaries

### Example

```python
from __future__ import annotations

from typing import Any
from pyspark.sql import DataFrame

def resolve_metric(
    spark: SparkSession,
    df: DataFrame,
    cfg: dict[str, Any],
) -> tuple[bool, str, str | None]:
    """Resolve a metric comparison."""
    ...
```

---

## Documentation Standards

### Docstring Format

```python
def _check_value_in_range(
    self,
    df: DataFrame,
    cfg: dict[str, Any]
) -> tuple[bool, str, str | None]:
    """
    Validates column values fall within specified numeric range.

    Use case: Ensure values are within valid bounds (e.g., ratings 1-5).

    Config:
        column: str - Column name to validate
        min_value: numeric - Minimum allowed value (inclusive)
        max_value: numeric - Maximum allowed value (inclusive)

    Returns:
        (passed, details, column)

    Example config:
        ```yaml
        value_in_range:
          enabled: true
          column: rating
          min_value: 1
          max_value: 5
        ```
    """
    pass
```

### Required Sections

1. Brief description (one line)
2. Use case (when to use)
3. Config parameters with types
4. Returns explanation
5. Example config (for complex checks)

---

## Code Quality Tools

### Ruff Configuration

Ruff is the primary linting and formatting tool. Key rules:

| Rule | Code | What It Checks |
|------|------|----------------|
| Security | S | Hardcoded passwords, SQL injection |
| Print | T20 | Use of `print()` statements |
| Datetime | DTZ | Timezone-naive datetime |
| Naming | N | PEP8 naming conventions |
| Pylint | PL | Complexity, too many arguments |
| Bugbear | B | Common bugs |

### Validation Workflow

```bash
# Check for issues
ruff check .

# Auto-fix and format
ruff check --fix . && ruff format .

# Verify clean
ruff check . && ruff format --check .
```

**Never present Python code that fails ruff validation.**

### Common Fixes

**Print statements:**
```python
# Wrong
print(f"Processing {table}")

# Correct
import logging
logger = logging.getLogger(__name__)
logger.info(f"Processing {table}")
```

**Timezone-naive datetime:**
```python
# Wrong
from datetime import datetime
now = datetime.now()

# Correct
from datetime import datetime, timezone
now = datetime.now(timezone.utc)
```

---

## Anti-Patterns

For the complete list of forbidden patterns and prohibited technologies, see [Governance - Anti-Patterns](governance.md#anti-patterns).

---

## Validation Checklist

Before submitting code, verify:

### Naming
- [ ] Function name is generic (no business terms)
- [ ] Parameter names are generic
- [ ] Uses standard parameter names

### Implementation
- [ ] Works for any dataset
- [ ] Handles composite keys (normalize to list)
- [ ] Graceful error handling
- [ ] No hardcoded business logic

### Return Values
- [ ] Returns `tuple[bool, str, str | None]`
- [ ] Details are narrative (~60 chars, starts with word)
- [ ] Column is name, comma-separated list, or `None`

### Code Quality
- [ ] Passes `ruff check .`
- [ ] Passes `ruff format --check .`
- [ ] Type hints present
- [ ] Docstring with config parameters

---

## Where Changes Belong

| Change Type | Location |
|-------------|----------|
| New metric | `neco_dq/core/metrics/` |
| New operator | `neco_dq/core/metrics/operators.py` |
| New validation | `neco_dq/test_suites/<layer>/` |
| New failure semantics | `neco_dq/core/policy/` |
| New visualization | `neco_dq/core/summary/` |
| New notification channel | `neco_dq/core/alerts/` |

**If a change requires edits across multiple areas, it's likely incorrect.**

---

## Quick Reference

### Commands

```bash
# Lint and format
ruff check . && ruff format .

# Run tests
make test

# Run tests with coverage
make test-cov
```

For project structure and component ownership, see [Governance - Component Ownership](governance.md#component-ownership-matrix).

---

