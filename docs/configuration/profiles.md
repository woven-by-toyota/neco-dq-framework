# Profiles

Multi-environment configuration using profiles.

---

## Overview

Profiles allow you to maintain a single configuration structure that adapts to different environments (dev, staging, prod) without duplicating files. Each profile defines environment-specific values that are injected into your YAML configuration via Jinja2 templates.

### When to Use Profiles

| Use Case | Example |
|----------|---------|
| **Multi-environment deployments** | Same config for dev/staging/prod with different catalogs |
| **Team-specific configs** | Different thresholds for different teams |
| **Feature flags** | Enable/disable checks per environment |
| **Parameterized validation** | Different SLAs per environment |

---

## Quick Start

### 1. Create `profiles.yml`

Place this file in your config directory (alongside your YAML config or at the root of your modular config).

```yaml
default_profile: dev

profiles:
  dev:
    target_catalog: dev_catalog
    variables:
      schema_prefix: dev_
      max_delay_hours: 48
      failure_threshold: 0.2

  prod:
    target_catalog: prod_catalog
    variables:
      schema_prefix: ""
      max_delay_hours: 24
      failure_threshold: 0.05
```

### 2. Use Variables in Config

Reference profile values using Jinja2 syntax:

```yaml
bronze:
  tables:
    - catalog: "{{ profile.target_catalog }}"
      schema: "{{ var.schema_prefix }}my_schema"
      table: my_table
      checks:
        freshness:
          enabled: true
          max_delay_hours: "{{ var.max_delay_hours }}"
```

### 3. Run with Profile

```python
from neco_dq.runner import run_dq

# Explicit profile parameter
results = run_dq("/Workspace/config/", profile="prod")

# Or use environment variable
# export NECO_DQ_PROFILE=prod
results = run_dq("/Workspace/config/")
```

---

## Profile Resolution Order

The framework resolves the active profile in this order (highest priority first):

| Priority | Source | Example |
|----------|--------|---------|
| 1 | `profile` parameter in `run_dq()` | `run_dq(..., profile="prod")` |
| 2 | `NECO_DQ_PROFILE` environment variable | `export NECO_DQ_PROFILE=prod` |
| 3 | `default_profile` in `profiles.yml` | `default_profile: dev` |
| 4 | Fallback to `"default"` | If no profile specified and no `default_profile` set |

---

## profiles.yml Structure

Profile names and variable names are completely up to your team. Use whatever naming convention makes sense for your organization.

```yaml
# Optional: set the default profile when none is specified
default_profile: dev

profiles:
  # Profile names are free-form: dev, prod, staging, team_a, daily_run, etc.
  dev:
    # Accessible as {{ profile.target_catalog }}
    target_catalog: dev_catalog

    # Variable names are free-form: use whatever your team prefers
    # Accessible as {{ var.name }}
    variables:
      schema_prefix: dev_
      max_delay_hours: 48
      enable_expensive_checks: false

    # Optional: override dq_policy settings for this profile
    policy:
      failure_threshold: 0.2
      block_test_suites:
        bronze: false

  staging:
    target_catalog: staging_catalog
    variables:
      schema_prefix: stg_
      max_delay_hours: 36
      enable_expensive_checks: true

  prod:
    target_catalog: prod_catalog
    variables:
      schema_prefix: ""
      max_delay_hours: 24
      enable_expensive_checks: true
    policy:
      failure_threshold: 0.05
```

### Profile Fields

| Field | Type | Description |
|-------|------|-------------|
| `target_catalog` | `str` | Default catalog, accessible as `{{ profile.target_catalog }}` |
| `variables` | `dict` | User-defined variables, accessible as `{{ var.name }}` |
| `policy` | `dict` | Policy overrides merged with global `dq_policy` |

**Naming is flexible:** Profile names (`dev`, `prod`, `team_analytics`, `nightly_run`) and variable names (`max_delay_hours`, `target_schema`, `enable_feature_x`) are entirely up to your team. The framework imposes no naming conventions.

---

## Profile Inheritance

Profiles can extend other profiles using the `extends` keyword. Child profiles inherit all values from the parent, with child values taking precedence.

```yaml
profiles:
  base:
    target_catalog: base_catalog
    variables:
      max_delay_hours: 24
      enable_alerts: true
      alert_channel: "#dq-alerts"

  dev:
    extends: base
    target_catalog: dev_catalog  # Override
    variables:
      max_delay_hours: 48        # Override
      # enable_alerts and alert_channel inherited from base

  prod:
    extends: base
    target_catalog: prod_catalog
    variables:
      max_delay_hours: 12        # Stricter than base
```

### Inheritance Rules

1. Child values override parent values at the same key
2. `variables` dicts are deep-merged (child adds/overrides individual keys)
3. Other fields are replaced entirely
4. Circular inheritance is detected and raises an error

---

## Jinja2 Template Syntax

### Available Variables

| Variable | Source | Example |
|----------|--------|---------|
| `{{ profile.name }}` | Active profile name | `"prod"` |
| `{{ profile.target_catalog }}` | Profile's `target_catalog` | `"prod_catalog"` |
| `{{ var.<name> }}` | Profile's `variables.<name>` | `{{ var.freshness_hours }}` |

### Type Conversion

Jinja2 templates always produce strings. The framework automatically converts them back to the appropriate types:

| Template Value | Converted To |
|----------------|--------------|
| `"123"` | `123` (int) |
| `"12.5"` | `12.5` (float) |
| `"true"` / `"false"` | `True` / `False` (bool) |
| `"[1, 2, 3]"` | `[1, 2, 3]` (list) |

### Conditionals

Use Jinja2 conditionals for environment-specific checks:

```yaml
checks:
  expensive_check:
    enabled: {{ var.enable_expensive_checks }}

{% if var.enable_alerts %}
  alert_channel: "{{ var.alert_channel }}"
{% endif %}
```

### Defaults

Use the `default` filter for optional variables:

```yaml
checks:
  freshness:
    max_delay_hours: {{ var.max_delay_hours | default(24) }}
```

### Lists and Dicts

Use the `tojson` filter for complex values:

```yaml
variables:
  required_columns: ["id", "created_at", "updated_at"]

# In config:
mandatory_fields:
  columns: {{ var.required_columns | tojson }}
```

---

## Policy Overrides

Profiles can override `dq_policy` settings. Profile policy values are merged with global policy, with profile values taking precedence.

### Example

**global/policy.yaml:**
```yaml
dq_policy:
  failure_threshold: 0.1
  block_test_suites:
    bronze: true
    silver: true
    gold: true
```

**profiles.yml:**
```yaml
profiles:
  dev:
    policy:
      failure_threshold: 0.5      # More lenient in dev
      block_on_threshold: false   # Don't escalate to FAIL
      block_test_suites:
        bronze: false             # Don't block on bronze in dev

  prod:
    policy:
      failure_threshold: 0.05     # Strict in prod
      block_on_threshold: true    # FAIL if threshold exceeded
```

**Result for `dev` profile:**
```yaml
dq_policy:
  failure_threshold: 0.5      # From profile
  block_on_threshold: false   # From profile
  block_test_suites:
    bronze: false             # From profile
    silver: true              # From global
    gold: true                # From global
```

### Policy Options

| Option | Type | Description |
|--------|------|-------------|
| `failure_threshold` | float | Informational threshold (0.0-1.0). Shown in summary when exceeded. |
| `block_on_threshold` | bool | When `true`, non-blocking failures exceeding threshold cause FAIL instead of WARN. |
| `block_test_suites.<suite>` | bool | Whether blocking failures in this suite cause pipeline to fail. |

### Threshold Behavior

- **Blocking failures**: Always cause FAIL regardless of threshold
- **Non-blocking failures**: Cause WARN by default
- **`block_on_threshold: true`**: Non-blocking failures exceeding threshold escalate to FAIL
- Summary displays threshold when configured: `Success Rate: 85.7%  │  Threshold: 10%  │  Status: WARN`

---

## File Location

The framework searches for `profiles.yml` (or `profiles.yaml`) in these locations:

| Config Type | Search Order |
|-------------|--------------|
| **Modular config** | `{config_dir}/profiles.yml` → `{config_dir}/global/profiles.yml` |
| **Single-file config** | `{parent_dir}/profiles.yml` → `{parent_dir}/global/profiles.yml` |

### Example Directory Structures

**Modular config (profiles.yml at root):**
```
/Workspace/dq-config/
├── profiles.yml              # ← Found first
├── global/
│   └── policy.yaml
└── suites/
    └── bronze/
        └── suite.yaml
```

**Modular config (profiles.yml in global/):**
```
/Workspace/dq-config/
├── global/
│   ├── profiles.yml          # ← Also valid location
│   └── policy.yaml
└── suites/
    └── bronze/
        └── suite.yaml
```

**Single-file config:**
```
/Workspace/dq-config/
├── profiles.yml              # ← Found first
└── bronze_suite.yaml
```

---

## Complete Example

### Directory Structure

```
/Workspace/dq-config/
├── profiles.yml
├── global/
│   └── policy.yaml
└── suites/
    └── bronze/
        ├── suite.yaml
        └── test_groups/
            └── ingestion.yaml
```

### profiles.yml

```yaml
default_profile: dev

profiles:
  base:
    variables:
      dq_catalog: dq_metadata_catalog
      dq_schema: dq_metadata
      enable_alerts: true

  dev:
    extends: base
    target_catalog: dev_catalog
    variables:
      schema_prefix: dev_
      max_delay_hours: 72
      min_record_count: 1
    policy:
      failure_threshold: 0.5
      block_test_suites:
        bronze: false

  staging:
    extends: base
    target_catalog: staging_catalog
    variables:
      schema_prefix: stg_
      max_delay_hours: 48
      min_record_count: 100

  prod:
    extends: base
    target_catalog: prod_catalog
    variables:
      schema_prefix: ""
      max_delay_hours: 24
      min_record_count: 1000
    policy:
      failure_threshold: 0.05
```

### global/policy.yaml

```yaml
dq_policy:
  failure_threshold: 0.1
  block_test_suites:
    bronze: true
    silver: true
    gold: true
  alerts:
    slack:
      enabled: "{{ var.enable_alerts }}"
```

### suites/bronze/suite.yaml

```yaml
suite: bronze

data_to_save:
  - table: "{{ var.dq_catalog }}.{{ var.dq_schema }}.dq_test_history"
    write_mode: append
```

### suites/bronze/test_groups/ingestion.yaml

```yaml
tables:
  - catalog: "{{ profile.target_catalog }}"
    schema: "{{ var.schema_prefix }}raw_data"
    table: events
    checks:
      freshness:
        enabled: true
        timestamp_column: batch_ingestion_at
        max_delay_hours: "{{ var.max_delay_hours }}"

      record_count:
        enabled: true
        min_rows: "{{ var.min_record_count }}"
```

### Usage

```python
from neco_dq.runner import run_dq

# Development (lenient thresholds, dev catalog)
results = run_dq("/Workspace/dq-config/", profile="dev")

# Production (strict thresholds, prod catalog)
results = run_dq("/Workspace/dq-config/", profile="prod")

# CI/CD pipeline (use environment variable)
# export NECO_DQ_PROFILE=staging
results = run_dq("/Workspace/dq-config/")
```

---

## Best Practices

### 1. Use Base Profile for Shared Config

```yaml
profiles:
  base:
    variables:
      dq_catalog: qa_dq
      common_setting: value

  dev:
    extends: base
    # Only override what differs

  prod:
    extends: base
    # Only override what differs
```

### 2. Keep Environment Differences Minimal

Focus profiles on values that truly differ:
- Catalog/schema names
- Thresholds and SLAs
- Feature flags

Avoid duplicating check definitions across profiles.

### 3. Use Defaults for Optional Variables

```yaml
max_delay_hours: {{ var.max_delay_hours | default(24) }}
```

This makes configs work even without profiles.

### 4. Document Profile-Specific Behavior

Add comments explaining why values differ:

```yaml
prod:
  variables:
    # Production SLA: data must be fresh within 24 hours
    max_delay_hours: 24
    # Production requires at least 1000 rows to catch ingestion failures
    min_record_count: 1000
```

### 5. Set Sensible Defaults in `default_profile`

```yaml
default_profile: dev  # Safe default for local development
```

---

## Troubleshooting

### Profile Not Found

```
ProfileNotFoundError: Unknown profile 'staging'. Available: dev, prod
```

**Solution:** Check profile name spelling in `profiles.yml`.

### Undefined Variable

```
jinja2.exceptions.UndefinedError: 'var' is undefined
```

**Solution:** Ensure `profiles.yml` exists and defines the variable, or use `| default()`.

### Circular Inheritance

```
CircularInheritanceError: Circular inheritance detected: dev -> staging -> dev
```

**Solution:** Review `extends` chain and remove the cycle.

### Type Conversion Issues

If a numeric value is being treated as a string, ensure you're not quoting it:

```yaml
# Wrong - will be string "24"
max_delay_hours: "{{ var.max_delay_hours }}"

# Right - auto-converted to int
max_delay_hours: {{ var.max_delay_hours }}
```

---

## See Also

- [Modular Config](modular-config.md) - Directory-based configuration
- [Single-File Configuration](single-file.md) - Single-file configuration
- [Template System](template-system.md) - Advanced Jinja2 patterns
- [Data Filtering](data-filtering.md) - Scope validation to subsets
