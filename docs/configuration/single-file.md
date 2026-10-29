# Single-File Configuration

Single-file configuration mode for simpler projects.

## Overview

Single-file mode uses one YAML file containing all configuration. It's straightforward for small projects but consider [modular configuration](modular-config.md) as your project grows.

### When to Use Single-File Mode

**Good for:**
- Quick prototyping and experimentation
- Simple projects with 1-2 suites
- Learning the framework
- Single-table validations
- Small teams

**Consider modular config when:**
- Configuration exceeds ~200 lines
- Multiple team members edit configs
- You want granular version control
- You need to reuse test patterns

---

## Basic Structure

A single-file config is the same content a modular tree holds, flattened into one
YAML: a top-level `dq_policy`, then one key per suite (`bronze`, `silver`, `gold`,
`great_expectations`), each with `tables` and `checks`. Suites also accept
`default_checks` and a `data_to_save` block. These behave identically to modular
mode, so they're documented there rather than repeated here:

- Policy options → [Configuration Basics · policy.yaml](modular-config.md#globalpolicyyaml)
- `default_checks` inheritance → [Configuration Basics · Default Checks](modular-config.md#default-checks-inheritance)
- Persistence / `data_to_save` → [Database Setup](database-setup.md)

A minimal single-file config:

```yaml
dq_policy:
  block_test_suites:
    bronze: true
  alerts:
    slack:
      enabled: true

bronze:
  default_checks:
    record_count:
      enabled: true
      severity: critical
      blocking: true

  tables:
    - catalog: my_catalog
      schema: my_schema_bronze
      table: raw_events
      checks:
        freshness:
          enabled: true
          timestamp_column: batch_raw_events_at
          max_delay_hours: 24

        mandatory_fields:
          enabled: true
          columns: [batch_id, project_id, order_id]

silver:
  tables:
    - catalog: my_catalog
      schema: my_schema_silver
      table: cleaned_events
      checks:
        referential_integrity:
          enabled: true
          source_column: order_id
          target:
            schema: my_schema_silver
            table: orders
            column: order_id
```

Add `gold:` and `great_expectations:` keys the same way. Each check's parameters
are documented on its suite page (see [Test Suites Overview](../test-suites/index.md)).

---

## Loading Configuration

Point `run_dq()` at the **file** (not a directory):

```python
from neco_dq.runner import run_dq

results = run_dq("/dbfs/config/dq_config.yaml")

# With a profile for multi-environment support
results = run_dq("/dbfs/config/dq_config.yaml", profile="prod")
```

See the [`run_dq()` API Reference](run-dq-api.md) for all options.

---

## Limitations of Single-File Mode

### 1. Poor Scalability

```yaml
# As configuration grows, file becomes unmanageable
bronze_suite:
  tables:
    - name: table1  # Line 50
      checks: [...]
    - name: table2  # Line 100
      checks: [...]
    # ... 20 more tables ...
    - name: table22  # Line 1500 - hard to find!
```

### 2. Difficult Collaboration

- Merge conflicts on single large file
- No granular review of changes
- Hard to track who changed what

### 3. No Reuse

- Cannot import common validation patterns
- Must copy-paste repeated validations
- No test group organization

### 4. Version Control Issues

- Every change touches one file
- Difficult to see scope of changes in git diff
- Cannot track suite evolution separately

---

## Using Profiles

Profiles work with single-file configs too: put a `profiles.yml` next to your YAML
file, use `{{ profile.target_catalog }}` / `{{ var.* }}` templates in the config,
and pass `profile="prod"` (or set `NECO_DQ_PROFILE`). The mechanics are identical to
modular mode, see [Multi-Environment (Profiles)](profiles.md) for the full setup,
inheritance and policy overrides.

---

## See Also

- [Modular Configuration](modular-config.md) - Recommended approach
- [Profiles](profiles.md) - Multi-environment configuration
