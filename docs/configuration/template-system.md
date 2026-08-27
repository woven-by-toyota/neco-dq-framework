# Template System (Planned)

Jinja2-powered configuration templates for generating DQ suite configurations from parameterized patterns.

!!! warning "Planned feature, not yet available"
    The template system is **not implemented** and cannot be used today. Everything
    below (registry, syntax, examples) describes the **proposed design**, not a
    shipping feature. Treat the code samples as a spec, not as working config. For
    multi-environment needs available today, use
    [Profiles](profiles.md), which cover the common parameterization cases.

---

## Overview

The template system provides:

- Jinja2-based YAML generation
- Tag-based template invocation
- Loop and conditional logic
- Centralized template registry

Templates are Jinja2 YAML files that generate complete suite configurations
when rendered with context variables.

---

## Architecture

```
config_resolver
  ↓
template_registry.yaml (name → path mapping)
  ↓
*.yaml templates (Jinja2 syntax)
  ↓
rendered config dict
  ↓
orchestrator
```

Templates are resolved **before** reaching the orchestrator.
Suites receive already-interpreted configuration.

---

## Template Registry

The registry maps template tags to file paths.

**File:** `neco_dq/config/template_registry.yaml`

```yaml
silver_standard: config/silver/silver_standard_template.yaml
ge_standard: config/ge/ge_standard_template.yaml
```

**Structure:**

- Keys: template tag (string identifier)
- Values: relative path from `neco_dq` package root

**Note:** Paths in the registry are relative to the `neco_dq` package directory.
For example, `config/silver/silver_standard_template.yaml` resolves to
`neco_dq/config/silver/silver_standard_template.yaml` in the repository.

New templates must be registered here.

---

## Invocation

Templates are invoked by passing the tag to the config resolver.

```python
from neco_dq.config.config_resolver import resolve_config

# Pass template tag as string
config = resolve_config("silver_standard")

# Resolver looks up path in registry and renders template
```

**No separate instance files.**

Template tags are passed directly to `resolve_config()`.

---

## Template Structure

Templates are Jinja2-enhanced YAML files.

### Minimal Template

```yaml
silver:
  tables:
{% for t in tables %}
    - catalog: "{{ t.catalog }}"
      schema: "{{ t.schema }}"
      table: "{{ t.table }}"
      checks:
        idempotency:
          key_column: "{{ t.pk_id }}"
          enabled: true
{% endfor %}
```

### Context Variables

Templates receive rendering context from external sources.

Example context:

```python
context = {
    "tables": [
        {"catalog": "main", "schema": "silver", "table": "orders", "pk_id": "order_id"},
        {"catalog": "main", "schema": "silver", "table": "customers", "pk_id": "customer_id"}
    ],
    "data_to_save": [
        {"dq_history_table": "main.dq.history", "dq_alerts_table": "main.dq.alerts"}
    ]
}
```

**Important:** Context is provided externally. Templates define structure, not data.

---

## Real Template Examples

### Silver Standard Template

**File:** `neco_dq/config/silver/silver_standard_template.yaml`

```yaml
silver:
  tables:
{% for t in tables %}
    - catalog: "{{ t.catalog }}"
      schema: "{{ t.schema }}"
      table: "{{ t.table }}"

      checks:

        idempotency:
          key_column:
          {% if t.pk_id is string %}
            {{ t.pk_id }}
          {% else %}
          {% for c in t.pk_id %}
            - {{ c }}
          {% endfor %}
          {% endif %}
          enabled: true
          blocking: true

{% if t.status_column is defined %}
        status_transition:
          key_column: "{{ t.pk_id[0] if t.pk_id is not string else t.pk_id }}"
          status_column: "{{ t.status_column }}"
          timestamp_column: "{{ t.timestamp_column }}"
          allowed_flow:
            - CREATED
            - AUTHORIZED
            - CAPTURED
            - FAILED
            - REFUNDED
          enabled: true
          blocking: true
{% endif %}
{% endfor %}

{% for d in data_to_save %}
  data_to_save:
    - table: "{{ d.dq_history_table }}"
      write_mode: append
      alerts: false
      columns_to_save:
        - test_suite
        - catalog
        - schema
        - table
        - run_timestamp
        - passed
{% endfor %}
```

**Key patterns:**

- Loop over `tables` list
- Conditional blocks with `{% if %}`
- Type checking: `is string`, `is defined`
- Nested attribute access: `t.pk_id[0]`

---

### Great Expectations Standard Template

**File:** `neco_dq/config/ge/ge_standard_template.yaml`

```yaml
great_expectations:
  tables:
{% for t in tables %}
    - catalog: "{{ t.catalog }}"
      schema: "{{ t.schema }}"
      table: "{{ t.table }}"
      checks:
        _dataset:
          min_rows: 1
          severity: error
          blocking: true

{% if t.pk_id is string %}
        {{ t.pk_id }}:
          dtype:
            values: ["str"]
            severity: error
            blocking: true
          nullable:
            enabled: false
          unique:
            enabled: true
            blocking: true
{% else %}
        {{ t.pk_id[0] }}:
          dtype:
            values: ["str"]
            severity: error
            blocking: true
          nullable:
            enabled: false
          unique:
            enabled: true
            blocking: true
            column_list: {{ t.pk_id | tojson }}
{% endif %}
{% endfor %}

{% for d in data_to_save %}
  data_to_save:
    - table: "{{ d.dq_history_table }}"
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
{% endfor %}
```

**Key patterns:**

- Branching on primary key structure (string vs list)
- Jinja2 filters: `| tojson`
- Nested YAML generation
- Multiple loops for different sections

---

## Jinja2 Features

### Loops

```yaml
{% for item in items %}
  - name: "{{ item.name }}"
    value: {{ item.value }}
{% endfor %}
```

### Conditionals

```yaml
{% if condition %}
  enabled: true
{% else %}
  enabled: false
{% endif %}
```

### Type Checking

```yaml
{% if variable is string %}
  single_value: "{{ variable }}"
{% else %}
  list_values: {{ variable | tojson }}
{% endif %}
```

### Defined Checks

```yaml
{% if optional_field is defined %}
  field: "{{ optional_field }}"
{% endif %}
```

### Filters

```yaml
{{ list_variable | tojson }}
{{ string_variable | upper }}
{{ number_variable | int }}
```

---

## When to Use Templates

**Use templates when:**

- Configuration follows a predictable pattern
- Multiple tables share similar validation structure
- Logic is parameterizable
- You want centralized pattern updates

**Avoid templates when:**

- Each table requires unique logic
- Configuration is simpler without abstraction
- Parameterization obscures intent
- Modular config directories are sufficient

---

## Templates vs Modular Config

| Template System | Modular Config |
|-----------------|----------------|
| Single Jinja2 file | Directory structure |
| Loop-based generation | Explicit file per table |
| Centralized pattern | Distributed configuration |
| Tag invocation | Path invocation |

**Preference:** Use modular config for explicit control.
Use templates for standardized patterns across many similar tables.

---

## Implementation Reference

### Config Resolver Logic

```python
# neco_dq/config/config_resolver.py:159-180

def resolve_config(config_ref: str | Path) -> dict[str, Any]:
    """
    Supported inputs:
    - path to YAML file (single-file config)
    - path to modular config directory
    - template registry tag
    """
    path = Path(config_ref).expanduser()

    if path.exists():
        path = path.resolve()

        if path.is_file():
            return _resolve_single_file_yaml(path)

        if path.is_dir():
            return _resolve_modular_config(path)

    if isinstance(config_ref, str):
        return _resolve_template_tag(config_ref)

    raise ValueError(f"Unable to resolve config reference: {config_ref}")
```

### Template Tag Resolution

```python
# neco_dq/config/config_resolver.py:32-42

def _resolve_template_tag(tag: str) -> dict[str, Any]:
    if not DEFAULT_TEMPLATE_REGISTRY.exists():
        raise FileNotFoundError(f"Template registry not found at {DEFAULT_TEMPLATE_REGISTRY}")

    registry = _load_yaml(DEFAULT_TEMPLATE_REGISTRY)

    if tag not in registry:
        raise KeyError(f"Template '{tag}' not found. Available: {list(registry.keys())}")

    resolved_path = Path(registry[tag]).expanduser().resolve()
    return _resolve_single_file_yaml(resolved_path)
```

---

## Adding New Templates

### Steps

1. Create Jinja2 YAML template file
2. Add entry to `template_registry.yaml`
3. Document expected context structure
4. Test with representative data

### Example: Custom Bronze Template

**1. Create template**

```yaml
# neco_dq/config/bronze/bronze_minimal_template.yaml

bronze:
  tables:
{% for t in tables %}
    - catalog: "{{ t.catalog }}"
      schema: "{{ t.schema }}"
      table: "{{ t.table }}"
      checks:
        row_count:
          min_rows: {{ t.min_rows | default(1) }}
          severity: critical
{% endfor %}
```

**2. Register**

```yaml
# neco_dq/config/template_registry.yaml

silver_standard: config/silver/silver_standard_template.yaml
ge_standard: config/ge/ge_standard_template.yaml
bronze_minimal: config/bronze/bronze_minimal_template.yaml  # NEW
```

**3. Invoke**

```python
config = resolve_config("bronze_minimal")
```

---

## Determinism Requirement

Templates must produce deterministic output.

**Forbidden:**

- Randomness
- Time-dependent behavior (unless explicit in context)
- External API calls during rendering
- Filesystem reads beyond template file

**Allowed:**

- Pure Jinja2 logic
- Context variable access
- Type checking and conditionals
- Filters and loops

---

## Template Design Rules

### 1. Generic Structure

Templates define structure, not business logic.

```yaml
# GOOD: Generic
{% for t in tables %}
  - table: "{{ t.name }}"
    check: row_count
    min: {{ t.threshold }}
{% endfor %}

# BAD: Hardcoded business logic
{% for t in tables %}
  - table: "{{ t.name }}"
    check: value_range_validation  # Too specific
{% endfor %}
```

### 2. Explicit Context

Document expected context variables.

```yaml
# Required context:
# - tables: list[dict]
#     - catalog: str
#     - schema: str
#     - table: str
#     - pk_id: str | list[str]
#     - status_column: str (optional)
```

### 3. Fail-Safe Defaults

Provide sensible defaults where possible.

```yaml
severity: {{ severity | default("error") }}
blocking: {{ blocking | default(true) }}
```

### 4. Readable Output

Generated YAML should be human-readable.

Use proper indentation and structure.

---

## Troubleshooting

### Template Not Found

```
KeyError: Template 'unknown' not found. Available: ['silver_standard', 'ge_standard']
```

**Solution:** Check `template_registry.yaml` for correct tag.

### Jinja2 Syntax Error

```
jinja2.exceptions.TemplateSyntaxError: unexpected '}'
```

**Solution:** Validate Jinja2 syntax. Common issues:

- Missing `{% endif %}`
- Unmatched braces `{{ }}`
- Invalid variable names

### Undefined Variable

```
jinja2.exceptions.UndefinedError: 'pk_id' is undefined
```

**Solution:** Ensure context provides required variables.

Use `{% if variable is defined %}` for optional fields.

### Invalid YAML Output

```
yaml.scanner.ScannerError: mapping values are not allowed here
```

**Solution:** Template generated invalid YAML structure.

Validate Jinja2 indentation and syntax.

---

## See Also

- [Modular Config](modular-config.md) - Directory-based configuration
- [Single-File Config](single-file.md) - Single-file configuration
