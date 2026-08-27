# Governance and Architecture

This document defines component ownership, boundaries, and architectural decisions for the Neco DQ Framework.

## Scope Definition

**neco_dq** is a Data Quality validation platform for Spark/Databricks environments.

| Responsibility | In Scope | Out of Scope |
|----------------|----------|--------------|
| **Data Validation** | Schema, completeness, consistency, freshness | Data transformation, ETL |
| **Configuration** | YAML parsing, template expansion, modular configs | Pipeline orchestration |
| **Execution** | Test suite orchestration, result collection | Job scheduling |
| **Reporting** | Summary generation, Slack alerts | Dashboards, BI tools |
| **Integration** | Great Expectations, Databricks | Other DQ tools (dbt tests, Soda) |

## Component Ownership Matrix

```
neco_dq/
├── config/                    # Configuration Resolution
│   ├── config_resolver.py     │ YAML parsing, template expansion
│   └── templates/             │ Jinja2 templates for config generation
│
├── core/                      # Core Engine
│   ├── orchestrators/         │ Test suite coordination
│   ├── policy/                │ Failure policy evaluation
│   ├── summary/               │ Result aggregation
│   ├── alerts/                │ Notification channels
│   ├── io/                    │ Result persistence
│   ├── metrics/               │ Observed/Expected/Operators
│   └── registry/              │ Suite registration
│
├── test_suites/               # Validation Logic
│   ├── base_test_suite.py     │ Abstract base class
│   ├── shared_checks.py       │ Mixin for common checks
│   ├── bronze/                │ Raw ingestion validation
│   ├── silver/                │ Transformation correctness
│   ├── gold/                  │ Aggregation validation
│   ├── ge/                    │ Great Expectations integration
│   └── profiling/             │ Auto-suggestion engine
│
└── runner.py                  # Entry Point
```

## Boundary Definitions

| Component | Owns | Does NOT Own |
|-----------|------|--------------|
| **Orchestrator** | Lifecycle, ordering, coordination | Metric computation, business logic |
| **Config Resolver** | YAML parsing, template expansion | Check execution, policy decisions |
| **Test Suites** | Check implementation, validation logic | Alerts, file I/O, cross-suite calls |
| **Policy** | Pass/fail decisions, exit codes | Metric computation, result formatting |
| **Summary** | Aggregation, formatting | Policy decisions, alerts |
| **Metrics** | Observed values, expected resolution, comparison | Suite-specific logic |

## Integration Points

| External System | Integration Type | Managed by |
|-----------------|------------------|------------|
| Databricks Unity Catalog | Read (tables) | Framework |
| Great Expectations | Delegated checks | Framework |
| Slack | Alerts | Framework |
| Delta Lake | Result persistence | Framework |
| Airflow/Databricks Workflows | Job scheduling | User |

---

## Solution Architecture

For detailed architecture documentation including system components, execution flow diagrams, and design principles, see [Architecture Overview](../architecture/overview.md).

---

## Mandatory Design Patterns

| Pattern | Where Applied | Rationale |
|---------|---------------|-----------|
| **Template Method** | `DQTestSuite.validate_table()` | Consistent execution flow |
| **Registry** | `test_suite_registry.py` | Decoupled suite loading |
| **Strategy** | `core/metrics/` | Pluggable metric types |
| **Mixin** | `SharedChecksMixin` | Code reuse without inheritance issues |
| **Result Contract** | All check methods | Consistent downstream processing |

## Mandatory Libraries & Frameworks

| Category | Required | Version | Rationale |
|----------|----------|---------|-----------|
| **Data Processing** | PySpark | >= 3.5.0 | Databricks standard |
| **Configuration** | PyYAML | >= 6.0 | Industry standard |
| **Templating** | Jinja2 | >= 3.1 | Mature, secure |
| **DQ Integration** | Great Expectations | >= 1.9.0 | Enterprise DQ |
| **Expression Eval** | asteval | >= 0.9.30 | Safe Python expressions |
| **Data Analysis** | pandas | >= 2.0, < 3.0 | PySpark compatibility |

## Prohibited Technologies

| Technology | Reason | Alternative |
|------------|--------|-------------|
| `eval()` / `exec()` | Security risk (code injection) | `asteval` library |
| `pickle` for config | Security risk (arbitrary code execution) | YAML |
| `subprocess` with shell=True | Command injection risk | Direct subprocess calls |
| Random behavior | Non-deterministic results | Explicit seeding if needed |
| Time-dependent defaults | Non-reproducible behavior | Explicit configuration |
| Global mutable state | Thread safety issues | Instance attributes |
| `spark.sql()` for simple ops | Less readable, harder to test | DataFrame API |
| Hardcoded credentials | Security risk | Environment variables |
| `print()` for logging | No log levels, hard to filter | `logging` module |
| pandas 3.x | PySpark compatibility issues | pandas >= 2.0, < 3.0 |

## Anti-Patterns

These patterns are **strictly forbidden** and will be rejected in code review:

1. **Suite reading YAML** - Configuration must be resolved before reaching suites
2. **Suite calling alerts** - Alerts are post-execution, orchestrator responsibility
3. **Suite invoking another suite** - Suites are independent plugins
4. **Policy logic inside metrics** - Metrics compute, policy evaluates
5. **Hardcoded constants replacing config** - All behavior via configuration
6. **Special cases in orchestrator** - Orchestrator is pure coordinator
7. **Business-specific naming** - All checks must be generic and reusable
8. **Cross-layer dependencies** - Each layer owns its responsibilities

---

## Determinism Requirement

neco_dq guarantees deterministic execution:

```
Same input + Same config = Same output
```

This means:

- **No randomness** - Results must be reproducible
- **No time-dependent behavior** - Unless explicitly configured (e.g., freshness checks)
- **No external dependencies that can change** - All inputs must be controlled
- **Reproducible results** - For audit, debugging, and regression testing

Any violation of determinism is a bug and must be fixed.

---

## Definition of Done

A change is acceptable only if:

- [ ] Registry works (suites load correctly)
- [ ] Result contracts unchanged (or explicitly versioned)
- [ ] Summary can read results
- [ ] Policies are enforceable
- [ ] No cross-layer violations
- [ ] Behavior is reproducible

---


