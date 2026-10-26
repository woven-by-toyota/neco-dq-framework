<p align="center">
  <img src="docs/assets/logo.svg" alt="neco dq framework" width="600">
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://woven-by-toyota.github.io/neco-dq-framework/"><img src="https://img.shields.io/badge/docs-online-1c1c1c.svg" alt="Documentation"></a>
</p>

<p align="center">
  <a href="https://woven-by-toyota.github.io/neco-dq-framework/getting-started/">Getting Started</a> •
  <a href="https://woven-by-toyota.github.io/neco-dq-framework/contributing/changelog/">Changelog</a>
</p>

---

**Neco DQ** is a declarative data quality framework for Databricks, built on Spark and organized around the medallion architecture. You define checks in YAML, one set per layer, and the framework runs them, gates your pipeline on the result, and stores the history.

Neco DQ keeps the setup declarative: ~35 built-in checks covering ingestion, transformation, aggregation, and schema contracts, a policy engine that decides what blocks and what only warns, and [Great Expectations](https://greatexpectations.io/) when you need it. All in one report, one history table, one Slack alert. The same `run_dq()` call runs in a notebook, a scheduled Job, or a DLT pipeline.

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
─────────────────────────────────────────────────────────────────────────
DQ POLICY RESULT → exit_code=20 | DQ completed with ERRORS
```

## Why Neco DQ

| | Neco DQ | Other tools |
|---|---|---|
| Medallion-aware suites | yes | no |
| Pipeline gating (exit codes) | yes | partial |
| Multi-env profiles | yes | partial |
| Auto-profiling + config suggestion | yes | no |
| GE integration, unified report | yes | no |
| One-call dashboard generation (Lakeview) | yes | no |
| Pure YAML config, no Python required | yes | varies |
| Databricks-native (Unity Catalog, Volumes) | yes | partial |

## What it does

- **Layer-aware checks.** Bronze, Silver, and Gold suites, each with checks built for that stage. Around 35 checks in total, from `freshness` and `schema_drift` to `referential_integrity`, `reconciliation_expr`, and `trend`, plus a general-purpose `compare_metric` check for anything the named ones don't cover.
- **YAML configuration.** A single file for small setups, or a modular directory that splits policy, suites, and per-table groups as the config grows.
- **Pipeline gating.** A policy engine turns results into an exit code. Every check has a severity and a blocking flag, so you decide what stops a pipeline and what only warns. There's also a global failure-rate threshold.
- **Multi-environment profiles.** Define `dev`, `staging`, and `prod` profiles with their own catalogs, variables, and policy overrides. Configs reference them through Jinja2 (`{{ var.name }}`), so the same config runs everywhere.
- **Result history and alerts.** Results persist to Delta tables for tracking over time, and a Slack integration sends a run summary.
- **Dashboard generation.** One call builds a Databricks AI/BI (Lakeview) dashboard on top of the history table, with KPIs, coverage, a dimension matrix, regression and trend widgets, from a bundled template.
- **Runs anywhere on Databricks.** The same `run_dq()` call works in a notebook, a scheduled Job, or a DLT pipeline.
- **Auto-profiling.** Point it at a table and it profiles the data, flags things like high-null columns or possible PII, and suggests a starting config.
- **Custom suites.** Write your own checks in plain PySpark and plug them into the framework as a first-class suite. Custom checks go through the same policy engine, result persistence, and alerting as built-in ones. No glue code required.
- **Built-in debug experience.** When a check fails, the framework automatically generates an investigation query you can run directly in Databricks to inspect the offending rows, no manual digging. Each failure result includes the SQL, the failure statistics, and an execution trace so you know exactly what happened and where to look.
- **Tag-based filtering.** Tag any check with one or more labels and filter at runtime with `tags=["critical"]`. The same config can drive a full nightly run, a quick smoke test, and an on-demand critical-only check without changing a single YAML file.
- **Extensible.** Skip whole suites from the command line, filter by table, and compose configs with Jinja2 templates.
- **Great Expectations integration.** When native checks aren't enough, the framework integrates [Great Expectations](https://greatexpectations.io/). The GE suite adds a wide expectation catalogue: not-null, uniqueness, value ranges, regex patterns, value sets, row-count bounds, column-pair rules, and strict schema contracts that suit producer-consumer handoffs. GE results flow through the same policy engine, Delta persistence, and Slack alerting as every other check, so you get one unified report rather than two separate tools to reconcile.

## Philosophy

Every team has a different definition of "bad data". A missing field might be a hard pipeline blocker for one team and a non-critical warning for another. Neco DQ is built around that reality: the framework provides the checks, you decide what each one means.

Every check exposes three independent controls:

| Parameter | What it controls |
|-----------|-----------------|
| `severity` | How the failure is classified (`critical`, `error`, `warning`, `info`) |
| `blocking` | Whether a failure stops the pipeline (`exit_code=20`) or only warns (`exit_code=10`) |
| `enabled` | Whether the check runs at all |

On top of that, checks can be tagged and runs can be filtered by tag, so the same config can serve a full nightly run, a quick smoke test, and an on-demand critical-only check, without duplicating YAML.

**Data privacy by design.** The framework reads your data to run checks, but never stores it. Every check computes statistics, counts, and aggregates on the Spark DataFrame and only the results (numbers, pass/fail status, query templates) are persisted to Delta. Row-level values never leave your environment.

This extends naturally to multi-environment deployments through **profiles**. You define a single suite and a single set of checks, then let each profile (`dev`, `staging`, `prod`) override the values that differ per environment: catalog names, thresholds, blocking flags, or entire policy sections, without duplicating config. The same `freshness` check can tolerate a 48-hour delay in dev and enforce a strict 2-hour window in production, all from one YAML file.

## Requirements

- A Databricks environment with Unity Catalog (the intended runtime)
- Python 3.11 or newer
- For local development and running the test suite: PySpark 3.5 or newer

## Installation

In a Databricks notebook or job:

```python
%pip install neco-dq-framework
```

You can also host the wheel on a Unity Catalog Volume, which is handy for pinning a version or working offline. Download it from PyPI once, upload the `.whl` to a Volume your workspace can read, then install from there:

```bash
pip download neco-dq-framework --no-deps -d ./wheelhouse
```

```python
%pip install /Volumes/<catalog>/<schema>/wheelhouse/neco_dq_framework-X.Y.Z-py3-none-any.whl
```

For local development (running the test suite, contributing), clone the repo and install from source. Requires [uv](https://docs.astral.sh/uv/getting-started/installation/):

```bash
git clone https://github.com/woven-by-toyota/neco-dq-framework.git
cd qa-nexus-dq-framework
uv sync --extra dev --frozen
```

All subsequent commands (`ruff`, `pytest`) should be prefixed with `uv run` to run inside the managed environment.

## Quick start

```python
from neco_dq.runner import run_dq

run_dq("/path/to/dq_config.yaml")
```

A minimal config:

```yaml
- catalog: my_catalog
  schema: silver
  table: events
  checks:
    idempotency:
      enabled: true          # enable/disable this check
      severity: critical     # critical | error | warning | info
      blocking: true         # if true, a failure raises exit_code=20
      tags: [daily, critical] # used to filter checks at runtime
      key_column: id         # column(s) that must be unique
```

`run_dq` returns a summary dict and accepts a few options for controlling a run:

```python
run_dq(
    "/path/to/config/",
    profile="prod",            # pick an environment profile
    run_suites=["silver"],     # or exclude_suites=[...]
    tags=["critical"],         # run only checks with this tag
)
```

The exit decision follows the policy: `0` when everything passes, `10` when only non-blocking checks fail, `20` when a blocking check fails or the failure rate crosses the threshold.

## Modular configuration

```
config/
├── global/
     ├── policy.yaml
     └── profiles.yaml
└── suites/
    ├── bronze/
    │   ├── suite.yaml
    │   └── test_groups/table_a.yaml
    ├── silver/
    │   ├── suite.yaml
    │   └── test_groups/table_b.yaml
    │   └── great_expectations/table_b.yaml       
    └── gold/
        ├── suite.yaml
        └── test_groups/table_c.yaml    
```

## Test suites

| Suite | What it validates | Example checks |
|-------|-------------------|----------------|
| **Bronze** | Raw ingestion | `record_count`, `record_count_anomaly`, `freshness`, `schema_drift`, `mandatory_fields`, `partition_exists`, plus binary-payload checks |
| **Silver** | Transformation correctness | `idempotency`, `referential_integrity`, `cardinality`, `status_transition`, `totals_consistency`, `cross_table_equality`, `temporal_consistency`, `flow_completeness`, `compare_metric` |
| **Gold** | Aggregation and reconciliation | `uniqueness`, `completeness`, `coverage`, `volume`, `trend`, `reconciliation_expr`, `cross_gold`, `compare_metric` |
| **Profiling** | Auto-suggest checks from data | Scans a table and understands its shape (null ratios, cardinality, value distributions, potential PII), then automatically suggests a ready-to-use DQ config tailored to that table. The fastest way to bootstrap quality checks on a new dataset |
| **Great Expectations** | Schema and data contracts | Built on [Great Expectations](https://greatexpectations.io/): not-null, uniqueness, ranges, regex, value sets, row-count bounds, column-pair rules. Results flow through the same policy, persistence, and alerting as native checks |

### The `compare_metric` check

Most checks answer a fixed question. `compare_metric` is the general-purpose one: you define a value to *observe* from the data and a value to *expect*, and it compares them. It's available in the Silver and Gold suites for the cases the named checks don't cover.

The observed value can be a count, an aggregate, a ratio, array statistics, a raw SQL result, or a ranking metric like Kendall's W or Borda count. The expected value can be a static number or pulled dynamically from another query or table. The two are compared with an operator such as `==`, `between`, `within_pct`, or `~` for approximate matches.

```yaml
compare_metric:
  enabled: true
  metric:
    type: aggregate
    function: sum
    column: amount
  expected_from:
    type: sql
    query: "SELECT SUM(amount) FROM {{ var.catalog }}.silver.orders"
  operator: within_pct
  tolerance: 0.5
```

## Development

This project needs Python 3.11 or newer. Check your interpreter with `python --version` before you start.

The quickest way to get up and running:

```bash
make test
```

This installs all dev dependencies (including PySpark) via `uv` and runs the full test suite.

For manual setup:

```bash
uv sync --extra dev --frozen

# Lint
uv run ruff check neco_dq/
uv run ruff format --check neco_dq/

# Run all tests
uv run pytest tests/ -v

# Run only unit tests (no Spark required)
uv run pytest tests/ -v -m "not spark"
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full contributor guide.

## Contributing

Contributions are welcome: bug reports, feature requests, new checks, or documentation improvements. See [CONTRIBUTING.md](CONTRIBUTING.md) to get started.

Created and maintained by **Claudio Basilico**. Originally developed at Toyota Motor Corporation.

## License

Apache 2.0. See [LICENSE](LICENSE).
