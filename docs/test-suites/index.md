# Test Suites Overview

Neco DQ groups its checks into **suites**. Each suite targets a different stage of
the medallion architecture and answers a different question about your data. This
page helps you pick the right one before diving into its (longer) reference page.

## Which suite do I need?

| Suite | Validates | Answers the question | Typical checks | Typical use |
|-------|-----------|----------------------|----------------|-------------|
| [**Bronze**](bronze.md) | Raw data right after ingestion | *"Did the data arrive, intact and fresh?"* | `record_count`, `freshness`, `schema_drift`, `mandatory_fields`, binary payloads | Often kept as warnings |
| [**Silver**](silver.md) | Cleaned/transformed data | *"Is the data correct, consistent and referentially sound?"* | `referential_integrity`, `business_rules`, `cardinality`, `compare_metric`, `cross_table_equality` | Usually set to block |
| [**Gold**](gold.md) | Aggregated business metrics | *"Do the KPIs reconcile and meet expectations?"* | `reconciliation_expr`, `compare_metric`, `coverage`, `trend`, `volume` | Usually set to block |
| [**Profiling**](profiling.md) | Any table (exploratory) | *"What does this data look like, and what should I check?"* | stats + auto-suggested checks | Non-blocking by design |
| [**Great Expectations**](great-expectations.md) | Column/schema contracts | *"Does each column honour its declared contract?"* | `not_null`, `unique`, `dtype`, `values_set`, `date_format` | Depends on config |
| [**Custom**](custom.md) | Anything the built-ins don't | *"How do I express a rule specific to my domain?"* | your own Python checks | Depends on config |

!!! note "\"Typical use\" is a convention, not a default"
    Whether a failing check fails the run or only warns is set per check with
    `blocking` (default `false`), governed by the [Failure Policy](../configuration/policy.md).
    The column above reflects how teams usually configure each suite, not a
    built-in behaviour.

## How to choose

- **Following the medallion flow?** Use Bronze → Silver → Gold on the matching
  layers. That's the common path and the three integrate cleanly (Gold checks can
  reconcile back to Silver).
- **New table, not sure what to check?** Start with **Profiling** in `suggest`
  mode: it inspects the data and proposes a starting config you then curate.
- **You think in column contracts** (a column must be non-null, an enum, a date in
  a given format)? **Great Expectations** is the most natural fit.
- **The rule is domain-specific** and no built-in check expresses it? Write a
  **Custom** suite.

!!! tip "Suites are independent"
    You don't have to use all of them. A config can enable just Bronze, or mix
    Silver + Great Expectations, whatever matches your tables. Every suite writes
    to the same result contract, one report, and one history table.

## Shared concepts

These apply across the medallion suites (Bronze/Silver/Gold), so they're
documented once:

- [**Thresholds**](../configuration/thresholds.md): allow a tolerated number or
  percentage of failing rows before a check fails.
- [**Compare Metric**](../configuration/compare-metric.md): the observed/expected
  comparison engine used by many Silver and Gold checks.
- [**Data Filtering**](../configuration/data-filtering.md) and
  [**Tag Filtering**](../configuration/tags-filtering.md): scope which rows and
  which checks run.
- [**Failure Policy**](../configuration/policy.md): how `blocking`, `severity` and
  the policy engine turn check results into a pass/warn/fail exit code.

## Next step

Pick a suite from the table above, or read
[Modular Configuration](../configuration/modular-config.md) to see how suites,
tables and checks fit together in a config tree.
