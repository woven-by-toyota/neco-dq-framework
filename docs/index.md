---
hide:
  - toc
---

<div style="text-align: center; margin: 1.5rem 0;">
  <img src="assets/logo.svg" alt="neco dq framework" width="480">
</div>

<span class="read-time">3 MIN READ</span>
<span class="page-subtitle">Data Quality validation framework for Databricks</span>

---

Neco DQ is a declarative data quality framework for Databricks, built on Spark and
organized around the medallion architecture. You define checks in YAML, one set per
layer, and the framework runs them, gates your pipeline on the result, and stores the
history. The same configuration runs unchanged in a notebook, a scheduled Job, or a DLT pipeline.

<div class="page-info" markdown>
<span class="page-info__title">Main features</span>

- **~35 built-in checks** - medallion-aware: Bronze ingestion, Silver correctness, Gold reconciliation.
- **Policy engine** - decides what blocks the pipeline vs what only warns, per suite and severity.
- **Great Expectations, integrated** - run GE column contracts alongside native checks, in one report.
- **Custom suites** - write your own checks in Python when the built-ins don't cover a rule.
- **Multi-environment profiles** - one config targets dev, staging and prod via profile variables.
- **Slack alerts** - send a summary of failures and warnings to a Slack channel per run.
- **Dashboard generation** - build a Databricks AI/BI (Lakeview) dashboard on the DQ history table.
- **One report, one history** - unified summary plus a Delta history table for trend analysis.

</div>

<p class="qa-section-label">Philosophy</p>

Every team has a different definition of "bad data". A missing field might be a hard
pipeline blocker for one team and a non-critical warning for another. Neco DQ is built
around that reality: the framework provides the checks, you decide what each one means.

Every check exposes three independent controls:

| Parameter | What it controls |
|-----------|-----------------|
| `severity` | How the failure is classified (`critical`, `error`, `warning`, `info`) |
| `blocking` | Whether a failure stops the pipeline (`exit_code=20`) or only warns (`exit_code=10`) |
| `enabled` | Whether the check runs at all |

On top of that, checks can be tagged and runs can be filtered by tag, so the same config
can serve a full nightly run, a quick smoke test, and an on-demand critical-only check,
without duplicating YAML.

**Define once, adopt across teams.** The checks, the execution engine, and the result
contract live in one place and are maintained once. A team doesn't write or maintain data
quality code, it writes a YAML config for its own tables and owns that config. Add a second
team, or a tenth, and none of them reimplements freshness, uniqueness, or reconciliation
logic: they reuse the same checks and inherit every fix and new capability. The effort of
maintaining the framework is paid once, not once per team, which is where a shared platform
pays off over data quality scripts copied from one pipeline to the next.

**Data privacy by design.** The framework reads your data to run checks, but never stores
it. Every check computes statistics, counts, and aggregates on the Spark DataFrame and
only the results (numbers, pass/fail status, query templates) are persisted to Delta.
Row-level values never leave your environment.

This extends naturally to multi-environment deployments through profiles. You define a
single suite and a single set of checks, then let each profile (dev, staging, prod)
override the values that differ per environment: catalog names, thresholds, blocking
flags, or entire policy sections, without duplicating config. The same `freshness` check
can tolerate a 48-hour delay in dev and enforce a strict 2-hour window in production, all
from one YAML file.

<p class="qa-section-label">Get started</p>

<div class="qa-grid">

<a class="qa-card" href="getting-started/">
<span class="qa-card__title">Getting Started</span>
<span class="qa-card__desc">Install, configure, and run your first data quality validation</span>
</a>

<a class="qa-card" href="installation-deployment/">
<span class="qa-card__title">Installation</span>
<span class="qa-card__desc">Deploy the framework on Databricks, configure the wheel, and set up result storage</span>
</a>

</div>

<p class="qa-section-label">Test suites</p>

<div class="qa-grid">

<a class="qa-card qa-card--bronze" href="test-suites/bronze/">
<span class="qa-card__title">Bronze</span>
<span class="qa-card__desc">Schema validation, null checks, and basic data integrity rules</span>
</a>

<a class="qa-card qa-card--silver" href="test-suites/silver/">
<span class="qa-card__title">Silver</span>
<span class="qa-card__desc">Business rule validation and referential integrity checks</span>
</a>

<a class="qa-card qa-card--gold" href="test-suites/gold/">
<span class="qa-card__title">Gold</span>
<span class="qa-card__desc">Aggregation accuracy, KPI validation, and cross-dataset consistency</span>
</a>

<a class="qa-card qa-card--secondary" href="test-suites/profiling/">
<span class="qa-card__title">Profiling</span>
<span class="qa-card__desc">Statistical profiling and data distribution analysis</span>
</a>

<a class="qa-card qa-card--secondary" href="test-suites/custom/">
<span class="qa-card__title">Custom Suites</span>
<span class="qa-card__desc">Write your own data quality rules beyond the standard tiers</span>
</a>

<a class="qa-card qa-card--secondary" href="test-suites/great-expectations/">
<span class="qa-card__title">Great Expectations</span>
<span class="qa-card__desc">Native Great Expectations integration for expectation-based validation</span>
</a>

</div>

<p class="qa-section-label">Guides</p>

<div class="qa-grid">

<a class="qa-card qa-card--secondary" href="configuration/database-setup/">
<span class="qa-card__title">Database Setup</span>
<span class="qa-card__desc">Configure catalog, schema, and table connections</span>
</a>

<a class="qa-card qa-card--secondary" href="configuration/modular-config/">
<span class="qa-card__title">Modular Config</span>
<span class="qa-card__desc">Split and compose configuration files across environments</span>
</a>

<a class="qa-card qa-card--secondary" href="configuration/run-dq-api/">
<span class="qa-card__title">Running DQ</span>
<span class="qa-card__desc">Run validations with run_dq(): suites, selectors, tags, and profiles</span>
</a>

<a class="qa-card qa-card--secondary" href="configuration/data-filtering/">
<span class="qa-card__title">Data Filtering</span>
<span class="qa-card__desc">Scope validation to specific partitions, dates, or row subsets</span>
</a>

<a class="qa-card qa-card--secondary" href="configuration/profiles/">
<span class="qa-card__title">Profiles</span>
<span class="qa-card__desc">Multi-environment configuration with dev, staging, and prod profiles</span>
</a>

</div>
