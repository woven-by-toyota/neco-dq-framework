# Roadmap

Where Neco DQ is heading. This page describes direction, not commitments. Items and their
order can change, and nothing here has a fixed release date.

!!! note "How to read this page"
    Every future item follows the same two rules the framework already lives by: it is
    **opt-in**, and it is **backward compatible**. A run that does not adopt a new feature
    behaves exactly as it does today. Where an item is still a proposal, its own page (or
    this section) says so.

---

## Horizons

| Horizon | Meaning |
|---------|---------|
| **Available today** | Shipped and documented. Safe to build on. |
| **Planned** | A design direction we intend to build. Details may still change. |
| **Exploring** | Early design under discussion. No commitment, no timeline. |

---

## Available today

The current release is a complete data quality framework. In short:

- Around 35 built-in checks across Bronze, Silver, and Gold.
- A policy engine that decides, per suite and severity, what blocks a pipeline and what only warns.
- Great Expectations column contracts running alongside native checks in one report.
- Custom Python suites for rules the built-ins do not cover.
- Multi-environment [profiles](configuration/profiles.md) so one config targets dev, staging, and prod.
- Slack alerts, a Lakeview dashboard, and a Delta history table for trends.

See the [home page](index.md) for the full feature list and [Getting Started](getting-started.md)
to run the framework.

---

## Planned

### Parameter naming consistency

Some parameters carry different names for the same idea across suites (for example, the column
a freshness check reads). The plan is to converge on one name per concept, keeping the old
names as accepted aliases so existing configs keep working without change.

---

## Exploring

These are under active design in the open. They are not scheduled, and the notes below describe
intent, not a shipping feature.

### Optional AI assistance

An optional, opt-in layer that helps teams author and understand data quality, without changing
how the engine runs. The design is built around a few fixed rules:

- **Off by default.** A team that does nothing gets no AI and no added cost. It turns on only
  through explicit configuration.
- **Privacy first.** Row-level values never leave the environment. The default target is a
  model running inside the workspace, and features that would touch data stay in-workspace
  unless a team explicitly opts into an external target.
- **Grounded, not trusted.** Anything the model generates (a suite config) is checked against
  the framework's own machine-readable capability description before it is used. Anything it
  explains (a failure) is advisory text, never a gate.

The candidate uses, roughly in order of how ready they are:

| Use | What it does |
|-----|--------------|
| Failure explanation | Turns failed-check metadata into a plain-language likely cause, attached to the summary. Safe metadata only. |
| Suite generation | Proposes a suite config from a table's profile or from a natural-language request, then validates it before writing. |
| Semantic checks | Runs classification or generation as an in-workspace SQL column, for rules that need to read text meaning. |
| Config repair | Given validation errors on a config, proposes a corrected one and re-checks it. |

### Portable execution beyond Databricks

Today the engine runs on Spark and Databricks. We are exploring a portable execution layer so
the same checks can run against other SQL engines, locally, or in CI, with credentials and
model access resolved per environment rather than assumed to be Databricks.

### Data contracts

A longer-term direction: schema-as-code, so a table's shape and expectations are declared once
and checked as a contract, with notification when a change would affect consumers. Still at the
design stage.

---

## Principles that will not change

Whatever lands, these hold:

- Existing runs keep working. New features are opt-in and backward compatible.
- Row-level values never leave your environment.
- The result contract stays stable, so history and dashboards remain comparable over time.
- AI, if adopted, is advisory or validated. It never gates a pipeline on unchecked output.

---

## Have input?

Priorities are shaped by real use. Open an issue or start a discussion on
[GitHub](https://github.com/woven-by-toyota/neco-dq-framework) with the checks, integrations,
or workflows you need.
