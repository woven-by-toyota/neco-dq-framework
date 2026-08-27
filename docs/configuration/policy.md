# Failure Policy

The **policy engine** decides the final outcome of a run: it looks at every check
result and returns a single exit code. It runs strictly **after** all suites have
executed, never during, so suites only produce results and the policy alone decides
consequences.

Policy is configured under `dq_policy` (in `global/policy.yaml` for modular configs,
or at the top level of a single-file config).

## The four knobs, and how they interact

Four separate settings decide whether a failing check fails the pipeline or only
warns. They are easy to confuse, so here is what each one actually does:

| Setting | Where | Default | Effect |
|---------|-------|---------|--------|
| `blocking` | per **check** | `false` | If a check with `blocking: true` fails, the run fails (exit 20). A failing non-blocking check only warns. |
| `severity` | per **check** | `warning` | Label (`critical` / `error` / `warning` / `info`) shown in the summary. It does **not** by itself decide the exit code, it describes the check. |
| `block_test_suites.<suite>` | per **suite** (policy) | `true` | Whether a suite is *allowed* to block at all. Set a suite to `false` and its blocking failures are downgraded to warnings. |
| `block_on_threshold` + `failure_threshold` | global (policy) | `false` / none | If enabled, a global failure rate above the threshold escalates warnings to a failure. |

!!! warning "`blocking` defaults to `false`"
    Every check is non-blocking unless you set `blocking: true`. There is no suite
    that makes its checks blocking automatically, not Silver, not Gold. The common
    convention is to set correctness checks (Silver/Gold) to `blocking: true` and
    leave Bronze ingestion checks as warnings, but that is your choice in config,
    not a framework default.

## How the exit code is decided

The engine evaluates results in this order (see `dq_failure_policy.py`):

1. **Any blocking failure in a suite that is allowed to block** → `FAIL` (exit 20).
   This wins over everything else.
2. Otherwise, **any non-blocking failure with severity `warning`, `error` or
   `critical`** → `WARN` (exit 10). Failures whose only severity is `info` do not
   raise a warning. If `block_on_threshold: true` and the global failure rate
   exceeds `failure_threshold`, this escalates to `FAIL` (exit 20) instead.
3. Otherwise → `SUCCESS` (exit 0).

### Exit codes

| Exit code | Status | Meaning |
|-----------|--------|---------|
| `0` | SUCCESS | All checks passed |
| `10` | WARN | Failures occurred, none blocking |
| `20` | FAIL | At least one blocking failure (or threshold escalation) |
| `99` | SYSTEM_ERROR | Invalid configuration or a malformed summary |

## Configuration

```yaml
dq_policy:
  # Optional: informational failure-rate threshold (0.0 to 1.0).
  # Shown in the summary when exceeded. On its own it does not change pass/fail.
  failure_threshold: 0.1

  # Optional: when true, a global failure rate above failure_threshold
  # turns WARN into FAIL.
  block_on_threshold: false

  # Which suites are allowed to block the pipeline.
  block_test_suites:
    profiling: false
    bronze: true
    silver: true
    gold: true
    great_expectations: true

  alerts:
    slack:
      enabled: true
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `failure_threshold` | float | none | Informational threshold (0.0 to 1.0), shown in the summary when the global failure rate exceeds it. |
| `block_on_threshold` | bool | `false` | When `true`, a global failure rate above `failure_threshold` escalates WARN to FAIL. |
| `block_test_suites.<suite>` | bool | `true` | Whether blocking failures in that suite fail the pipeline. Set to `false` to make a suite warning-only. |
| `alerts.slack.enabled` | bool | `false` | Send a Slack summary. See [Slack Alerts](slack-alerts.md). |

!!! note "Threshold is measured globally"
    `failure_threshold` compares total failed checks against total checks across
    **all** suites, not per suite.

## Per-environment overrides

Policy values can differ per environment through profiles, so dev can be
warning-only while prod blocks. See [Policy Overrides](profiles.md#policy-overrides).

## Related

- [Getting Started](../getting-started.md) shows the exit codes in a real run summary.
- [Architecture → Policy Engine](../architecture/overview.md#6-policy-engine) covers where the engine sits in the flow.
