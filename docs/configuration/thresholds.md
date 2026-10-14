# Thresholds

Many row-level checks support a `threshold` configuration that tolerates a certain
number or percentage of failing rows before the check itself fails. Thresholds work
the same way across the Bronze, Silver and Gold suites, so they're documented once
here. Each suite page lists *which* of its checks honour a threshold.

## How it works

```yaml
threshold:
  max_failure_percent: 5.0    # Allow up to 5% of rows to fail
  max_failure_count: 100      # Allow up to 100 rows to fail
```

**Evaluation logic:**

- If both parameters are specified, **both** must be satisfied (AND logic).
- If neither is specified, zero tolerance applies (the original strict behaviour).
- A threshold can be defined at the check level or as a suite-level default.

## Where it applies

Not every check is threshold-based. Row-level checks that count failing rows support
`threshold`; checks with their own tolerance semantics use a dedicated parameter
instead (e.g. `min_valid_percent`, `tolerance_abs`, `tolerance_percent`, `max_delay_hours`)
or are binary pass/fail. See the per-suite tables:

- [Bronze - supported checks](../test-suites/bronze.md#threshold-configuration)
- [Silver - supported checks](../test-suites/silver.md#threshold-configuration)
- [Gold - supported checks](../test-suites/gold.md#threshold-configuration)

!!! tip "Threshold vs check-specific tolerance"
    If a check doesn't support `threshold`, it usually has a purpose-built tolerance
    knob documented in its Check Details entry. Look there before assuming a check is
    strict zero-tolerance.

The `threshold` keys themselves (`max_failure_percent`, `max_failure_count`) are
unchanged. For how the check-specific tolerance and column keys are named, and the
deprecated spellings they replace, see
[Parameter Naming Conventions](naming-conventions.md).
