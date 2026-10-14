# Slack Alerts

Send DQ results to Slack channels.

---

## Quick Start

### 1. Get Webhook URL

### 2. Configure Webhook

**Option A: Databricks Secrets (recommended)**

```bash
databricks secrets create-scope dq-alerts
databricks secrets put-secret dq-alerts slack_webhook_url --string-value "https://hooks.slack.com/services/XXX/YYY/ZZZ"
```

These exact names (`dq-alerts` / `slack_webhook_url`) are what the framework
reads — creating the secret is all that is needed, with no code or task wiring
of your own. See [Webhook Resolution](#webhook-resolution).

**Option B: Environment Variable**

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXX/YYY/ZZZ"
```

For notebooks and local runs only — not reliable on Databricks jobs, and unusable
on serverless tasks (see [Webhook Resolution](#webhook-resolution)). Use the
secret there.

### 3. Enable in Config

```yaml
dq_policy:
  alerts:
    slack:
      enabled: true
```

---

## Configuration Options

```yaml
dq_policy:
  alerts:
    slack:
      enabled: true
      send_always: false       # Send on PASS/WARN too (default: only FAIL)
      mention_on_fail: false   # Add @channel on FAIL
      include_job_link: true   # Include Databricks job URL
      max_failures_shown: 5    # Max items in Top Failures
      compact_mode: false      # Single-line summary
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `false` | Enable Slack alerts |
| `send_always` | bool | `false` | Send on PASS and WARN (not just FAIL) |
| `mention_on_fail` | bool | `false` | Add `@channel` mention on FAIL |
| `include_job_link` | bool | `true` | Include Databricks job URL (if available) |
| `max_failures_shown` | int | `5` | Number of failures shown in Top Failures |
| `compact_mode` | bool | `false` | Use single-line compact format |

---

## Message Format

### Standard Format

```
*Data Quality Summary*
`prod` • `staging` • 2026-06-12 15:30 UTC

*Status:* *FAIL* (3 blocking)
*Success Rate:* 90.2% (55/61 checks)  •  Threshold: 10%

bronze         🟨    10/11
silver         🟩    15/15
gold           🟥    30/35

*Top Failures:*
• `freshness` → gold.orders (updated_at) (x4)
• `referential_integrity` → gold.orders (customer_id) (x2)

🔗 View Job
```

The context line shows the active profile and the detected environment. The
profile comes from the `profile` argument; the environment is auto-detected
from the Databricks workspace URL. Each is shown only when known, so the line
may carry one, both, or neither.

Each Top Failures line pairs a check with the table it actually failed on,
followed by the affected columns and the failure count (`(x4)`).

**Elements:**

| Element | When Shown |
|---------|------------|
| Profile name | If profile is set and not "default" |
| Environment | If auto-detected from the Databricks workspace (not "unknown") |
| Threshold | If `failure_threshold` is configured |
| Top Failures | If there are failures |
| Job link | If running on Databricks and `include_job_link: true` |

### Compact Format

With `compact_mode: true`:

```
*DQ FAIL* | `qa` | 2026-06-12 15:30 UTC
Success: *90.2%* (55/61) | *3 blocking*
```

---

## Status Colors

Messages have a colored sidebar based on status:

| Status | Color | Emoji |
|--------|-------|-------|
| FAIL | Red | 🟥 |
| WARN | Orange | 🟨 |
| PASS | Green | 🟩 |

---

## When Alerts Are Sent

| Status | `send_always: false` | `send_always: true` |
|--------|---------------------|---------------------|
| FAIL | sent | sent |
| WARN | not sent | sent |
| PASS | not sent | sent |

---

## Environment Detection

The environment shown in the context line is detected from the Databricks
workspace URL: a host containing `dev`, `stage`/`stg`, or `prod` maps to `dev`,
`staging`, or `prod`. Outside Databricks, the framework reads the
`DQ_ENVIRONMENT` variable. If neither resolves, the environment is `unknown`
and is omitted from the message.

---

## Webhook Resolution

The framework looks for the webhook URL in this order:

1. **Databricks secret** (`scope: dq-alerts`, `key: slack_webhook_url`)
2. **Environment variable** `SLACK_WEBHOOK_URL`

The scope and key names are **fixed** — the framework reads exactly `dq-alerts` /
`slack_webhook_url` and this is not configurable. Create the secret under those
names and you are done: the framework resolves it itself at runtime via
`dbutils`, so the webhook never has to pass through your run script, your task
parameters, or any wiring of your own. A scope under any other name will not be
found.

On Databricks jobs, prefer the secret over the environment variable. A
`{{secrets/scope/key}}` reference does **not** resolve inside task parameters,
and a serverless task has no cluster environment variables in which to set
`SLACK_WEBHOOK_URL` — so the secret is the only path that works there. The
environment variable is meant for notebooks and local runs.

The secret lookup is best-effort: if the secret cannot be read — for example the
scope exists but the job lacks permission on it — the framework silently falls
back to the environment variable and logs the reason only at debug level. So a
misconfigured secret looks the same as no secret at all.

If neither is found, alerts are skipped with a warning:
```
Slack webhook URL not configured. Skipping alerts.
```

---

## Examples

### Alerts on Failures Only (Default)

```yaml
dq_policy:
  alerts:
    slack:
      enabled: true
```

### Always Send with @channel on Failure

```yaml
dq_policy:
  alerts:
    slack:
      enabled: true
      send_always: true
      mention_on_fail: true
```

### Compact Mode for High-Frequency Jobs

```yaml
dq_policy:
  alerts:
    slack:
      enabled: true
      compact_mode: true
      max_failures_shown: 3
```

### With Profiles

Different Slack behavior per environment:

```yaml
# profiles.yml
profiles:
  dev:
    policy:
      alerts:
        slack:
          enabled: false  # No alerts in dev

  prod:
    policy:
      alerts:
        slack:
          enabled: true
          send_always: true
          mention_on_fail: true
```

---

## Troubleshooting

### "Slack webhook URL not configured"

- Check Databricks secret exists: `databricks secrets list dq-alerts`
- Confirm the exact names: scope `dq-alerts`, key `slack_webhook_url` (both are fixed)
- Check env var is set: `echo $SLACK_WEBHOOK_URL`

### Secret exists but alerts still skip

- The job may lack read permission on the `dq-alerts` scope. The framework falls
  back to the env var on any read failure and logs the reason only at debug
  level, so a permission problem is invisible at the default log level. Check the
  scope ACLs, or raise the log level to see the underlying error.

### Message Not Appearing

- Verify webhook URL is valid (test with curl)
- Check `enabled: true` in config
- For PASS/WARN, ensure `send_always: true`

### No Job Link

- Job link only appears when running as a Databricks job
- Running in notebook or locally: no job link

---

## See Also

- [Modular Config](modular-config.md) - Policy configuration
- [Profiles](profiles.md) - Per-environment alert settings
