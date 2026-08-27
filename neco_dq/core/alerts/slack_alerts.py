# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from datetime import UTC, datetime
from typing import Any

import requests

try:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
except Exception:
    DBUtils = None
    SparkSession = None


logger = logging.getLogger("neco_dq")

STATUS_EMOJI = {
    "FAIL": "🟥",
    "WARN": "🟨",
    "PASS": "🟩",
}

STATUS_COLOR = {
    "FAIL": "#E53935",
    "WARN": "#FB8C00",
    "PASS": "#43A047",
}

SUITE_DISPLAY_MAP = {
    "great_expectations@bronze": "ge@bronze",
    "great_expectations@silver": "ge@silver",
    "great_expectations@gold": "ge@gold",
}


def _get_slack_webhook_url() -> str | None:
    if SparkSession and DBUtils:
        try:
            spark = SparkSession.getActiveSession()
            if spark:
                dbutils = DBUtils(spark)
                return dbutils.secrets.get(scope="dq-alerts", key="slack_webhook_url")
        except Exception:
            logger.debug("Unable to read Slack webhook from Databricks secrets.")

    return os.getenv("SLACK_WEBHOOK_URL")


def _get_job_url() -> str | None:
    if SparkSession and DBUtils:
        try:
            spark = SparkSession.getActiveSession()
            if spark:
                dbutils = DBUtils(spark)
                ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                browser_host = ctx.browserHostName().get()
                job_id = ctx.jobId().get()
                run_id = ctx.idInJob().get()
                return f"https://{browser_host}/jobs/{job_id}/runs/{run_id}"
        except Exception:
            logger.debug("Unable to build Databricks job URL from notebook context.", exc_info=True)
    return None


def _get_environment() -> str:
    if SparkSession and DBUtils:
        try:
            spark = SparkSession.getActiveSession()
            if spark:
                dbutils = DBUtils(spark)
                ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                workspace = ctx.browserHostName().get()
                if "dev" in workspace:
                    return "dev"
                if "stage" in workspace or "stg" in workspace:
                    return "staging"
                if "prod" in workspace:
                    return "prod"
        except Exception:
            logger.debug("Unable to detect environment from Databricks context.", exc_info=True)
    return os.getenv("DQ_ENVIRONMENT", "unknown")


def _format_top_failures(summary: dict[str, Any], max_failures: int = 5) -> list[str]:
    """Build the 'Top Failures' lines as accurate `check → suite.table` entries.

    Reads the summary's failure_details (real check↔table pairs). Falls back to
    issues_by_type (check only, no table) for summaries produced before
    failure_details existed, so it never invents check/table combinations.
    """
    failures = []

    for engine, s in summary.items():
        display_name = SUITE_DISPLAY_MAP.get(engine, engine)

        details = s.get("failure_details")
        if details:
            for d in details:
                failures.append(
                    {
                        "check": d.get("check", "unknown"),
                        "suite": display_name,
                        "table": d.get("table", ""),
                        "columns": d.get("columns", ""),
                        "count": d.get("count", 0),
                    }
                )
        else:
            # Fallback: no per-table linkage available, list the check only.
            for check_name, issue in s.get("issues_by_type", {}).items():
                failures.append(
                    {
                        "check": check_name,
                        "suite": display_name,
                        "table": "",
                        "columns": issue.get("columns", ""),
                        "count": issue.get("count", 0),
                    }
                )

    failures.sort(key=lambda x: x["count"], reverse=True)

    result = []
    for f in failures[:max_failures]:
        target = f"{f['suite']}.{f['table']}" if f["table"] else f["suite"]
        col_info = f" ({f['columns']})" if f["columns"] else ""
        result.append(f"• `{f['check']}` → {target}{col_info} (x{f['count']})")

    return result


def _compute_totals(summary: dict[str, Any]) -> tuple[dict[str, int], list[str]]:
    totals = {"checks": 0, "passed": 0, "failed": 0, "blocking": 0}
    suite_rows = []

    for engine, s in summary.items():
        status = s.get("status", "UNKNOWN")
        checks = s.get("total_checks", 0)
        passed = s.get("passed", 0)
        failed = s.get("failed", 0)
        blocking = s.get("blocking_failures", 0)

        totals["checks"] += checks
        totals["passed"] += passed
        totals["failed"] += failed
        totals["blocking"] += blocking

        display_name = SUITE_DISPLAY_MAP.get(engine, engine)
        emoji = STATUS_EMOJI.get(status, "❓")
        suite_rows.append(f"{display_name:<14} {emoji}  {passed:>4}/{checks:<4}")

    return totals, suite_rows


def _build_compact_message(
    totals: dict[str, int],
    overall_status: str,
    success_pct: float,
    context_str: str | None,
    timestamp: str,
    mention: str,
) -> str:
    context_part = f" | {context_str}" if context_str else ""
    text = (
        f"{mention}*DQ {overall_status}*{context_part} | {timestamp}\n"
        f"Success: *{success_pct:.1f}%* ({totals['passed']}/{totals['checks']})"
    )
    if totals["blocking"] > 0:
        text += f" | *{totals['blocking']} blocking*"
    return text


def _build_full_message(
    totals: dict[str, int],
    suite_rows: list[str],
    overall_status: str,
    success_pct: float,
    context_str: str | None,
    timestamp: str,
    mention: str,
    failure_threshold: float | None,
    summary: dict[str, Any],
    max_failures_shown: int,
) -> str:
    header = f"{mention}*Data Quality Summary*"
    meta = f"{context_str} • {timestamp}" if context_str else timestamp

    status_line = f"*Status:* *{overall_status}*"
    if totals["blocking"] > 0:
        status_line += f" ({totals['blocking']} blocking)"

    rate_line = f"*Success Rate:* {success_pct:.1f}% ({totals['passed']}/{totals['checks']} checks)"
    if failure_threshold is not None:
        rate_line += f"  •  Threshold: {failure_threshold * 100:.0f}%"

    suite_block = "```\n" + "\n".join(suite_rows) + "\n```"

    text = f"{header}\n{meta}\n\n{status_line}\n{rate_line}\n\n{suite_block}"

    if totals["failed"] > 0:
        top_failures = _format_top_failures(summary, max_failures_shown)
        if top_failures:
            text += "\n*Top Failures:*\n" + "\n".join(top_failures)

    return text


def send_slack_summary(
    summary: dict[str, Any],
    alert_config: dict[str, Any] | None = None,
    failure_threshold: float | None = None,
    profile_name: str | None = None,
) -> None:
    webhook_url = _get_slack_webhook_url()
    if not webhook_url:
        logger.warning("Slack webhook URL not configured. Skipping alerts.")
        return

    if alert_config is None:
        alert_config = {}

    mention_on_fail = alert_config.get("mention_on_fail", False)
    include_job_link = alert_config.get("include_job_link", True)
    max_failures_shown = alert_config.get("max_failures_shown", 5)
    compact_mode = alert_config.get("compact_mode", False)

    totals, suite_rows = _compute_totals(summary)

    if totals["blocking"] > 0:
        overall_status = "FAIL"
    elif totals["failed"] > 0:
        overall_status = "WARN"
    else:
        overall_status = "PASS"

    success_pct = (totals["passed"] / totals["checks"] * 100) if totals["checks"] > 0 else 0
    color = STATUS_COLOR.get(overall_status, "#808080")

    # Build context string: profile and/or environment (skip "unknown" or "default")
    env = _get_environment()
    context_parts = []
    if profile_name and profile_name != "default":
        context_parts.append(profile_name)
    if env and env != "unknown":
        context_parts.append(env)
    context_str = " • ".join(f"`{p}`" for p in context_parts) if context_parts else None

    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    mention = "<!channel> " if mention_on_fail and overall_status == "FAIL" else ""

    if compact_mode:
        text = _build_compact_message(totals, overall_status, success_pct, context_str, timestamp, mention)
    else:
        text = _build_full_message(
            totals,
            suite_rows,
            overall_status,
            success_pct,
            context_str,
            timestamp,
            mention,
            failure_threshold,
            summary,
            max_failures_shown,
        )

    job_url = _get_job_url() if include_job_link else None
    if job_url:
        text += f"\n\n<{job_url}|🔗 View Job>"

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": text}}],
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Slack summary sent successfully")
    except Exception as exc:
        logger.warning("Failed to send Slack summary: %s", exc)
