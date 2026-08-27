# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any
from neco_dq.core.policy.exit_codes import DQExitCode
from neco_dq.core.policy.policy_result import DQPolicyResult
from neco_dq.core.alerts.slack_alerts import send_slack_summary


class DQFailurePolicy:
    """
    Evaluates DQ summary and decides final pipeline outcome.
    Applies engine-level blocking rules from dq_policy.
    """

    def __init__(self, config: dict[str, Any], profile_name: str | None = None):
        self.config = config
        self.profile_name = profile_name
        self.failure_threshold: float | None = None
        self.block_on_threshold: bool = config.get("block_on_threshold", False)
        raw_threshold = self.config.get("failure_threshold")
        if raw_threshold is not None:
            try:
                threshold_value = float(raw_threshold)
            except (TypeError, ValueError) as exc:
                raise ValueError("DQFailurePolicy: 'failure_threshold' must be a float between 0.0 and 1.0.") from exc
            if not 0.0 <= threshold_value <= 1.0:
                raise ValueError(
                    f"DQFailurePolicy: 'failure_threshold' must be between 0.0 and 1.0, got {threshold_value}."
                )
            self.failure_threshold = threshold_value

    def _send_slack_if_enabled(self, summary: dict[str, Any], is_failure: bool) -> None:
        slack_config = self.config.get("alerts", {}).get("slack", {})
        if not slack_config.get("enabled", False):
            return

        send_always = slack_config.get("send_always", False)
        if is_failure or send_always:
            send_slack_summary(summary, slack_config, self.failure_threshold, self.profile_name)

    def evaluate(self, summary: dict[str, Any]) -> DQPolicyResult:
        blocking_engines: list[str] = []
        has_warning = False
        total_checks_global = 0
        total_failed_global = 0

        for engine_name, engine_summary in summary.items():
            if not isinstance(engine_summary, dict):
                return DQPolicyResult(DQExitCode.SYSTEM_ERROR, f"Invalid summary for test suite '{engine_name}'")

            blocking_failures = engine_summary.get("blocking_failures", 0)
            failed_by_severity = engine_summary.get("failed_by_severity", {})

            test_suites_can_block = self.config.get("block_test_suites", {}).get(engine_name, True)

            # --- Blocking failures always cause FAIL ---
            if blocking_failures > 0 and test_suites_can_block:
                blocking_engines.append(f"{engine_name} ({blocking_failures} blocking failure(s))")

            # --- Non-blocking failures with warn severity cause WARN ---
            warn_severities = ("warning", "error", "critical")
            if any(failed_by_severity.get(sev, 0) > 0 for sev in warn_severities):
                has_warning = True

            total_checks_global += engine_summary.get("total_checks", 0)
            total_failed_global += engine_summary.get("failed", 0)

        exceeds_threshold = self._exceeds_failure_threshold_global(total_checks_global, total_failed_global)

        if blocking_engines:
            result = DQPolicyResult(
                DQExitCode.DQ_ERROR, f"DQ FAILED: blocking test suite(s) detected → {', '.join(blocking_engines)}"
            )
            self._send_slack_if_enabled(summary, is_failure=True)
            return result

        if has_warning:
            # If block_on_threshold is true and threshold exceeded, treat as blocking
            if self.block_on_threshold and self.failure_threshold is not None and exceeds_threshold:
                threshold_pct = self.failure_threshold * 100
                msg = f"DQ FAILED: failure rate exceeds {threshold_pct:.0f}% threshold (block_on_threshold=true)"
                self._send_slack_if_enabled(summary, is_failure=True)
                return DQPolicyResult(DQExitCode.DQ_ERROR, msg)

            msg = "DQ completed with WARNINGS"
            if self.failure_threshold is not None and exceeds_threshold:
                msg += f" (failure rate exceeds {self.failure_threshold * 100:.0f}% threshold)"
            self._send_slack_if_enabled(summary, is_failure=False)
            return DQPolicyResult(DQExitCode.DQ_WARN, msg)

        self._send_slack_if_enabled(summary, is_failure=False)
        return DQPolicyResult(DQExitCode.SUCCESS, "DQ completed successfully")

    def _exceeds_failure_threshold_global(self, total_checks: int, total_failed: int) -> bool:
        """Check if the global failure rate across all suites exceeds the threshold."""
        if self.failure_threshold is None:
            return False

        if total_checks == 0:
            return False

        return (total_failed / total_checks) > self.failure_threshold
