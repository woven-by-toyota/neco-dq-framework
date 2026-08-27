# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for DQ Failure Policy.

Tests verify that the failure policy correctly evaluates DQ summaries
and returns appropriate exit codes based on blocking failures and warnings.
"""

import allure

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

from neco_dq.core.policy.dq_failure_policy import DQFailurePolicy
from neco_dq.core.policy.exit_codes import DQExitCode


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.FAILURE_POLICY)
class TestDQFailurePolicyInit:
    """Tests for DQFailurePolicy initialization."""

    @allure.title("Policy initializes with default config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_init_default_config(self):
        """Policy should initialize with empty config using defaults."""
        with allure.step("Setup test data"):
            config = {}

        with allure.step("Execute policy initialization"):
            policy = DQFailurePolicy(config)

        with allure.step("Verify: Policy initializes with default config"):
            assert policy.failure_threshold is None
            assert policy.config == {}

    @allure.title("Policy respects failure_threshold setting")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_init_custom_failure_threshold(self):
        """Policy should respect failure_threshold configuration."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1}

        with allure.step("Execute policy initialization"):
            policy = DQFailurePolicy(config)

        with allure.step("Verify: Policy respects failure_threshold setting"):
            assert policy.failure_threshold == 0.1


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.FAILURE_POLICY)
class TestDQFailurePolicyEvaluate:
    """Tests for DQFailurePolicy.evaluate() method."""

    @allure.title("Returns SUCCESS when all checks pass")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_evaluate_success(self):
        """Policy should return SUCCESS when no failures."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "bronze": {
                    "blocking_failures": 0,
                    "failed_by_severity": {},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Returns SUCCESS when all checks pass"):
            assert result.exit_code == DQExitCode.SUCCESS
            assert "successfully" in result.message

    @allure.title("Returns DQ_WARN when warnings exist")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_evaluate_warning(self):
        """Policy should return DQ_WARN when only warnings exist."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "silver": {
                    "blocking_failures": 0,
                    "failed_by_severity": {"warning": 2},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Returns DQ_WARN when warnings exist"):
            assert result.exit_code == DQExitCode.DQ_WARN
            assert "WARNINGS" in result.message

    @allure.title("Returns DQ_ERROR when blocking failures exist")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_evaluate_blocking_failure(self):
        """Policy should return DQ_ERROR when blocking failures exist."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "gold": {
                    "blocking_failures": 3,
                    "failed_by_severity": {"error": 3},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Returns DQ_ERROR when blocking failures exist"):
            assert result.exit_code == DQExitCode.DQ_ERROR
            assert "FAILED" in result.message
            assert "gold" in result.message

    @allure.title("Returns SYSTEM_ERROR for invalid summary")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_evaluate_invalid_summary(self):
        """Policy should return SYSTEM_ERROR for invalid summary format."""
        with allure.step("Setup test data"):
            config = {}
            summary = {"bronze": "invalid_not_a_dict"}

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Returns SYSTEM_ERROR for invalid summary"):
            assert result.exit_code == DQExitCode.SYSTEM_ERROR
            assert "Invalid summary" in result.message

    @allure.title("Respects block_test_suites configuration")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_evaluate_block_test_suites_disabled(self):
        """Policy should not block when suite blocking is disabled but warn on error."""
        with allure.step("Setup test data"):
            config = {"block_test_suites": {"bronze": False}}
            summary = {
                "bronze": {
                    "blocking_failures": 5,
                    "failed_by_severity": {"error": 5},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Respects block_test_suites configuration"):
            # Should not block (blocking disabled) but warns (error severity)
            assert result.exit_code == DQExitCode.DQ_WARN

    @allure.title("Multiple suites - one blocking")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_evaluate_multiple_suites_one_blocking(self):
        """Policy should fail if any suite has blocking failures."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "bronze": {
                    "blocking_failures": 0,
                    "failed_by_severity": {},
                },
                "silver": {
                    "blocking_failures": 1,
                    "failed_by_severity": {"error": 1},
                },
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Multiple suites - one blocking"):
            assert result.exit_code == DQExitCode.DQ_ERROR
            assert "silver" in result.message

    @allure.title("Empty summary returns SUCCESS")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_evaluate_empty_summary(self):
        """Policy should return SUCCESS for empty summary."""
        with allure.step("Setup test data"):
            config = {}
            summary = {}

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Empty summary returns SUCCESS"):
            assert result.exit_code == DQExitCode.SUCCESS


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.FAILURE_POLICY)
class TestDQFailurePolicyThreshold:
    """Tests for failure_threshold functionality."""

    @allure.title("No threshold - blocks on any failure")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_no_threshold_blocks_on_failure(self):
        """Without threshold, any blocking failure should block."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "bronze": {
                    "total_checks": 100,
                    "failed": 1,
                    "blocking_failures": 1,
                    "failed_by_severity": {"error": 1},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: No threshold - blocks on any failure"):
            assert result.exit_code == DQExitCode.DQ_ERROR

    @allure.title("Threshold exceeded - blocks pipeline")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_threshold_exceeded_blocks(self):
        """Should block when failure rate exceeds threshold."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1}
            summary = {
                "silver": {
                    "total_checks": 100,
                    "failed": 15,  # 15% > 10% threshold
                    "blocking_failures": 15,
                    "failed_by_severity": {"error": 15},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Threshold exceeded - blocks pipeline"):
            assert result.exit_code == DQExitCode.DQ_ERROR
            assert "silver" in result.message

    @allure.title("Non-blocking failures with error severity - WARN regardless of threshold")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_not_exceeded_warns_on_error(self):
        """Non-blocking failures with error severity always produce WARN."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1}
            summary = {
                "gold": {
                    "total_checks": 100,
                    "failed": 5,  # 5% < 10% threshold
                    "blocking_failures": 0,  # non-blocking
                    "failed_by_severity": {"error": 5},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Non-blocking failures with error severity - WARN"):
            assert result.exit_code == DQExitCode.DQ_WARN

    @allure.title("Non-blocking failures with low severity - SUCCESS")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_not_exceeded_low_severity_success(self):
        """Non-blocking failures with low severity return SUCCESS."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1}
            summary = {
                "gold": {
                    "total_checks": 100,
                    "failed": 5,  # 5% < 10% threshold
                    "blocking_failures": 0,  # non-blocking
                    "failed_by_severity": {"low": 5},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Non-blocking with low severity - SUCCESS"):
            assert result.exit_code == DQExitCode.SUCCESS

    @allure.title("Non-blocking failures at threshold boundary - WARN")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_threshold_at_boundary_warns(self):
        """Non-blocking failures with error severity at threshold boundary still WARN."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1}
            summary = {
                "bronze": {
                    "total_checks": 100,
                    "failed": 10,  # exactly 10% == threshold
                    "blocking_failures": 0,  # non-blocking
                    "failed_by_severity": {"error": 10},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Non-blocking at threshold boundary - WARN"):
            assert result.exit_code == DQExitCode.DQ_WARN

    @allure.title("Zero total checks - blocks on failure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_zero_total_checks_blocks(self):
        """Should block when total_checks is zero (edge case)."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1}
            summary = {
                "bronze": {
                    "total_checks": 0,
                    "failed": 0,
                    "blocking_failures": 1,
                    "failed_by_severity": {},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Zero total checks - blocks on failure"):
            assert result.exit_code == DQExitCode.DQ_ERROR

    @allure.title("Blocking failures always block regardless of threshold")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_threshold_multiple_suites(self):
        """Blocking failures always cause FAIL, threshold is informational only."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1}
            summary = {
                "bronze": {
                    "total_checks": 100,
                    "failed": 5,  # 5% - below threshold
                    "blocking_failures": 5,  # blocking = FAIL
                    "failed_by_severity": {"error": 5},
                },
                "silver": {
                    "total_checks": 100,
                    "failed": 20,  # 20% - above threshold
                    "blocking_failures": 20,  # blocking = FAIL
                    "failed_by_severity": {"error": 20},
                },
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Blocking failures always cause FAIL"):
            assert result.exit_code == DQExitCode.DQ_ERROR
            # Both suites have blocking failures, both are in the message
            assert "silver" in result.message
            assert "bronze" in result.message


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.FAILURE_POLICY)
class TestDQFailurePolicySeverity:
    """Tests for severity-based warning behavior."""

    @allure.title("Critical severity non-blocking triggers warning")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_critical_non_blocking_warns(self):
        """Non-blocking failures with critical severity should warn."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "silver": {
                    "blocking_failures": 0,
                    "failed_by_severity": {"critical": 2},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Critical severity non-blocking triggers warning"):
            assert result.exit_code == DQExitCode.DQ_WARN

    @allure.title("Error severity non-blocking triggers warning")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_error_non_blocking_warns(self):
        """Non-blocking failures with error severity should warn."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "gold": {
                    "blocking_failures": 0,
                    "failed_by_severity": {"error": 3},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Error severity non-blocking triggers warning"):
            assert result.exit_code == DQExitCode.DQ_WARN

    @allure.title("Low severity non-blocking returns SUCCESS")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_low_non_blocking_success(self):
        """Non-blocking failures with low severity should return SUCCESS."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "bronze": {
                    "blocking_failures": 0,
                    "failed_by_severity": {"low": 5},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Low severity non-blocking returns SUCCESS"):
            assert result.exit_code == DQExitCode.SUCCESS

    @allure.title("Mixed severities - highest wins")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_mixed_severities_warns(self):
        """Mixed severities with at least one critical/error should warn."""
        with allure.step("Setup test data"):
            config = {}
            summary = {
                "silver": {
                    "blocking_failures": 0,
                    "failed_by_severity": {"low": 10, "warning": 2, "error": 1},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Mixed severities - highest wins"):
            assert result.exit_code == DQExitCode.DQ_WARN


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.FAILURE_POLICY)
class TestDQFailurePolicyBlockOnThreshold:
    """Tests for block_on_threshold functionality."""

    @allure.title("block_on_threshold defaults to false")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_block_on_threshold_default_false(self):
        """block_on_threshold should default to false when not configured."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1}

        with allure.step("Execute policy initialization"):
            policy = DQFailurePolicy(config)

        with allure.step("Verify: block_on_threshold defaults to false"):
            assert policy.block_on_threshold is False

    @allure.title("block_on_threshold=true blocks on exceeded threshold")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_block_on_threshold_true_exceeds_blocks(self):
        """With block_on_threshold=true, exceeding threshold should FAIL."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1, "block_on_threshold": True}
            summary = {
                "silver": {
                    "total_checks": 100,
                    "failed": 15,  # 15% > 10% threshold
                    "blocking_failures": 0,  # non-blocking
                    "failed_by_severity": {"error": 15},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: block_on_threshold=true blocks on exceeded threshold"):
            assert result.exit_code == DQExitCode.DQ_ERROR
            assert "block_on_threshold" in result.message

    @allure.title("block_on_threshold=false only warns on exceeded threshold")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_block_on_threshold_false_only_warns(self):
        """With block_on_threshold=false, exceeding threshold should only WARN."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.1, "block_on_threshold": False}
            summary = {
                "silver": {
                    "total_checks": 100,
                    "failed": 15,  # 15% > 10% threshold
                    "blocking_failures": 0,  # non-blocking
                    "failed_by_severity": {"error": 15},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: block_on_threshold=false only warns"):
            assert result.exit_code == DQExitCode.DQ_WARN
            assert "threshold" in result.message

    @allure.title("block_on_threshold=true with threshold not exceeded - WARN")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_block_on_threshold_not_exceeded_warns(self):
        """With block_on_threshold=true but threshold not exceeded, only WARN."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.2, "block_on_threshold": True}
            summary = {
                "gold": {
                    "total_checks": 100,
                    "failed": 10,  # 10% < 20% threshold
                    "blocking_failures": 0,  # non-blocking
                    "failed_by_severity": {"error": 10},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Threshold not exceeded - only WARN"):
            assert result.exit_code == DQExitCode.DQ_WARN

    @allure.title("block_on_threshold without failure_threshold has no effect")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_block_on_threshold_without_threshold(self):
        """block_on_threshold without failure_threshold should have no effect."""
        with allure.step("Setup test data"):
            config = {"block_on_threshold": True}  # no failure_threshold
            summary = {
                "bronze": {
                    "total_checks": 100,
                    "failed": 50,  # 50% but no threshold defined
                    "blocking_failures": 0,  # non-blocking
                    "failed_by_severity": {"error": 50},
                }
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Without threshold, only WARN (no blocking)"):
            assert result.exit_code == DQExitCode.DQ_WARN

    @allure.title("block_on_threshold uses global rate - one suite high, global below threshold")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_block_on_threshold_uses_global_rate_no_block(self):
        """block_on_threshold must not block when global rate is below threshold, even if one suite exceeds it."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.05, "block_on_threshold": True}
            summary = {
                "bronze": {
                    "total_checks": 250,
                    "failed": 0,
                    "blocking_failures": 0,
                    "failed_by_severity": {},
                },
                "silver": {
                    "total_checks": 5,
                    "failed": 4,  # 80% per-suite, but 4/255 = 1.6% globally
                    "blocking_failures": 0,
                    "failed_by_severity": {"error": 4},
                },
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Global rate 1.6% is below 5% threshold - only WARN"):
            assert result.exit_code == DQExitCode.DQ_WARN

    @allure.title("block_on_threshold uses global rate - global exceeds threshold")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_block_on_threshold_uses_global_rate_blocks(self):
        """block_on_threshold must block when the global failure rate exceeds the threshold."""
        with allure.step("Setup test data"):
            config = {"failure_threshold": 0.05, "block_on_threshold": True}
            summary = {
                "bronze": {
                    "total_checks": 100,
                    "failed": 3,
                    "blocking_failures": 0,
                    "failed_by_severity": {"error": 3},
                },
                "silver": {
                    "total_checks": 100,
                    "failed": 4,  # combined: 7/200 = 3.5% still below threshold
                    "blocking_failures": 0,
                    "failed_by_severity": {"error": 4},
                },
                "gold": {
                    "total_checks": 100,
                    "failed": 10,  # combined: 17/300 = 5.67% > 5% threshold
                    "blocking_failures": 0,
                    "failed_by_severity": {"error": 10},
                },
            }

        with allure.step("Execute policy evaluation"):
            policy = DQFailurePolicy(config)
            result = policy.evaluate(summary)

        with allure.step("Verify: Global rate 5.67% exceeds 5% threshold - FAIL"):
            assert result.exit_code == DQExitCode.DQ_ERROR
            assert "block_on_threshold" in result.message
