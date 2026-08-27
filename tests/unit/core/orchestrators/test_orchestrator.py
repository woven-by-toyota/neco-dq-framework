# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for DQ Orchestrator.

Tests verify orchestrator initialization and exclusion logic.
Integration tests (initialize_test_suites_from_config, run_all) require Spark.
"""

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

from neco_dq.core.orchestrators.dq_orchestrator import DQOrchestrator


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.ORCHESTRATOR)
class TestDQOrchestratorInit:
    """Tests for DQOrchestrator initialization."""

    @allure.title("Orchestrator initializes with minimal config")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_init_minimal_config(self):
        """Orchestrator should initialize with minimal configuration."""
        with allure.step("Setup test data"):
            config = {"bronze": {"tables": []}}

        with allure.step("Execute orchestrator initialization"):
            orchestrator = DQOrchestrator(config)

        with allure.step("Verify: Orchestrator initializes with minimal config"):
            assert orchestrator.config == config
            assert orchestrator.engines == {}
            assert orchestrator.run_suites is None
            assert orchestrator.exclude_suites == set()

    @allure.title("Orchestrator respects run_suites filter")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_init_with_run_suites(self):
        """Orchestrator should store run_suites as set."""
        with allure.step("Setup test data"):
            config = {}

        with allure.step("Execute orchestrator initialization"):
            orchestrator = DQOrchestrator(config, run_suites=["bronze", "silver"])

        with allure.step("Verify: Orchestrator respects run_suites filter"):
            assert orchestrator.run_suites == {"bronze", "silver"}

    @allure.title("Orchestrator respects exclude_suites filter")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_init_with_exclude_suites(self):
        """Orchestrator should store exclude_suites as set."""
        with allure.step("Setup test data"):
            config = {}

        with allure.step("Execute orchestrator initialization"):
            orchestrator = DQOrchestrator(config, exclude_suites=["profiling"])

        with allure.step("Verify: Orchestrator respects exclude_suites filter"):
            assert orchestrator.exclude_suites == {"profiling"}

    @allure.title("Orchestrator stores selector")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_init_with_selector(self):
        """Orchestrator should store table selector."""
        with allure.step("Setup test data"):
            config = {}
            selector = {"bronze": ["table1", "table2"]}

        with allure.step("Execute orchestrator initialization"):
            orchestrator = DQOrchestrator(config, selector=selector)

        with allure.step("Verify: Orchestrator stores selector"):
            assert orchestrator.selector == selector

    @allure.title("Orchestrator extracts dq_policy from config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_init_extracts_policy(self):
        """Orchestrator should extract dq_policy from config."""
        with allure.step("Setup test data"):
            config = {"dq_policy": {"fail_on_error": False}}

        with allure.step("Execute orchestrator initialization"):
            orchestrator = DQOrchestrator(config)

        with allure.step("Verify: Orchestrator extracts dq_policy from config"):
            assert orchestrator.dq_policy == {"fail_on_error": False}


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.ORCHESTRATOR)
class TestDQOrchestratorIsExcluded:
    """Tests for DQOrchestrator._is_excluded() method."""

    @allure.title("Suite not excluded by default")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_is_excluded_default(self):
        """Suite should not be excluded when exclude_suites is empty."""
        with allure.step("Setup test data"):
            orchestrator = DQOrchestrator({})

        with allure.step("Execute is_excluded check"):
            result = orchestrator._is_excluded("bronze")

        with allure.step("Verify: Suite not excluded by default"):
            assert result is False

    @allure.title("Suite excluded when in exclude_suites")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_is_excluded_direct(self):
        """Suite should be excluded when directly listed."""
        with allure.step("Setup test data"):
            orchestrator = DQOrchestrator({}, exclude_suites=["bronze", "silver"])

        with allure.step("Execute is_excluded check"):
            result = orchestrator._is_excluded("bronze")

        with allure.step("Verify: Suite excluded when in exclude_suites"):
            assert result is True

    @allure.title("Child suite excluded when parent excluded")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_is_excluded_parent(self):
        """Child suite should be excluded when parent is excluded."""
        with allure.step("Setup test data"):
            orchestrator = DQOrchestrator({}, exclude_suites=["bronze"])

        with allure.step("Execute is_excluded check"):
            # great_expectations@bronze should be excluded because bronze is excluded
            result = orchestrator._is_excluded("great_expectations@bronze")

        with allure.step("Verify: Child suite excluded when parent excluded"):
            assert result is True

    @allure.title("Child suite not excluded when different parent")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_is_excluded_different_parent(self):
        """Child suite should not be excluded when different parent excluded."""
        with allure.step("Setup test data"):
            orchestrator = DQOrchestrator({}, exclude_suites=["bronze"])

        with allure.step("Execute is_excluded check"):
            result = orchestrator._is_excluded("great_expectations@silver")

        with allure.step("Verify: Child suite not excluded when different parent"):
            assert result is False


# =============================================================================
# Integration Test Placeholders (require Spark / full config)
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.ORCHESTRATOR)
class TestDQOrchestratorIntegration:
    """Integration tests for DQOrchestrator.

    Note: These tests require Spark and full configuration.
    They are placeholders for integration tests on Databricks.
    """

    @pytest.mark.spark
    @allure.title("Profiling suite with discover_tables is loaded (no explicit tables)")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_initialize_loads_profiling_with_discover_tables(self, spark):
        """A profiling suite that uses discover_tables (no explicit `tables`) must still load.

        Regression: the load guard skipped any section without a non-empty `tables` key,
        which dropped discover_tables-based profiling before discovery could run.
        """
        with allure.step("Setup profiling config that relies on discover_tables"):
            config = {
                "profiling": {
                    "mode": "suggest",
                    "discover_tables": {
                        "catalog": "test_catalog",
                        "schema_pattern": "raw_*",
                    },
                }
            }
            orchestrator = DQOrchestrator(config)

        with allure.step("Initialize test suites from config"):
            orchestrator.initialize_test_suites_from_config(spark)

        with allure.step("Verify: the profiling suite was loaded despite no explicit tables"):
            assert "profiling" in orchestrator.engines

    @pytest.mark.skip(reason="Requires Spark - run as integration test")
    @allure.title("Initialize test suites from config")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_initialize_test_suites_from_config(self):
        """Orchestrator should load test suites from YAML config."""
        # Integration test: requires Spark and valid config
        pass

    @pytest.mark.skip(reason="Requires Spark - run as integration test")
    @allure.title("Initialize filters tables by selector")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_initialize_with_selector(self):
        """Orchestrator should filter tables when selector is provided."""
        # Integration test: requires Spark and valid config
        pass

    @pytest.mark.skip(reason="Requires Spark - run as integration test")
    @allure.title("Run all executes registered suites")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_all(self):
        """Orchestrator should execute all registered test suites."""
        # Integration test: requires Spark and registered suites
        pass

    @pytest.mark.skip(reason="Requires Spark - run as integration test")
    @allure.title("Run all handles suite crash gracefully")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_run_all_suite_crash(self):
        """Orchestrator should handle suite crash and continue."""
        # Integration test: requires Spark
        pass
