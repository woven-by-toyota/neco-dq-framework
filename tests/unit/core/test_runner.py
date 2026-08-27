# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Tests for DQ Runner.

Unit tests with mocking and integration test placeholders for Databricks.
"""

import os
from unittest.mock import MagicMock, patch

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag


# =============================================================================
# Unit Tests (with mocking)
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.RUNNER)
class TestRunDQUnit:
    """Unit tests for run_dq() function with mocked dependencies."""

    @allure.title("run_dq sets TQDM_DISABLE when verbose=False")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_run_dq_disables_tqdm_by_default(self, monkeypatch):
        """run_dq should disable tqdm progress bars when verbose=False."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()
            mock_orchestrator = MagicMock()
            mock_orchestrator.run_all.return_value = {"total": 0, "passed": 0}
            # Clear env before test (monkeypatch auto-restores after test)
            monkeypatch.delenv("TQDM_DISABLE", raising=False)

        with allure.step("Execute run_dq with verbose=False"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.resolve_config") as mock_resolve,
                patch("neco_dq.runner.DQOrchestrator") as mock_orch_cls,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_resolve.return_value = {"bronze": {}}
                mock_orch_cls.return_value = mock_orchestrator

                from neco_dq.runner import run_dq

                run_dq("test_config.yaml", verbose=False)

        with allure.step("Verify: TQDM_DISABLE is set"):
            assert os.environ.get("TQDM_DISABLE") == "1"

    @allure.title("run_dq passes run_suites to orchestrator")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_run_dq_passes_run_suites(self):
        """run_dq should pass run_suites filter to orchestrator."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()
            mock_orchestrator = MagicMock()
            mock_orchestrator.run_all.return_value = {"total": 0, "passed": 0}

        with allure.step("Execute run_dq with run_suites"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.resolve_config") as mock_resolve,
                patch("neco_dq.runner.DQOrchestrator") as mock_orch_cls,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_resolve.return_value = {"bronze": {}, "silver": {}}
                mock_orch_cls.return_value = mock_orchestrator

                from neco_dq.runner import run_dq

                run_dq("test_config.yaml", run_suites=["bronze"])

        with allure.step("Verify: run_suites passed to orchestrator"):
            mock_orch_cls.assert_called_once()
            call_kwargs = mock_orch_cls.call_args[1]
            assert call_kwargs["run_suites"] == ["bronze"]

    @allure.title("run_dq passes exclude_suites to orchestrator")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_run_dq_passes_exclude_suites(self):
        """run_dq should pass exclude_suites filter to orchestrator."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()
            mock_orchestrator = MagicMock()
            mock_orchestrator.run_all.return_value = {"total": 0, "passed": 0}

        with allure.step("Execute run_dq with exclude_suites"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.resolve_config") as mock_resolve,
                patch("neco_dq.runner.DQOrchestrator") as mock_orch_cls,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_resolve.return_value = {"bronze": {}, "silver": {}}
                mock_orch_cls.return_value = mock_orchestrator

                from neco_dq.runner import run_dq

                run_dq("test_config.yaml", exclude_suites=["gold"])

        with allure.step("Verify: exclude_suites passed to orchestrator"):
            call_kwargs = mock_orch_cls.call_args[1]
            assert call_kwargs["exclude_suites"] == ["gold"]

    @allure.title("run_dq passes selector to orchestrator")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_run_dq_passes_selector(self):
        """run_dq should pass selector filter to orchestrator."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()
            mock_orchestrator = MagicMock()
            mock_orchestrator.run_all.return_value = {"total": 0, "passed": 0}

        with allure.step("Execute run_dq with selector"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.resolve_config") as mock_resolve,
                patch("neco_dq.runner.DQOrchestrator") as mock_orch_cls,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_resolve.return_value = {"bronze": {}}
                mock_orch_cls.return_value = mock_orchestrator

                from neco_dq.runner import run_dq

                selector = {"bronze": ["table1", "table2"]}
                run_dq("test_config.yaml", selector=selector)

        with allure.step("Verify: selector passed to orchestrator"):
            call_kwargs = mock_orch_cls.call_args[1]
            assert call_kwargs["selector"] == {"bronze": ["table1", "table2"]}

    @allure.title("run_dq loads single custom suite file")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_run_dq_loads_single_custom_suite(self):
        """run_dq should load and register a single custom suite file."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()
            mock_orchestrator = MagicMock()
            mock_orchestrator.run_all.return_value = {"total": 0, "passed": 0}
            mock_suite_cls = MagicMock()
            mock_suite_cls.__name__ = "MyCustomSuite"

        with allure.step("Execute run_dq with custom_suite_files"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.resolve_config") as mock_resolve,
                patch("neco_dq.runner.DQOrchestrator") as mock_orch_cls,
                patch("neco_dq.runner.load_custom_suite_from_file") as mock_load,
                patch("neco_dq.runner.register_custom_suite") as mock_register,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_resolve.return_value = {"my_custom": {}}
                mock_orch_cls.return_value = mock_orchestrator
                mock_load.return_value = ("my_custom", mock_suite_cls)

                from neco_dq.runner import run_dq

                run_dq("test_config.yaml", custom_suite_files="/path/to/suite.py")

        with allure.step("Verify: custom suite loaded and registered"):
            mock_load.assert_called_once_with("/path/to/suite.py")
            mock_register.assert_called_once_with("my_custom", mock_suite_cls)

    @allure.title("run_dq loads multiple custom suite files")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_run_dq_loads_multiple_custom_suites(self):
        """run_dq should load and register multiple custom suite files."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()
            mock_orchestrator = MagicMock()
            mock_orchestrator.run_all.return_value = {"total": 0, "passed": 0}

        with allure.step("Execute run_dq with multiple custom_suite_files"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.resolve_config") as mock_resolve,
                patch("neco_dq.runner.DQOrchestrator") as mock_orch_cls,
                patch("neco_dq.runner.load_custom_suite_from_file") as mock_load,
                patch("neco_dq.runner.register_custom_suite") as mock_register,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_resolve.return_value = {}
                mock_orch_cls.return_value = mock_orchestrator

                # Setup different return values for each file
                mock_cls1 = MagicMock(__name__="Suite1")
                mock_cls2 = MagicMock(__name__="Suite2")
                mock_load.side_effect = [
                    ("suite1", mock_cls1),
                    ("suite2", mock_cls2),
                ]

                from neco_dq.runner import run_dq

                run_dq(
                    "test_config.yaml",
                    custom_suite_files=["/path/to/suite1.py", "/path/to/suite2.py"],
                )

        with allure.step("Verify: both suites loaded and registered"):
            assert mock_load.call_count == 2
            assert mock_register.call_count == 2

    @allure.title("run_dq raises on custom suite load failure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_run_dq_raises_on_custom_suite_error(self):
        """run_dq should raise exception when custom suite fails to load."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()

        with allure.step("Execute run_dq with invalid custom suite"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.load_custom_suite_from_file") as mock_load,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_load.side_effect = ValueError("Invalid suite file")

                from neco_dq.runner import run_dq

                with pytest.raises(ValueError, match="Invalid suite file"):
                    run_dq("test_config.yaml", custom_suite_files="/bad/path.py")

    @allure.title("run_dq returns orchestrator result")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_run_dq_returns_orchestrator_result(self):
        """run_dq should return the result from orchestrator.run_all()."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()
            mock_orchestrator = MagicMock()
            expected_result = {"total": 10, "passed": 8, "failed": 2}
            mock_orchestrator.run_all.return_value = expected_result

        with allure.step("Execute run_dq"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.resolve_config") as mock_resolve,
                patch("neco_dq.runner.DQOrchestrator") as mock_orch_cls,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_resolve.return_value = {"bronze": {}}
                mock_orch_cls.return_value = mock_orchestrator

                from neco_dq.runner import run_dq

                result = run_dq("test_config.yaml")

        with allure.step("Verify: orchestrator result returned"):
            assert result == expected_result

    @allure.title("run_dq enables verbose mode with debug logging")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_run_dq_verbose_mode(self, monkeypatch):
        """run_dq should enable debug logging when verbose=True."""
        with allure.step("Setup mocks"):
            mock_spark = MagicMock()
            mock_orchestrator = MagicMock()
            mock_orchestrator.run_all.return_value = {"total": 0, "passed": 0}
            # Clear env before test (monkeypatch auto-restores after test)
            monkeypatch.delenv("TQDM_DISABLE", raising=False)

        with allure.step("Execute run_dq with verbose=True"):
            with (
                patch("neco_dq.runner.SparkSession") as mock_spark_session,
                patch("neco_dq.runner.resolve_config") as mock_resolve,
                patch("neco_dq.runner.DQOrchestrator") as mock_orch_cls,
            ):
                mock_spark_session.builder.getOrCreate.return_value = mock_spark
                mock_resolve.return_value = {"bronze": {}}
                mock_orch_cls.return_value = mock_orchestrator

                from neco_dq.runner import run_dq

                run_dq("test_config.yaml", verbose=True)

        with allure.step("Verify: TQDM_DISABLE is not set when verbose"):
            # When verbose=True, TQDM_DISABLE should not be set to "1"
            assert os.environ.get("TQDM_DISABLE") != "1"


# =============================================================================
# Integration Tests (placeholders)
# =============================================================================


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.RUNNER)
class TestRunDQIntegration:
    """Integration tests for run_dq() function.

    Note: These tests require Spark and valid configuration files.
    They are placeholders for integration tests on Databricks.
    """

    @pytest.mark.skip(reason="Requires Spark and config - run as integration test")
    @allure.title("Run DQ with legacy YAML config")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_dq_legacy_yaml(self):
        """run_dq should execute with legacy YAML configuration."""
        # Integration test: requires Spark and YAML config file
        pass

    @pytest.mark.skip(reason="Requires Spark and config - run as integration test")
    @allure.title("Run DQ with modular config directory")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_dq_modular_config(self):
        """run_dq should execute with modular dq-config directory."""
        # Integration test: requires Spark and config directory
        pass

    @pytest.mark.skip(reason="Requires Spark and config - run as integration test")
    @allure.title("Run DQ with run_suites filter")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_dq_with_run_suites(self):
        """run_dq should only execute specified suites."""
        # Integration test: requires Spark
        pass

    @pytest.mark.skip(reason="Requires Spark and config - run as integration test")
    @allure.title("Run DQ with exclude_suites filter")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_dq_with_exclude_suites(self):
        """run_dq should skip excluded suites."""
        # Integration test: requires Spark
        pass

    @pytest.mark.skip(reason="Requires Spark and config - run as integration test")
    @allure.title("Run DQ with table selector")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_dq_with_selector(self):
        """run_dq should filter tables per suite with selector."""
        # Integration test: requires Spark
        pass

    @pytest.mark.skip(reason="Requires Spark and config - run as integration test")
    @allure.title("Run DQ returns summary dict")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_run_dq_returns_summary(self):
        """run_dq should return aggregated summary dictionary."""
        # Integration test: requires Spark
        pass

    @pytest.mark.skip(reason="Requires Spark and config - run as integration test")
    @allure.title("Run DQ raises on blocking failures")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_run_dq_raises_on_blocking(self):
        """run_dq should raise RuntimeError on blocking DQ failures."""
        # Integration test: requires Spark and config that fails
        pass
