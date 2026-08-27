# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Test Suite Registry.

Tests verify that the registry correctly maps suite names to classes
and that dynamic loading works properly.
"""

import tempfile
import textwrap
from pathlib import Path

import allure
import pytest

from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

from neco_dq.core.registry.test_suite_registry import (
    TEST_SUITES_REGISTRY,
    _derive_suite_config_key,
    load_custom_suite_from_file,
    load_test_suite_from_registry,
    register_custom_suite,
)


# -----------------------------------------------------------------------------
# Fixtures for custom suite testing
# -----------------------------------------------------------------------------


@pytest.fixture
def valid_custom_suite_file():
    """Create a temporary file with a valid custom suite."""
    content = textwrap.dedent("""\
        from neco_dq.test_suites.base_test_suite import DQTestSuite
        from neco_dq.core.io.result_writer import DQResultWriter


        class MyCustomSuite(DQTestSuite):
            \"\"\"Valid custom suite for testing.\"\"\"

            def __init__(self, spark, cfg: dict):
                self.spark = spark
                self.cfg = cfg
                self.writer = DQResultWriter(spark)

            @property
            def _suite_name(self) -> str:
                return "my_custom"

            @property
            def _suite_config_key(self) -> str:
                return "my_custom"

            def _check_test_check(self, df, cfg):
                return True, "ok", None
        """)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(content)
        f.flush()
        yield f.name
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def empty_python_file():
    """Create a temporary Python file with no DQTestSuite class."""
    content = textwrap.dedent("""\
        class SomeOtherClass:
            pass

        def some_function():
            return 42
        """)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(content)
        f.flush()
        yield f.name
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def multiple_suites_file():
    """Create a temporary file with multiple DQTestSuite classes."""
    content = textwrap.dedent("""\
        from neco_dq.test_suites.base_test_suite import DQTestSuite
        from neco_dq.core.io.result_writer import DQResultWriter


        class FirstSuite(DQTestSuite):
            def __init__(self, spark, cfg):
                self.spark = spark
                self.cfg = cfg
                self.writer = DQResultWriter(spark)

            @property
            def _suite_name(self):
                return "first"

            @property
            def _suite_config_key(self):
                return "first"


        class SecondSuite(DQTestSuite):
            def __init__(self, spark, cfg):
                self.spark = spark
                self.cfg = cfg
                self.writer = DQResultWriter(spark)

            @property
            def _suite_name(self):
                return "second"

            @property
            def _suite_config_key(self):
                return "second"
        """)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(content)
        f.flush()
        yield f.name
    Path(f.name).unlink(missing_ok=True)


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.TEST_SUITE_REGISTRY)
class TestSuiteRegistry:
    """Tests for TEST_SUITES_REGISTRY dictionary."""

    @allure.title("Registry contains all expected suites")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_registry_contains_all_suites(self):
        """Registry should contain all expected suite keys."""
        with allure.step("Setup test data"):
            expected_suites = {"profiling", "bronze", "silver", "gold", "great_expectations"}

        with allure.step("Verify: registry contains all expected suites"):
            assert set(TEST_SUITES_REGISTRY.keys()) == expected_suites

    @allure.title("Registry values are valid class paths")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_registry_values_are_class_paths(self):
        """Registry values should be valid Python class paths."""
        with allure.step("Verify: all registry values are valid class paths"):
            for suite_name, class_path in TEST_SUITES_REGISTRY.items():
                assert "." in class_path, f"{suite_name} class path missing module separator"
                assert class_path.endswith("TestSuite"), f"{suite_name} class should end with TestSuite"


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.TEST_SUITE_REGISTRY)
class TestLoadFromRegistry:
    """Tests for load_test_suite_from_registry function."""

    @allure.title("Load bronze suite successfully")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_bronze_suite(self):
        """Should successfully load Bronze suite class."""
        with allure.step("Execute load from registry"):
            suite_class = load_test_suite_from_registry("bronze")

        with allure.step("Verify: Bronze suite class loaded"):
            assert suite_class is not None
            assert suite_class.__name__ == "SparkBronzeDQTestSuite"

    @allure.title("Load silver suite successfully")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_silver_suite(self):
        """Should successfully load Silver suite class."""
        with allure.step("Execute load from registry"):
            suite_class = load_test_suite_from_registry("silver")

        with allure.step("Verify: Silver suite class loaded"):
            assert suite_class is not None
            assert suite_class.__name__ == "SparkSilverDQTestSuite"

    @allure.title("Load gold suite successfully")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_gold_suite(self):
        """Should successfully load Gold suite class."""
        with allure.step("Execute load from registry"):
            suite_class = load_test_suite_from_registry("gold")

        with allure.step("Verify: Gold suite class loaded"):
            assert suite_class is not None
            assert suite_class.__name__ == "SparkGoldDQTestSuite"

    @allure.title("Load great_expectations suite successfully")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_ge_suite(self):
        """Should successfully load Great Expectations suite class."""
        with allure.step("Execute load from registry"):
            suite_class = load_test_suite_from_registry("great_expectations")

        with allure.step("Verify: GE suite class loaded"):
            assert suite_class is not None
            assert suite_class.__name__ == "GEDQTestSuite"

    @allure.title("Load profiling suite successfully")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_profiling_suite(self):
        """Should successfully load Profiling suite class."""
        with allure.step("Execute load from registry"):
            suite_class = load_test_suite_from_registry("profiling")

        with allure.step("Verify: Profiling suite class loaded"):
            assert suite_class is not None
            assert suite_class.__name__ == "SparkProfilingDQTestSuite"

    @allure.title("Raise ValueError for unknown suite")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_load_unknown_suite_raises_error(self):
        """Should raise ValueError for unregistered suite name."""
        with allure.step("Execute load with unknown suite"):
            with pytest.raises(ValueError, match="not registered"):
                load_test_suite_from_registry("unknown_suite")

    @allure.title("All registered suites are loadable")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_all_suites_loadable(self):
        """All suites in registry should be loadable without errors."""
        with allure.step("Verify: all registered suites can be loaded"):
            for suite_name in TEST_SUITES_REGISTRY:
                suite_class = load_test_suite_from_registry(suite_name)
                assert suite_class is not None, f"Failed to load {suite_name}"
                assert hasattr(suite_class, "__name__"), f"{suite_name} has no __name__"


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.TEST_SUITE_REGISTRY)
class TestDeriveSuiteConfigKey:
    """Tests for _derive_suite_config_key function."""

    @allure.title("Derive config key from MyCustomSuite")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_derive_my_custom_suite(self):
        """MyCustomSuite should derive to my_custom."""
        with allure.step("Execute derive config key"):
            result = _derive_suite_config_key("MyCustomSuite", Path("/fake/test.py"))

        with allure.step("Verify: config key is my_custom"):
            assert result == "my_custom"

    @allure.title("Derive config key from OrderChecksDQTestSuite")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_derive_dq_test_suite_suffix(self):
        """OrderChecksDQTestSuite should derive to order_checks."""
        with allure.step("Execute derive config key"):
            result = _derive_suite_config_key("OrderChecksDQTestSuite", Path("/fake/test.py"))

        with allure.step("Verify: config key is order_checks"):
            assert result == "order_checks"

    @allure.title("Derive config key from DataQualityTestSuite")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_derive_test_suite_suffix(self):
        """DataQualityTestSuite should derive to data_quality."""
        with allure.step("Execute derive config key"):
            result = _derive_suite_config_key("DataQualityTestSuite", Path("/fake/test.py"))

        with allure.step("Verify: config key is data_quality"):
            assert result == "data_quality"

    @allure.title("Derive config key from simple Suite class")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_derive_suite_suffix_only(self):
        """Suite should fallback to filename."""
        with allure.step("Execute derive config key"):
            result = _derive_suite_config_key("Suite", Path("/fake/my_checks.py"))

        with allure.step("Verify: config key is derived from filename"):
            assert result == "my_checks"

    @allure.title("Derive config key with CamelCase")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_derive_camel_case_conversion(self):
        """CustomerOrderValidationSuite should convert CamelCase to snake_case."""
        with allure.step("Execute derive config key"):
            result = _derive_suite_config_key("CustomerOrderValidationSuite", Path("/fake/test.py"))

        with allure.step("Verify: config key is snake_case"):
            assert result == "customer_order_validation"


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.TEST_SUITE_REGISTRY)
class TestLoadCustomSuiteFromFile:
    """Tests for load_custom_suite_from_file function."""

    @allure.title("Load valid custom suite from file")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_valid_custom_suite(self, valid_custom_suite_file):
        """Should load a valid custom suite from file."""
        with allure.step("Execute load custom suite"):
            suite_name, suite_class = load_custom_suite_from_file(valid_custom_suite_file)

        with allure.step("Verify: suite loaded correctly"):
            assert suite_name == "my_custom"
            assert suite_class.__name__ == "MyCustomSuite"
            assert hasattr(suite_class, "_suite_name")

    @allure.title("Raise FileNotFoundError for missing file")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_load_missing_file_raises_error(self):
        """Should raise FileNotFoundError for non-existent file."""
        with allure.step("Execute load with missing file"):
            with pytest.raises(FileNotFoundError, match="not found"):
                load_custom_suite_from_file("/nonexistent/path/to/suite.py")

    @allure.title("Raise ValueError for file without DQTestSuite")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_load_empty_file_raises_error(self, empty_python_file):
        """Should raise ValueError if no DQTestSuite subclass found."""
        with allure.step("Execute load with empty file"):
            with pytest.raises(ValueError, match="No DQTestSuite subclass found"):
                load_custom_suite_from_file(empty_python_file)

    @allure.title("Raise ValueError for file with multiple DQTestSuites")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_load_multiple_suites_raises_error(self, multiple_suites_file):
        """Should raise ValueError if multiple DQTestSuite subclasses found."""
        with allure.step("Execute load with multiple suites"):
            with pytest.raises(ValueError, match="Multiple DQTestSuite subclasses"):
                load_custom_suite_from_file(multiple_suites_file)


@allure.feature(AllureFeature.CORE)
@allure.story(AllureStory.TEST_SUITE_REGISTRY)
class TestRegisterCustomSuite:
    """Tests for register_custom_suite function."""

    @allure.title("Register custom suite with class")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_register_with_class(self, valid_custom_suite_file):
        """Should register custom suite using class object."""
        with allure.step("Setup: load suite class"):
            _, suite_class = load_custom_suite_from_file(valid_custom_suite_file)

        with allure.step("Execute register custom suite"):
            register_custom_suite("test_custom", suite_class)

        with allure.step("Verify: suite is in registry"):
            assert "test_custom" in TEST_SUITES_REGISTRY

        with allure.step("Cleanup: remove from registry"):
            del TEST_SUITES_REGISTRY["test_custom"]

    @allure.title("Register custom suite with string path")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_register_with_string_path(self):
        """Should register custom suite using string class path."""
        with allure.step("Execute register with string"):
            register_custom_suite("test_string", "some.module.SomeClass")

        with allure.step("Verify: suite is in registry"):
            assert "test_string" in TEST_SUITES_REGISTRY
            assert TEST_SUITES_REGISTRY["test_string"] == "some.module.SomeClass"

        with allure.step("Cleanup: remove from registry"):
            del TEST_SUITES_REGISTRY["test_string"]
