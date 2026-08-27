# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import importlib
import importlib.util
import inspect
import re
import sys
import uuid
from pathlib import Path

from neco_dq.test_suites.base_test_suite import DQTestSuite

"""
Dynamic registry of DQ suites.

Keys = YAML section names
Values = Fully qualified Python class paths
"""

TEST_SUITES_REGISTRY = {
    "profiling": "neco_dq.test_suites.profiling.profiling_dq_test_suite.SparkProfilingDQTestSuite",
    "bronze": "neco_dq.test_suites.bronze.bronze_dq_test_suite.SparkBronzeDQTestSuite",
    "silver": "neco_dq.test_suites.silver.silver_dq_test_suite.SparkSilverDQTestSuite",
    "gold": "neco_dq.test_suites.gold.gold_dq_test_suite.SparkGoldDQTestSuite",
    "great_expectations": "neco_dq.test_suites.ge.ge_dq_test_suite.GEDQTestSuite",
}


def load_test_suite_from_registry(name: str):
    """
    Dynamically loads a test suite class from the registry.

    Example:
    load_test_suite_from_registry("bronze") ->
        returns SparkBronzeDQTestSuite class
    """

    if name not in TEST_SUITES_REGISTRY:
        raise ValueError(f"Test suite '{name}' is not registered.")

    class_path = TEST_SUITES_REGISTRY[name]

    module_path, class_name = class_path.rsplit(".", 1)

    try:
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except Exception as e:
        raise ImportError(f"Failed to import {class_path}") from e


def load_custom_suite_from_file(file_path: str) -> tuple[str, type]:
    """
    Dynamically load a custom test suite class from a Python file.

    The file must contain exactly one class that extends DQTestSuite.
    The suite name is derived from the class's _suite_config_key property.

    Args:
        file_path: Path to the Python file containing the custom suite.
                   Supports local paths and /Workspace/ paths (Databricks).

    Returns:
        tuple of (suite_name, suite_class)
        - suite_name: The config key for YAML mapping (from _suite_config_key)
        - suite_class: The loaded class

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If no valid DQTestSuite subclass is found
        ImportError: If the module fails to load

    Example:
        suite_name, suite_cls = load_custom_suite_from_file("/Workspace/my_suite.py")
        # suite_name = "my_custom" (from class._suite_config_key)
        # suite_cls = MyCustomSuite class
    """
    path = Path(file_path)

    # For Databricks /Workspace/ paths, we need special handling
    if file_path.startswith("/Workspace/"):
        # In Databricks, /Workspace/ files are accessible via dbutils or SDK
        # For now, we try direct file access which works in some contexts
        if not path.exists():
            raise FileNotFoundError(
                f"Custom suite file not found: {file_path}. For /Workspace/ paths, ensure the file is accessible."
            )
    elif not path.exists():
        raise FileNotFoundError(f"Custom suite file not found: {file_path}")

    # Generate unique module name to avoid conflicts
    module_name = f"custom_suite_{path.stem}_{uuid.uuid4().hex}"

    # Load the module from file
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to create module spec for {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        del sys.modules[module_name]
        raise ImportError(f"Failed to load module from {file_path}: {e}") from e

    # Find all classes that extend DQTestSuite
    suite_classes = []
    for name, obj in inspect.getmembers(module, inspect.isclass):
        # Check if it's a subclass of DQTestSuite but not DQTestSuite itself
        if issubclass(obj, DQTestSuite) and obj is not DQTestSuite and obj.__module__ == module_name:
            suite_classes.append((name, obj))

    if not suite_classes:
        raise ValueError(
            f"No DQTestSuite subclass found in {file_path}. The file must contain a class that extends DQTestSuite."
        )

    if len(suite_classes) > 1:
        class_names = [name for name, _ in suite_classes]
        raise ValueError(
            f"Multiple DQTestSuite subclasses found in {file_path}: {class_names}. "
            "The file must contain exactly one DQTestSuite subclass."
        )

    class_name, suite_cls = suite_classes[0]

    # Derive suite config key from class name
    suite_config_key = _derive_suite_config_key(class_name, path)

    return suite_config_key, suite_cls


def _derive_suite_config_key(class_name: str, file_path: Path) -> str:
    """
    Derive suite config key from class name.

    Converts CamelCase to snake_case and removes common suffixes.
    Falls back to filename if class name doesn't yield a valid key.

    Examples:
        MyCustomSuite -> my_custom
        CustomDQTestSuite -> custom
        MyChecksSuite -> my_checks
    """
    name = class_name
    for suffix in ("DQTestSuite", "TestSuite", "Suite"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break

    # CamelCase to snake_case
    if name:
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

    # Fallback: derive from filename
    return file_path.stem.lower().replace("-", "_")


def register_custom_suite(name: str, class_path_or_class: str | type) -> None:
    """
    Register a custom test suite in the registry.

    Args:
        name: The suite name (used as YAML config key)
        class_path_or_class: Either a fully qualified class path string
                             or the class itself
    """
    if isinstance(class_path_or_class, type):
        # Store as "module.ClassName" format
        class_path = f"{class_path_or_class.__module__}.{class_path_or_class.__name__}"
    else:
        class_path = class_path_or_class

    TEST_SUITES_REGISTRY[name] = class_path
