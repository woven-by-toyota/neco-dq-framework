# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Assertion helpers for DQ check results.

This module provides standardized assertion functions for verifying
DQ check results (success, value, column, details).

Usage:
    from tests.utils.assertions import assert_check_success, assert_value_equals

    def test_example():
        result = some_dq_function()
        assert_check_success(result)
        assert_value_equals(result, expected=42)
"""

from typing import Any

import allure
import pytest

# Type alias for DQ check result tuple
DQResult = tuple[bool, Any, str | None, str]


@allure.step("Assert check succeeded")
def assert_check_success(result: DQResult, message: str = "") -> None:
    """Assert that a DQ check succeeded.

    Args:
        result: Tuple of (success, value, column, details)
        message: Optional custom error message

    Raises:
        AssertionError: If success is not True
    """
    success, _value, _column, details = result
    error_msg = f"{message}\nExpected success=True but got success={success}\nDetails: {details}"
    assert success is True, error_msg


@allure.step("Assert check failed")
def assert_check_failed(result: DQResult, message: str = "") -> None:
    """Assert that a DQ check failed.

    Args:
        result: Tuple of (success, value, column, details)
        message: Optional custom error message

    Raises:
        AssertionError: If success is not False
    """
    success, _value, _column, details = result
    error_msg = f"{message}\nExpected success=False but got success={success}\nDetails: {details}"
    assert success is False, error_msg


@allure.step("Assert value equals: {expected}")
def assert_value_equals(result: DQResult, expected: Any, message: str = "") -> None:
    """Assert that the check result value equals expected.

    Args:
        result: Tuple of (success, value, column, details)
        expected: Expected value
        message: Optional custom error message

    Raises:
        AssertionError: If values don't match
    """
    _success, value, _column, details = result
    error_msg = f"{message}\nExpected value: {expected}\nActual value: {value}\nDetails: {details}"
    assert value == expected, error_msg


@allure.step("Assert value approximately equals: {expected}")
def assert_value_approx(
    result: DQResult,
    expected: float,
    rel: float | None = None,
    abs_tol: float | None = None,
    message: str = "",
) -> None:
    """Assert that the check result value approximately equals expected.

    Uses pytest.approx for floating point comparison.

    Args:
        result: Tuple of (success, value, column, details)
        expected: Expected value
        rel: Relative tolerance (default: 1e-6)
        abs_tol: Absolute tolerance
        message: Optional custom error message

    Raises:
        AssertionError: If values don't approximately match
    """
    _success, value, _column, details = result
    error_msg = f"{message}\nExpected value ~= {expected}\nActual value: {value}\nDetails: {details}"
    assert value == pytest.approx(expected, rel=rel, abs=abs_tol), error_msg


@allure.step("Assert details contain: {substring}")
def assert_details_contain(result: DQResult, substring: str, message: str = "") -> None:
    """Assert that the check details contain a substring.

    Args:
        result: Tuple of (success, value, column, details)
        substring: Substring to search for in details
        message: Optional custom error message

    Raises:
        AssertionError: If substring not found in details
    """
    _success, _value, _column, details = result
    error_msg = f"{message}\nExpected '{substring}' in details\nActual details: {details}"
    assert substring in details, error_msg


@allure.step("Assert column equals: {expected}")
def assert_column_equals(result: DQResult, expected: str | None, message: str = "") -> None:
    """Assert that the check result column equals expected.

    Args:
        result: Tuple of (success, value, column, details)
        expected: Expected column name (or None)
        message: Optional custom error message

    Raises:
        AssertionError: If column doesn't match
    """
    _success, _value, column, details = result
    error_msg = f"{message}\nExpected column: {expected}\nActual column: {column}\nDetails: {details}"
    assert column == expected, error_msg


@allure.step("Assert result is dict with keys")
def assert_result_is_grouped(result: DQResult, expected_keys: list | None = None, message: str = "") -> None:
    """Assert that the check result value is a grouped dict.

    Args:
        result: Tuple of (success, value, column, details)
        expected_keys: Optional list of expected keys
        message: Optional custom error message

    Raises:
        AssertionError: If value is not a dict or keys don't match
    """
    _success, value, _column, details = result
    error_msg = f"{message}\nExpected dict value\nActual value type: {type(value)}\nDetails: {details}"
    assert isinstance(value, dict), error_msg

    if expected_keys is not None:
        actual_keys = set(value.keys())
        expected_set = set(expected_keys)
        key_error_msg = f"{message}\nExpected keys: {expected_set}\nActual keys: {actual_keys}"
        assert actual_keys == expected_set, key_error_msg


@allure.step("Assert grouped value equals: {key} = {expected}")
def assert_grouped_value_equals(result: DQResult, key: tuple, expected: Any, message: str = "") -> None:
    """Assert that a grouped result has expected value for a key.

    Args:
        result: Tuple of (success, value, column, details)
        key: The group key (usually a tuple)
        expected: Expected value for that key
        message: Optional custom error message

    Raises:
        AssertionError: If key not found or value doesn't match
    """
    _success, value, _column, details = result
    assert isinstance(value, dict), f"Expected dict value, got {type(value)}"
    assert key in value, f"Key {key} not found in result. Available keys: {list(value.keys())}"

    actual = value[key]
    error_msg = f"{message}\nFor key {key}:\nExpected: {expected}\nActual: {actual}\nDetails: {details}"
    assert actual == expected, error_msg
