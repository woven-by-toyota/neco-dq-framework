# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for comparison operators.

These tests validate the operator functions used by compare_metric checks
to compare observed values against expected values.
"""

import allure
import pytest

from neco_dq.core.metrics.operators import (
    OPERATORS,
    _approx_eq,
    _between,
    _eq,
    _ge,
    _gt,
    _in,
    _le,
    _lt,
    _ne,
    _not_in,
    _within_pct,
    compare,
)
from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.BASIC_OPERATORS)
class TestEqualityOperators:
    """Tests for equality operators (_eq, _ne)."""

    @allure.title("Equal operator returns True for equal values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_eq_equal_values(self):
        with allure.step("Verify: Eq Equal Values"):
            assert _eq(42, 42) is True

            assert _eq("hello", "hello") is True

            assert _eq([1, 2, 3], [1, 2, 3]) is True

    @allure.title("Equal operator returns False for different values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_eq_different_values(self):
        with allure.step("Verify: Eq Different Values"):
            assert _eq(42, 43) is False

            assert _eq(42, "42") is False

    @allure.title("Not equal operator returns True for different values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_ne_different_values(self):
        with allure.step("Verify: Ne Different Values"):
            assert _ne(42, 43) is True

            assert _ne(42, "42") is True

    @allure.title("Not equal operator returns False for equal values")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_ne_equal_values(self):
        with allure.step("Verify: Ne Equal Values"):
            assert _ne(42, 42) is False


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.BASIC_OPERATORS)
class TestComparisonOperators:
    """Tests for comparison operators (_gt, _ge, _lt, _le)."""

    @allure.title("Greater than operator works correctly")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_gt(self):
        with allure.step("Verify: Gt"):
            assert _gt(10, 5) is True

            assert _gt(5, 5) is False

            assert _gt(3, 5) is False

    @allure.title("Greater than or equal operator works correctly")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_ge(self):
        with allure.step("Verify: Ge"):
            assert _ge(10, 5) is True

            assert _ge(5, 5) is True

            assert _ge(3, 5) is False

    @allure.title("Less than operator works correctly")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_lt(self):
        with allure.step("Verify: Lt"):
            assert _lt(3, 5) is True

            assert _lt(5, 5) is False

            assert _lt(10, 5) is False

    @allure.title("Less than or equal operator works correctly")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_le(self):
        with allure.step("Verify: Le"):
            assert _le(3, 5) is True

            assert _le(5, 5) is True

            assert _le(10, 5) is False


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.COLLECTION_OPERATORS)
class TestInOperator:
    """Tests for the _in operator with single and multiple values."""

    @allure.title("In operator returns True when single value is in collection")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_in_single_value_found(self):
        with allure.step("Verify: In Single Value Found"):
            assert _in(2, [1, 2, 3]) is True

            assert _in("b", {"a", "b", "c"}) is True

    @allure.title("In operator returns False when single value is not in collection")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_in_single_value_not_found(self):
        with allure.step("Verify: In Single Value Not Found"):
            assert _in(5, [1, 2, 3]) is False

            assert _in("z", {"a", "b", "c"}) is False

    @allure.title("In operator returns True when all list values are in collection")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_in_list_all_found(self):
        with allure.step("Verify: In List All Found"):
            assert _in([1, 2], [1, 2, 3, 4]) is True

            assert _in((1, 3), [1, 2, 3, 4]) is True

    @allure.title("In operator returns False when any list value is missing")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_in_list_some_missing(self):
        with allure.step("Verify: In List Some Missing"):
            assert _in([1, 5], [1, 2, 3, 4]) is False

            assert _in([5, 6], [1, 2, 3, 4]) is False


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.COLLECTION_OPERATORS)
class TestNotInOperator:
    """Tests for the _not_in operator with single and multiple values."""

    @allure.title("Not in operator returns True when single value is not in collection")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_not_in_single_value_missing(self):
        with allure.step("Verify: Not In Single Value Missing"):
            assert _not_in(5, [1, 2, 3]) is True

            assert _not_in("z", {"a", "b", "c"}) is True

    @allure.title("Not in operator returns False when single value is in collection")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_not_in_single_value_found(self):
        with allure.step("Verify: Not In Single Value Found"):
            assert _not_in(2, [1, 2, 3]) is False

            assert _not_in("b", {"a", "b", "c"}) is False

    @allure.title("Not in operator returns True when all list values are missing")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_not_in_list_all_missing(self):
        with allure.step("Verify: Not In List All Missing"):
            assert _not_in([5, 6], [1, 2, 3, 4]) is True

            assert _not_in((7, 8), [1, 2, 3, 4]) is True

    @allure.title("Not in operator returns False when any list value is present")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_not_in_list_some_present(self):
        with allure.step("Verify: Not In List Some Present"):
            assert _not_in([1, 5], [1, 2, 3, 4]) is False

            assert _not_in([1, 2], [1, 2, 3, 4]) is False


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.RANGE_OPERATORS)
class TestBetweenOperator:
    """Tests for the _between operator with bounds validation."""

    @allure.title("Between operator returns True when value is within bounds")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_between_within_bounds(self):
        with allure.step("Verify: Between Within Bounds"):
            assert _between(5, [1, 10]) is True

            assert _between(1, [1, 10]) is True

            assert _between(10, [1, 10]) is True

    @allure.title("Between operator returns False when value is outside bounds")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_between_outside_bounds(self):
        with allure.step("Verify: Between Outside Bounds"):
            assert _between(0, [1, 10]) is False

            assert _between(11, [1, 10]) is False

    @allure.title("Between operator works with tuple bounds")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_between_with_tuple(self):
        with allure.step("Verify: Between With Tuple"):
            assert _between(5, (1, 10)) is True

    @allure.title("Between operator raises ValueError for invalid bounds")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_between_invalid_bounds(self):
        with allure.step("Verify: Between Invalid Bounds"):
            with pytest.raises(ValueError, match="exactly 2 elements"):
                _between(5, 10)

            with pytest.raises(ValueError, match="exactly 2 elements"):
                _between(5, [1])

            with pytest.raises(ValueError, match="exactly 2 elements"):
                _between(5, [1, 5, 10])


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.TOLERANCE_OPERATORS)
class TestWithinPctOperator:
    """Tests for the _within_pct operator with percentage tolerance."""

    @allure.title("Within percentage returns True when within tolerance")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_within_pct_success(self):
        with allure.step("Verify: Within Pct Success"):
            assert _within_pct(100.5, 100) is True

            assert _within_pct(105, 100, pct=5.0) is True

            assert _within_pct(100, 100) is True

    @allure.title("Within percentage returns False when outside tolerance")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_within_pct_failure(self):
        with allure.step("Verify: Within Pct Failure"):
            assert _within_pct(102, 100) is False

            assert _within_pct(110, 100, pct=5.0) is False

    @allure.title("Within percentage handles zero expected value")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_within_pct_zero_expected(self):
        with allure.step("Verify: Within Pct Zero Expected"):
            assert _within_pct(0, 0) is True

            assert _within_pct(0.001, 0) is False

    @allure.title("Within percentage handles TypeError gracefully")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_within_pct_type_error(self):
        with allure.step("Verify: Within Pct Type Error"):
            assert _within_pct("a", "a") is True

            assert _within_pct("a", "b") is False


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.TOLERANCE_OPERATORS)
class TestApproxEqOperator:
    """Tests for the _approx_eq (~) operator with absolute tolerance."""

    @allure.title("Approximate equal returns True when within tolerance")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_approx_eq_success(self):
        with allure.step("Verify: Approx Eq Success"):
            assert _approx_eq(1.0000001, 1.0) is True

            assert _approx_eq(1.1, 1.0, tol=0.2) is True

            assert _approx_eq(1.0, 1.0) is True

    @allure.title("Approximate equal returns False when outside tolerance")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_approx_eq_failure(self):
        with allure.step("Verify: Approx Eq Failure"):
            assert _approx_eq(1.001, 1.0) is False

            assert _approx_eq(1.5, 1.0, tol=0.2) is False

    @allure.title("Approximate equal handles TypeError gracefully")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_approx_eq_type_error(self):
        with allure.step("Verify: Approx Eq Type Error"):
            assert _approx_eq("a", "a") is True

            assert _approx_eq("a", "b") is False


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.COMPARE_DISPATCHER)
class TestCompareDispatcher:
    """Tests for the main compare() function that dispatches to operators."""

    @allure.title("Compare dispatches to correct operator")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_compare_basic_operators(self):
        with allure.step("Verify: Compare Basic Operators"):
            assert compare(5, 5, "==") is True
            assert compare(5, 6, "==") is False

            assert compare(5, 6, "!=") is True
            assert compare(5, 5, "!=") is False

            assert compare(10, 5, ">") is True

            assert compare(5, 5, ">=") is True

            assert compare(3, 5, "<") is True

            assert compare(5, 5, "<=") is True

    @allure.title("Compare handles collection operators")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_compare_collection_operators(self):
        with allure.step("Verify: Compare Collection Operators"):
            assert compare(2, [1, 2, 3], "in") is True

            assert compare(5, [1, 2, 3], "not_in") is True

            assert compare(5, [1, 10], "between") is True

    @allure.title("Compare passes tolerance to approx_eq operator")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_compare_approx_with_tolerance(self):
        with allure.step("Verify: Compare Approx With Tolerance"):
            assert compare(1.0000001, 1.0, "~") is True

            assert compare(1.1, 1.0, "~", tolerance=0.2) is True
            assert compare(1.5, 1.0, "~", tolerance=0.2) is False

    @allure.title("Compare passes tolerance to within_pct operator")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_compare_within_pct_with_tolerance(self):
        with allure.step("Verify: Compare Within Pct With Tolerance"):
            assert compare(100.5, 100, "within_pct") is True

            assert compare(110, 100, "within_pct", tolerance=15.0) is True
            assert compare(120, 100, "within_pct", tolerance=15.0) is False

    @allure.title("Compare raises ValueError for unsupported operator")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_compare_unsupported_operator(self):
        with allure.step("Verify: Compare Unsupported Operator"):
            with pytest.raises(ValueError, match="unsupported_operator"):
                compare(1, 2, "invalid_op")


@allure.feature(AllureFeature.OPERATORS)
@allure.story(AllureStory.OPERATORS_REGISTRY)
class TestOperatorsRegistry:
    """Tests for the OPERATORS registry dictionary."""

    @allure.title("OPERATORS registry contains all expected operators")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_operators_registry_completeness(self):
        with allure.step("Setup test data"):
            expected_operators = {"==", "!=", ">", ">=", "<", "<=", "in", "not_in", "between", "within_pct", "~"}

        with allure.step("Verify: Operators Registry Completeness"):
            assert set(OPERATORS.keys()) == expected_operators

    @allure.title("OPERATORS registry values are callable")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_operators_registry_callable(self):
        with allure.step("Verify: Operators Registry Callable"):
            for name, func in OPERATORS.items():
                assert callable(func), f"Operator {name} is not callable"
