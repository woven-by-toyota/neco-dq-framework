# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any
from collections.abc import Callable


def _eq(a, b) -> bool:
    return a == b


def _ne(a, b) -> bool:
    return a != b


def _gt(a, b) -> bool:
    return a > b


def _ge(a, b) -> bool:
    return a >= b


def _lt(a, b) -> bool:
    return a < b


def _le(a, b) -> bool:
    return a <= b


def _in(a, b) -> bool:
    """Check that value(s) are in the given collection."""
    if isinstance(a, (list, tuple)):
        return all(v in b for v in a)
    return a in b


def _not_in(a, b) -> bool:
    """Check that value(s) are NOT in the given collection."""
    if isinstance(a, (list, tuple)):
        return all(v not in b for v in a)
    return a not in b


def _between(a, b) -> bool:
    """Check that a is between b[0] and b[1] (inclusive)."""
    required_bounds = 2
    if not isinstance(b, (list, tuple)) or len(b) != required_bounds:
        raise ValueError(f"between operator requires a list/tuple of exactly {required_bounds} elements [min, max]")
    return b[0] <= a <= b[1]


def _within_pct(a, b, pct: float = 1.0) -> bool:
    """Check that a is within pct% of b (default 1%)."""
    try:
        if b == 0:
            return a == 0
        diff_pct = abs(a - b) / abs(b) * 100
        return diff_pct <= pct
    except TypeError:
        return a == b


def _approx_eq(a, b, tol: float = 1e-6) -> bool:
    try:
        return abs(a - b) <= tol
    except TypeError:
        return a == b


OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "==": _eq,
    "!=": _ne,
    ">": _gt,
    ">=": _ge,
    "<": _lt,
    "<=": _le,
    "in": _in,
    "not_in": _not_in,
    "between": _between,
    "within_pct": _within_pct,
    "~": _approx_eq,
}


def compare(
    observed: Any,
    expected: Any,
    operator: str,
    tolerance: float | None = None,
) -> bool:
    if operator not in OPERATORS:
        raise ValueError(f"unsupported_operator={operator}")

    op_fn = OPERATORS[operator]

    # Pass tolerance to operators that support it
    if operator == "~" and tolerance is not None:
        return op_fn(observed, expected, tol=tolerance)
    if operator == "within_pct" and tolerance is not None:
        return op_fn(observed, expected, pct=tolerance)

    return op_fn(observed, expected)
