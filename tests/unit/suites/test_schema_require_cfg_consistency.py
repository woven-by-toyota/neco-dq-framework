# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Schema-vs-code consistency: every required key is an accepted schema key.

Statically scans each ``_check_<name>`` method for the string keys passed to
``_require_cfg(cfg, ...)`` and asserts each one is in ``allowed_keys(suite, name)``
for the suite(s) that expose the check. This catches drift where a check starts
requiring a key the schema does not declare — which would make the validation
hook flag a valid config as an unknown key.
"""

import ast
from pathlib import Path

import pytest

from neco_dq.core.registry.check_param_schema import allowed_keys, get_check_schema

_ROOT = Path(__file__).resolve().parents[3] / "neco_dq" / "test_suites"

# Suite files map to exactly one suite.
_SUITE_FILES = {
    "bronze": _ROOT / "bronze" / "bronze_dq_test_suite.py",
    "silver": _ROOT / "silver" / "silver_dq_test_suite.py",
    "gold": _ROOT / "gold" / "gold_dq_test_suite.py",
}
# Shared mixins are exposed by whichever suites list the check in the schema.
_SHARED_DIR = _ROOT / "shared_checks"
_ALL_SUITES = ("bronze", "silver", "gold")


def _require_cfg_keys(func: ast.FunctionDef) -> list[str]:
    """Literal string keys passed to ``_require_cfg(cfg, "k1", "k2", ...)``."""
    keys: list[str] = []
    for node in ast.walk(func):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
        if name != "_require_cfg":
            continue
        for arg in node.args[1:]:  # skip the leading cfg argument
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                keys.append(arg.value)
    return keys


def _check_methods(path: Path):
    """Yield (check_name, func_node) for each ``_check_<name>`` def in the file."""
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("_check_"):
            yield node.name[len("_check_") :], node


def _build_cases():
    """(suite, check_name, required_keys) tuples to validate."""
    cases = []
    # Suite-local checks: validate against that one suite.
    for suite, path in _SUITE_FILES.items():
        for check_name, func in _check_methods(path):
            keys = _require_cfg_keys(func)
            if keys and get_check_schema(suite, check_name) is not None:
                cases.append((suite, check_name, tuple(sorted(set(keys)))))
    # Shared checks: validate against every suite that exposes them.
    for path in sorted(_SHARED_DIR.glob("*.py")):
        for check_name, func in _check_methods(path):
            keys = _require_cfg_keys(func)
            if not keys:
                continue
            for suite in _ALL_SUITES:
                if get_check_schema(suite, check_name) is not None:
                    cases.append((suite, check_name, tuple(sorted(set(keys)))))
    return cases


_CASES = _build_cases()


def test_consistency_cases_discovered():
    """Guard against the AST scan silently matching nothing."""
    assert _CASES, "no _require_cfg cases discovered — scan is broken"


@pytest.mark.parametrize(
    ("suite", "check_name", "required_keys"),
    _CASES,
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_required_keys_are_allowed_by_schema(suite, check_name, required_keys):
    allowed = allowed_keys(suite, check_name)
    assert allowed is not None, f"{suite}.{check_name} missing from schema"
    missing = sorted(k for k in required_keys if k not in allowed)
    assert not missing, (
        f"{suite}.{check_name}: _require_cfg keys {missing} are not declared in the "
        f"schema's allowed keys {sorted(allowed)} — the validation hook would reject them"
    )
