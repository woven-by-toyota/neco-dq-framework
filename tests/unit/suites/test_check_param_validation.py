# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Alias resolution, deprecation collection, and unknown-key validation (§6/§7).

These exercise ``DQTestSuite._resolve_and_validate_cfg`` directly through a
minimal concrete stub — no Spark session required, since the hook only reads
config and the schema.
"""

import logging

import pytest

from neco_dq.test_suites.base_test_suite import DQTestSuite


class _StubSuite(DQTestSuite):
    """Minimal concrete suite exposing only what the validation hook touches."""

    def __init__(self, cfg: dict | None = None):
        self.cfg = cfg or {}
        self._current_table_fq = "cat.sch.tbl"
        self._deprecations_log = None


class _SilverStub(_StubSuite):
    @property
    def _suite_name(self) -> str:
        return "silver"

    @property
    def _suite_config_key(self) -> str:
        return "silver"


# --------------------------------------------------------------------------
# Alias resolution + deprecation collection (§6)
# --------------------------------------------------------------------------


def test_alias_renames_old_key_to_canonical():
    suite = _SilverStub()
    resolved = suite._resolve_and_validate_cfg("idempotency", {"key_column": ["id"]})
    assert resolved["key_columns"] == ["id"]
    assert "key_column" not in resolved


def test_alias_prefers_new_key_when_both_present():
    suite = _SilverStub()
    resolved = suite._resolve_and_validate_cfg("idempotency", {"key_column": ["old"], "key_columns": ["new"]})
    assert resolved["key_columns"] == ["new"]


def test_deprecation_collected_once_per_table_with_count():
    suite = _SilverStub()
    # Same deprecated key seen on two distinct tables -> table_count == 2.
    suite._current_table_fq = "cat.sch.t1"
    suite._resolve_and_validate_cfg("idempotency", {"key_column": ["id"]})
    suite._current_table_fq = "cat.sch.t2"
    suite._resolve_and_validate_cfg("idempotency", {"key_column": ["id"]})

    deps = suite.get_deprecations()
    assert len(deps) == 1
    assert deps[0] == {"check": "idempotency", "old": "key_column", "new": "key_columns", "table_count": 2}


def test_no_deprecation_when_only_canonical_key_used():
    suite = _SilverStub()
    suite._resolve_and_validate_cfg("idempotency", {"key_columns": ["id"]})
    assert suite.get_deprecations() == []


# --------------------------------------------------------------------------
# Unknown-key validation strictness (§7)
# --------------------------------------------------------------------------


def test_unknown_key_warns_by_default(caplog):
    suite = _SilverStub()  # no dq_policy -> defaults to "warn"
    with caplog.at_level(logging.WARNING, logger="neco_dq"):
        resolved = suite._resolve_and_validate_cfg("idempotency", {"key_columns": ["id"], "kye_columns": ["x"]})
    assert "kye_columns" in resolved  # warn does not strip the key
    assert any("unknown parameter 'kye_columns'" in r.message for r in caplog.records)


def test_unknown_key_warning_suggests_closest_key(caplog):
    suite = _SilverStub()
    with caplog.at_level(logging.WARNING, logger="neco_dq"):
        suite._resolve_and_validate_cfg("idempotency", {"key_columns": ["id"], "kye_columns": ["x"]})
    assert any("did you mean 'key_columns'?" in r.message for r in caplog.records)


def test_unknown_key_errors_when_policy_is_error():
    suite = _SilverStub({"dq_policy": {"unknown_check_params": "error"}})
    with pytest.raises(ValueError, match="unknown parameter 'bogus'"):
        suite._resolve_and_validate_cfg("idempotency", {"key_columns": ["id"], "bogus": 1})


def test_unknown_key_ignored_when_policy_is_ignore(caplog):
    suite = _SilverStub({"dq_policy": {"unknown_check_params": "ignore"}})
    with caplog.at_level(logging.WARNING, logger="neco_dq"):
        suite._resolve_and_validate_cfg("idempotency", {"key_columns": ["id"], "bogus": 1})
    assert caplog.records == []


def test_invalid_policy_value_falls_back_to_warn():
    suite = _SilverStub({"dq_policy": {"unknown_check_params": "nonsense"}})
    assert suite._unknown_check_params_mode == "warn"


def test_forbidden_key_warns(caplog):
    # flow_completeness forbids target_* keys (current table is implicit target).
    suite = _SilverStub()
    with caplog.at_level(logging.WARNING, logger="neco_dq"):
        suite._resolve_and_validate_cfg(
            "flow_completeness",
            {"source_schema": "s", "source_table": "t", "join_columns": ["id"], "target_table": "x"},
        )
    assert any("'target_table' is not allowed" in r.message for r in caplog.records)


def test_forbidden_key_errors_when_policy_is_error():
    suite = _SilverStub({"dq_policy": {"unknown_check_params": "error"}})
    with pytest.raises(ValueError, match="'target_table' is not allowed"):
        suite._resolve_and_validate_cfg(
            "flow_completeness",
            {"source_schema": "s", "source_table": "t", "join_columns": ["id"], "target_table": "x"},
        )


def test_check_absent_from_schema_is_not_validated(caplog):
    suite = _SilverStub()
    with caplog.at_level(logging.WARNING, logger="neco_dq"):
        resolved = suite._resolve_and_validate_cfg("not_a_real_check", {"whatever": 1})
    assert resolved == {"whatever": 1}
    assert caplog.records == []
