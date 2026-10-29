# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Alias resolution, deprecation collection, and unknown-key validation (§6/§7).

These exercise ``DQTestSuite._resolve_and_validate_cfg`` directly through a
minimal concrete stub — no Spark session required, since the hook only reads
config and the schema.
"""

import logging
from typing import ClassVar

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


# --------------------------------------------------------------------------
# Nested aliases must not mutate shared config (§6)
# --------------------------------------------------------------------------


class _GoldStub(_StubSuite):
    @property
    def _suite_name(self) -> str:
        return "gold"

    @property
    def _suite_config_key(self) -> str:
        return "gold"


def test_nested_alias_does_not_mutate_input_cfg():
    suite = _GoldStub()
    shared = {"metric": "count", "history": {"tolerance_pct": 5}}
    resolved = suite._resolve_and_validate_cfg("volume", shared)
    assert resolved["history"] == {"tolerance_percent": 5}
    assert shared == {"metric": "count", "history": {"tolerance_pct": 5}}


def test_nested_alias_on_shared_defaults_counts_every_table():
    suite = _GoldStub()
    shared = {"metric": "count", "history": {"tolerance_pct": 5}}
    suite._current_table_fq = "cat.sch.t1"
    suite._resolve_and_validate_cfg("volume", shared)
    suite._current_table_fq = "cat.sch.t2"
    suite._resolve_and_validate_cfg("volume", shared)

    deps = suite.get_deprecations()
    assert deps == [
        {"check": "volume", "old": "history.tolerance_pct", "new": "history.tolerance_percent", "table_count": 2}
    ]


# --------------------------------------------------------------------------
# Policy propagation and fatal config errors through the orchestrator (§7)
# --------------------------------------------------------------------------


class _RecordingSuite:
    """Stands in for a registered suite class; records the config it got."""

    created: ClassVar[list[dict]] = []

    def __init__(self, spark, cfg):
        self.cfg = cfg
        _RecordingSuite.created.append(cfg)

    def set_profile_context(self, *_args):
        pass

    def set_tags_filter(self, *_args):
        pass


def test_orchestrator_passes_dq_policy_to_suites(monkeypatch):
    from neco_dq.core.orchestrators import dq_orchestrator

    _RecordingSuite.created = []
    monkeypatch.setattr(dq_orchestrator, "load_test_suite_from_registry", lambda _name: _RecordingSuite)
    policy = {"unknown_check_params": "error"}
    config = {"dq_policy": policy, "silver": {"tables": [{"catalog": "c", "schema": "s", "table": "t"}]}}

    dq_orchestrator.DQOrchestrator(config).initialize_test_suites_from_config(spark=None)

    assert len(_RecordingSuite.created) == 1
    assert _RecordingSuite.created[0]["dq_policy"] == policy


def test_suite_built_by_orchestrator_honors_error_policy(monkeypatch):
    from neco_dq.core.orchestrators import dq_orchestrator

    class _Silver(_SilverStub):
        def __init__(self, _spark, cfg):
            super().__init__(cfg)

    monkeypatch.setattr(dq_orchestrator, "load_test_suite_from_registry", lambda _name: _Silver)
    config = {
        "dq_policy": {"unknown_check_params": "error"},
        "silver": {"tables": [{"catalog": "c", "schema": "s", "table": "t"}]},
    }
    orch = dq_orchestrator.DQOrchestrator(config)
    orch.initialize_test_suites_from_config(spark=None)

    with pytest.raises(ValueError, match="unknown parameter 'bogus'"):
        orch.engines["silver"]._resolve_and_validate_cfg("idempotency", {"key_columns": ["id"], "bogus": 1})


class _RaisingEngine:
    def __init__(self, exc: Exception):
        self._exc = exc

    def run(self):
        raise self._exc


def test_config_error_in_suite_is_fatal_for_run():
    from neco_dq.core.orchestrators.dq_orchestrator import DQOrchestrator
    from neco_dq.core.registry.check_param_schema import CheckConfigError

    orch = DQOrchestrator({})
    orch.register_engine("silver", _RaisingEngine(CheckConfigError("[silver] check 'idempotency': bad")))

    with pytest.raises(CheckConfigError, match="check 'idempotency'"):
        orch.run_all()


def test_error_policy_raises_check_config_error():
    from neco_dq.core.registry.check_param_schema import CheckConfigError

    suite = _SilverStub({"dq_policy": {"unknown_check_params": "error"}})
    with pytest.raises(CheckConfigError):
        suite._resolve_and_validate_cfg("idempotency", {"key_columns": ["id"], "bogus": 1})


def test_generic_suite_crash_still_reported_not_raised():
    from neco_dq.core.orchestrators.dq_orchestrator import DQOrchestrator

    orch = DQOrchestrator({})
    orch.register_engine("silver", _RaisingEngine(RuntimeError("boom")))

    summary = orch.run_all()
    assert summary["silver"]["status"] == "system_error"


# --------------------------------------------------------------------------
# Check identity stays stable for legacy configs (§6)
# --------------------------------------------------------------------------


@pytest.mark.spark
def test_check_ids_unchanged_for_legacy_multi_instance_config(spark, monkeypatch):
    from neco_dq.test_suites.silver.silver_dq_test_suite import SparkSilverDQTestSuite

    suite = SparkSilverDQTestSuite(spark, {"silver": {"tables": [], "default_checks": {}}})
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "code"])
    monkeypatch.setattr(suite, "_read_table", lambda *_a: df)

    table_cfg = {
        "catalog": "c",
        "schema": "s",
        "table": "t",
        "checks": {
            "idempotency": [
                {"enabled": True, "key_column": ["id"]},
                {"enabled": True, "key_column": ["id", "code"]},
            ]
        },
    }
    result = suite.validate_table(table_cfg)

    names = {c["check_name"] for c in result["checks"]}
    assert "idempotency[key_column=id]" in names
    assert "idempotency[key_column=id,code]" in names


# --------------------------------------------------------------------------
# Underscore-prefixed metadata keys (profiling suggestions) are not params
# --------------------------------------------------------------------------


def test_underscore_metadata_keys_not_flagged(caplog):
    suite = _SilverStub()
    cfg = {"key_columns": ["id"], "_confidence": "high", "_reason": "PK detected", "_comment": "review"}
    with caplog.at_level(logging.WARNING, logger="neco_dq"):
        suite._resolve_and_validate_cfg("idempotency", cfg)
    assert caplog.records == []


def test_underscore_metadata_keys_allowed_in_error_mode():
    suite = _SilverStub({"dq_policy": {"unknown_check_params": "error"}})
    resolved = suite._resolve_and_validate_cfg("idempotency", {"key_columns": ["id"], "_reason": "PK detected"})
    assert resolved["_reason"] == "PK detected"
