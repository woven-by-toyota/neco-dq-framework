# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import allure
import pytest
import yaml

from neco_dq.config.config_resolver import resolve_config
from neco_dq.config.profile_loader import build_context
from tests.constants import AllureFeature, AllureSeverity, AllureStory, AllureTag

pytestmark = pytest.mark.spark


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_MODULAR_OUTPUT)
class TestModularOutputHelpers:
    @allure.title("_write_text_file writes content to a local path")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_write_text_file_local(self, make_suite, tmp_path):
        """Given a target path in a non-existent subdirectory, when _write_text_file is called,
        then the file is created with the exact content provided."""
        suite = make_suite({})
        target = tmp_path / "sub" / "out.yaml"
        suite._write_text_file(str(target), "hello: world\n")
        assert target.read_text() == "hello: world\n"


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_MODULAR_OUTPUT)
class TestCatalogParameterization:
    @allure.title("parameterize_catalog replaces catalog with Jinja placeholder")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_parameterize_catalog_true(self, make_suite):
        """Given a table config with a real catalog name, when _parameterize_table_cfg is called
        with parameterize_catalog=True, then the catalog is replaced with the Jinja placeholder
        without mutating the original dict."""
        suite = make_suite({})
        cfg = {"catalog": "analytics_prod", "schema": "bronze", "table": "orders"}
        result = suite._parameterize_table_cfg(cfg, parameterize_catalog=True)
        assert result["catalog"] == "{{ profile.target_catalog }}"
        assert result["schema"] == "bronze"
        assert result["table"] == "orders"
        # original not mutated
        assert cfg["catalog"] == "analytics_prod"

    @allure.title("parameterize_catalog False leaves catalog untouched")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_parameterize_catalog_false(self, make_suite):
        """Given a table config with a real catalog name, when _parameterize_table_cfg is called
        with parameterize_catalog=False, then the catalog field is left unchanged."""
        suite = make_suite({})
        cfg = {"catalog": "analytics_prod", "schema": "bronze", "table": "orders"}
        result = suite._parameterize_table_cfg(cfg, parameterize_catalog=False)
        assert result["catalog"] == "analytics_prod"


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_MODULAR_OUTPUT)
class TestModularConfigWriter:
    """Tests for _write_suggest_to_modular_config — writes modular config directory tree."""

    _SUGGEST_RESULTS: ClassVar[list] = [
        {
            "test_suite": "profiling",
            "mode": "suggest",
            "catalog": "analytics_dev",
            "schema": "bronze",
            "table": "orders",
            "row_count": 100,
            "suggestions": {
                "table": "analytics_dev.bronze.orders",
                "row_count": 100,
                "column_count": 3,
                "bronze": {
                    "catalog": "analytics_dev",
                    "schema": "bronze",
                    "table": "orders",
                    "checks": {
                        "record_count": {"enabled": True},
                        "mandatory_fields": {"enabled": True, "columns": ["id"]},
                    },
                },
                "silver": {
                    "catalog": "analytics_dev",
                    "schema": "bronze",
                    "table": "orders",
                    "checks": {
                        "idempotency": {"enabled": True, "key_columns": ["id"]},
                    },
                },
                "great_expectations": {
                    "catalog": "analytics_dev",
                    "schema": "bronze",
                    "table": "orders",
                    "checks": {
                        "id": {"dtype": {"values": ["int"], "severity": "error"}},
                    },
                },
            },
        }
    ]

    def _default_options(self, **overrides) -> dict:
        opts = {
            "include_profiles": False,
            "include_policy": True,
            "parameterize_catalog": False,
            "overwrite": False,
            "dry_run": False,
        }
        opts.update(overrides)
        return opts

    @allure.title("policy.yaml is written and contains dq_policy key")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_policy_yaml_written(self, make_suite, tmp_path):
        """Given a suggest results list, when the modular config writer runs,
        then global/policy.yaml is created with dq_policy and block_test_suites keys."""
        suite = make_suite({})
        suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), self._default_options())
        policy_file = tmp_path / "global" / "policy.yaml"
        assert policy_file.exists()
        loaded = yaml.safe_load(policy_file.read_text())
        assert "dq_policy" in loaded
        assert "block_test_suites" in loaded["dq_policy"]
        assert loaded["dq_policy"]["block_test_suites"]["bronze"] is True
        assert loaded["dq_policy"]["block_test_suites"]["profiling"] is False

    @allure.title("suite.yaml is written per suite with suite key")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_suite_yaml_written(self, make_suite, tmp_path):
        """Given a suggest results list with bronze and silver sections, when the modular config writer runs,
        then a suite.yaml is created under each suite directory with the correct suite key."""
        suite = make_suite({})
        suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), self._default_options())
        bronze_suite_yaml = tmp_path / "suites" / "bronze" / "suite.yaml"
        assert bronze_suite_yaml.exists()
        loaded = yaml.safe_load(bronze_suite_yaml.read_text())
        assert loaded["suite"] == "bronze"

        silver_suite_yaml = tmp_path / "suites" / "silver" / "suite.yaml"
        assert silver_suite_yaml.exists()
        loaded_silver = yaml.safe_load(silver_suite_yaml.read_text())
        assert loaded_silver["suite"] == "silver"

    @allure.title("test_groups file is written with tables list containing catalog/schema/table/checks")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_test_groups_yaml_written(self, make_suite, tmp_path):
        """Given a suggest results list, when the modular config writer runs, then a test_groups yaml
        is created with a tables list containing the correct catalog, schema, table, and checks."""
        suite = make_suite({})
        suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), self._default_options())
        safe_name = "orders"  # v1.2.2: modular file named by table only (catalog/schema implied by dir layout)
        tg_file = tmp_path / "suites" / "bronze" / "test_groups" / f"{safe_name}.yaml"
        assert tg_file.exists()
        loaded = yaml.safe_load(tg_file.read_text())
        assert "tables" in loaded
        assert isinstance(loaded["tables"], list)
        assert len(loaded["tables"]) == 1
        entry = loaded["tables"][0]
        assert entry["catalog"] == "analytics_dev"
        assert entry["schema"] == "bronze"
        assert entry["table"] == "orders"
        assert "checks" in entry
        assert entry["checks"]["record_count"]["enabled"] is True

    @allure.title("GE config written under owning suite great_expectations dir")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_ge_yaml_written_under_owning_suite(self, make_suite, tmp_path):
        """Given a suggest results list with a great_expectations section, when the modular config writer runs,
        then the GE config is placed under the owning suite's great_expectations directory."""
        suite = make_suite({})
        suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), self._default_options())
        safe_name = "orders"  # v1.2.2: modular file named by table only (catalog/schema implied by dir layout)
        # silver is present → GE goes under silver
        ge_file = tmp_path / "suites" / "silver" / "great_expectations" / f"{safe_name}.yaml"
        assert ge_file.exists()
        loaded = yaml.safe_load(ge_file.read_text())
        assert "catalog" in loaded
        assert loaded["catalog"] == "analytics_dev"
        assert "checks" in loaded

    @allure.title("dry_run returns paths without writing any files")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_dry_run_returns_paths_no_files(self, make_suite, tmp_path):
        """Given dry_run=True, when the modular config writer runs,
        then a non-empty path list is returned but no files or directories are created on disk."""
        suite = make_suite({})
        paths = suite._write_suggest_to_modular_config(
            self._SUGGEST_RESULTS, str(tmp_path), self._default_options(dry_run=True)
        )
        assert isinstance(paths, list)
        assert len(paths) > 0
        # No files should be written
        assert not (tmp_path / "global").exists()
        assert not (tmp_path / "suites").exists()
        # Returned list must include the policy path (regression guard)
        assert any(p.replace("\\", "/").endswith("global/policy.yaml") for p in paths)

    @allure.title("overwrite=False skips files that already exist")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_overwrite_false_skips_existing(self, make_suite, tmp_path):
        """Given a file already written by the modular config writer,
        when the writer is called again with overwrite=False, then the existing file content is preserved."""
        suite = make_suite({})
        options = self._default_options(overwrite=False)
        # Write once
        suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), options)
        policy_file = tmp_path / "global" / "policy.yaml"

        # Manually change the file content
        policy_file.write_text("dq_policy: {tampered: true}\n")
        # Write again with overwrite=False — should skip
        suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), options)
        assert policy_file.read_text() == "dq_policy: {tampered: true}\n"

    @allure.title("overwrite=True replaces existing files")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_overwrite_true_replaces_existing(self, make_suite, tmp_path):
        """Given a file already written by the modular config writer,
        when the writer is called again with overwrite=True, then the file is replaced with fresh dq_policy content."""
        suite = make_suite({})
        options_no_overwrite = self._default_options(overwrite=False)
        options_overwrite = self._default_options(overwrite=True)
        # Write once
        suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), options_no_overwrite)
        policy_file = tmp_path / "global" / "policy.yaml"
        policy_file.write_text("dq_policy: {tampered: true}\n")
        # Write again with overwrite=True — should overwrite
        suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), options_overwrite)
        loaded = yaml.safe_load(policy_file.read_text())
        assert "block_test_suites" in loaded["dq_policy"]

    @allure.title("results with error key are skipped")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE, AllureTag.SPARK)
    def test_error_results_skipped(self, make_suite, tmp_path):
        """Given a results list where an entry carries an error key, when the modular config writer runs,
        then no suite yaml files are written for that entry."""
        suite = make_suite({})
        results_with_error = [{"error": "table not found", "catalog": "x", "schema": "y", "table": "z"}]
        suite._write_suggest_to_modular_config(results_with_error, str(tmp_path), self._default_options())
        # Only policy.yaml would be written (no suite files from errored result)
        written_suites = list((tmp_path / "suites").glob("**/*.yaml")) if (tmp_path / "suites").exists() else []
        assert written_suites == []

    @allure.title("include_profiles=True writes global/profiles.yml")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_include_profiles_writes_profiles_yml(self, make_suite, tmp_path):
        """Given include_profiles=True, when the modular config writer runs,
        then global/profiles.yml is created with a default_profile and a dev profiles entry
        whose target_catalog matches the source catalog."""
        suite = make_suite({})
        suite._write_suggest_to_modular_config(
            self._SUGGEST_RESULTS, str(tmp_path), self._default_options(include_profiles=True)
        )
        profiles_file = tmp_path / "global" / "profiles.yml"
        assert profiles_file.exists()
        loaded = yaml.safe_load(profiles_file.read_text())
        assert "default_profile" in loaded
        assert "profiles" in loaded
        assert "dev" in loaded["profiles"]
        assert loaded["profiles"]["dev"]["target_catalog"] == "analytics_dev"

    @allure.title("parameterize_catalog=True replaces catalog with Jinja placeholder")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_parameterize_catalog_in_test_groups(self, make_suite, tmp_path):
        """Given parameterize_catalog=True, when the modular config writer runs,
        then the catalog field in every test_groups yaml entry is replaced with the Jinja placeholder."""
        suite = make_suite({})
        suite._write_suggest_to_modular_config(
            self._SUGGEST_RESULTS, str(tmp_path), self._default_options(parameterize_catalog=True)
        )
        safe_name = "orders"  # v1.2.2: modular file named by table only (catalog/schema implied by dir layout)
        tg_file = tmp_path / "suites" / "bronze" / "test_groups" / f"{safe_name}.yaml"
        loaded = yaml.safe_load(tg_file.read_text())
        entry = loaded["tables"][0]
        assert entry["catalog"] == "{{ profile.target_catalog }}"

    @allure.title("writer returns list of written file paths")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_returns_list_of_written_paths(self, make_suite, tmp_path):
        """Given a standard suggest results list, when the modular config writer runs,
        then the return value is a non-empty list of strings each pointing to an existing file."""
        suite = make_suite({})
        paths = suite._write_suggest_to_modular_config(self._SUGGEST_RESULTS, str(tmp_path), self._default_options())
        assert isinstance(paths, list)
        assert len(paths) > 0
        # All returned paths should be strings
        assert all(isinstance(p, str) for p in paths)
        # All returned paths should actually exist
        for p in paths:
            assert Path(p).exists()


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_MODULAR_OUTPUT)
class TestModularOutputDryRunOverwrite:
    """Focused tests for dry_run and overwrite semantics of the modular config writer."""

    _SUGGEST_RESULTS: ClassVar[list] = [
        {
            "catalog": "c",
            "schema": "s",
            "table": "t",
            "row_count": 4,
            "suggestions": {
                "table": "c.s.t",
                "row_count": 4,
                "column_count": 3,
                "bronze": {
                    "catalog": "c",
                    "schema": "s",
                    "table": "t",
                    "checks": {"record_count": {"enabled": True}},
                },
            },
        }
    ]

    def _default_options(self, **overrides) -> dict:
        opts = {
            "include_profiles": False,
            "include_policy": True,
            "parameterize_catalog": False,
            "overwrite": False,
            "dry_run": False,
        }
        opts.update(overrides)
        return opts

    @allure.title("dry_run=True returns non-empty path list without creating any files on disk")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_dry_run_writes_nothing(self, make_suite, tmp_path):
        """dry_run=True must compute the would-be paths without touching the filesystem.

        Given a fresh tmp directory and dry_run=True,
        When _write_suggest_to_modular_config is called,
        Then the returned list is non-empty AND neither global/ nor suites/ dirs are created.
        """
        suite = make_suite({})
        paths = suite._write_suggest_to_modular_config(
            self._SUGGEST_RESULTS, str(tmp_path), self._default_options(dry_run=True)
        )
        assert len(paths) > 0, "dry_run must still return the list of would-be paths"
        assert not (tmp_path / "global").exists(), "dry_run must not create global/ directory"
        assert not (tmp_path / "suites").exists(), "dry_run must not create suites/ directory"

    @allure.title("overwrite=False skips existing files; overwrite=True replaces them")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_overwrite_false_skips_existing(self, make_suite, tmp_path):
        """overwrite flag controls whether existing local files are replaced.

        Given a sentinel file at global/policy.yaml,
        When the writer is called with overwrite=False,
        Then the sentinel content is unchanged.
        When called again with overwrite=True,
        Then the file content is replaced with valid dq_policy YAML.
        """
        import yaml as _yaml

        suite = make_suite({})
        sentinel = "SENTINEL\n"

        # Pre-create the target file with sentinel content
        policy_path = tmp_path / "global" / "policy.yaml"
        policy_path.parent.mkdir(parents=True, exist_ok=True)
        policy_path.write_text(sentinel)

        # overwrite=False — sentinel must survive
        suite._write_suggest_to_modular_config(
            self._SUGGEST_RESULTS, str(tmp_path), self._default_options(overwrite=False)
        )
        assert policy_path.read_text() == sentinel, "overwrite=False must leave existing file untouched"

        # overwrite=True — file must be replaced with real dq_policy content
        suite._write_suggest_to_modular_config(
            self._SUGGEST_RESULTS, str(tmp_path), self._default_options(overwrite=True)
        )
        loaded = _yaml.safe_load(policy_path.read_text())
        assert "dq_policy" in loaded, "overwrite=True must replace the file with valid policy content"
        assert policy_path.read_text() != sentinel, "overwrite=True must replace sentinel content"


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_MODULAR_OUTPUT)
class TestModularOutputDispatch:
    """End-to-end test: run() → _write_suggest_output → modular config writer."""

    @allure.title("suggest_output.modular_config_path triggers modular config tree creation on disk")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_dispatch_modular_config_path_writes_tree(self, make_suite, df_mixed, materialize_table, tmp_path):
        """Given a profiling suite configured in suggest mode with a modular_config_path, when run() is called,
        then the config tree is written to disk including global/policy.yaml and per-suite suite.yaml files."""
        catalog, schema, table = materialize_table("p2_dispatch", "orders", df_mixed)
        suite = make_suite(
            {
                "mode": "suggest",
                "tables": [{"catalog": catalog, "schema": schema, "table": table}],
                "suggest_output": {"modular_config_path": str(tmp_path), "include_policy": True},
            }
        )
        suite.run()
        assert (tmp_path / "global" / "policy.yaml").exists()
        assert list(tmp_path.glob("suites/*/suite.yaml"))


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_MODULAR_OUTPUT)
class TestProfileAwareRoundTrip:
    """Round-trip test: writer emits profiles.yml + parameterized catalog → resolver renders it back."""

    @allure.title("profile-aware modular config round-trips through the real resolver")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_round_trip_parameterized_catalog_resolves_via_profile(
        self, make_suite, df_mixed, materialize_table, tmp_path
    ):
        """Run suggest mode with parameterize_catalog+include_profiles, then prove the resolver
        renders {{ profile.target_catalog }} back to the real catalog name.

        Given a profiling suggest run with parameterize_catalog=True and include_profiles=True,
        When the generated modular config tree is fed back through resolve_config with profile="dev",
        Then the table's catalog field is the real catalog value (not the Jinja placeholder).
        """
        catalog, schema, table = materialize_table("p2_round_trip", "events", df_mixed)

        suite = make_suite(
            {
                "mode": "suggest",
                "tables": [{"catalog": catalog, "schema": schema, "table": table}],
                "suggest_output": {
                    "modular_config_path": str(tmp_path),
                    "parameterize_catalog": True,
                    "include_profiles": True,
                    "include_policy": True,
                },
            }
        )
        suite.run()

        # ── Step A: verify global/profiles.yml exists and has expected shape ──
        profiles_file = tmp_path / "global" / "profiles.yml"
        assert profiles_file.exists(), "global/profiles.yml must be written when include_profiles=True"

        profiles_data = yaml.safe_load(profiles_file.read_text())
        assert "default_profile" in profiles_data, "profiles.yml must contain default_profile"
        assert "profiles" in profiles_data, "profiles.yml must contain profiles dict"
        assert "dev" in profiles_data["profiles"], "profiles.yml must contain a 'dev' profile"
        assert "target_catalog" in profiles_data["profiles"]["dev"], "dev profile must define target_catalog"

        emitted_catalog = profiles_data["profiles"]["dev"]["target_catalog"]
        assert emitted_catalog == catalog, (
            f"profiles.yml target_catalog must equal the table's catalog '{catalog}', got '{emitted_catalog}'"
        )

        # ── Step B: verify placeholder is stored on disk (unrendered) ──
        tg_files = list(tmp_path.glob("suites/*/test_groups/*.yaml"))
        assert tg_files, "At least one test_groups yaml must be written"

        placeholder_found = False
        for tg_file in tg_files:
            raw = tg_file.read_text()
            loaded = yaml.safe_load(raw)
            tables_list = loaded.get("tables", [])
            for entry in tables_list:
                if entry.get("table") == table:
                    assert entry["catalog"] == "{{ profile.target_catalog }}", (
                        f"On-disk catalog must be the Jinja placeholder, got: {entry['catalog']!r}"
                    )
                    placeholder_found = True

        assert placeholder_found, f"No test_groups entry found for table '{table}'"

        # ── Step C: feed the tree through the REAL resolver with profile="dev" ──
        # Mirror runner.run_dq wiring: build_context(project_path, profile) → resolve_config(ref, context)
        context = build_context(str(tmp_path), "dev")
        assert context.profile_name == "dev"
        assert context.target_catalog == catalog, (
            f"DQContext.target_catalog must equal '{catalog}', got '{context.target_catalog}'"
        )

        resolved = resolve_config(str(tmp_path), context=context)

        # ── Step D: assert at least one suite in resolved config has the table
        #    with catalog rendered to the real value (not the placeholder) ──
        rendered_catalog_found = False
        for key, value in resolved.items():
            if key == "dq_policy":
                continue
            suite_tables = value.get("tables", []) if isinstance(value, dict) else []
            for entry in suite_tables:
                if entry.get("table") == table:
                    rendered = entry.get("catalog")
                    assert rendered == catalog, f"Resolved catalog must be '{catalog}' (rendered), got: {rendered!r}"
                    assert rendered != "{{ profile.target_catalog }}", (
                        "Jinja placeholder must be rendered — resolver left it unresolved"
                    )
                    rendered_catalog_found = True

        assert rendered_catalog_found, (
            f"No resolved table entry found for '{table}' — resolver may have failed to load suites"
        )


@allure.feature(AllureFeature.PROFILING_SUITE)
@allure.story(AllureStory.PROFILING_MODULAR_OUTPUT)
class TestCheckSpacing:
    @allure.title("blank_between_checks spaces out checks but keeps the YAML valid")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_blank_lines_between_checks_round_trip(self, make_suite):
        """Given a checks dict with multiple entries, when _format_yaml_suggestions is called
        with blank_between_checks=True, then blank lines separate all checks after the first
        while the YAML remains parseable."""
        suite = make_suite({})
        entry = {
            "tables": [
                {
                    "catalog": "c",
                    "schema": "s",
                    "table": "t",
                    "checks": {
                        "uniqueness": {"enabled": True, "columns": ["a"]},
                        "volume": {"enabled": False, "metric": "COUNT(*)"},
                        "freshness": {"enabled": True, "column": "x"},
                    },
                }
            ]
        }

        with allure.step("Format with blank_between_checks=True"):
            text = suite._format_yaml_suggestions(entry, blank_between_checks=True)

        with allure.step("Verify: a blank line precedes the 2nd and 3rd checks"):
            assert "\n\n    volume:" in text
            assert "\n\n    freshness:" in text
            # the first check has no leading blank line inside the checks block
            assert "\n\n    uniqueness:" not in text

        with allure.step("Verify: the spaced YAML still parses correctly"):
            parsed = yaml.safe_load(text)
            checks = parsed["tables"][0]["checks"]
            assert set(checks) == {"uniqueness", "volume", "freshness"}
            assert checks["volume"]["enabled"] is False

    @allure.title("Default formatting has no blank lines between checks")
    @allure.severity(AllureSeverity.MINOR)
    @allure.tag(AllureTag.POSITIVE, AllureTag.SPARK)
    def test_no_blank_lines_by_default(self, make_suite):
        """Given a checks dict with multiple entries, when _format_yaml_suggestions is called
        without blank_between_checks, then the output contains no blank lines."""
        suite = make_suite({})
        entry = {"tables": [{"checks": {"a": {"enabled": True}, "b": {"enabled": True}}}]}
        text = suite._format_yaml_suggestions(entry)
        assert "\n\n" not in text
