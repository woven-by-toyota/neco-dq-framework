# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for config_resolver module."""

import math
from unittest.mock import MagicMock, patch

import allure
import pytest
import yaml

import neco_dq.config.config_resolver as config_module
from neco_dq.config.config_resolver import (
    _auto_convert_types,
    _convert_string_value,
    _get_workspace_client,
    _list_workspace_dir,
    _load_yaml_from_workspace,
    _read_workspace_file,
    _is_databricks_path,
    _load_yaml,
    _resolve_ge_tables,
    _resolve_legacy_yaml,
    _resolve_modular_config,
    _resolve_suite,
    _resolve_test_groups,
    resolve_config,
)
from tests.constants import AllureEpic, AllureFeature, AllureSeverity, AllureStory, AllureTag


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestLoadYaml:
    """Tests for _load_yaml function."""

    @allure.title("Load valid YAML file")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_valid_yaml(self, tmp_path):
        """Load a valid YAML file returns dict."""
        with allure.step("Setup test data"):
            yaml_content = {"key": "value", "number": 42}
            yaml_file = tmp_path / "test.yaml"
            yaml_file.write_text(yaml.dump(yaml_content))

        with allure.step("Execute function under test"):
            result = _load_yaml(yaml_file)

        with allure.step("Verify: Load a valid YAML file returns dict"):
            assert result == yaml_content

    @allure.title("Load empty YAML file returns empty dict")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_empty_yaml(self, tmp_path):
        """Load an empty YAML file returns empty dict."""
        with allure.step("Setup test data"):
            yaml_file = tmp_path / "empty.yaml"
            yaml_file.write_text("")

        with allure.step("Execute function under test"):
            result = _load_yaml(yaml_file)

        with allure.step("Verify: Load an empty YAML file returns empty dict"):
            assert result == {}

    @allure.title("Load YAML with nested structure")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_nested_yaml(self, tmp_path):
        """Load YAML with nested structure."""
        with allure.step("Setup test data"):
            yaml_content = {"tables": [{"name": "table1", "checks": {"idempotency": {"enabled": True}}}]}
            yaml_file = tmp_path / "nested.yaml"
            yaml_file.write_text(yaml.dump(yaml_content))

        with allure.step("Execute function under test"):
            result = _load_yaml(yaml_file)

        with allure.step("Verify: Load YAML with nested structure"):
            assert result == yaml_content
            assert result["tables"][0]["name"] == "table1"


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestResolveLegacyYaml:
    """Tests for _resolve_legacy_yaml function."""

    @allure.title("Resolve valid legacy YAML")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_valid_legacy_yaml(self, tmp_path):
        """Resolve a valid legacy YAML file."""
        with allure.step("Setup test data"):
            yaml_content = {"bronze": {"tables": []}, "dq_policy": {}}
            yaml_file = tmp_path / "config.yaml"
            yaml_file.write_text(yaml.dump(yaml_content))

        with allure.step("Execute function under test"):
            result = _resolve_legacy_yaml(yaml_file)

        with allure.step("Verify: Resolve a valid legacy YAML file"):
            assert result == yaml_content

    @allure.title("Resolve invalid YAML structure raises error")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_resolve_invalid_yaml_structure(self, tmp_path):
        """Resolve YAML that's not a dict raises ValueError."""
        with allure.step("Setup test data"):
            yaml_file = tmp_path / "invalid.yaml"
            yaml_file.write_text("- item1\n- item2")  # List, not dict

        with allure.step("Execute function under test"):
            with pytest.raises(ValueError, match="Invalid YAML structure"):
                _resolve_legacy_yaml(yaml_file)


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestIsDatabricksPath:
    """Tests for _is_databricks_path function."""

    @allure.title("Workspace path is Databricks path")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_workspace_path(self):
        """Workspace path is identified as Databricks path."""
        with allure.step("Verify: Workspace path is Databricks path"):
            assert _is_databricks_path("/Workspace/Users/test/config") is True

    @allure.title("Volumes path is Databricks path")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_volumes_path(self):
        """Volumes path is identified as Databricks path."""
        with allure.step("Verify: Volumes path is Databricks path"):
            assert _is_databricks_path("/Volumes/catalog/schema/volume/config") is True

    @allure.title("Local path is not Databricks path")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_local_path(self):
        """Local path is not identified as Databricks path."""
        with allure.step("Verify: Local path is not Databricks path"):
            assert _is_databricks_path("/home/user/config") is False
            assert _is_databricks_path("./config") is False
            assert _is_databricks_path("config.yaml") is False


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestResolveTestGroups:
    """Tests for _resolve_test_groups function."""

    @allure.title("Resolve test groups from directory")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_test_groups(self, tmp_path):
        """Resolve test groups from directory with multiple YAML files."""
        with allure.step("Setup test data"):
            test_groups_dir = tmp_path / "test_groups"
            test_groups_dir.mkdir()

            # Create two test group files
            (test_groups_dir / "table1.yaml").write_text(yaml.dump({"tables": [{"name": "table1", "checks": {}}]}))
            (test_groups_dir / "table2.yaml").write_text(yaml.dump({"tables": [{"name": "table2", "checks": {}}]}))

        with allure.step("Execute function under test"):
            result = _resolve_test_groups(test_groups_dir)

        with allure.step("Verify: Resolve test groups from directory"):
            assert len(result) == 2
            names = [t["name"] for t in result]
            assert "table1" in names
            assert "table2" in names

    @allure.title("Resolve test groups handles single table format")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_test_groups_single_table_format(self, tmp_path):
        """Resolve test groups handles YAML without 'tables' key."""
        with allure.step("Setup test data"):
            test_groups_dir = tmp_path / "test_groups"
            test_groups_dir.mkdir()

            # Single table format (no "tables" wrapper)
            (test_groups_dir / "single.yaml").write_text(
                yaml.dump({"name": "single_table", "checks": {"idempotency": {}}})
            )

        with allure.step("Execute function under test"):
            result = _resolve_test_groups(test_groups_dir)

        with allure.step("Verify: Resolve test groups handles single table format"):
            assert len(result) == 1
            assert result[0]["name"] == "single_table"

    @allure.title("Resolve test groups returns empty for non-existent dir")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_test_groups_nonexistent_dir(self, tmp_path):
        """Resolve test groups returns empty list for non-existent directory."""
        with allure.step("Setup test data"):
            nonexistent_dir = tmp_path / "does_not_exist"

        with allure.step("Execute function under test"):
            result = _resolve_test_groups(nonexistent_dir)

        with allure.step("Verify: Resolve test groups returns empty for non-existent dir"):
            assert result == []


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestResolveGeTables:
    """Tests for _resolve_ge_tables function."""

    @allure.title("Resolve GE tables from directory")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_ge_tables(self, tmp_path):
        """Resolve GE tables from great_expectations directory."""
        with allure.step("Setup test data"):
            ge_dir = tmp_path / "great_expectations"
            ge_dir.mkdir()

            (ge_dir / "table1.yaml").write_text(
                yaml.dump({"tables": [{"table": "table1", "expectations": [{"type": "not_null"}]}]})
            )

        with allure.step("Execute function under test"):
            result = _resolve_ge_tables(ge_dir)

        with allure.step("Verify: Resolve GE tables from directory"):
            assert len(result) == 1
            assert result[0]["table"] == "table1"

    @allure.title("Resolve GE tables sets default table name from filename")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_ge_tables_default_name(self, tmp_path):
        """Resolve GE tables uses filename as default table name."""
        with allure.step("Setup test data"):
            ge_dir = tmp_path / "great_expectations"
            ge_dir.mkdir()

            # No "table" key and no "tables" wrapper
            (ge_dir / "my_table.yaml").write_text(yaml.dump({"expectations": [{"type": "not_null", "column": "id"}]}))

        with allure.step("Execute function under test"):
            result = _resolve_ge_tables(ge_dir)

        with allure.step("Verify: Resolve GE tables sets default table name from filename"):
            assert len(result) == 1
            assert result[0]["table"] == "my_table"

    @allure.title("Resolve GE tables returns empty for non-existent dir")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_ge_tables_nonexistent_dir(self, tmp_path):
        """Resolve GE tables returns empty list for non-existent directory."""
        with allure.step("Setup test data"):
            nonexistent_dir = tmp_path / "does_not_exist"

        with allure.step("Execute function under test"):
            result = _resolve_ge_tables(nonexistent_dir)

        with allure.step("Verify: Resolve GE tables returns empty for non-existent dir"):
            assert result == []

    @allure.title("Resolve GE tables raises on invalid YAML")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_resolve_ge_tables_invalid_yaml(self, tmp_path):
        """Resolve GE tables raises error for invalid YAML structure."""
        with allure.step("Setup test data"):
            ge_dir = tmp_path / "great_expectations"
            ge_dir.mkdir()

            # List instead of dict
            (ge_dir / "invalid.yaml").write_text("- item1\n- item2")

        with allure.step("Execute function under test"):
            with pytest.raises(ValueError, match="Invalid GE yaml"):
                _resolve_ge_tables(ge_dir)


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestResolveSuite:
    """Tests for _resolve_suite function."""

    @allure.title("Resolve suite with test groups")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_suite_with_test_groups(self, tmp_path):
        """Resolve a suite directory with test groups."""
        with allure.step("Setup test data"):
            suite_dir = tmp_path / "bronze"
            suite_dir.mkdir()

            # Create suite.yaml
            (suite_dir / "suite.yaml").write_text(
                yaml.dump({"enabled": True, "data_to_save": "catalog.schema.results"})
            )

            # Create test_groups
            test_groups = suite_dir / "test_groups"
            test_groups.mkdir()
            (test_groups / "table1.yaml").write_text(yaml.dump({"tables": [{"name": "table1"}]}))

        with allure.step("Execute function under test"):
            result = _resolve_suite(suite_dir)

        with allure.step("Verify: Resolve suite with test groups"):
            assert "bronze" in result
            assert result["bronze"]["enabled"] is True
            assert "tables" in result["bronze"]
            assert len(result["bronze"]["tables"]) == 1

    @allure.title("Resolve suite with great expectations")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_suite_with_ge(self, tmp_path):
        """Resolve a suite directory with great_expectations."""
        with allure.step("Setup test data"):
            suite_dir = tmp_path / "silver"
            suite_dir.mkdir()

            (suite_dir / "suite.yaml").write_text(
                yaml.dump({"enabled": True, "data_to_save": "catalog.schema.results"})
            )

            ge_dir = suite_dir / "great_expectations"
            ge_dir.mkdir()
            (ge_dir / "table1.yaml").write_text(yaml.dump({"table": "table1", "expectations": []}))

        with allure.step("Execute function under test"):
            result = _resolve_suite(suite_dir)

        with allure.step("Verify: Resolve suite with great expectations"):
            assert "silver" in result
            assert "great_expectations@silver" in result
            assert result["great_expectations@silver"]["parent_suite"] == "silver"

    @allure.title("Resolve suite raises on missing suite.yaml")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_resolve_suite_missing_suite_yaml(self, tmp_path):
        """Resolve suite raises error when suite.yaml is missing."""
        with allure.step("Setup test data"):
            suite_dir = tmp_path / "bronze"
            suite_dir.mkdir()
            # No suite.yaml created

        with allure.step("Execute function under test"):
            with pytest.raises(FileNotFoundError, match=r"Missing suite\.yaml"):
                _resolve_suite(suite_dir)


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestResolveModularConfig:
    """Tests for _resolve_modular_config function."""

    @allure.title("Resolve complete modular config")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_modular_config(self, tmp_path):
        """Resolve a complete modular config directory."""
        with allure.step("Setup test data"):
            # Create global/policy.yaml
            global_dir = tmp_path / "global"
            global_dir.mkdir()
            (global_dir / "policy.yaml").write_text(yaml.dump({"dq_policy": {"fail_on_critical": True}}))

            # Create suites/bronze
            suites_dir = tmp_path / "suites"
            suites_dir.mkdir()
            bronze_dir = suites_dir / "bronze"
            bronze_dir.mkdir()
            (bronze_dir / "suite.yaml").write_text(yaml.dump({"enabled": True}))

            test_groups = bronze_dir / "test_groups"
            test_groups.mkdir()
            (test_groups / "table1.yaml").write_text(yaml.dump({"tables": [{"name": "table1"}]}))

        with allure.step("Execute function under test"):
            result = _resolve_modular_config(tmp_path)

        with allure.step("Verify: Resolve complete modular config"):
            assert "dq_policy" in result
            assert result["dq_policy"]["fail_on_critical"] is True
            assert "bronze" in result
            assert result["bronze"]["enabled"] is True

    @allure.title("Resolve modular config raises on missing policy")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_resolve_modular_config_missing_policy(self, tmp_path):
        """Resolve modular config raises error when global/policy.yaml is missing."""
        with allure.step("Setup test data"):
            # No global/policy.yaml
            suites_dir = tmp_path / "suites"
            suites_dir.mkdir()

        with allure.step("Execute function under test"):
            with pytest.raises(FileNotFoundError, match=r"Missing global/policy\.yaml"):
                _resolve_modular_config(tmp_path)

    @allure.title("Resolve modular config raises on missing suites")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_resolve_modular_config_missing_suites(self, tmp_path):
        """Resolve modular config raises error when suites/ is missing."""
        with allure.step("Setup test data"):
            global_dir = tmp_path / "global"
            global_dir.mkdir()
            (global_dir / "policy.yaml").write_text(yaml.dump({"dq_policy": {}}))
            # No suites/ directory

        with allure.step("Execute function under test"):
            with pytest.raises(FileNotFoundError, match="Missing suites/ directory"):
                _resolve_modular_config(tmp_path)

    @allure.title("Resolve modular config raises on no valid suites")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_resolve_modular_config_no_valid_suites(self, tmp_path):
        """Resolve modular config raises error when no valid suites found."""
        with allure.step("Setup test data"):
            global_dir = tmp_path / "global"
            global_dir.mkdir()
            (global_dir / "policy.yaml").write_text(yaml.dump({"dq_policy": {}}))

            suites_dir = tmp_path / "suites"
            suites_dir.mkdir()
            # Create a file instead of directory
            (suites_dir / "not_a_suite.txt").write_text("invalid")

        with allure.step("Execute function under test"):
            with pytest.raises(ValueError, match="No test suites found"):
                _resolve_modular_config(tmp_path)


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestResolveConfig:
    """Tests for resolve_config public API."""

    @allure.title("Resolve config from YAML file")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_config_yaml_file(self, tmp_path):
        """Resolve config from a single YAML file."""
        with allure.step("Setup test data"):
            yaml_content = {"bronze": {"tables": []}, "dq_policy": {}}
            yaml_file = tmp_path / "config.yaml"
            yaml_file.write_text(yaml.dump(yaml_content))

        with allure.step("Execute function under test"):
            result = resolve_config(str(yaml_file))

        with allure.step("Verify: Resolve config from YAML file"):
            assert result == yaml_content

    @allure.title("Resolve config from modular directory")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_config_modular_dir(self, tmp_path):
        """Resolve config from a modular config directory."""
        with allure.step("Setup test data"):
            # Create modular structure
            global_dir = tmp_path / "global"
            global_dir.mkdir()
            (global_dir / "policy.yaml").write_text(yaml.dump({"dq_policy": {}}))

            suites_dir = tmp_path / "suites"
            suites_dir.mkdir()
            bronze_dir = suites_dir / "bronze"
            bronze_dir.mkdir()
            (bronze_dir / "suite.yaml").write_text(yaml.dump({"enabled": True}))

            test_groups = bronze_dir / "test_groups"
            test_groups.mkdir()
            (test_groups / "t1.yaml").write_text(yaml.dump({"tables": [{"name": "t1"}]}))

        with allure.step("Execute function under test"):
            result = resolve_config(str(tmp_path))

        with allure.step("Verify: Resolve config from modular directory"):
            assert "dq_policy" in result
            assert "bronze" in result

    @allure.title("Resolve config raises on invalid reference")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_resolve_config_invalid_reference(self):
        """Resolve config raises error for invalid reference."""
        with allure.step("Execute function under test"):
            with pytest.raises((FileNotFoundError, ValueError, KeyError)):
                resolve_config("/nonexistent/path/config.yaml")

    @allure.title("Resolve config with Path object")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_config_path_object(self, tmp_path):
        """Resolve config accepts Path object."""
        with allure.step("Setup test data"):
            yaml_content = {"bronze": {"tables": []}, "dq_policy": {}}
            yaml_file = tmp_path / "config.yaml"
            yaml_file.write_text(yaml.dump(yaml_content))

        with allure.step("Execute function under test"):
            result = resolve_config(yaml_file)  # Path object, not string

        with allure.step("Verify: Resolve config with Path object"):
            assert result == yaml_content


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestDatabricksFunctions:
    """Tests for Databricks-specific functions using mocks.

    Note: Some tests require the databricks SDK to be installed.
    Tests that call functions with internal databricks imports are skipped
    when the SDK is not available.
    """

    @allure.title("Get workspace client creates singleton")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_get_workspace_client_singleton(self):
        """Test that _get_workspace_client creates a singleton."""
        pytest.importorskip("databricks.sdk")

        with allure.step("Setup mock"):
            with patch("databricks.sdk.WorkspaceClient") as mock_client_class:
                mock_instance = MagicMock()
                mock_client_class.return_value = mock_instance
                # Reset the global singleton
                config_module._workspace_client = None

                with allure.step("Execute function twice"):
                    client1 = _get_workspace_client()
                    client2 = _get_workspace_client()

                with allure.step("Verify: singleton pattern - same instance returned"):
                    assert client1 is client2
                    mock_client_class.assert_called_once()

                with allure.step("Cleanup"):
                    config_module._workspace_client = None

    @allure.title("Read workspace file decodes base64 content")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_read_workspace_file_decodes_content(self):
        """Test reading a file from Databricks Workspace decodes base64."""
        pytest.importorskip("databricks.sdk")
        import base64

        with allure.step("Setup mock"):
            content = "key: value\nnumber: 42"
            encoded = base64.b64encode(content.encode()).decode()

            mock_client = MagicMock()
            mock_response = MagicMock()
            mock_response.content = encoded
            mock_client.workspace.export.return_value = mock_response

            with patch.object(config_module, "_get_workspace_client", return_value=mock_client):
                with allure.step("Execute function"):
                    result = _read_workspace_file("/Workspace/test/config.yaml")

                with allure.step("Verify: content is decoded correctly"):
                    assert result == content
                    mock_client.workspace.export.assert_called_once()

    @allure.title("Read workspace file raises on empty content")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_read_workspace_file_empty_raises(self):
        """Test that empty file raises FileNotFoundError."""
        pytest.importorskip("databricks.sdk")

        with allure.step("Setup mock with empty content"):
            mock_client = MagicMock()
            mock_response = MagicMock()
            mock_response.content = None
            mock_client.workspace.export.return_value = mock_response

            with patch.object(config_module, "_get_workspace_client", return_value=mock_client):
                with allure.step("Execute and verify exception"):
                    with pytest.raises(FileNotFoundError, match="Empty or missing"):
                        _read_workspace_file("/Workspace/test/empty.yaml")

    @allure.title("List workspace directory returns paths")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    @patch("neco_dq.config.config_resolver._get_workspace_client")
    def test_list_workspace_dir(self, mock_get_client):
        """Test listing a Databricks Workspace directory."""
        with allure.step("Setup mock"):
            mock_client = MagicMock()
            mock_item1 = MagicMock()
            mock_item1.path = "/Workspace/test/file1.yaml"
            mock_item2 = MagicMock()
            mock_item2.path = "/Workspace/test/file2.yaml"
            mock_client.workspace.list.return_value = [mock_item1, mock_item2]
            mock_get_client.return_value = mock_client

        with allure.step("Execute function"):
            result = _list_workspace_dir("/Workspace/test")

        with allure.step("Verify: paths are returned"):
            assert len(result) == 2
            assert "/Workspace/test/file1.yaml" in result
            assert "/Workspace/test/file2.yaml" in result

    @allure.title("Load YAML from workspace parses content")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    @patch("neco_dq.config.config_resolver._read_workspace_file")
    def test_load_yaml_from_workspace(self, mock_read):
        """Test loading and parsing YAML from Workspace."""
        with allure.step("Setup mock"):
            mock_read.return_value = "key: value\nnumber: 42"

        with allure.step("Execute function"):
            result = _load_yaml_from_workspace("/Workspace/test/config.yaml")

        with allure.step("Verify: YAML is parsed correctly"):
            assert result == {"key": "value", "number": 42}

    @allure.title("Load YAML from workspace handles empty file")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    @patch("neco_dq.config.config_resolver._read_workspace_file")
    def test_load_yaml_from_workspace_empty(self, mock_read):
        """Test loading empty YAML returns empty dict."""
        with allure.step("Setup mock"):
            mock_read.return_value = ""

        with allure.step("Execute function"):
            result = _load_yaml_from_workspace("/Workspace/test/empty.yaml")

        with allure.step("Verify: empty dict returned"):
            assert result == {}


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.JINJA_RENDERING)
class TestRenderWithContext:
    """Tests for _render_with_context function."""

    @allure.title("Render with context resolves profile variables")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_render_profile_variables(self):
        """Jinja2 rendering should resolve {{ profile.target_catalog }}."""
        from neco_dq.config.config_resolver import _render_with_context
        from neco_dq.config.context import DQContext

        content = 'catalog: "{{ profile.target_catalog }}"'
        ctx = DQContext(profile_name="prod", target_catalog="analytics_prod")

        result = _render_with_context(content, ctx)

        assert result == 'catalog: "analytics_prod"'

    @allure.title("Render with context resolves var variables")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_render_var_variables(self):
        """Jinja2 rendering should resolve {{ var.name }}."""
        from neco_dq.config.config_resolver import _render_with_context
        from neco_dq.config.context import DQContext

        content = "max_delay_hours: {{ var.freshness_hours }}"
        ctx = DQContext(variables={"freshness_hours": 12})

        result = _render_with_context(content, ctx)

        assert result == "max_delay_hours: 12"

    @allure.title("Render with context handles defaults")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_render_with_defaults(self):
        """Jinja2 rendering should handle | default() filter."""
        from neco_dq.config.config_resolver import _render_with_context
        from neco_dq.config.context import DQContext

        content = "max_delay_hours: {{ var.undefined_var | default(24) }}"
        ctx = DQContext(variables={})

        result = _render_with_context(content, ctx)

        assert result == "max_delay_hours: 24"

    @allure.title("Render with context raises on undefined without default")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_render_undefined_raises(self):
        """Jinja2 rendering should raise UndefinedError for undefined variables."""
        from jinja2 import UndefinedError

        from neco_dq.config.config_resolver import _render_with_context
        from neco_dq.config.context import DQContext

        content = "value: {{ var.undefined_var }}"
        ctx = DQContext(variables={})

        with pytest.raises(UndefinedError):
            _render_with_context(content, ctx)

    @allure.title("Render without context skips non-Jinja content")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_render_no_jinja_syntax(self):
        """Content without Jinja2 syntax should be returned unchanged."""
        from neco_dq.config.config_resolver import _render_with_context

        content = "catalog: my_catalog\nschema: bronze"

        result = _render_with_context(content, None)

        assert result == content

    @allure.title("Render with None context uses empty context for defaults")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_render_none_context_with_defaults(self):
        """None context should allow | default() to work."""
        from neco_dq.config.config_resolver import _render_with_context

        content = "catalog: \"{{ profile.target_catalog | default('fallback') }}\""

        result = _render_with_context(content, None)

        assert result == 'catalog: "fallback"'


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestAutoConvertTypes:
    """Tests for _auto_convert_types function."""

    @allure.title("Auto convert string integers to int")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_convert_string_int(self):
        """_auto_convert_types converts string integers to int."""
        from neco_dq.config.config_resolver import _auto_convert_types

        result = _auto_convert_types({"value": "123"})

        assert result["value"] == 123
        assert isinstance(result["value"], int)

    @allure.title("Auto convert string floats to float")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_convert_string_float(self):
        """_auto_convert_types converts string floats to float."""
        from neco_dq.config.config_resolver import _auto_convert_types

        result = _auto_convert_types({"value": "12.5"})

        assert result["value"] == 12.5
        assert isinstance(result["value"], float)

    @allure.title("Auto convert string booleans to bool")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_convert_string_bool(self):
        """_auto_convert_types converts string booleans to bool."""
        from neco_dq.config.config_resolver import _auto_convert_types

        result = _auto_convert_types({"enabled": "true", "disabled": "false", "upper": "TRUE"})

        assert result["enabled"] is True
        assert result["disabled"] is False
        assert result["upper"] is True

    @allure.title("Auto convert preserves non-numeric strings")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_preserve_non_numeric_strings(self):
        """_auto_convert_types preserves strings that aren't numbers or bools."""
        from neco_dq.config.config_resolver import _auto_convert_types

        result = _auto_convert_types({"name": "test_table", "path": "/some/path"})

        assert result["name"] == "test_table"
        assert result["path"] == "/some/path"

    @allure.title("Auto convert handles nested structures")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_convert_nested_structures(self):
        """_auto_convert_types handles nested dicts and lists."""
        from neco_dq.config.config_resolver import _auto_convert_types

        data = {
            "checks": {
                "freshness": {"max_delay_hours": "24", "enabled": "true"},
                "thresholds": ["10", "20", "30"],
            }
        }

        result = _auto_convert_types(data)

        assert result["checks"]["freshness"]["max_delay_hours"] == 24
        assert result["checks"]["freshness"]["enabled"] is True
        assert result["checks"]["thresholds"] == [10, 20, 30]

    @allure.title("Auto convert preserves already typed values")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_preserve_already_typed(self):
        """_auto_convert_types preserves values that are already the correct type."""
        from neco_dq.config.config_resolver import _auto_convert_types

        data = {"int_val": 123, "float_val": 12.5, "bool_val": True, "none_val": None}

        result = _auto_convert_types(data)

        assert result == data

    @allure.title("Auto convert parses string lists (Python repr)")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_convert_string_list_python(self):
        """_auto_convert_types converts Python repr lists to actual lists."""
        from neco_dq.config.config_resolver import _auto_convert_types

        result = _auto_convert_types({"columns": "['col_a', 'col_b', 'col_c']"})

        assert result["columns"] == ["col_a", "col_b", "col_c"]
        assert isinstance(result["columns"], list)

    @allure.title("Auto convert parses string lists (JSON)")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_convert_string_list_json(self):
        """_auto_convert_types converts JSON lists to actual lists."""
        from neco_dq.config.config_resolver import _auto_convert_types

        result = _auto_convert_types({"columns": '["col_a", "col_b"]'})

        assert result["columns"] == ["col_a", "col_b"]
        assert isinstance(result["columns"], list)

    @allure.title("Auto convert parses string dicts")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_convert_string_dict(self):
        """_auto_convert_types converts string dicts to actual dicts."""
        from neco_dq.config.config_resolver import _auto_convert_types

        result = _auto_convert_types({"config": '{"key": "value", "count": 10}'})

        assert result["config"] == {"key": "value", "count": 10}
        assert isinstance(result["config"], dict)


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestMergeProfilePolicy:
    """Tests for _merge_profile_policy function."""

    @allure.title("Profile policy overrides global policy")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_profile_policy_overrides_global(self):
        """Profile policy values override global policy values."""
        from neco_dq.config.config_resolver import _merge_profile_policy
        from neco_dq.config.context import DQContext

        config = {"dq_policy": {"failure_threshold": 0.1, "alerts": {"slack": True}}}
        context = DQContext(policy={"failure_threshold": 0.05})

        result = _merge_profile_policy(config, context)

        assert result["dq_policy"]["failure_threshold"] == 0.05
        assert result["dq_policy"]["alerts"]["slack"] is True  # preserved

    @allure.title("No profile policy leaves config unchanged")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_no_profile_policy_unchanged(self):
        """Config is unchanged when no profile policy exists."""
        from neco_dq.config.config_resolver import _merge_profile_policy
        from neco_dq.config.context import DQContext

        config = {"dq_policy": {"failure_threshold": 0.1}}
        context = DQContext()  # empty policy

        result = _merge_profile_policy(config, context)

        assert result["dq_policy"]["failure_threshold"] == 0.1

    @allure.title("Block test suites are deep merged")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_block_test_suites_deep_merge(self):
        """block_test_suites dict is deep merged, not replaced."""
        from neco_dq.config.config_resolver import _merge_profile_policy
        from neco_dq.config.context import DQContext

        config = {"dq_policy": {"block_test_suites": {"bronze": True, "silver": True, "gold": True}}}
        context = DQContext(policy={"block_test_suites": {"bronze": False}})

        result = _merge_profile_policy(config, context)

        assert result["dq_policy"]["block_test_suites"]["bronze"] is False  # overridden
        assert result["dq_policy"]["block_test_suites"]["silver"] is True  # preserved
        assert result["dq_policy"]["block_test_suites"]["gold"] is True  # preserved

    @allure.title("Profile policy creates dq_policy if missing")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_creates_dq_policy_if_missing(self):
        """Profile policy creates dq_policy section if it doesn't exist."""
        from neco_dq.config.config_resolver import _merge_profile_policy
        from neco_dq.config.context import DQContext

        config = {"bronze": {"tables": []}}  # no dq_policy
        context = DQContext(policy={"failure_threshold": 0.2})

        result = _merge_profile_policy(config, context)

        assert result["dq_policy"]["failure_threshold"] == 0.2


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestResolveConfigWithContext:
    """Tests for resolve_config with DQContext."""

    @allure.title("Resolve config with context renders Jinja2 in legacy YAML")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_legacy_yaml_with_context(self, tmp_path):
        """resolve_config should render Jinja2 templates in legacy YAML."""
        from neco_dq.config.config_resolver import resolve_config
        from neco_dq.config.context import DQContext

        yaml_content = """
bronze:
  tables:
    - catalog: "{{ profile.target_catalog }}"
      schema: bronze
      table: orders
      checks:
        freshness:
          enabled: true
          max_delay_hours: {{ var.freshness_hours }}
dq_policy:
  blocking_enabled: true
"""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        ctx = DQContext(
            profile_name="prod",
            target_catalog="analytics_prod",
            variables={"freshness_hours": 12},
        )

        result = resolve_config(str(yaml_file), context=ctx)

        assert result["bronze"]["tables"][0]["catalog"] == "analytics_prod"
        assert result["bronze"]["tables"][0]["checks"]["freshness"]["max_delay_hours"] == 12

    @allure.title("Resolve config without context works for legacy configs")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_resolve_legacy_yaml_no_context(self, tmp_path):
        """resolve_config without context should work for configs without Jinja2."""
        from neco_dq.config.config_resolver import resolve_config

        yaml_content = """
bronze:
  tables:
    - catalog: my_catalog
      schema: bronze
      table: orders
dq_policy:
  blocking_enabled: true
"""
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        result = resolve_config(str(yaml_file), context=None)

        assert result["bronze"]["tables"][0]["catalog"] == "my_catalog"


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.CONFIG_RESOLVER)
class TestConvertStringValue:
    """Tests for _convert_string_value type coercion."""

    @allure.title("Non-finite float strings stay strings (nan/inf)")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    @pytest.mark.parametrize("value", ["nan", "NaN", "NAN", "inf", "-inf", "Infinity", "-Infinity"])
    def test_convert_non_finite_float_stays_string(self, value):
        """float('nan')/float('inf') must not be injected into config; keep as string."""
        with allure.step("Execute function under test"):
            result = _convert_string_value(value)

        with allure.step("Verify: value remains the original string, not a float"):
            assert result == value
            assert isinstance(result, str)

    @allure.title("Finite numeric strings still convert to numbers")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_convert_finite_numbers_still_work(self):
        """Regular numeric coercion must keep working after the non-finite guard."""
        with allure.step("Execute function under test and verify"):
            assert _convert_string_value("42") == 42
            assert _convert_string_value("12.5") == 12.5
            assert _convert_string_value("-3.14") == -3.14
            assert _convert_string_value("true") is True
            assert _convert_string_value("plain") == "plain"

    @allure.title("values_set with 'nan' string does not become float nan")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_auto_convert_values_set_no_real_nan(self):
        """A values_set list containing 'nan' must not yield a real float('nan')."""
        with allure.step("Setup config-shaped dict with a 'nan' allowed value"):
            cfg = {
                "checks": {"input_type": {"values_set": {"enabled": True, "values": ["nan", "email", "date", "url"]}}}
            }

        with allure.step("Execute function under test"):
            out = _auto_convert_types(cfg)

        with allure.step("Verify: no element is a real float('nan')"):
            values = out["checks"]["input_type"]["values_set"]["values"]
            assert not any(isinstance(v, float) and math.isnan(v) for v in values)
            assert values == ["nan", "email", "date", "url"]
