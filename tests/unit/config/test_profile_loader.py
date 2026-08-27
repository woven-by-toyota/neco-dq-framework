# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for profile_loader module."""

import os
from unittest.mock import patch

import allure
import pytest
import yaml

from tests.constants import AllureEpic, AllureFeature, AllureSeverity, AllureStory, AllureTag


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.PROFILE_LOADER)
class TestLoadProfiles:
    """Tests for load_profiles function."""

    @allure.title("Load profiles returns None if file missing")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_profiles_missing_file(self, tmp_path):
        """load_profiles should return None if profiles.yml does not exist."""
        from neco_dq.config.profile_loader import load_profiles

        result = load_profiles(str(tmp_path))

        assert result is None

    @allure.title("Load profiles parses valid YAML")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_profiles_valid_yaml(self, tmp_path):
        """load_profiles should parse valid profiles.yml."""
        from neco_dq.config.profile_loader import load_profiles

        profiles_content = {
            "profiles": {
                "dev": {"target_catalog": "dev_catalog", "variables": {"freshness_hours": 48}},
                "prod": {"target_catalog": "prod_catalog", "variables": {"freshness_hours": 12}},
            },
            "default_profile": "dev",
        }
        profiles_file = tmp_path / "profiles.yml"
        profiles_file.write_text(yaml.dump(profiles_content))

        result = load_profiles(str(tmp_path))

        assert result == profiles_content

    @allure.title("Load profiles handles empty file")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_load_profiles_empty_file(self, tmp_path):
        """load_profiles should return empty dict for empty file."""
        from neco_dq.config.profile_loader import load_profiles

        profiles_file = tmp_path / "profiles.yml"
        profiles_file.write_text("")

        result = load_profiles(str(tmp_path))

        assert result == {}

    @allure.title("Load profiles supports .yaml extension")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_profiles_yaml_extension(self, tmp_path):
        """load_profiles should find profiles.yaml if profiles.yml not present."""
        from neco_dq.config.profile_loader import load_profiles

        profiles_content = {
            "profiles": {"dev": {"target_catalog": "dev_catalog"}},
            "default_profile": "dev",
        }
        profiles_file = tmp_path / "profiles.yaml"
        profiles_file.write_text(yaml.dump(profiles_content))

        result = load_profiles(str(tmp_path))

        assert result == profiles_content

    @allure.title("Load profiles prefers .yml over .yaml")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_load_profiles_yml_precedence(self, tmp_path):
        """load_profiles should prefer profiles.yml when both extensions exist."""
        from neco_dq.config.profile_loader import load_profiles

        yml_content = {"profiles": {"from_yml": {"target_catalog": "yml_catalog"}}}
        yaml_content = {"profiles": {"from_yaml": {"target_catalog": "yaml_catalog"}}}

        (tmp_path / "profiles.yml").write_text(yaml.dump(yml_content))
        (tmp_path / "profiles.yaml").write_text(yaml.dump(yaml_content))

        result = load_profiles(str(tmp_path))

        assert "from_yml" in result["profiles"]
        assert "from_yaml" not in result["profiles"]

    @allure.title("Load profiles finds file in global/ subdirectory")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_load_profiles_global_subdir(self, tmp_path):
        """load_profiles should find profiles in global/ subdirectory."""
        from neco_dq.config.profile_loader import load_profiles

        global_dir = tmp_path / "global"
        global_dir.mkdir()

        profiles_content = {
            "profiles": {"dev": {"target_catalog": "dev_catalog"}},
            "default_profile": "dev",
        }
        (global_dir / "profiles.yaml").write_text(yaml.dump(profiles_content))

        result = load_profiles(str(tmp_path))

        assert result == profiles_content

    @allure.title("Load profiles prefers root over global/")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_load_profiles_root_precedence_over_global(self, tmp_path):
        """load_profiles should prefer root profiles over global/ subdirectory."""
        from neco_dq.config.profile_loader import load_profiles

        global_dir = tmp_path / "global"
        global_dir.mkdir()

        root_content = {"profiles": {"from_root": {"target_catalog": "root_catalog"}}}
        global_content = {"profiles": {"from_global": {"target_catalog": "global_catalog"}}}

        (tmp_path / "profiles.yml").write_text(yaml.dump(root_content))
        (global_dir / "profiles.yml").write_text(yaml.dump(global_content))

        result = load_profiles(str(tmp_path))

        assert "from_root" in result["profiles"]
        assert "from_global" not in result["profiles"]


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.PROFILE_LOADER)
class TestResolveProfileName:
    """Tests for resolve_profile_name function."""

    @allure.title("Explicit profile takes precedence")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_explicit_profile_precedence(self):
        """Explicit profile parameter should take highest precedence."""
        from neco_dq.config.profile_loader import resolve_profile_name

        profiles_config = {"default_profile": "dev"}

        result = resolve_profile_name(profiles_config, explicit_profile="prod")

        assert result == "prod"

    @allure.title("Env var takes precedence over default_profile")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_env_var_precedence(self):
        """NECO_DQ_PROFILE env var should take precedence over default_profile."""
        from neco_dq.config.profile_loader import resolve_profile_name

        profiles_config = {"default_profile": "dev"}

        with patch.dict(os.environ, {"NECO_DQ_PROFILE": "staging"}):
            result = resolve_profile_name(profiles_config, explicit_profile=None)

        assert result == "staging"

    @allure.title("Default profile from config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_default_profile_from_config(self):
        """Should use default_profile from config when no explicit or env var."""
        from neco_dq.config.profile_loader import resolve_profile_name

        profiles_config = {"default_profile": "dev"}

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("NECO_DQ_PROFILE", None)
            result = resolve_profile_name(profiles_config, explicit_profile=None)

        assert result == "dev"

    @allure.title("Fallback to 'default' when no config")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_fallback_to_default(self):
        """Should fallback to 'default' when no profiles_config."""
        from neco_dq.config.profile_loader import resolve_profile_name

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("NECO_DQ_PROFILE", None)
            result = resolve_profile_name(None, explicit_profile=None)

        assert result == "default"


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.PROFILE_LOADER)
class TestProfileInheritance:
    """Tests for profile inheritance functionality."""

    @allure.title("Profile inherits from parent")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_simple_inheritance(self, tmp_path):
        """Child profile inherits values from parent profile."""
        from neco_dq.config.profile_loader import build_context

        profiles_content = {
            "profiles": {
                "base": {
                    "target_catalog": "analytics",
                    "variables": {"freshness_hours": 24, "min_count": 1000},
                },
                "dev": {
                    "extends": "base",
                    "target_catalog": "analytics_dev",
                },
            },
        }
        (tmp_path / "profiles.yml").write_text(yaml.dump(profiles_content))

        ctx = build_context(str(tmp_path), profile="dev")

        assert ctx.target_catalog == "analytics_dev"  # overridden
        assert ctx.variables["freshness_hours"] == 24  # inherited
        assert ctx.variables["min_count"] == 1000  # inherited

    @allure.title("Profile variables merge with parent")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_variables_deep_merge(self, tmp_path):
        """Child variables merge with parent variables (child overrides)."""
        from neco_dq.config.profile_loader import build_context

        profiles_content = {
            "profiles": {
                "base": {
                    "variables": {"a": 1, "b": 2, "c": 3},
                },
                "child": {
                    "extends": "base",
                    "variables": {"b": 20, "d": 4},  # override b, add d
                },
            },
        }
        (tmp_path / "profiles.yml").write_text(yaml.dump(profiles_content))

        ctx = build_context(str(tmp_path), profile="child")

        assert ctx.variables == {"a": 1, "b": 20, "c": 3, "d": 4}

    @allure.title("Multi-level inheritance chain")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_multi_level_inheritance(self, tmp_path):
        """Profile can inherit from profile that inherits from another."""
        from neco_dq.config.profile_loader import build_context

        profiles_content = {
            "profiles": {
                "base": {"variables": {"level": "base", "from_base": True}},
                "middle": {"extends": "base", "variables": {"level": "middle", "from_middle": True}},
                "child": {"extends": "middle", "variables": {"level": "child"}},
            },
        }
        (tmp_path / "profiles.yml").write_text(yaml.dump(profiles_content))

        ctx = build_context(str(tmp_path), profile="child")

        assert ctx.variables["level"] == "child"
        assert ctx.variables["from_base"] is True
        assert ctx.variables["from_middle"] is True

    @allure.title("Circular inheritance raises error")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_circular_inheritance_error(self, tmp_path):
        """Circular inheritance is detected and raises error."""
        from neco_dq.config.profile_loader import CircularInheritanceError, build_context

        profiles_content = {
            "profiles": {
                "a": {"extends": "b"},
                "b": {"extends": "c"},
                "c": {"extends": "a"},  # circular!
            },
        }
        (tmp_path / "profiles.yml").write_text(yaml.dump(profiles_content))

        with pytest.raises(CircularInheritanceError) as exc_info:
            build_context(str(tmp_path), profile="a")

        assert "a" in exc_info.value.chain

    @allure.title("Inheritance from non-existent parent raises error")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_missing_parent_error(self, tmp_path):
        """Extending non-existent profile raises ProfileNotFoundError."""
        from neco_dq.config.profile_loader import ProfileNotFoundError, build_context

        profiles_content = {
            "profiles": {
                "child": {"extends": "nonexistent"},
            },
        }
        (tmp_path / "profiles.yml").write_text(yaml.dump(profiles_content))

        with pytest.raises(ProfileNotFoundError) as exc_info:
            build_context(str(tmp_path), profile="child")

        assert exc_info.value.profile_name == "nonexistent"


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.PROFILE_LOADER)
class TestBuildContext:
    """Tests for build_context function."""

    @allure.title("Build context returns default for missing profiles.yml")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_build_context_no_profiles_file(self, tmp_path):
        """build_context should return default context when profiles.yml missing."""
        from neco_dq.config.context import DQContext
        from neco_dq.config.profile_loader import build_context

        result = build_context(str(tmp_path), profile=None)

        assert result == DQContext.default()

    @allure.title("Build context returns default for None project_path")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_build_context_none_project_path(self):
        """build_context should return default context when project_path is None."""
        from neco_dq.config.context import DQContext
        from neco_dq.config.profile_loader import build_context

        result = build_context(None, profile=None)

        assert result == DQContext.default()

    @allure.title("Build context loads profile correctly")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_build_context_loads_profile(self, tmp_path):
        """build_context should load and return context for valid profile."""
        from neco_dq.config.profile_loader import build_context

        profiles_content = {
            "profiles": {
                "prod": {
                    "target_catalog": "analytics_prod",
                    "variables": {"freshness_hours": 12},
                },
            },
            "default_profile": "prod",
        }
        profiles_file = tmp_path / "profiles.yml"
        profiles_file.write_text(yaml.dump(profiles_content))

        result = build_context(str(tmp_path), profile="prod")

        assert result.profile_name == "prod"
        assert result.target_catalog == "analytics_prod"
        assert result.variables["freshness_hours"] == 12

    @allure.title("Build context raises ProfileNotFoundError for unknown profile")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.NEGATIVE)
    def test_build_context_unknown_profile(self, tmp_path):
        """build_context should raise ProfileNotFoundError for unknown profile."""
        from neco_dq.config.profile_loader import ProfileNotFoundError, build_context

        profiles_content = {
            "profiles": {"dev": {"target_catalog": "dev_catalog"}},
            "default_profile": "dev",
        }
        profiles_file = tmp_path / "profiles.yml"
        profiles_file.write_text(yaml.dump(profiles_content))

        with pytest.raises(ProfileNotFoundError) as exc_info:
            build_context(str(tmp_path), profile="nonexistent")

        assert exc_info.value.profile_name == "nonexistent"
        assert "dev" in exc_info.value.available
