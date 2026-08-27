# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for DQContext dataclass."""

import allure

from tests.constants import AllureEpic, AllureFeature, AllureSeverity, AllureStory, AllureTag


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.PROFILE_CONTEXT)
class TestDQContextDefault:
    """Tests for DQContext.default() factory method."""

    @allure.title("Default context has profile_name 'default'")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_default_profile_name(self):
        """Default context should have profile_name='default'."""
        from neco_dq.config.context import DQContext

        ctx = DQContext.default()

        assert ctx.profile_name == "default"

    @allure.title("Default context has empty target_catalog")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_default_target_catalog(self):
        """Default context should have empty target_catalog."""
        from neco_dq.config.context import DQContext

        ctx = DQContext.default()

        assert ctx.target_catalog == ""

    @allure.title("Default context has empty variables")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_default_variables(self):
        """Default context should have empty variables dict."""
        from neco_dq.config.context import DQContext

        ctx = DQContext.default()

        assert ctx.variables == {}


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.PROFILE_CONTEXT)
class TestDQContextFromProfile:
    """Tests for DQContext.from_profile() factory method."""

    @allure.title("Create context from profile data")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_from_profile_extracts_all_fields(self):
        """from_profile should extract all fields from profile data."""
        from neco_dq.config.context import DQContext

        profile_data = {
            "target_catalog": "analytics_prod",
            "variables": {"freshness_hours": 12, "min_count": 1000},
        }

        ctx = DQContext.from_profile(profile_data, "prod")

        assert ctx.profile_name == "prod"
        assert ctx.target_catalog == "analytics_prod"
        assert ctx.variables == {"freshness_hours": 12, "min_count": 1000}

    @allure.title("Create context with missing optional fields")
    @allure.severity(AllureSeverity.NORMAL)
    @allure.tag(AllureTag.EDGE_CASE)
    def test_from_profile_missing_optional_fields(self):
        """from_profile should handle missing optional fields with defaults."""
        from neco_dq.config.context import DQContext

        profile_data = {"target_catalog": "my_catalog"}

        ctx = DQContext.from_profile(profile_data, "minimal")

        assert ctx.profile_name == "minimal"
        assert ctx.target_catalog == "my_catalog"
        assert ctx.variables == {}


@allure.epic(AllureEpic.DATA_QUALITY)
@allure.feature(AllureFeature.CONFIG)
@allure.story(AllureStory.PROFILE_CONTEXT)
class TestDQContextJinjaContext:
    """Tests for DQContext.to_jinja_context() method."""

    @allure.title("Jinja context has correct structure")
    @allure.severity(AllureSeverity.CRITICAL)
    @allure.tag(AllureTag.POSITIVE)
    def test_to_jinja_context_structure(self):
        """to_jinja_context should return dict with 'profile' and 'var' keys."""
        from neco_dq.config.context import DQContext

        ctx = DQContext(
            profile_name="prod",
            target_catalog="analytics_prod",
            variables={"freshness_hours": 12},
        )

        jinja_ctx = ctx.to_jinja_context()

        assert "profile" in jinja_ctx
        assert "var" in jinja_ctx
        assert jinja_ctx["profile"]["name"] == "prod"
        assert jinja_ctx["profile"]["target_catalog"] == "analytics_prod"
        assert jinja_ctx["var"]["freshness_hours"] == 12
