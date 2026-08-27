# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""DQContext dataclass for runtime configuration context.

This module provides the DQContext dataclass that holds profile-specific
configuration data used during config resolution and Jinja2 rendering.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DQContext:
    """Runtime context for config resolution.

    Holds profile-specific data that is made available to Jinja2 templates
    during configuration loading.

    Attributes:
        profile_name: Name of the active profile (e.g., "dev", "prod").
        target_catalog: Default catalog for table references.
        variables: User-defined variables accessible as {{ var.name }}.
        policy: Policy overrides for this profile (merged with global policy).
    """

    profile_name: str = "default"
    target_catalog: str = ""
    variables: dict[str, Any] = field(default_factory=dict)
    policy: dict[str, Any] = field(default_factory=dict)

    def to_jinja_context(self) -> dict[str, Any]:
        """Return dict for Jinja2 template rendering.

        Returns:
            Dict with 'profile' and 'var' keys for use in Jinja2 templates.
            Templates can access values as {{ profile.target_catalog }} or {{ var.name }}.

        Note:
            ``target_catalog`` is omitted when empty so that
            ``{{ profile.target_catalog | default(...) }}`` falls back correctly
            for legacy configs running without a profiles.yml.
        """
        profile: dict[str, Any] = {"name": self.profile_name}
        if self.target_catalog:
            profile["target_catalog"] = self.target_catalog
        return {
            "profile": profile,
            "var": self.variables,
        }

    @classmethod
    def default(cls) -> "DQContext":
        """Create default context (no profile active).

        Returns:
            DQContext with default values, suitable for legacy configs
            that don't use profiles.
        """
        return cls()

    @classmethod
    def from_profile(cls, profile_data: dict[str, Any], profile_name: str) -> "DQContext":
        """Create context from loaded profile data.

        Args:
            profile_data: Dict containing profile configuration from profiles.yml.
            profile_name: Name of the profile being loaded.

        Returns:
            DQContext populated with profile data.
        """
        return cls(
            profile_name=profile_name,
            target_catalog=profile_data.get("target_catalog", ""),
            variables=profile_data.get("variables", {}),
            policy=profile_data.get("policy", {}),
        )
