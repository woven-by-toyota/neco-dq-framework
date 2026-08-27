# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

"""Profile loader for multi-environment configuration.

This module handles loading and resolving profiles from profiles.yml,
enabling environment-specific configuration for DQ checks.
"""

import logging
import os
from pathlib import Path
from typing import Any

import yaml

from neco_dq.config.context import DQContext

logger = logging.getLogger(__name__)


class ProfileNotFoundError(Exception):
    """Raised when requested profile does not exist in profiles.yml."""

    def __init__(self, profile_name: str, available: list[str]):
        self.profile_name = profile_name
        self.available = available
        super().__init__(f"Unknown profile '{profile_name}'. Available: {', '.join(available)}")


def _is_databricks_path(path_str: str) -> bool:
    """Check if path is a Databricks Workspace or Volume path."""
    return path_str.startswith("/Workspace/") or path_str.startswith("/Volumes/")


def _load_profiles_from_databricks(profiles_path: str) -> dict[str, Any] | None:
    """Load profiles file from Databricks path.

    Args:
        profiles_path: Full path to profiles file in Databricks.

    Returns:
        Parsed profiles content, or None if file does not exist.
    """
    if profiles_path.startswith("/Volumes/"):
        path = Path(profiles_path)
        if not path.exists():
            return None
        with path.open() as f:
            return yaml.safe_load(f) or {}

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        response = w.workspace.export(profiles_path)
        if response.content:
            import base64

            content = base64.b64decode(response.content).decode("utf-8")
            return yaml.safe_load(content) or {}
        return {}
    except Exception as e:
        error_str = str(e)
        if "RESOURCE_DOES_NOT_EXIST" in error_str or "does not exist" in error_str.lower():
            return None
        logger.debug("Failed to load profiles from %s: %s", profiles_path, e)
        return None


def load_profiles(project_path: str) -> dict[str, Any] | None:
    """Load profiles file from project directory.

    Search order:
    1. {project_path}/profiles.yml
    2. {project_path}/profiles.yaml
    3. {project_path}/global/profiles.yml
    4. {project_path}/global/profiles.yaml

    Args:
        project_path: Path to project root directory.

    Returns:
        Parsed profiles content, or None if file does not exist.
    """
    search_dirs = ["", "global"]

    if _is_databricks_path(project_path):
        for subdir in search_dirs:
            base = f"{project_path.rstrip('/')}/{subdir}".rstrip("/")
            for ext in ("yml", "yaml"):
                profiles_path = f"{base}/profiles.{ext}"
                result = _load_profiles_from_databricks(profiles_path)
                if result is not None:
                    return result
        return None

    # Local filesystem
    for subdir in search_dirs:
        base = Path(project_path) / subdir if subdir else Path(project_path)
        for ext in ("yml", "yaml"):
            profiles_path = base / f"profiles.{ext}"
            if profiles_path.exists():
                with profiles_path.open() as f:
                    return yaml.safe_load(f) or {}

    return None


def resolve_profile_name(
    profiles_config: dict[str, Any] | None,
    explicit_profile: str | None,
) -> str:
    """Resolve profile name with precedence order.

    Precedence (highest to lowest):
    1. Explicit profile parameter
    2. NECO_DQ_PROFILE environment variable
    3. default_profile from profiles.yml
    4. "default" fallback

    Args:
        profiles_config: Parsed profiles.yml content (can be None).
        explicit_profile: Profile name passed explicitly (can be None).

    Returns:
        Resolved profile name.
    """
    if explicit_profile:
        return explicit_profile

    env_profile = os.environ.get("NECO_DQ_PROFILE")
    if env_profile:
        return env_profile

    if profiles_config:
        return profiles_config.get("default_profile", "default")

    return "default"


class CircularInheritanceError(Exception):
    """Raised when circular profile inheritance is detected."""

    def __init__(self, chain: list[str]):
        self.chain = chain
        super().__init__(f"Circular inheritance detected: {' -> '.join(chain)}")


def _resolve_profile_inheritance(
    profile_name: str,
    profiles: dict[str, dict[str, Any]],
    visited: list[str] | None = None,
) -> dict[str, Any]:
    """Resolve profile with inheritance chain.

    Merges parent profile data with child, where child values override parent.
    Supports deep merging for 'variables' dict.

    Args:
        profile_name: Name of profile to resolve.
        profiles: All profiles from profiles.yml.
        visited: Chain of visited profiles (for circular detection).

    Returns:
        Fully resolved profile data with inherited values.

    Raises:
        CircularInheritanceError: If circular inheritance is detected.
        ProfileNotFoundError: If parent profile doesn't exist.
    """
    if visited is None:
        visited = []

    if profile_name in visited:
        raise CircularInheritanceError([*visited, profile_name])

    visited = [*visited, profile_name]

    profile_data = profiles.get(profile_name, {})
    parent_name = profile_data.get("extends")

    if not parent_name:
        # No inheritance, return copy without 'extends' key
        return {k: v for k, v in profile_data.items() if k != "extends"}

    if parent_name not in profiles:
        raise ProfileNotFoundError(parent_name, list(profiles.keys()))

    # Recursively resolve parent
    parent_data = _resolve_profile_inheritance(parent_name, profiles, visited)

    # Merge: parent first, then child overrides
    merged = dict(parent_data)
    for key, value in profile_data.items():
        if key == "extends":
            continue
        if key == "variables" and isinstance(value, dict) and isinstance(merged.get("variables"), dict):
            # Deep merge variables
            merged["variables"] = {**merged["variables"], **value}
        else:
            merged[key] = value

    return merged


def build_context(project_path: str | None, profile: str | None) -> DQContext:
    """Build DQContext from profile configuration.

    This is the main entry point for profile resolution. It loads profiles.yml,
    resolves the profile name, resolves inheritance, and returns a populated DQContext.

    Args:
        project_path: Path to project root directory (can be None for template tags).
        profile: Explicit profile name (can be None to use env var or default).

    Returns:
        DQContext populated with profile data, or default context if no profiles.

    Raises:
        ProfileNotFoundError: If explicit profile does not exist in profiles.yml.
        CircularInheritanceError: If circular inheritance is detected.
    """
    if not project_path:
        return DQContext.default()

    profiles_config = load_profiles(project_path)

    if profiles_config is None:
        return DQContext.default()

    profile_name = resolve_profile_name(profiles_config, profile)

    profiles = profiles_config.get("profiles", {})

    if profile_name == "default" and "default" not in profiles:
        return DQContext.default()

    if profile_name not in profiles:
        raise ProfileNotFoundError(profile_name, list(profiles.keys()))

    # Resolve inheritance chain
    resolved_profile = _resolve_profile_inheritance(profile_name, profiles)

    return DQContext.from_profile(resolved_profile, profile_name)
