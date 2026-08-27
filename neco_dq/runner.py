# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import os
import sys
import logging
from collections.abc import Iterable
from pathlib import Path

from pyspark.sql import SparkSession

from neco_dq import __version__ as dq_version
from neco_dq.core.orchestrators.dq_orchestrator import DQOrchestrator
from neco_dq.config.config_resolver import resolve_config
from neco_dq.config.profile_loader import build_context
from neco_dq.core.registry.test_suite_registry import (
    load_custom_suite_from_file,
    register_custom_suite,
)


def _get_project_path(config_ref: str) -> str | None:
    """Extract project root path from config reference.

    For modular config directories, returns the directory path.
    For single YAML files, returns the parent directory.
    For template tags, returns None (no project context).

    Args:
        config_ref: Path to config file, directory, or template tag.

    Returns:
        Project root path, or None if not determinable.
    """
    if config_ref.startswith("/Workspace/") or config_ref.startswith("/Volumes/"):
        if config_ref.endswith((".yaml", ".yml")):
            return str(Path(config_ref).parent)
        return config_ref

    path = Path(config_ref).expanduser()

    if path.exists():
        if path.is_dir():
            return str(path.resolve())
        if path.is_file():
            return str(path.parent.resolve())

    return None


def run_dq(
    config_ref: str,
    profile: str | None = None,
    run_suites: Iterable[str] | None = None,
    exclude_suites: Iterable[str] | None = None,
    selector: dict[str, list[str]] | None = None,
    tags: list[str] | None = None,
    custom_suite_files: str | list[str] | None = None,
    verbose: bool = False,
):
    """
    Unified entrypoint for running the DQ framework.

    Supported config inputs:
    - modular dq-config directory (recommended)
    - single YAML file

    Args:
        config_ref: Path to config file or directory.
        profile: Profile name from profiles.yml. If not provided,
            uses NECO_DQ_PROFILE env var or default_profile from profiles.yml.
        run_suites: Only these test suites will be executed.
        exclude_suites: These test suites will be skipped.
        selector: Per-suite table filtering (e.g., {"bronze": ["orders", "users"]}).
        tags: Only run checks with matching tags (e.g., ["critical", "daily"]).
        custom_suite_files: Path(s) to Python file(s) containing custom DQTestSuite(s).
        verbose: Show progress bars and debug info (default: False).

    Returns:
        dict[str, Any]: Summary with results per test suite.

    Raises:
        RuntimeError: If DQ policy evaluation fails (blocking failures or threshold exceeded).

    Example:
        from neco_dq.runner import run_dq

        # Basic usage
        run_dq("dq-config/")

        # With profile
        run_dq("dq-config/", profile="prod")

        # Filter suites and tags
        run_dq("dq-config/", run_suites=["bronze"], tags=["critical"])
    """

    # Disable progress bars (tqdm) unless verbose mode
    if not verbose:
        os.environ["TQDM_DISABLE"] = "1"

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="[%(levelname)s] %(name)s: %(message)s",
        force=True,  # Override existing logging config (needed for Databricks)
    )
    logger = logging.getLogger("neco_dq")
    if verbose:
        logger.setLevel(logging.DEBUG)

    # Load and register custom suites if provided
    custom_suite_names = []
    if custom_suite_files:
        # Normalize to list
        files = [custom_suite_files] if isinstance(custom_suite_files, str) else list(custom_suite_files)

        for file_path in files:
            logger.info("Loading custom suite from: %s", file_path)
            try:
                suite_name, suite_cls = load_custom_suite_from_file(file_path)
                register_custom_suite(suite_name, suite_cls)
                custom_suite_names.append(suite_name)
                logger.info(
                    "Custom suite registered: %s (%s)",
                    suite_name,
                    suite_cls.__name__,
                )
            except Exception as e:
                logger.error("Failed to load custom suite from %s: %s", file_path, e)
                raise

    spark = SparkSession.builder.getOrCreate()

    # Build context from profile
    project_path = _get_project_path(config_ref)
    context = build_context(project_path, profile)

    if context.profile_name != "default":
        logger.info("Using profile: %s", context.profile_name)

    logger.info("Resolving config: %s", config_ref)
    full_config = resolve_config(config_ref, context=context)

    logger.info("Starting DQ v%s", dq_version)

    orchestrator = DQOrchestrator(
        full_config,
        run_suites=run_suites,
        exclude_suites=exclude_suites,
        selector=selector,
        tags=tags,
        profile_name=context.profile_name,
        target_catalog=context.target_catalog or None,
    )

    orchestrator.initialize_test_suites_from_config(spark)
    return orchestrator.run_all()
