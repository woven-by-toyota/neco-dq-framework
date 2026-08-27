# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from neco_dq.test_suites.base_test_suite import DQTestSuite
from neco_dq.core.registry.test_suite_registry import load_test_suite_from_registry
from neco_dq.core.summary.terminal_logger import DQSummaryTerminalLogger
from neco_dq.core.policy.dq_failure_policy import DQFailurePolicy
from neco_dq.core.policy.exit_codes import DQExitCode


logger = logging.getLogger("neco_dq")
logging.getLogger("great_expectations").setLevel(logging.WARNING)


class DQOrchestrator:
    """
    Orchestrator that dynamically loads DQ test suites based on YAML config,
    logs terminal summaries and enforces DQ failure policy (exit-code based).
    """

    def __init__(
        self,
        config: dict[str, Any],
        run_suites: list[str] | None = None,
        exclude_suites: list[str] | None = None,
        selector: dict[str, list[str]] | None = None,
        tags: list[str] | None = None,
        profile_name: str | None = None,
        target_catalog: str | None = None,
    ):
        self.config = config
        self.engines: dict[str, DQTestSuite] = {}

        self.dq_policy: dict[str, Any] = config.get("dq_policy", {})

        self.run_suites = set(run_suites) if run_suites else None
        self.exclude_suites = set(exclude_suites) if exclude_suites else set()
        self.selector = selector or {}
        self.tags = tags
        self.profile_name = profile_name
        self.target_catalog = target_catalog
        self._unique_tables_count: int = 0

    def register_engine(self, name: str, engine: DQTestSuite):
        self.engines[name] = engine
        logger.info(f"Test suite registered: {name}")

    def _is_excluded(self, section_name: str) -> bool:
        if section_name in self.exclude_suites:
            return True

        if "@" in section_name:
            _, parent = section_name.split("@", 1)
            if parent in self.exclude_suites:
                return True

        return False

    def _check_has_matching_tag(self, cfg: dict[str, Any]) -> bool:
        """Check if a single check config has matching tags."""
        if not self.tags:
            return False
        check_tags = cfg.get("tags", [])
        return bool(check_tags and set(check_tags) & set(self.tags))

    def _table_has_tagged_checks(self, table_cfg: dict[str, Any], is_ge: bool = False) -> bool:
        """Check if a table config has any checks with matching tags."""
        if not self.tags:
            return True

        checks = table_cfg.get("checks", {})
        if not checks:
            return False

        return self._ge_has_tagged_checks(checks) if is_ge else self._standard_has_tagged_checks(checks)

    def _ge_has_tagged_checks(self, checks: dict) -> bool:
        """Check GE structure: {column: {rule: {tags: [...]}}}."""
        for rules in checks.values():
            if not isinstance(rules, dict):
                continue
            for rule_cfg in rules.values():
                if isinstance(rule_cfg, dict) and self._check_has_matching_tag(rule_cfg):
                    return True
        return False

    def _standard_has_tagged_checks(self, checks: dict) -> bool:
        """Check standard structure: {check_name: [{tags: [...]}] or {tags: [...]}}."""
        for check_cfg in checks.values():
            instances = check_cfg if isinstance(check_cfg, list) else [check_cfg]
            for cfg in instances:
                if isinstance(cfg, dict) and self._check_has_matching_tag(cfg):
                    return True
        return False

    def _should_skip_section(self, section_name: str) -> bool:
        """Check if a config section should be skipped."""
        if section_name == "dq_policy":
            return True
        if self._is_excluded(section_name):
            return True
        if self.run_suites:
            if section_name in self.run_suites:
                return False
            if "@" in section_name:
                _, parent_suite = section_name.split("@", 1)
                return parent_suite not in self.run_suites
            return True
        return False

    def _filter_tables_by_selector(self, section_name: str, engine_cfg: dict[str, Any]) -> dict[str, Any]:
        """Filter tables by selector parameter."""
        selected_tables = self.selector.get(section_name)
        if not selected_tables and "@" in section_name:
            _, parent_suite = section_name.split("@", 1)
            selected_tables = self.selector.get(parent_suite)
        if selected_tables:
            engine_cfg["tables"] = [t for t in engine_cfg.get("tables", []) if t.get("table") in selected_tables]
        return engine_cfg

    def _filter_tables_by_tags(self, section_name: str, engine_cfg: dict[str, Any]) -> dict[str, Any]:
        """Filter tables to only include those with tagged checks."""
        if not self.tags:
            return engine_cfg
        is_ge = section_name.startswith("great_expectations")
        tables = engine_cfg.get("tables", [])
        filtered = [t for t in tables if self._table_has_tagged_checks(t, is_ge)]
        engine_cfg["tables"] = filtered
        return engine_cfg

    def initialize_test_suites_from_config(self, spark=None):
        logger.info("Loading test suites...")

        loaded_suites: list[tuple[str, int, list[str]]] = []

        for section_name, section_cfg in self.config.items():
            if self._should_skip_section(section_name):
                continue

            try:
                engine_name = section_name
                engine_cfg = dict(section_cfg)

                if "@" in section_name:
                    engine_name, parent_suite = section_name.split("@", 1)
                    engine_cfg = {**engine_cfg, "parent_suite": parent_suite}

                engine_cfg = self._filter_tables_by_selector(section_name, engine_cfg)
                engine_cfg = self._filter_tables_by_tags(section_name, engine_cfg)

                # Skip suite entirely if no tables remain after tag filtering.
                # Exception: a suite using discover_tables resolves its tables at
                # runtime (inside the suite), so it has no explicit `tables` here.
                if not engine_cfg.get("tables") and not engine_cfg.get("discover_tables"):
                    continue

                engine_cls = load_test_suite_from_registry(engine_name)
                engine = engine_cls(spark, {engine_name: engine_cfg})

                # Set profile context for execution trace (only if profile is active)
                if self.profile_name and self.profile_name != "default":
                    engine.set_profile_context(self.profile_name, self.target_catalog)

                # Apply tags filter if configured
                if self.tags:
                    engine.set_tags_filter(self.tags)

                self.engines[section_name] = engine

                # Collect table names for unique count
                tables = engine_cfg.get("tables", [])
                table_names = [f"{t.get('catalog')}.{t.get('schema')}.{t.get('table')}" for t in tables]
                loaded_suites.append((section_name, len(tables), table_names))

            except ValueError:
                logger.debug("No test suite registered for section '%s'", section_name)

            except Exception as e:
                logger.exception(
                    "Failed initializing test suite '%s': %s",
                    section_name,
                    e,
                )

        # Sort by layer order: bronze → silver → gold, then alphabetically
        layer_order = {"bronze": 0, "silver": 1, "gold": 2}

        def _sort_key(item: tuple[str, int, list[str]]) -> tuple[int, str]:
            name = item[0]
            # Extract base layer (handle great_expectations@gold -> gold)
            base = name.split("@")[-1] if "@" in name else name
            return (layer_order.get(base, 99), name)

        loaded_suites.sort(key=_sort_key)

        all_tables: set[str] = set()
        for suite_name, table_count, table_names in loaded_suites:
            logger.info("  → %s: %d tables", suite_name, table_count)
            all_tables.update(table_names)

        total_tables = sum(t[1] for t in loaded_suites)
        self._unique_tables_count = len(all_tables)
        logger.info(
            "Loaded %d test suites (%d tables, %d unique)",
            len(loaded_suites),
            total_tables,
            self._unique_tables_count,
        )

    def run_all(self) -> dict[str, Any]:
        final_summary: dict[str, Any] = {}

        logger.info("Running checks...")

        for name, engine in self.engines.items():
            logger.info("  → %s", name)

            try:
                raw_results = engine.run()
                summary = engine.build_summary(raw_results)
                final_summary[name] = summary
                logger.info(
                    "    ✓ %d checks (%d passed, %d failed)",
                    summary.get("total_checks", 0),
                    summary.get("passed", 0),
                    summary.get("failed", 0),
                )

            except Exception as e:
                logger.exception(f"Test suite {name} crashed: {e}")
                final_summary[name] = {
                    "engine": name,
                    "tables_failed": 1,
                    "failed_by_severity": {"error": 1},
                    "status": "system_error",
                }
                logger.info("    ✗ crashed")

        failure_threshold = self.dq_policy.get("failure_threshold")
        DQSummaryTerminalLogger(logger, failure_threshold=failure_threshold).log(
            final_summary, unique_tables=self._unique_tables_count
        )

        policy = DQFailurePolicy(self.dq_policy, profile_name=self.profile_name)
        result = policy.evaluate(final_summary)

        logger.info("Policy: %s (exit_code=%s)", result.message, result.exit_code.value)

        if result.exit_code in (
            DQExitCode.DQ_ERROR,
            DQExitCode.SYSTEM_ERROR,
        ):
            raise RuntimeError(result.message)

        logger.info("DQ run completed.")
        return final_summary
