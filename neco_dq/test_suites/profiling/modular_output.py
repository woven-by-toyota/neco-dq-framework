# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import copy
from pathlib import Path
from typing import Any


class ModularConfigOutputMixin:
    """Writes profiling suggestions as a modular-config directory tree."""

    def _parameterize_table_cfg(self, table_cfg: dict[str, Any], parameterize_catalog: bool) -> dict[str, Any]:
        """Return a copy of the table config, optionally replacing the catalog with a profile placeholder."""
        result = copy.deepcopy(table_cfg)
        if parameterize_catalog:
            result["catalog"] = "{{ profile.target_catalog }}"
        return result

    def _write_suggest_to_modular_config(
        self,
        suggest_results: list[dict[str, Any]],
        output_path: str,
        options: dict[str, Any],
    ) -> list[str]:
        """Write profiling suggestions as a modular config directory tree.

        Emits a directory structure that ``config_resolver._resolve_modular_config``
        can read back directly.  The resolver is the source of truth — this method
        must produce files whose shapes match what the resolver parses.

        Directory layout produced::

            <output_path>/
              global/
                policy.yaml          # {"dq_policy": {...}}
                profiles.yml         # only when include_profiles=True
              suites/
                <suite>/
                  suite.yaml         # {"suite": "<suite>"}
                  test_groups/
                    <safe_table>.yaml  # {"tables": [{catalog, schema, table, checks}]}
                  great_expectations/
                    <safe_table>.yaml  # {catalog, schema, table, checks}  (bare dict)

        Args:
            suggest_results: List of result dicts from suggest mode.  Results
                that contain an ``"error"`` key are silently skipped.
            output_path: Root directory to write the modular config tree into.
            options: Behaviour flags:

                - ``include_policy`` (bool, default True): write
                  ``global/policy.yaml`` with sensible blocking defaults.
                  Set to False ONLY when merging into an existing tree that
                  already contains ``global/policy.yaml`` — omitting this
                  file makes the emitted tree unreadable by
                  ``resolve_config``, which requires it (raises
                  ``FileNotFoundError`` without it).
                - ``include_profiles`` (bool, default False): write
                  ``global/profiles.yml``.  The ``target_catalog`` is derived
                  from the first non-error result's catalog.
                - ``parameterize_catalog`` (bool, default False): replace the
                  catalog field in every table entry with the Jinja placeholder
                  ``{{ profile.target_catalog }}``.
                - ``overwrite`` (bool, default False): when *False*, any file
                  that already exists on the **local** filesystem is skipped.
                  This check is best-effort — on Databricks the underlying
                  ``_write_text_file`` always overwrites regardless of this
                  flag.
                - ``dry_run`` (bool, default False): compute and return the
                  list of paths that *would* be written, but write nothing.

        Returns:
            List of absolute file paths that were written (or that would have
            been written when ``dry_run=True``).
        """
        include_policy = options.get("include_policy", True)
        include_profiles = options.get("include_profiles", False)
        parameterize_catalog = options.get("parameterize_catalog", False)
        overwrite = options.get("overwrite", False)
        dry_run = options.get("dry_run", False)

        root = Path(output_path)
        written: list[str] = []

        # ------------------------------------------------------------------
        # Helpers
        # ------------------------------------------------------------------
        def _maybe_write(file_path: Path, content: str) -> None:
            """Write content to file_path honouring overwrite / dry_run."""
            abs_str = str(file_path)
            if dry_run:
                written.append(abs_str)
                return
            if not overwrite and file_path.exists():
                return
            self._write_text_file(abs_str, content)
            written.append(abs_str)

        # Pre-scan to detect table name collisions across schemas
        _all_fq = [
            r.get("suggestions", {}).get("table", "")
            for r in suggest_results
            if "error" not in r and r.get("suggestions")
        ]
        _table_names = [t.rsplit(".", 1)[-1] for t in _all_fq]
        _ambiguous = {name for name in _table_names if _table_names.count(name) > 1}

        _min_parts_for_schema = 2

        def _safe_name(fq_table: str) -> str:
            """Filesystem-safe stem. Uses schema__table when two tables share the same name."""
            parts = fq_table.rsplit(".", maxsplit=2)
            table_only = parts[-1] if fq_table else fq_table
            if table_only in _ambiguous and len(parts) >= _min_parts_for_schema:
                return f"{parts[-2]}__{table_only}".replace(".", "_")
            return table_only.replace(".", "_")

        # ------------------------------------------------------------------
        # Collect per-suite data
        # ------------------------------------------------------------------
        # suite_tables[suite_name] = list of (safe_name, parameterized_entry) pairs
        suite_tables: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        # ge_entries[owning_suite][safe_name] = ge entry dict
        ge_entries: dict[str, dict[str, dict[str, Any]]] = {}

        # Track the first real catalog for profiles.yml
        first_catalog: str | None = None

        non_ge_suites = ("bronze", "silver", "gold")

        for result in suggest_results:
            if "error" in result:
                continue

            suggestions = result.get("suggestions", {})
            if not suggestions:
                continue

            fq_table = suggestions.get("table", "")
            safe = _safe_name(fq_table)

            # Track first real catalog for profiles
            if first_catalog is None:
                first_catalog = result.get("catalog")

            # Determine GE owning suite (prefer silver, else first present non-GE suite)
            present_non_ge = [s for s in non_ge_suites if s in suggestions]
            if "silver" in present_non_ge:
                ge_owning_suite = "silver"
            elif present_non_ge:
                ge_owning_suite = present_non_ge[0]
            else:
                ge_owning_suite = "bronze"

            # Collect non-GE suite entries
            for suite_name in non_ge_suites:
                if suite_name not in suggestions:
                    continue
                suite_data = suggestions[suite_name]
                entry = {
                    "catalog": suite_data["catalog"],
                    "schema": suite_data["schema"],
                    "table": suite_data["table"],
                    "checks": suite_data.get("checks", {}),
                }
                parameterized = self._parameterize_table_cfg(entry, parameterize_catalog)
                suite_tables.setdefault(suite_name, [])
                suite_tables[suite_name].append((safe, parameterized))

            # Collect GE entry
            if "great_expectations" in suggestions:
                ge_data = suggestions["great_expectations"]
                ge_entry = {
                    "catalog": ge_data["catalog"],
                    "schema": ge_data["schema"],
                    "table": ge_data["table"],
                    "checks": ge_data.get("checks", {}),
                }
                ge_parameterized = self._parameterize_table_cfg(ge_entry, parameterize_catalog)
                ge_entries.setdefault(ge_owning_suite, {})
                ge_entries[ge_owning_suite][safe] = ge_parameterized

        # ------------------------------------------------------------------
        # Write global/policy.yaml
        # ------------------------------------------------------------------
        if include_policy:
            policy_content = self._format_yaml_suggestions(
                {
                    "dq_policy": {
                        "block_test_suites": {
                            "profiling": False,
                            "bronze": True,
                            "silver": True,
                            "gold": True,
                        }
                    }
                }
            )
            _maybe_write(root / "global" / "policy.yaml", policy_content)

        # ------------------------------------------------------------------
        # Write global/profiles.yml
        # ------------------------------------------------------------------
        if include_profiles:
            target_catalog = first_catalog or "analytics_dev"
            profiles_content = self._format_yaml_suggestions(
                {
                    "default_profile": "dev",
                    "profiles": {
                        "dev": {
                            "target_catalog": target_catalog,
                            "variables": {},
                        }
                    },
                }
            )
            _maybe_write(root / "global" / "profiles.yml", profiles_content)

        # ------------------------------------------------------------------
        # Write per-suite files
        # ------------------------------------------------------------------
        # All suites that have at least one table entry OR GE entries
        all_suites = set(suite_tables.keys()) | set(ge_entries.keys())

        for suite_name in sorted(all_suites):
            suite_dir = root / "suites" / suite_name

            # suite.yaml — {"suite": <name>} (default_checks not required by resolver)
            suite_yaml_content = self._format_yaml_suggestions({"suite": suite_name})
            _maybe_write(suite_dir / "suite.yaml", suite_yaml_content)

            # test_groups/<safe_table>.yaml — {"tables": [entry]}
            for safe, entry in suite_tables.get(suite_name, []):
                tg_content = self._format_yaml_suggestions({"tables": [entry]}, blank_between_checks=True)
                _maybe_write(suite_dir / "test_groups" / f"{safe}.yaml", tg_content)

            # great_expectations/<safe_table>.yaml — bare dict (no tables wrapper)
            for safe, ge_entry in ge_entries.get(suite_name, {}).items():
                ge_content = self._format_yaml_suggestions(ge_entry, blank_between_checks=True)
                _maybe_write(suite_dir / "great_expectations" / f"{safe}.yaml", ge_content)

        return written
