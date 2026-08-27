# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import math
from pathlib import Path
from typing import Any

import yaml

from jinja2 import Environment, BaseLoader, StrictUndefined, select_autoescape

from neco_dq.config.context import DQContext

# ---------------------------------------------------------
# Registry (legacy templates)
# ---------------------------------------------------------
DEFAULT_TEMPLATE_REGISTRY = Path(__file__).resolve().parents[1] / "config" / "template_registry.yaml"

# ---------------------------------------------------------
# Workspace SDK client (lazy loaded)
# ---------------------------------------------------------
_workspace_client = None


def _get_workspace_client():
    """Get or create a Databricks WorkspaceClient."""
    global _workspace_client  # noqa: PLW0603
    if _workspace_client is None:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415

        _workspace_client = WorkspaceClient()
    return _workspace_client


def _read_workspace_file(path_str: str) -> str:
    """Read a file from Databricks Workspace using the SDK."""
    import base64  # noqa: PLC0415

    from databricks.sdk.service.workspace import ExportFormat  # noqa: PLC0415

    client = _get_workspace_client()
    response = client.workspace.export(path=path_str, format=ExportFormat.SOURCE)
    if response.content:
        return base64.b64decode(response.content).decode("utf-8")
    raise FileNotFoundError(f"Empty or missing file: {path_str}")


def _list_workspace_dir(path_str: str) -> list[str]:
    """List contents of a Databricks Workspace directory using the SDK."""
    client = _get_workspace_client()
    items = list(client.workspace.list(path=path_str))
    return [item.path for item in items if item.path]


# ---------------------------------------------------------
# Utils
# ---------------------------------------------------------
def _try_parse_collection(value: str) -> list | dict | None:
    """Try to parse string as JSON or Python list/dict literal."""
    stripped = value.strip()
    if not stripped.startswith(("[", "{")):
        return None

    import ast  # noqa: PLC0415
    import json  # noqa: PLC0415

    # Try JSON first
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass  # Not valid JSON; try a Python literal next.
    # Try Python literal (handles ['a', 'b'] syntax)
    try:
        parsed = ast.literal_eval(stripped)
        if isinstance(parsed, (list, dict)):
            return parsed
    except (ValueError, SyntaxError):
        pass  # Not a list/dict literal either; fall back to None.
    return None


def _convert_string_value(value: str) -> int | float | bool | list | dict | str:
    """Convert a string to int, float, bool, list, or dict if possible."""
    # Try list/dict
    collection = _try_parse_collection(value)
    if collection is not None:
        return collection
    # Try int
    try:
        return int(value)
    except ValueError:
        pass
    # Try float, but reject non-finite forms (nan, inf, -inf, infinity).
    # float("nan") succeeds and would inject a real float('nan') into config
    # values (e.g. a values_set allowed-value list), which crashes downstream
    # consumers such as Great Expectations. These were never intended as numeric
    # config literals - keep them as plain strings.
    try:
        parsed_float = float(value)
        if math.isfinite(parsed_float):
            return parsed_float
    except ValueError:
        pass
    # Try bool
    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    return value


def _auto_convert_types(obj: Any) -> Any:
    """Recursively convert string values that look like numbers or booleans.

    This handles values from Jinja2 templating which always produces strings
    when the template is quoted in YAML (e.g., "{{ var.x }}").

    Converts:
    - "123" -> 123 (int)
    - "12.5" -> 12.5 (float)
    - "true"/"false" -> True/False (bool)
    """
    if isinstance(obj, dict):
        return {k: _auto_convert_types(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_auto_convert_types(item) for item in obj]
    if isinstance(obj, str):
        return _convert_string_value(obj)
    return obj


def _load_yaml(path: Path, context: DQContext | None = None) -> dict[str, Any]:
    with path.open() as f:
        content = f.read()
    rendered = _render_with_context(content, context)
    parsed = yaml.safe_load(rendered) or {}
    return _auto_convert_types(parsed)


def _render_with_context(content: str, context: DQContext | None) -> str:
    """Render Jinja2 templates in YAML content.

    Behavior:
    - No Jinja2 syntax in content: return unchanged (fast path)
    - Jinja2 syntax present: render with context (or empty context if None)

    Uses StrictUndefined so {{ var.x }} fails if x not defined,
    but {{ var.x | default(y) }} works even without a profile.

    Args:
        content: Raw YAML content that may contain Jinja2 templates.
        context: DQContext with profile data, or None for empty context.

    Returns:
        Rendered content with Jinja2 templates resolved.

    Raises:
        jinja2.UndefinedError: If a variable is undefined and has no default.
    """
    if "{{" not in content and "{%" not in content:
        return content

    import json  # noqa: PLC0415

    # This renders YAML config text, not HTML, so HTML-escaping would corrupt
    # values and no browser/XSS context applies. select_autoescape with
    # default_for_string=False keeps escaping off for our from_string() render
    # while following the Jinja2-recommended construction (no blanket disable).
    env = Environment(
        loader=BaseLoader(),
        undefined=StrictUndefined,
        autoescape=select_autoescape(default_for_string=False),
    )
    env.filters["tojson"] = json.dumps
    template = env.from_string(content)

    jinja_context = context.to_jinja_context() if context else {"profile": {}, "var": {}}

    return template.render(**jinja_context)


def _load_yaml_from_workspace(path_str: str, context: DQContext | None = None) -> dict[str, Any]:
    """Load YAML from Databricks Workspace path."""
    import logging  # noqa: PLC0415

    logger = logging.getLogger("neco_dq")
    content = _read_workspace_file(path_str)
    logger.debug("Loaded %d bytes from %s", len(content), path_str)
    logger.debug("Content preview: %s...", content[:500] if len(content) > 500 else content)  # noqa: PLR2004

    rendered = _render_with_context(content, context)
    parsed = yaml.safe_load(rendered) or {}
    logger.debug("Parsed YAML keys: %s", list(parsed.keys()) if isinstance(parsed, dict) else type(parsed))
    return _auto_convert_types(parsed)


# ---------------------------------------------------------
# single YAML
# ---------------------------------------------------------
def _resolve_legacy_yaml(path: Path, context: DQContext | None = None) -> dict[str, Any]:
    cfg = _load_yaml(path, context)
    if not isinstance(cfg, dict):
        raise ValueError(f"Invalid YAML structure in {path}")
    return cfg


# ---------------------------------------------------------
# template registry
# ---------------------------------------------------------
def _resolve_template_tag(tag: str, context: DQContext | None = None) -> dict[str, Any]:
    if not DEFAULT_TEMPLATE_REGISTRY.exists():
        raise FileNotFoundError(
            f"Config not found: '{tag}'. "
            f"Ensure the path exists or use an absolute path to a YAML file or modular config directory."
        )

    registry = _load_yaml(DEFAULT_TEMPLATE_REGISTRY)

    if tag not in registry:
        raise KeyError(f"Template '{tag}' not found. Available: {list(registry.keys())}")

    resolved_path = Path(registry[tag]).expanduser().resolve()
    return _resolve_legacy_yaml(resolved_path, context)


# ---------------------------------------------------------
# modular config
# ---------------------------------------------------------
def _resolve_test_groups(test_groups_dir: Path, context: DQContext | None = None) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []

    if not _path_exists(test_groups_dir):
        return tables

    try:
        yaml_files = sorted(test_groups_dir.glob("*.yaml"))
    except (OSError, PermissionError):
        return tables

    for f in yaml_files:
        cfg = _load_yaml(f, context)

        if "tables" in cfg:
            tables.extend(cfg["tables"])
        else:
            tables.append(cfg)

    return tables


def _resolve_ge_tables(ge_dir: Path, context: DQContext | None = None) -> list[dict[str, Any]]:
    ge_tables: list[dict[str, Any]] = []

    if not _path_exists(ge_dir):
        return ge_tables

    try:
        yaml_files = sorted(ge_dir.glob("*.yaml"))
    except (OSError, PermissionError):
        return ge_tables

    for f in yaml_files:
        ge_cfg = _load_yaml(f, context)

        if not isinstance(ge_cfg, dict):
            raise ValueError(f"Invalid GE yaml: {f}")

        if "tables" in ge_cfg:
            if not isinstance(ge_cfg["tables"], list):
                raise ValueError(f"'tables' must be a list in {f}")
            ge_tables.extend(ge_cfg["tables"])
        else:
            ge_cfg.setdefault("table", f.stem)
            ge_tables.append(ge_cfg)

    return ge_tables


def _resolve_suite(suite_dir: Path, context: DQContext | None = None) -> dict[str, Any]:
    suite_name = suite_dir.name
    resolved: dict[str, Any] = {}

    suite_yaml = suite_dir / "suite.yaml"
    if not _path_exists(suite_yaml):
        raise FileNotFoundError(f"Missing suite.yaml in {suite_dir}")

    suite_cfg = _load_yaml(suite_yaml, context)
    if not isinstance(suite_cfg, dict):
        raise ValueError(f"Invalid suite.yaml structure in {suite_dir}")

    tables = _resolve_test_groups(suite_dir / "test_groups", context)
    if tables:
        suite_cfg["tables"] = tables

    resolved[suite_name] = suite_cfg

    ge_tables = _resolve_ge_tables(suite_dir / "great_expectations", context)
    if ge_tables:
        resolved[f"great_expectations@{suite_name}"] = {
            "parent_suite": suite_name,
            "tables": ge_tables,
            "data_to_save": suite_cfg.get("data_to_save"),
        }

    return resolved


# ---------------------------------------------------------
# Modular config: root
# ---------------------------------------------------------
def _resolve_modular_config(root: Path, context: DQContext | None = None) -> dict[str, Any]:
    config: dict[str, Any] = {}

    # -----------------------------
    # global policy
    # -----------------------------
    policy_file = root / "global" / "policy.yaml"
    if not _path_exists(policy_file):
        raise FileNotFoundError("Missing global/policy.yaml")

    policy_cfg = _load_yaml(policy_file, context)
    config["dq_policy"] = policy_cfg.get("dq_policy", {})

    # -----------------------------
    # suites
    # -----------------------------
    suites_dir = root / "suites"
    if not _path_exists(suites_dir):
        raise FileNotFoundError("Missing suites/ directory")

    try:
        suite_dirs = sorted(suites_dir.iterdir())
    except (OSError, PermissionError) as e:
        raise FileNotFoundError(f"Cannot read suites directory: {e}") from e

    for suite_dir in suite_dirs:
        # Check if it's a directory by trying to list it
        try:
            list(suite_dir.iterdir())
        except (NotADirectoryError, OSError):
            continue

        suites = _resolve_suite(suite_dir, context)
        config.update(suites)

    # Verify at least one test suite was loaded (config should have more than just dq_policy)
    if len(config) == 1 and "dq_policy" in config:
        raise ValueError(
            "No test suites found in modular config. Ensure suites/ directory contains valid suite subdirectories."
        )

    return config


# ---------------------------------------------------------
# Databricks path helpers
# ---------------------------------------------------------
_DATABRICKS_PATH_MODE = False


def _is_databricks_path(path_str: str) -> bool:
    """Check if path is a Databricks-specific path (Workspace or Volumes)."""
    return path_str.startswith("/Workspace/") or path_str.startswith("/Volumes/")


def _path_exists(path: Path) -> bool:
    """Check if path exists, handling Databricks paths."""
    if _DATABRICKS_PATH_MODE:
        # For Databricks paths, we can't use .exists() - try to open instead
        try:
            if path.suffix in (".yaml", ".yml"):
                with path.open():
                    return True
            else:
                # For directories, try to list
                list(path.iterdir())
                return True
        except (FileNotFoundError, OSError, PermissionError):
            return False
    return path.exists()


def _resolve_databricks_path(path_str: str, context: DQContext | None = None) -> dict[str, Any]:
    """
    Resolve a Databricks-specific path using the Databricks SDK.

    For /Workspace/ paths, uses the Workspace API to read files.
    For /Volumes/ paths, uses direct file I/O (Volumes are FUSE-mounted).
    """
    # /Volumes/ paths are FUSE-mounted and can use direct I/O
    if path_str.startswith("/Volumes/"):
        global _DATABRICKS_PATH_MODE  # noqa: PLW0603
        _DATABRICKS_PATH_MODE = True
        try:
            path = Path(path_str)
            if path_str.endswith((".yaml", ".yml")):
                return _resolve_legacy_yaml(path, context)
            return _resolve_modular_config(path, context)
        finally:
            _DATABRICKS_PATH_MODE = False

    # /Workspace/ paths require the SDK
    if path_str.endswith((".yaml", ".yml")):
        # Single YAML file
        cfg = _load_yaml_from_workspace(path_str, context)
        if not isinstance(cfg, dict):
            raise ValueError(f"Invalid YAML structure in {path_str}")
        return cfg

    # Modular config directory
    return _resolve_modular_config_from_workspace(path_str, context)


def _resolve_modular_config_from_workspace(root_path: str, context: DQContext | None = None) -> dict[str, Any]:  # noqa: PLR0912, PLR0915
    """Resolve a modular config from Databricks Workspace using SDK."""
    import logging  # noqa: PLC0415

    from databricks.sdk.errors import NotFound, ResourceDoesNotExist  # noqa: PLC0415

    logger = logging.getLogger("neco_dq")
    config: dict[str, Any] = {}

    # Load global policy
    policy_path = f"{root_path}/global/policy.yaml"
    try:
        policy_cfg = _load_yaml_from_workspace(policy_path, context)
        config["dq_policy"] = policy_cfg.get("dq_policy", {})
    except (FileNotFoundError, NotFound, ResourceDoesNotExist) as e:
        raise FileNotFoundError(f"Missing global/policy.yaml: {e}") from e

    # List suites directory
    suites_path = f"{root_path}/suites"
    try:
        suite_dirs = _list_workspace_dir(suites_path)
    except (FileNotFoundError, NotFound, ResourceDoesNotExist) as e:
        raise FileNotFoundError(f"Missing or unreadable suites/ directory: {e}") from e

    for suite_dir_path in suite_dirs:
        suite_name = suite_dir_path.split("/")[-1]

        # Load suite.yaml
        suite_yaml_path = f"{suite_dir_path}/suite.yaml"
        try:
            suite_cfg = _load_yaml_from_workspace(suite_yaml_path, context)
        except (FileNotFoundError, NotFound, ResourceDoesNotExist):
            logger.debug("Skipping %s: no suite.yaml found", suite_dir_path)
            continue  # Skip if no suite.yaml

        if not isinstance(suite_cfg, dict):
            continue

        # Load test_groups (optional)
        test_groups_path = f"{suite_dir_path}/test_groups"
        try:
            test_group_files = _list_workspace_dir(test_groups_path)
            tables = []
            for tg_file in test_group_files:
                if tg_file.endswith(".yaml"):
                    tg_cfg = _load_yaml_from_workspace(tg_file, context)
                    if "tables" in tg_cfg:
                        tables.extend(tg_cfg["tables"])
                    else:
                        tables.append(tg_cfg)
            if tables:
                suite_cfg["tables"] = tables
        except (FileNotFoundError, NotFound, ResourceDoesNotExist):
            pass  # No test_groups directory is OK

        config[suite_name] = suite_cfg

        # Load great_expectations if present (optional)
        ge_path = f"{suite_dir_path}/great_expectations"
        try:
            ge_files = _list_workspace_dir(ge_path)
            ge_tables = []
            for ge_file in ge_files:
                if ge_file.endswith(".yaml"):
                    ge_cfg = _load_yaml_from_workspace(ge_file, context)
                    if "tables" in ge_cfg:
                        ge_tables.extend(ge_cfg["tables"])
                    else:
                        ge_cfg.setdefault("table", ge_file.split("/")[-1].replace(".yaml", ""))
                        ge_tables.append(ge_cfg)
            if ge_tables:
                config[f"great_expectations@{suite_name}"] = {
                    "parent_suite": suite_name,
                    "tables": ge_tables,
                    "data_to_save": suite_cfg.get("data_to_save"),
                }
        except (FileNotFoundError, NotFound, ResourceDoesNotExist):
            pass  # No great_expectations directory is OK

    if len(config) == 1 and "dq_policy" in config:
        raise ValueError("No test suites found in modular config.")

    return config


# ---------------------------------------------------------
# Public API
# ---------------------------------------------------------
def _merge_profile_policy(config: dict[str, Any], context: DQContext | None) -> dict[str, Any]:
    """Merge profile policy overrides into dq_policy.

    Profile policy values override global policy values.

    Args:
        config: Resolved configuration dictionary.
        context: DQContext with optional policy overrides.

    Returns:
        Config with merged policy.
    """
    if not context or not context.policy:
        return config

    # Get existing dq_policy or empty dict
    global_policy = config.get("dq_policy", {})

    # Merge: profile policy overrides global
    merged_policy = {**global_policy, **context.policy}

    # Deep merge block_test_suites if both exist
    if "block_test_suites" in global_policy and "block_test_suites" in context.policy:
        merged_policy["block_test_suites"] = {
            **global_policy.get("block_test_suites", {}),
            **context.policy.get("block_test_suites", {}),
        }

    config["dq_policy"] = merged_policy
    return config


def resolve_config(config_ref: str | Path, context: DQContext | None = None) -> dict[str, Any]:
    """
    Supported inputs:
    - path to YAML file (legacy)
    - path to modular config directory
    - Databricks Workspace path (/Workspace/...)
    - Databricks Volume path (/Volumes/...)
    - template registry tag

    Args:
        config_ref: Path to config file, directory, or template tag.
        context: Optional DQContext for Jinja2 variable resolution.

    Returns:
        Resolved configuration dictionary.
    """
    config_str = str(config_ref)

    # Handle Databricks-specific paths first
    # (pathlib.exists() doesn't work for /Workspace/ and /Volumes/ paths)
    if _is_databricks_path(config_str):
        config = _resolve_databricks_path(config_str, context)
        return _merge_profile_policy(config, context)

    # Handle regular filesystem paths
    path = Path(config_ref).expanduser()

    if path.exists():
        path = path.resolve()

        if path.is_file():
            config = _resolve_legacy_yaml(path, context)
            return _merge_profile_policy(config, context)

        if path.is_dir():
            config = _resolve_modular_config(path, context)
            return _merge_profile_policy(config, context)

    # Not a path on disk: fall back to a template registry tag (non-path refs).
    if isinstance(config_ref, str):
        try:
            config = _resolve_template_tag(config_ref, context)
            return _merge_profile_policy(config, context)
        except (FileNotFoundError, KeyError):
            pass

    # Neither a path nor a known template tag - provide a helpful error message.
    raise FileNotFoundError(
        f"Config path not found: '{config_ref}'. "
        f"Ensure the path exists and points to a valid YAML file or modular config directory."
    )
