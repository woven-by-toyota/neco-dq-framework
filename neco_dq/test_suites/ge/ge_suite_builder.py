# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import logging
import great_expectations as gx


# ----------------------------------------------------------------------
# Metadata helpers
# ----------------------------------------------------------------------


def _apply_meta(expectation, cfg: dict):
    """
    Attach framework metadata to a GE expectation.
    """
    expectation.meta = {
        "severity": cfg.get("severity", "warning"),
        "blocking": cfg.get("blocking", False),
        "rule_description": cfg.get("rule_description"),
    }
    return expectation


def _get_row_condition_kwargs(cfg: dict) -> dict:
    """
    Extract row_condition parameters from config if present.

    Config:
        row_condition: Pandas query expression to filter rows (e.g., "status == 'ACTIVE'")

    Returns:
        dict with row_condition and condition_parser if row_condition is configured,
        empty dict otherwise.

    Note:
        Uses pandas query syntax. Examples:
        - "status == 'ACTIVE'"
        - "amount > 0"
        - "category in ['A', 'B']"
    """
    row_condition = cfg.get("row_condition")
    if row_condition:
        return {
            "row_condition": row_condition,
            "condition_parser": "pandas",
        }
    return {}


def _add_column_exists(suite: gx.ExpectationSuite, column: str):
    """
    Column existence is always enforced and always blocking.
    """
    exp = gx.expectations.ExpectColumnToExist(column=column)
    exp.meta = {"severity": "critical", "blocking": True}
    suite.add_expectation(exp)


# ----------------------------------------------------------------------
# Column-level rule handlers
# ----------------------------------------------------------------------


def _handle_nullable(suite, col, cfg):
    """
    Handle nullable rule - enforces NOT NULL when nullable is disabled.

    Config:
        enabled: When False, column must not contain nulls (default: True, meaning nulls allowed)
        mostly: Fraction of values that must be non-null (default: 1.0)
        row_condition: Optional pandas query to filter rows (e.g., "status == 'ACTIVE'")
    """
    if cfg.get("enabled", True) is False:
        exp = gx.expectations.ExpectColumnValuesToNotBeNull(
            column=col,
            mostly=cfg.get("mostly", 1.0),
            **_get_row_condition_kwargs(cfg),
        )
        suite.add_expectation(_apply_meta(exp, cfg))


def _handle_not_null(suite, col, cfg):
    """
    Handle not_null rule - enforces column contains no null values.

    Config:
        enabled: When True, column must not contain nulls (default: False)
        mostly: Fraction of values that must be non-null (default: 1.0)
        row_condition: Optional pandas query to filter rows (e.g., "status == 'ACTIVE'")
    """
    if cfg.get("enabled", False):
        exp = gx.expectations.ExpectColumnValuesToNotBeNull(
            column=col,
            mostly=cfg.get("mostly", 1.0),
            **_get_row_condition_kwargs(cfg),
        )
        suite.add_expectation(_apply_meta(exp, cfg))


def _handle_unique(suite, col, cfg):
    """
    Handle unique rule - enforces column values are unique.

    Config:
        enabled: When True, column must contain unique values (default: False)
        column_list: Optional list of columns for compound uniqueness check
        row_condition: Optional pandas query to filter rows (single column only)

    Note:
        row_condition is only supported for single-column uniqueness checks.
        Compound uniqueness (column_list) does not support row_condition.
    """
    if not cfg.get("enabled", False):
        return

    if cfg.get("column_list"):
        base_list = cfg["column_list"]
        column_list = base_list if col in base_list else [col, *base_list]
        exp = gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=column_list)
    else:
        exp = gx.expectations.ExpectColumnValuesToBeUnique(
            column=col,
            **_get_row_condition_kwargs(cfg),
        )

    suite.add_expectation(_apply_meta(exp, cfg))


def _handle_dtype(suite, col, cfg):
    """
    Handle dtype rule - enforces column data type is in allowed list.

    Config:
        enabled: When True, column type is validated (default: False)
        values: List of allowed data types (e.g., ["StringType", "IntegerType"])
    """
    if not cfg.get("enabled", False):
        return
    values = cfg.get("values", [])
    if not values:
        return
    exp = gx.expectations.ExpectColumnValuesToBeInTypeList(
        column=col,
        type_list=values,
    )
    suite.add_expectation(_apply_meta(exp, cfg))


def _handle_between(suite, col, cfg):
    """
    Handle between rule - enforces column values are within a range.

    Config:
        enabled: When True, values are validated against range (default: False)
        min: Minimum allowed value (optional)
        max: Maximum allowed value (optional)
        mostly: Fraction of values that must be within range (default: 1.0)
        row_condition: Optional pandas query to filter rows (e.g., "status == 'ACTIVE'")
    """
    if not cfg.get("enabled", False):
        return
    exp = gx.expectations.ExpectColumnValuesToBeBetween(
        column=col,
        min_value=cfg.get("min"),
        max_value=cfg.get("max"),
        mostly=cfg.get("mostly", 1.0),
        **_get_row_condition_kwargs(cfg),
    )
    suite.add_expectation(_apply_meta(exp, cfg))


def _handle_values_set(suite, col, cfg):
    """
    Handle values_set rule - enforces column values are in allowed set.

    Config:
        enabled: When True, values are validated against set (default: False)
        values: List of allowed values
        mostly: Fraction of values that must be in set (default: 1.0)
        row_condition: Optional pandas query to filter rows (e.g., "status == 'ACTIVE'")
    """
    if not cfg.get("enabled", False):
        return
    values = cfg.get("values", [])
    if not values:
        return
    exp = gx.expectations.ExpectColumnValuesToBeInSet(
        column=col,
        value_set=values,
        mostly=cfg.get("mostly", 1.0),
        **_get_row_condition_kwargs(cfg),
    )
    suite.add_expectation(_apply_meta(exp, cfg))


def _handle_regex(suite, col, cfg):
    """
    Handle regex rule - enforces column values match a regex pattern.

    Config:
        enabled: When True, values are validated against pattern (default: False)
        pattern: Regular expression pattern to match
        mostly: Fraction of values that must match (default: 1.0)
        row_condition: Optional pandas query to filter rows (e.g., "status == 'ACTIVE'")
    """
    if not cfg.get("enabled", False):
        return
    pattern = cfg.get("pattern")
    if not pattern:
        return
    exp = gx.expectations.ExpectColumnValuesToMatchRegex(
        column=col,
        regex=pattern,
        mostly=cfg.get("mostly", 1.0),
        **_get_row_condition_kwargs(cfg),
    )
    suite.add_expectation(_apply_meta(exp, cfg))


def _handle_length(suite, col, cfg):
    """
    Handle length rule - enforces string value lengths fall within a range.

    Config:
        enabled: When True, value lengths are validated against the range (default: False)
        min: Minimum allowed length (optional)
        max: Maximum allowed length (optional)
        mostly: Fraction of values that must fall within range (default: 1.0)
        row_condition: Optional pandas query to filter rows (e.g., "status == 'ACTIVE'")
    """
    if not cfg.get("enabled", False):
        return
    exp = gx.expectations.ExpectColumnValueLengthsToBeBetween(
        column=col,
        min_value=cfg.get("min"),
        max_value=cfg.get("max"),
        mostly=cfg.get("mostly", 1.0),
        **_get_row_condition_kwargs(cfg),
    )
    suite.add_expectation(_apply_meta(exp, cfg))


def _handle_date_format(suite, col, cfg):
    """
    Handle date_format rule - enforces string values match a strftime date format.

    Config:
        enabled: When True, values are validated against the format (default: False)
        format: strftime pattern to match (e.g., "%Y-%m-%d")
        mostly: Fraction of values that must match (default: 1.0)
        row_condition: Optional pandas query to filter rows (e.g., "status == 'ACTIVE'")

    Note: formats containing %z or %Z are skipped. GE validates the format string at
    construction time using a naive datetime, so %z is rejected before any data is checked.
    Even if the construction were to succeed, strptime would fail on values with a timezone
    suffix (e.g. +00:00) because the suffix is not consumed. Use the regex check instead
    for timezone-aware timestamp columns.
    """
    if not cfg.get("enabled", False):
        return
    date_format = cfg.get("format")
    if not date_format:
        return
    if "%z" in date_format or "%Z" in date_format:
        logging.getLogger(__name__).warning(
            "date_format check skipped for column '%s': %%z/%%Z directives are not supported. "
            "Use the regex check to validate timezone-aware timestamp formats.",
            col,
        )
        return
    exp = gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
        column=col,
        strftime_format=date_format,
        mostly=cfg.get("mostly", 1.0),
        **_get_row_condition_kwargs(cfg),
    )
    suite.add_expectation(_apply_meta(exp, cfg))


def _handle_not_in_set(suite, col, cfg):
    """
    Handle not_in_set rule - enforces column values are NOT in a forbidden set.

    Useful as a blocklist for placeholder/dirty values (e.g., "N/A", "unknown", "-1").

    Config:
        enabled: When True, values are validated against the forbidden set (default: False)
        values: List of forbidden values
        mostly: Fraction of values that must NOT be in the set (default: 1.0)
        row_condition: Optional pandas query to filter rows (e.g., "status == 'ACTIVE'")
    """
    if not cfg.get("enabled", False):
        return
    values = cfg.get("values", [])
    if not values:
        return
    exp = gx.expectations.ExpectColumnValuesToNotBeInSet(
        column=col,
        value_set=values,
        mostly=cfg.get("mostly", 1.0),
        **_get_row_condition_kwargs(cfg),
    )
    suite.add_expectation(_apply_meta(exp, cfg))


def _handle_distinct_values_in_set(suite, col, cfg):
    """
    Handle distinct_values_in_set rule - enforces the set of distinct values is a
    subset of the allowed set.

    Unlike values_set (which checks each row with a `mostly` tolerance), this checks
    the column's distinct value set as a whole, so it is robust for enum columns.

    Config:
        enabled: When True, distinct values are validated against the set (default: False)
        values: List of allowed values
    """
    if not cfg.get("enabled", False):
        return
    values = cfg.get("values", [])
    if not values:
        return
    exp = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
        column=col,
        value_set=values,
    )
    suite.add_expectation(_apply_meta(exp, cfg))


COLUMN_RULE_HANDLERS = {
    "nullable": _handle_nullable,
    "not_null": _handle_not_null,
    "unique": _handle_unique,
    "dtype": _handle_dtype,
    "between": _handle_between,
    "values_set": _handle_values_set,
    "regex": _handle_regex,
    "length": _handle_length,
    "date_format": _handle_date_format,
    "not_in_set": _handle_not_in_set,
    "distinct_values_in_set": _handle_distinct_values_in_set,
}


# ----------------------------------------------------------------------
# Dataset-level rules
# ----------------------------------------------------------------------


def _handle_dataset_rules(suite, dataset_rules: dict, schema: dict):
    """
    Dataset-level rules:
    - min_rows
    - max_rows
    - integrity.pair_gte
    - integrity.pair_eq
    - integrity.pair_gt
    - no_duplicate
    - multicolumn_sum
    """

    # -----------------------------
    # Volume
    # -----------------------------
    min_rows = dataset_rules.get("min_rows")
    max_rows = dataset_rules.get("max_rows")

    if min_rows is not None or max_rows is not None:
        exp = gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=min_rows,
            max_value=max_rows,
        )
        suite.add_expectation(_apply_meta(exp, dataset_rules))

    # -----------------------------
    # Integrity / cross-column
    # -----------------------------
    integrity = dataset_rules.get("integrity", {})

    for rule in integrity.get("pair_gte", []):
        exp = gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A=rule["column_A"],
            column_B=rule["column_B"],
            or_equal=True,
            mostly=rule.get("mostly", 1.0),
            ignore_row_if=rule.get("ignore_row_if", "either_value_is_missing"),
        )
        suite.add_expectation(_apply_meta(exp, rule))

    for rule in integrity.get("pair_eq", []):
        exp = gx.expectations.ExpectColumnPairValuesToBeEqual(
            column_A=rule["column_A"],
            column_B=rule["column_B"],
            mostly=rule.get("mostly", 1.0),
            ignore_row_if=rule.get("ignore_row_if", "either_value_is_missing"),
        )
        suite.add_expectation(_apply_meta(exp, rule))

    for rule in integrity.get("pair_gt", []):
        exp = gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A=rule["column_A"],
            column_B=rule["column_B"],
            or_equal=False,
            mostly=rule.get("mostly", 1.0),
            ignore_row_if=rule.get("ignore_row_if", "either_value_is_missing"),
        )
        suite.add_expectation(_apply_meta(exp, rule))

    # -----------------------------
    # No duplicates
    # -----------------------------
    no_dup_cfg = dataset_rules.get("no_duplicate", {})

    if no_dup_cfg.get("enabled"):
        cols_to_check = None

        # Priority 1: explicit column_list
        if no_dup_cfg.get("column_list"):
            cols_to_check = no_dup_cfg["column_list"]

        # Priority 2: schema-defined columns
        elif no_dup_cfg.get("use_schema_columns"):
            cols_to_check = [col for col in schema if col != "_dataset"]

        # Guardrail
        if not cols_to_check:
            raise ValueError(
                "no_duplicate enabled but no columns provided. Define 'column_list' or set 'use_schema_columns: true'."
            )

        exp = gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=cols_to_check)
        suite.add_expectation(_apply_meta(exp, no_dup_cfg))

    # -----------------------------
    # Multi-column sum (per-row arithmetic invariant, e.g. subtotal + tax == total)
    # -----------------------------
    for rule in dataset_rules.get("multicolumn_sum", []):
        if not rule.get("enabled", True):
            continue
        column_list = rule.get("column_list")
        sum_total = rule.get("sum_total")
        if not column_list or sum_total is None:
            raise ValueError("multicolumn_sum rule requires 'column_list' and 'sum_total'.")
        exp = gx.expectations.ExpectMulticolumnSumToEqual(
            column_list=column_list,
            sum_total=sum_total,
            mostly=rule.get("mostly", 1.0),
            ignore_row_if=rule.get("ignore_row_if", "all_values_are_missing"),
        )
        suite.add_expectation(_apply_meta(exp, rule))


# ----------------------------------------------------------------------
# Builder
# ----------------------------------------------------------------------


def build_suite_from_schema(schema: dict, suite_name: str) -> gx.ExpectationSuite:
    """
    Build a Great Expectations suite from a declarative schema.

    Args:
        schema: Dictionary mapping column names to their validation rules.
                Special key "_dataset" contains dataset-level rules.
        suite_name: Name for the expectation suite (will be suffixed with "_suite")

    Returns:
        gx.ExpectationSuite: Configured Great Expectations suite

    Column-level rules (per column):
        - nullable: Allow/disallow null values
        - not_null: Require non-null values
        - unique: Require unique values
        - dtype: Validate data type
        - between: Validate value range
        - values_set: Validate against allowed values
        - regex: Validate against pattern
        - length: Validate string value lengths fall within a range
        - date_format: Validate string values match a strftime date format
        - not_in_set: Validate values are not in a forbidden set (blocklist)
        - distinct_values_in_set: Validate the distinct value set is a subset of allowed

    Dataset-level rules (_dataset key):
        - min_rows/max_rows: Row count bounds
        - integrity.pair_gte/pair_eq/pair_gt: Column pair comparisons
        - no_duplicate: Compound uniqueness check
        - multicolumn_sum: Per-row arithmetic invariant across columns
    """

    suite = gx.ExpectationSuite(f"{suite_name}_suite")

    # --------------------------------------------------
    # Column-level rules
    # --------------------------------------------------
    for col, rules in schema.items():
        if col == "_dataset":
            continue

        _add_column_exists(suite, col)

        for rule_name, cfg in rules.items():
            handler = COLUMN_RULE_HANDLERS.get(rule_name)
            if handler:
                handler(suite, col, cfg)

    # --------------------------------------------------
    # Dataset-level rules
    # --------------------------------------------------
    _handle_dataset_rules(suite, schema.get("_dataset", {}), schema)

    return suite
