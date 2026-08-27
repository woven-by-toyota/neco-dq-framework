# SPDX-FileCopyrightText: 2026 Toyota Motor Corporation
#
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import re
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from neco_dq.test_suites.profiling.constants import (
    _DATE_FORMAT_CANDIDATES,
    _DATE_FORMAT_MIN_RATIO,
    _DATE_FORMAT_SAMPLE_SIZE,
    _JSON_DETECTION_MIN_RATIO,
    _JSON_DETECTION_SAMPLE_SIZE,
)

logger = logging.getLogger(__name__)


class JsonDetectionMixin:
    """Mixin providing JSON column detection and field extraction utilities."""

    def _detect_json_column(self, df: DataFrame, column: str) -> dict[str, Any]:
        """
        Detect if a string column contains JSON data.

        Samples non-null values and checks if they start with '[' or '{'.
        Returns info about JSON structure if detected.

        Configuration via module constants:
        - _JSON_DETECTION_SAMPLE_SIZE: rows to sample (default: 50)
        - _JSON_DETECTION_MIN_RATIO: min ratio of JSON-like values (default: 0.8)

        Returns:
            dict with: is_json, is_array, sample_fields, field_types
        """
        result = {"is_json": False, "is_array": False, "sample_fields": []}

        try:
            # Sample non-null values
            sample_df = df.filter(f.col(column).isNotNull()).select(column).limit(_JSON_DETECTION_SAMPLE_SIZE)
            sample_values = [row[column] for row in sample_df.collect()]

            if not sample_values:
                return result

            # Count JSON-like values
            json_like_count, array_count, object_count = self._count_json_like_values(sample_values)

            # Consider JSON if ratio of JSON-like values meets threshold
            if len(sample_values) > 0 and json_like_count / len(sample_values) >= _JSON_DETECTION_MIN_RATIO:
                result["is_json"] = True
                result["is_array"] = array_count > object_count
                sample_fields, field_types = self._extract_json_sample_fields(sample_values)
                result["sample_fields"] = sample_fields
                result["field_types"] = field_types

        except Exception:
            logger.exception("Failed to detect JSON column '%s'", column)

        return result

    def _count_json_like_values(self, sample_values: list) -> tuple[int, int, int]:
        """Count JSON-like values and classify as array or object."""
        json_like_count = 0
        array_count = 0
        object_count = 0

        for val in sample_values:
            if not val or not isinstance(val, str):
                continue
            val_stripped = val.strip()
            if val_stripped.startswith("["):
                json_like_count += 1
                array_count += 1
            elif val_stripped.startswith("{"):
                json_like_count += 1
                object_count += 1

        return json_like_count, array_count, object_count

    def _extract_json_sample_fields(self, sample_values: list) -> tuple[list[str], dict[str, str]]:
        """Extract sample field names and infer types from first valid JSON.

        Supports nested structures using dot-notation (e.g., "rows.row_code").
        When a field contains an array of objects, extracts nested fields with parent prefix.

        Returns:
            tuple of (field_names, field_types) where field_types maps field name to inferred type
        """
        for val in sample_values:
            if not val:
                continue
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], dict):
                    # Top-level is array of objects
                    first_elem = parsed[0]
                    fields, types = self._extract_fields_recursive(first_elem, prefix="", max_fields=10)
                    return fields, types
                if isinstance(parsed, dict):
                    # Top-level is object - may contain nested arrays
                    fields, types = self._extract_fields_recursive(parsed, prefix="", max_fields=15)
                    return fields, types
            except (json.JSONDecodeError, TypeError):
                continue
        return [], {}

    def _extract_fields_recursive(
        self, obj: dict, prefix: str = "", max_fields: int = 15, max_depth: int = 2
    ) -> tuple[list[str], dict[str, str]]:
        """
        Recursively extract fields from a JSON object, including nested arrays.

        Args:
            obj: Dictionary to extract fields from
            prefix: Dot-notation prefix for nested fields (e.g., "rows.")
            max_fields: Maximum total fields to extract
            max_depth: Maximum nesting depth to traverse

        Returns:
            tuple of (field_names, field_types)
        """
        fields: list[str] = []
        types: dict[str, str] = {}

        for key, value in obj.items():
            if len(fields) >= max_fields:
                break

            full_key = f"{prefix}{key}" if prefix else key

            # Check if value is an array of objects (nested structure)
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict) and max_depth > 0:
                # Add the array field itself
                fields.append(full_key)
                # Recursively extract nested fields with dot-notation prefix
                nested_fields, nested_types = self._extract_fields_recursive(
                    value[0], prefix=f"{full_key}.", max_fields=max_fields - len(fields), max_depth=max_depth - 1
                )
                fields.extend(nested_fields)
                types.update(nested_types)
            else:
                # Regular field - add with type inference
                fields.append(full_key)
                types[full_key] = self._infer_json_field_type(value)

        return fields, types

    def _infer_json_field_type(self, value: Any) -> str:
        """Infer the type of a JSON field value."""
        if value is None:
            return "string"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "double"
        if isinstance(value, str):
            return self._infer_string_type(value)
        return "string"

    def _infer_string_type(self, value: str) -> str:
        """Infer type from string value that might represent a number or boolean."""
        if re.fullmatch(r"-?\d+", value):
            return "int"
        if re.fullmatch(r"-?\d+(\.\d+)?([eE][+-]?\d+)?", value):
            return "double"
        if value.lower() in ("true", "false"):
            return "boolean"
        return "string"

    def _detect_date_format(self, df: DataFrame, column: str) -> str | None:
        """
        Detect whether a string column holds dates in a consistent strftime format.

        Samples non-null values and returns the first candidate format (from
        _DATE_FORMAT_CANDIDATES) that at least _DATE_FORMAT_MIN_RATIO of the sampled
        values parse against. Returns None if no format matches consistently.

        This only applies to string columns: real date/timestamp columns are typed
        and validated through dtype, not this string-format check.
        """
        try:
            sample_df = df.filter(f.col(column).isNotNull()).select(column).limit(_DATE_FORMAT_SAMPLE_SIZE)
            sample_values = [row[column] for row in sample_df.collect()]
        except Exception:
            return None

        sample_values = [v for v in sample_values if isinstance(v, str) and v.strip()]
        if not sample_values:
            return None

        for fmt in _DATE_FORMAT_CANDIDATES:
            matches = sum(1 for v in sample_values if self._matches_strftime(v.strip(), fmt))
            if matches / len(sample_values) >= _DATE_FORMAT_MIN_RATIO:
                return fmt
        return None

    @staticmethod
    def _matches_strftime(value: str, fmt: str) -> bool:
        """Return True if value parses cleanly against the given strftime format."""
        try:
            # Only validating that the string parses against the format; the parsed
            # datetime is discarded, so timezone-awareness is irrelevant here.
            datetime.strptime(value, fmt)  # noqa: DTZ007
            return True
        except (ValueError, TypeError):
            return False
